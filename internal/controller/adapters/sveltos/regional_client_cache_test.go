// Copyright 2025
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sveltos

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	fluxmeta "github.com/fluxcd/pkg/apis/meta"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
)

const (
	rccNamespace  = "default"
	rccSysNS      = "kcm-system"
	rccRegion     = "region1"
	rccSecretName = "region1-kubeconfig"
)

// rccObjects returns the management-cluster objects needed to resolve
// serviceSet -> ClusterDeployment -> Credential -> Region -> kubeconfig Secret
// for a regional target.
func rccObjects() []client.Object {
	return []client.Object{
		&kcmv1.ClusterDeployment{
			ObjectMeta: metav1.ObjectMeta{Namespace: rccNamespace, Name: "cluster1"},
			Spec:       kcmv1.ClusterDeploymentSpec{Credential: "cred1"},
		},
		&kcmv1.Credential{
			ObjectMeta: metav1.ObjectMeta{Namespace: rccNamespace, Name: "cred1"},
			Spec:       kcmv1.CredentialSpec{Region: rccRegion},
		},
		&kcmv1.Region{
			ObjectMeta: metav1.ObjectMeta{Name: rccRegion},
			Spec: kcmv1.RegionSpec{
				KubeConfig: &fluxmeta.SecretKeyReference{Name: rccSecretName, Key: "value"},
			},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: rccSysNS, Name: rccSecretName},
			Data:       map[string][]byte{"value": []byte("kubeconfig-v1")},
		},
	}
}

func rccScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := kcmv1.AddToScheme(s); err != nil {
		t.Fatalf("add kcmv1: %v", err)
	}
	return s
}

func rccServiceSet(cluster string) *kcmv1.ServiceSet {
	return &kcmv1.ServiceSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: rccNamespace, Name: "ss1"},
		Spec:       kcmv1.ServiceSetSpec{Cluster: cluster},
	}
}

// newCountingCache builds a cache backed by the given objects with a build func
// that counts calls and returns a fresh sentinel client each time.
func newCountingCache(t *testing.T, objs ...client.Object) (*regionalClientCache, *int32) {
	t.Helper()
	s := rccScheme(t)
	mgmt := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
	c := newRegionalClientCache(mgmt, rccSysNS)
	var count int32
	c.build = func(_ context.Context, _ client.Client, _, _ string) (client.Client, error) {
		atomic.AddInt32(&count, 1)
		return fake.NewClientBuilder().WithScheme(s).Build(), nil
	}
	return c, &count
}

func TestRegionalClientCache_MissThenHit(t *testing.T) {
	c, count := newCountingCache(t, rccObjects()...)
	ctx := context.Background()
	ss := rccServiceSet("cluster1")

	cl1, isRegional, err := c.get(ctx, ss)
	if err != nil {
		t.Fatalf("first get: %v", err)
	}
	if !isRegional {
		t.Errorf("expected isRegional=true for a region-backed ServiceSet")
	}
	cl2, _, err := c.get(ctx, ss)
	if err != nil {
		t.Fatalf("second get: %v", err)
	}
	if got := atomic.LoadInt32(count); got != 1 {
		t.Errorf("build called %d times, want 1 (second get should hit the cache)", got)
	}
	if cl1 != cl2 {
		t.Errorf("expected the cached client instance to be reused")
	}
}

func TestRegionalClientCache_InvalidateOnKubeconfigChange(t *testing.T) {
	objs := rccObjects()
	c, count := newCountingCache(t, objs...)
	ctx := context.Background()
	ss := rccServiceSet("cluster1")

	if _, _, err := c.get(ctx, ss); err != nil {
		t.Fatalf("first get: %v", err)
	}

	// Mutate the kubeconfig secret so its resourceVersion changes.
	sec := &corev1.Secret{}
	if err := c.mgmtClient.Get(ctx, client.ObjectKey{Namespace: rccSysNS, Name: rccSecretName}, sec); err != nil {
		t.Fatalf("get secret: %v", err)
	}
	sec.Data["value"] = []byte("kubeconfig-v2")
	if err := c.mgmtClient.Update(ctx, sec); err != nil {
		t.Fatalf("update secret: %v", err)
	}

	if _, _, err := c.get(ctx, ss); err != nil {
		t.Fatalf("get after kubeconfig change: %v", err)
	}
	if got := atomic.LoadInt32(count); got != 2 {
		t.Errorf("build called %d times, want 2 (kubeconfig change must rebuild)", got)
	}
}

func TestRegionalClientCache_LocalPassthrough(t *testing.T) {
	c, count := newCountingCache(t, rccObjects()...)
	ctx := context.Background()

	// self-management ServiceSet (empty .spec.cluster) -> management client, uncached
	cl, isRegional, err := c.get(ctx, rccServiceSet(""))
	if err != nil {
		t.Fatalf("self-management get: %v", err)
	}
	if isRegional {
		t.Errorf("self-management ServiceSet must not be regional")
	}
	if cl != c.mgmtClient {
		t.Errorf("self-management must return the management client")
	}
	if got := atomic.LoadInt32(count); got != 0 {
		t.Errorf("build called %d times for self-management, want 0", got)
	}
}

func TestRegionalClientCache_NoRegionPassthrough(t *testing.T) {
	objs := rccObjects()
	// Credential with no region -> management client, uncached.
	for _, o := range objs {
		if cred, ok := o.(*kcmv1.Credential); ok {
			cred.Spec.Region = ""
		}
	}
	c, count := newCountingCache(t, objs...)
	ctx := context.Background()

	cl, isRegional, err := c.get(ctx, rccServiceSet("cluster1"))
	if err != nil {
		t.Fatalf("no-region get: %v", err)
	}
	if isRegional {
		t.Errorf("credential with empty region must not be regional")
	}
	if cl != c.mgmtClient {
		t.Errorf("empty region must return the management client")
	}
	if got := atomic.LoadInt32(count); got != 0 {
		t.Errorf("build called %d times for empty region, want 0", got)
	}
}

func TestRegionalClientCache_Concurrent(t *testing.T) {
	c, count := newCountingCache(t, rccObjects()...)
	ctx := context.Background()
	ss := rccServiceSet("cluster1")

	const n = 32
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			if _, _, err := c.get(ctx, ss); err != nil {
				t.Errorf("concurrent get: %v", err)
			}
		}()
	}
	wg.Wait()

	// The double-checked pattern allows a small number of redundant cold-start
	// builds, but it must be bounded (far below n) and never racy (run with -race).
	if got := atomic.LoadInt32(count); got < 1 || got > n/2 {
		t.Errorf("build called %d times under concurrency, want a small number (1..%d)", got, n/2)
	}
}
