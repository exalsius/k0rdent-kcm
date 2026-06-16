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
	"fmt"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
	kubeutil "github.com/K0rdent/kcm/internal/util/kube"
	schemeutil "github.com/K0rdent/kcm/internal/util/scheme"
)

// regionalClientBuilder builds a controller-runtime client for a (non-empty) region name.
// It is a field on regionalClientCache so tests can inject a fake/counting builder.
type regionalClientBuilder func(ctx context.Context, mgmtClient client.Client, systemNamespace, region string) (client.Client, error)

func defaultRegionalClientBuilder(ctx context.Context, mgmtClient client.Client, systemNamespace, region string) (client.Client, error) {
	return kubeutil.GetRegionalClientByRegionName(ctx, mgmtClient, systemNamespace, region, schemeutil.GetRegionalSchemeWithSveltos)
}

type regionalClientEntry struct {
	cl    client.Client
	token string
}

// regionalClientCache caches one controller-runtime client per region (keyed by
// region name) so the ServiceSet reconciler and poller do not rebuild a client —
// and re-run full API discovery against the regional apiserver — for every
// ServiceSet on every poll tick. A cached client is invalidated when the
// region's kubeconfig Secret changes, tracked by that Secret's resourceVersion.
//
// Building a controller-runtime client embeds a deferred/dynamic discovery
// RESTMapper, so a cached client still resolves newly-added CRDs on the regional
// cluster (re-discovery on a mapper miss); caching only avoids the per-call full
// discovery. The clients are direct (no informers), so caching them does not
// leak background watches.
type regionalClientCache struct {
	mgmtClient      client.Client
	systemNamespace string
	build           regionalClientBuilder

	mu      sync.RWMutex
	entries map[string]regionalClientEntry
}

func newRegionalClientCache(mgmtClient client.Client, systemNamespace string) *regionalClientCache {
	return &regionalClientCache{
		mgmtClient:      mgmtClient,
		systemNamespace: systemNamespace,
		build:           defaultRegionalClientBuilder,
		entries:         make(map[string]regionalClientEntry),
	}
}

// get returns the client used to manage the ProjectSveltos objects for the given
// ServiceSet, and whether that client targets a remote regional cluster
// (isRegional). Self-management ServiceSets (empty .spec.cluster) and ServiceSets
// whose credential has no region target the management cluster and return the
// (uncached) management client. Regional targets return a cached client, building
// and caching one only on a miss or when the region's kubeconfig changed.
func (c *regionalClientCache) get(ctx context.Context, serviceSet *kcmv1.ServiceSet) (client.Client, bool, error) {
	// The ServiceSet created for self-managing the management cluster has an empty
	// .spec.cluster because it does not match any ClusterDeployment.
	if serviceSet.Spec.Cluster == "" {
		return c.mgmtClient, false, nil
	}

	cd := new(kcmv1.ClusterDeployment)
	cdKey := client.ObjectKey{Namespace: serviceSet.Namespace, Name: serviceSet.Spec.Cluster}
	if err := c.mgmtClient.Get(ctx, cdKey, cd); err != nil {
		return nil, false, fmt.Errorf("failed to get %s ClusterDeployment: %w", cdKey, err)
	}

	cred := new(kcmv1.Credential)
	credKey := client.ObjectKey{Namespace: cd.Namespace, Name: cd.Spec.Credential}
	if err := c.mgmtClient.Get(ctx, credKey, cred); err != nil {
		return nil, false, fmt.Errorf("failed to get %s Credential: %w", credKey, err)
	}

	region := cred.Spec.Region
	// An empty region means the target is the management cluster (see
	// GetRegionalClientByRegionName).
	if region == "" {
		return c.mgmtClient, false, nil
	}

	token, err := c.regionToken(ctx, region)
	if err != nil {
		return nil, true, err
	}

	// Fast path: a valid cached client for this region.
	c.mu.RLock()
	entry, ok := c.entries[region]
	c.mu.RUnlock()
	if ok && entry.token == token {
		return entry.cl, true, nil
	}

	// Miss or stale: build outside the lock. A rare concurrent miss for the same
	// region may build more than once; the redundant client is discarded below.
	cl, err := c.build(ctx, c.mgmtClient, c.systemNamespace, region)
	if err != nil {
		return nil, true, fmt.Errorf("failed to get regional client for region %s: %w", region, err)
	}

	c.mu.Lock()
	if cur, ok := c.entries[region]; ok && cur.token == token {
		cl = cur.cl // another goroutine already cached a current client; reuse it
	} else {
		c.entries[region] = regionalClientEntry{cl: cl, token: token}
	}
	c.mu.Unlock()

	return cl, true, nil
}

// regionToken returns an invalidation token for the region's regional client: the
// resourceVersion of the region's kubeconfig Secret. All reads are served from
// the manager cache, so this is cheap.
func (c *regionalClientCache) regionToken(ctx context.Context, region string) (string, error) {
	rgn := new(kcmv1.Region)
	if err := c.mgmtClient.Get(ctx, client.ObjectKey{Name: region}, rgn); err != nil {
		return "", fmt.Errorf("failed to get %s region: %w", region, err)
	}

	secretRef, err := kubeutil.GetRegionalKubeconfigSecretRef(rgn)
	if err != nil {
		return "", fmt.Errorf("failed to get kubeconfig secret reference for region %s: %w", region, err)
	}

	namespace := c.systemNamespace
	if rgn.Spec.ClusterDeployment != nil && rgn.Spec.ClusterDeployment.Namespace != "" {
		namespace = rgn.Spec.ClusterDeployment.Namespace
	}

	secret := new(corev1.Secret)
	secretKey := client.ObjectKey{Namespace: namespace, Name: secretRef.Name}
	if err := c.mgmtClient.Get(ctx, secretKey, secret); err != nil {
		return "", fmt.Errorf("failed to get kubeconfig secret %s for region %s: %w", secretKey, region, err)
	}

	return secret.ResourceVersion, nil
}
