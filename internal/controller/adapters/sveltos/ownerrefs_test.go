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
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
)

func TestDesiredProfileOwnerRefs(t *testing.T) {
	serviceSet := &kcmv1.ServiceSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-serviceset",
			Namespace: "test-namespace",
			UID:       "serviceset-uid",
		},
	}
	ssRef := *metav1.NewControllerRef(serviceSet, kcmv1.GroupVersion.WithKind(kcmv1.ServiceSetKind))
	otherRef := metav1.OwnerReference{
		APIVersion: "example.com/v1",
		Kind:       "Other",
		Name:       "other",
		UID:        "other-uid",
	}
	// A ServiceSet ref recorded under a different (older) group version must also
	// be treated as a non-match here, since isServiceSetOwnerRef matches the
	// current GroupVersion only; document that with an explicit case.
	staleVersionSSRef := metav1.OwnerReference{
		APIVersion: "k0rdent.mirantis.com/v1alpha1",
		Kind:       kcmv1.ServiceSetKind,
		Name:       serviceSet.Name,
		UID:        "serviceset-uid",
	}

	tests := []struct {
		name       string
		isRegional bool
		current    []metav1.OwnerReference
		want       []metav1.OwnerReference
	}{
		{
			name:       "local, no existing refs -> ServiceSet controller ref added",
			isRegional: false,
			current:    nil,
			want:       []metav1.OwnerReference{ssRef},
		},
		{
			name:       "regional, no existing refs -> no refs",
			isRegional: true,
			current:    nil,
			want:       []metav1.OwnerReference{},
		},
		{
			name:       "regional, existing ServiceSet ref -> stripped",
			isRegional: true,
			current:    []metav1.OwnerReference{ssRef},
			want:       []metav1.OwnerReference{},
		},
		{
			name:       "regional, ServiceSet ref + other ref -> only ServiceSet stripped",
			isRegional: true,
			current:    []metav1.OwnerReference{ssRef, otherRef},
			want:       []metav1.OwnerReference{otherRef},
		},
		{
			name:       "local, existing ServiceSet ref -> exactly one ServiceSet ref (no dup)",
			isRegional: false,
			current:    []metav1.OwnerReference{ssRef},
			want:       []metav1.OwnerReference{ssRef},
		},
		{
			name:       "local, other ref present -> other kept, ServiceSet ref appended",
			isRegional: false,
			current:    []metav1.OwnerReference{otherRef},
			want:       []metav1.OwnerReference{otherRef, ssRef},
		},
		{
			name:       "regional, stale-version ServiceSet ref is not matched -> kept",
			isRegional: true,
			current:    []metav1.OwnerReference{staleVersionSSRef},
			want:       []metav1.OwnerReference{staleVersionSSRef},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := desiredProfileOwnerRefs(tt.isRegional, serviceSet, tt.current)
			if len(got) != len(tt.want) || !reflect.DeepEqual(got, tt.want) {
				t.Errorf("desiredProfileOwnerRefs() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestIsServiceSetOwnerRef(t *testing.T) {
	serviceSet := &kcmv1.ServiceSet{
		ObjectMeta: metav1.ObjectMeta{Name: "ss", UID: "uid"},
	}
	ssRef := *metav1.NewControllerRef(serviceSet, kcmv1.GroupVersion.WithKind(kcmv1.ServiceSetKind))

	if !isServiceSetOwnerRef(ssRef) {
		t.Errorf("expected current ServiceSet controller ref to match")
	}
	if isServiceSetOwnerRef(metav1.OwnerReference{APIVersion: kcmv1.GroupVersion.String(), Kind: "ClusterDeployment"}) {
		t.Errorf("non-ServiceSet kind in same group must not match")
	}
	if isServiceSetOwnerRef(metav1.OwnerReference{APIVersion: "other/v1", Kind: kcmv1.ServiceSetKind}) {
		t.Errorf("ServiceSet kind in a different group must not match")
	}
}
