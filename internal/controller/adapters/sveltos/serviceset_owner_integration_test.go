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
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	addoncontrollerv1beta1 "github.com/projectsveltos/addon-controller/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kcmv1 "github.com/K0rdent/kcm/api/v1beta1"
)

// These tests exercise the ownerReference handling of createOrUpdateProfile
// directly (rather than through a full regional Reconcile), passing the envtest
// client as the "regional" client. This lets us assert the cross-cluster
// ownerReference fix without standing up a second cluster.
var _ = Describe("ServiceSet Profile ownerReference handling", func() {
	var (
		reconciler ServiceSetReconciler
		namespace  corev1.Namespace
		serviceSet *kcmv1.ServiceSet
		spec       *addoncontrollerv1beta1.Spec
	)

	BeforeEach(func() {
		reconciler = ServiceSetReconciler{
			Client:   cl,
			timeFunc: func() time.Time { return time.Date(2025, 12, 1, 0, 0, 0, 0, time.UTC) },
		}

		namespace = corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "ownerref-test-"}}
		Expect(cl.Create(ctx, &namespace)).To(Succeed())

		serviceSet = &kcmv1.ServiceSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "test-serviceset",
				Namespace: namespace.Name,
				UID:       "test-serviceset-uid",
			},
		}
		spec = &addoncontrollerv1beta1.Spec{}
	})

	AfterEach(func() {
		Expect(cl.Delete(ctx, &namespace)).To(Succeed())
	})

	expectedServiceSetRef := func(ss *kcmv1.ServiceSet) metav1.OwnerReference {
		return *metav1.NewControllerRef(ss, kcmv1.GroupVersion.WithKind(kcmv1.ServiceSetKind))
	}

	It("omits the ServiceSet ownerReference when creating a Profile on a regional cluster", func() {
		Expect(reconciler.createOrUpdateProfile(ctx, cl, true, serviceSet, spec)).To(Succeed())

		profile := &addoncontrollerv1beta1.Profile{}
		Expect(cl.Get(ctx, client.ObjectKeyFromObject(serviceSet), profile)).To(Succeed())
		Expect(profile.OwnerReferences).To(BeEmpty())
	})

	It("keeps the ServiceSet ownerReference when creating a Profile locally", func() {
		Expect(reconciler.createOrUpdateProfile(ctx, cl, false, serviceSet, spec)).To(Succeed())

		profile := &addoncontrollerv1beta1.Profile{}
		Expect(cl.Get(ctx, client.ObjectKeyFromObject(serviceSet), profile)).To(Succeed())
		Expect(profile.OwnerReferences).To(ConsistOf(expectedServiceSetRef(serviceSet)))
	})

	It("strips an existing dangling ServiceSet ownerReference on a regional reconcile", func() {
		By("pre-creating a Profile that carries the (cross-cluster) ServiceSet ownerReference")
		existing := &addoncontrollerv1beta1.Profile{
			ObjectMeta: metav1.ObjectMeta{
				Name:            serviceSet.Name,
				Namespace:       serviceSet.Namespace,
				Labels:          map[string]string{kcmv1.KCMManagedLabelKey: kcmv1.KCMManagedLabelValue},
				OwnerReferences: []metav1.OwnerReference{expectedServiceSetRef(serviceSet)},
			},
			Spec: *spec,
		}
		Expect(cl.Create(ctx, existing)).To(Succeed())

		By("reconciling the Profile as regional")
		Expect(reconciler.createOrUpdateProfile(ctx, cl, true, serviceSet, spec)).To(Succeed())

		profile := &addoncontrollerv1beta1.Profile{}
		Expect(cl.Get(ctx, client.ObjectKeyFromObject(serviceSet), profile)).To(Succeed())
		Expect(profile.OwnerReferences).To(BeEmpty())
	})
})
