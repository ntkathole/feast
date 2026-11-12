/*
Copyright 2024 Feast Community.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package services

import (
	"context"
	"fmt"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	"github.com/feast-dev/feast/infra/feast-operator/internal/controller/handler"
	routev1 "github.com/openshift/api/route/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func findContainer(containers []corev1.Container, name string) *corev1.Container {
	for i := range containers {
		if containers[i].Name == name {
			return &containers[i]
		}
	}
	return nil
}

func findVolumeMount(mounts []corev1.VolumeMount, name string) *corev1.VolumeMount {
	for i := range mounts {
		if mounts[i].Name == name {
			return &mounts[i]
		}
	}
	return nil
}

func findVolume(volumes []corev1.Volume, name string) *corev1.Volume {
	for i := range volumes {
		if volumes[i].Name == name {
			return &volumes[i]
		}
	}
	return nil
}

var _ = Describe("UI OAuth Proxy", func() {
	var (
		featureStore       *feastdevv1.FeatureStore
		feast              *FeastServices
		typeNamespacedName types.NamespacedName
		ctx                context.Context
	)

	buildDeployment := func() *appsv1.Deployment {
		Expect(feast.ApplyDefaults()).To(Succeed())
		applySpecToStatus(featureStore)
		feast.refreshFeatureStore(ctx, typeNamespacedName)
		deployment := feast.initFeastDeploy()
		Expect(feast.setDeployment(deployment)).To(Succeed())
		return deployment
	}

	BeforeEach(func() {
		ctx = context.Background()
		typeNamespacedName = types.NamespacedName{
			Name:      "oauth-proxy-fs",
			Namespace: DefaultNs,
		}
		featureStore = &feastdevv1.FeatureStore{
			ObjectMeta: metav1.ObjectMeta{
				Name:      typeNamespacedName.Name,
				Namespace: typeNamespacedName.Namespace,
			},
			Spec: feastdevv1.FeatureStoreSpec{
				FeastProject: "testproject",
				Services: &feastdevv1.FeatureStoreServices{
					UI: &feastdevv1.ServerConfigs{
						ContainerConfigs: feastdevv1.ContainerConfigs{
							DefaultCtrConfigs: feastdevv1.DefaultCtrConfigs{
								Image: ptr.To("test-image"),
							},
						},
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, featureStore)).To(Succeed())
		applySpecToStatus(featureStore)
		feast = &FeastServices{
			Handler: handler.FeastHandler{
				Client:       k8sClient,
				Context:      ctx,
				Scheme:       k8sClient.Scheme(),
				FeatureStore: featureStore,
			},
		}
	})

	AfterEach(func() {
		isOpenShift = false
		// Clean up the cluster-scoped binding if a test created it.
		crb := &rbacv1.ClusterRoleBinding{
			ObjectMeta: metav1.ObjectMeta{Name: feast.oauthProxyAuthDelegatorName()},
		}
		_ = k8sClient.Delete(ctx, crb)
		Expect(k8sClient.Delete(ctx, featureStore)).To(Succeed())
	})

	Context("on OpenShift with kubernetes authorization", func() {
		BeforeEach(func() {
			testSetIsOpenShift()
		})

		It("should determine the proxy is required", func() {
			Expect(feast.ApplyDefaults()).To(Succeed())
			applySpecToStatus(featureStore)
			feast.refreshFeatureStore(ctx, typeNamespacedName)
			Expect(feast.shouldDeployOAuthProxy()).To(BeTrue())
		})

		It("should add the oauth-proxy sidecar alongside the UI container", func() {
			deployment := buildDeployment()
			containers := deployment.Spec.Template.Spec.Containers

			ui := findContainer(containers, string(UIFeastType))
			Expect(ui).NotTo(BeNil())
			// The UI serves plain HTTP on the loopback interface behind the proxy.
			Expect(ui.Ports).To(HaveLen(1))
			Expect(ui.Ports[0].ContainerPort).To(Equal(FeastServiceConstants[UIFeastType].TargetHttpPort))

			proxy := findContainer(containers, oauthProxyContainerName)
			Expect(proxy).NotTo(BeNil())
			Expect(proxy.Image).To(Equal(DefaultOAuthProxyImage))
			Expect(proxy.Ports).To(HaveLen(1))
			Expect(proxy.Ports[0].ContainerPort).To(Equal(OAuthProxyPort))

			args := strings.Join(proxy.Args, " ")
			Expect(args).To(ContainSubstring("--provider=openshift"))
			Expect(args).To(ContainSubstring("--pass-access-token"))
			Expect(args).To(ContainSubstring(fmt.Sprintf("--https-address=:%d", OAuthProxyPort)))
			Expect(args).To(ContainSubstring(fmt.Sprintf("--upstream=http://localhost:%d", FeastServiceConstants[UIFeastType].TargetHttpPort)))
			Expect(args).To(ContainSubstring("--openshift-service-account=" + feast.initFeastSA().Name))

			mounts := proxy.VolumeMounts
			Expect(findVolumeMount(mounts, oauthProxyTLSVolumeName)).NotTo(BeNil())
			Expect(findVolumeMount(mounts, oauthProxyCookieVolume)).NotTo(BeNil())

			Expect(findVolume(deployment.Spec.Template.Spec.Volumes, oauthProxyTLSVolumeName)).NotTo(BeNil())
			Expect(findVolume(deployment.Spec.Template.Spec.Volumes, oauthProxyCookieVolume)).NotTo(BeNil())
		})

		It("should expose the proxy via the Service and reencrypt Route", func() {
			Expect(feast.ApplyDefaults()).To(Succeed())
			applySpecToStatus(featureStore)
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			svc := feast.initFeastSvc(UIFeastType)
			Expect(feast.setService(svc, UIFeastType, false)).To(Succeed())
			Expect(svc.Spec.Ports).To(HaveLen(1))
			Expect(svc.Spec.Ports[0].Port).To(Equal(int32(HttpsPort)))
			Expect(svc.Spec.Ports[0].TargetPort.IntValue()).To(Equal(int(OAuthProxyPort)))
			Expect(svc.Annotations).To(HaveKey(openshiftServingCertSecretAnnotation))

			route := feast.initRoute(UIFeastType)
			Expect(feast.setRoute(route, UIFeastType)).To(Succeed())
			Expect(route.Spec.Port.TargetPort.IntValue()).To(Equal(int(OAuthProxyPort)))
			Expect(route.Spec.TLS).NotTo(BeNil())
			Expect(route.Spec.TLS.Termination).To(Equal(routev1.TLSTerminationReencrypt))
		})

		It("should create the cookie secret and auth-delegator ClusterRoleBinding", func() {
			Expect(feast.ApplyDefaults()).To(Succeed())
			applySpecToStatus(featureStore)
			feast.refreshFeatureStore(ctx, typeNamespacedName)

			Expect(feast.reconcileOAuthProxy()).To(Succeed())

			secret := &corev1.Secret{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{
				Name:      feast.oauthProxyCookieSecretName(),
				Namespace: feast.Handler.FeatureStore.Namespace,
			}, secret)).To(Succeed())
			Expect(secret.Data[oauthProxyCookieFileName]).NotTo(BeEmpty())

			crb := &rbacv1.ClusterRoleBinding{}
			Expect(k8sClient.Get(ctx, client.ObjectKey{Name: feast.oauthProxyAuthDelegatorName()}, crb)).To(Succeed())
			Expect(crb.RoleRef.Name).To(Equal("system:auth-delegator"))
			Expect(crb.Subjects).To(HaveLen(1))
			Expect(crb.Subjects[0].Name).To(Equal(feast.initFeastSA().Name))
		})

		It("should honor image and resources overrides", func() {
			customImage := "example.com/my-oauth-proxy:1.2.3"
			customResources := corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("250m"),
				},
			}
			featureStore.Spec.Services.OAuthProxy = &feastdevv1.OAuthProxyConfigs{
				Image:     ptr.To(customImage),
				Resources: &customResources,
			}
			Expect(k8sClient.Update(ctx, featureStore)).To(Succeed())

			deployment := buildDeployment()
			proxy := findContainer(deployment.Spec.Template.Spec.Containers, oauthProxyContainerName)
			Expect(proxy).NotTo(BeNil())
			Expect(proxy.Image).To(Equal(customImage))
			Expect(proxy.Resources.Requests.Cpu().String()).To(Equal("250m"))
		})
	})

	Context("when the proxy is not required", func() {
		It("should not add the sidecar when not on OpenShift", func() {
			isOpenShift = false
			deployment := buildDeployment()
			Expect(findContainer(deployment.Spec.Template.Spec.Containers, oauthProxyContainerName)).To(BeNil())
			Expect(feast.shouldDeployOAuthProxy()).To(BeFalse())
		})

		It("should not add the sidecar when authorization is oidc", func() {
			testSetIsOpenShift()
			// Set the applied authz directly; oidc selects the Keycloak SPA path,
			// not the proxy. (Avoids ApplyDefaults validating the OIDC secret.)
			featureStore.Status.Applied.Services = featureStore.Spec.Services.DeepCopy()
			featureStore.Status.Applied.AuthzConfig = &feastdevv1.AuthzConfig{
				OidcAuthz: &feastdevv1.OidcAuthz{
					SecretRef: &corev1.LocalObjectReference{Name: "oidc-secret"},
				},
			}
			Expect(feast.shouldDeployOAuthProxy()).To(BeFalse())
		})

		It("should remove the auth-delegator ClusterRoleBinding on teardown", func() {
			testSetIsOpenShift()
			Expect(feast.ApplyDefaults()).To(Succeed())
			applySpecToStatus(featureStore)
			feast.refreshFeatureStore(ctx, typeNamespacedName)
			Expect(feast.reconcileOAuthProxy()).To(Succeed())

			// Simulate switching the proxy off.
			isOpenShift = false
			Expect(feast.removeOAuthProxy()).To(Succeed())

			crb := &rbacv1.ClusterRoleBinding{}
			err := k8sClient.Get(ctx, client.ObjectKey{Name: feast.oauthProxyAuthDelegatorName()}, crb)
			Expect(apierrors.IsNotFound(err)).To(BeTrue())
		})
	})
})
