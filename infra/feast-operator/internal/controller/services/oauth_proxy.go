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
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	routev1 "github.com/openshift/api/route/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// shouldDeployOAuthProxy reports whether the UI server must be fronted by an
// OpenShift oauth-proxy sidecar. On OpenShift a kubernetes-auth UI cannot work
// without it (the browser has no way to obtain a token), so it is derived from
// the deployment rather than an explicit toggle: OpenShift + a UI server +
// kubernetes authorization (the default when authz is unset). It is never used
// for oidc (the SPA logs in via Keycloak) or no_auth.
func (feast *FeastServices) shouldDeployOAuthProxy() bool {
	if !IsOpenShift() || !feast.isUiServer() {
		return false
	}
	authzConfig := feast.Handler.FeatureStore.Status.Applied.AuthzConfig
	return authzConfig != nil && authzConfig.KubernetesAuthz != nil
}

// getOAuthProxyConfigs returns the optional user overrides for the oauth-proxy
// sidecar, or nil when none were provided.
func (feast *FeastServices) getOAuthProxyConfigs() *feastdevv1.OAuthProxyConfigs {
	services := feast.Handler.FeatureStore.Status.Applied.Services
	if services == nil {
		return nil
	}
	return services.OAuthProxy
}

// getOAuthProxyImage resolves the sidecar image.
// Order: spec override → RELATED_IMAGE_OAUTH_PROXY env → built-in default.
func (feast *FeastServices) getOAuthProxyImage() string {
	if cfg := feast.getOAuthProxyConfigs(); cfg != nil && cfg.Image != nil && len(*cfg.Image) > 0 {
		return *cfg.Image
	}
	if img, exists := os.LookupEnv(oauthProxyImageVar); exists && len(img) > 0 {
		return img
	}
	return DefaultOAuthProxyImage
}

func (feast *FeastServices) getOAuthProxyResources() corev1.ResourceRequirements {
	if cfg := feast.getOAuthProxyConfigs(); cfg != nil && cfg.Resources != nil {
		return *cfg.Resources
	}
	return corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("64Mi"),
		},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}
}

// oauthProxyServingCertSecretName is the OpenShift service-serving-cert secret
// (created from the Service annotation) that the proxy uses to terminate TLS.
func (feast *FeastServices) oauthProxyServingCertSecretName() string {
	return feast.GetFeastServiceName(UIFeastType) + tlsNameSuffix
}

func (feast *FeastServices) oauthProxyCookieSecretName() string {
	return feast.GetFeastServiceName(UIFeastType) + "-oauth-cookie"
}

// oauthProxyAuthDelegatorName is a cluster-unique name for the ClusterRoleBinding
// that grants the Feast ServiceAccount token/subject access review permissions.
func (feast *FeastServices) oauthProxyAuthDelegatorName() string {
	fs := feast.Handler.FeatureStore
	return GetFeastName(fs) + "-" + fs.Namespace + "-oauth-proxy"
}

// setOAuthProxyContainer appends the oauth-proxy sidecar (and its volumes) to
// the pod. The UI container serves plain HTTP on the loopback interface; the
// proxy performs the OpenShift login, terminates TLS with the serving cert, and
// forwards the authenticated user's token upstream via --pass-access-token
// (which Feast's REST server reads from X-Forwarded-Access-Token).
func (feast *FeastServices) setOAuthProxyContainer(podSpec *corev1.PodSpec) {
	fs := feast.Handler.FeatureStore
	saName := feast.initFeastSA().Name
	svcName := feast.GetFeastServiceName(UIFeastType)

	probeHandler := corev1.ProbeHandler{
		HTTPGet: &corev1.HTTPGetAction{
			Path:   "/oauth/healthz",
			Port:   intstr.FromInt(int(OAuthProxyPort)),
			Scheme: corev1.URISchemeHTTPS,
		},
	}

	container := corev1.Container{
		Name:  oauthProxyContainerName,
		Image: feast.getOAuthProxyImage(),
		Args: []string{
			"--provider=openshift",
			fmt.Sprintf("--https-address=:%d", OAuthProxyPort),
			fmt.Sprintf("--openshift-service-account=%s", saName),
			fmt.Sprintf("--upstream=http://localhost:%d", FeastServiceConstants[UIFeastType].TargetHttpPort),
			fmt.Sprintf("--tls-cert=%s/tls.crt", oauthProxyTLSMountPath),
			fmt.Sprintf("--tls-key=%s/tls.key", oauthProxyTLSMountPath),
			fmt.Sprintf("--cookie-secret-file=%s/%s", oauthProxyCookieMountPath, oauthProxyCookieFileName),
			fmt.Sprintf("--cookie-name=_feast_%s_proxy", fs.Name),
			// Forward the user's OpenShift token upstream so Feast's own
			// kubernetes RBAC (TokenReview + Permission objects) applies.
			"--pass-access-token",
			"--skip-auth-regex=^/oauth/healthz",
			// Coarse gate: the user must be able to 'get' the UI Service in this
			// namespace. Fine-grained authorization is still enforced by Feast.
			fmt.Sprintf(`--openshift-delegate-urls={"/":{"resource":"services","verb":"get","name":"%s","namespace":"%s"}}`, svcName, fs.Namespace),
		},
		Ports: []corev1.ContainerPort{
			{
				Name:          oauthProxyContainerName,
				ContainerPort: OAuthProxyPort,
				Protocol:      corev1.ProtocolTCP,
			},
		},
		Resources: feast.getOAuthProxyResources(),
		LivenessProbe: &corev1.Probe{
			ProbeHandler:        probeHandler,
			InitialDelaySeconds: 30,
			TimeoutSeconds:      5,
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler:        probeHandler,
			InitialDelaySeconds: 5,
			TimeoutSeconds:      5,
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      oauthProxyTLSVolumeName,
				MountPath: oauthProxyTLSMountPath,
				ReadOnly:  true,
			},
			{
				Name:      oauthProxyCookieVolume,
				MountPath: oauthProxyCookieMountPath,
				ReadOnly:  true,
			},
		},
	}

	defaultMode := int32(0o440)
	podSpec.Volumes = append(podSpec.Volumes,
		corev1.Volume{
			Name: oauthProxyTLSVolumeName,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  feast.oauthProxyServingCertSecretName(),
					DefaultMode: &defaultMode,
				},
			},
		},
		corev1.Volume{
			Name: oauthProxyCookieVolume,
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName:  feast.oauthProxyCookieSecretName(),
					DefaultMode: &defaultMode,
				},
			},
		},
	)

	podSpec.Containers = append(podSpec.Containers, container)
}

// setOAuthProxyService exposes the proxy (not the UI) on the HTTPS port and
// requests an OpenShift serving certificate for it.
func (feast *FeastServices) setOAuthProxyService(svc *corev1.Service) error {
	if svc.Annotations == nil {
		svc.Annotations = map[string]string{}
	}
	svc.Annotations[openshiftServingCertSecretAnnotation] = feast.oauthProxyServingCertSecretName() // pragma: allowlist secret
	https := HttpsScheme
	svc.Spec = corev1.ServiceSpec{
		Selector: feast.getSelectorLabels(),
		Type:     corev1.ServiceTypeClusterIP,
		Ports: []corev1.ServicePort{
			{
				Name:        HttpsScheme,
				Port:        HttpsPort,
				Protocol:    corev1.ProtocolTCP,
				TargetPort:  intstr.FromInt(int(OAuthProxyPort)),
				AppProtocol: &https,
			},
		},
	}
	return controllerutil.SetControllerReference(feast.Handler.FeatureStore, svc, feast.Handler.Scheme)
}

// setOAuthProxyRoute points the UI Route at the proxy port with reencrypt TLS.
func (feast *FeastServices) setOAuthProxyRoute(route *routev1.Route) error {
	route.Labels = feast.getFeastTypeLabels(UIFeastType)
	route.Spec = routev1.RouteSpec{
		To: routev1.RouteTargetReference{
			Kind: "Service",
			Name: feast.GetFeastServiceName(UIFeastType),
		},
		Port: &routev1.RoutePort{
			TargetPort: intstr.FromInt(int(OAuthProxyPort)),
		},
		TLS: &routev1.TLSConfig{
			Termination:                   routev1.TLSTerminationReencrypt,
			InsecureEdgeTerminationPolicy: routev1.InsecureEdgeTerminationPolicyRedirect,
		},
	}
	return controllerutil.SetControllerReference(feast.Handler.FeatureStore, route, feast.Handler.Scheme)
}

// reconcileOAuthProxy creates the cluster resources the sidecar needs: a cookie
// signing secret and a ClusterRoleBinding to system:auth-delegator (for the
// TokenReview/SubjectAccessReview calls oauth-proxy makes).
func (feast *FeastServices) reconcileOAuthProxy() error {
	if !feast.shouldDeployOAuthProxy() {
		return feast.removeOAuthProxy()
	}
	if err := feast.createOAuthProxyCookieSecret(); err != nil {
		return err
	}
	return feast.createOAuthProxyClusterRoleBinding()
}

func (feast *FeastServices) createOAuthProxyCookieSecret() error {
	logger := log.FromContext(feast.Handler.Context)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      feast.oauthProxyCookieSecretName(),
			Namespace: feast.Handler.FeatureStore.Namespace,
		},
	}
	secret.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Secret"))
	op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, secret, func() error {
		secret.Labels = feast.getFeastTypeLabels(UIFeastType)
		// Generate the cookie secret once and never rotate it on reconcile, so
		// existing sessions keep working.
		if len(secret.Data[oauthProxyCookieFileName]) == 0 {
			buf := make([]byte, 32)
			if _, err := rand.Read(buf); err != nil {
				return err
			}
			if secret.Data == nil {
				secret.Data = map[string][]byte{}
			}
			secret.Data[oauthProxyCookieFileName] = []byte(base64.StdEncoding.EncodeToString(buf))
		}
		return controllerutil.SetControllerReference(feast.Handler.FeatureStore, secret, feast.Handler.Scheme)
	})
	if err != nil {
		return err
	}
	if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "Secret", secret.Name, "operation", op)
	}
	return nil
}

func (feast *FeastServices) createOAuthProxyClusterRoleBinding() error {
	logger := log.FromContext(feast.Handler.Context)
	crb := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: feast.oauthProxyAuthDelegatorName()},
	}
	crb.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("ClusterRoleBinding"))
	op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, crb, func() error {
		crb.Labels = feast.getFeastTypeLabels(UIFeastType)
		crb.Subjects = []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      feast.initFeastSA().Name,
				Namespace: feast.Handler.FeatureStore.Namespace,
			},
		}
		crb.RoleRef = rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "ClusterRole",
			Name:     "system:auth-delegator",
		}
		// Cluster-scoped object: a namespaced owner reference is not allowed, so
		// it is cleaned up explicitly in removeOAuthProxy.
		return nil
	})
	if err != nil {
		return err
	}
	if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "ClusterRoleBinding", crb.Name, "operation", op)
	}
	return nil
}

// removeOAuthProxy tears down the proxy's cluster resources when it is no longer
// needed (UI removed, or authz switched to oidc/no_auth).
func (feast *FeastServices) removeOAuthProxy() error {
	crb := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{Name: feast.oauthProxyAuthDelegatorName()},
	}
	crb.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("ClusterRoleBinding"))
	if err := feast.Handler.Client.Get(feast.Handler.Context, client.ObjectKeyFromObject(crb), crb); err == nil {
		if err := feast.Handler.Client.Delete(feast.Handler.Context, crb); err != nil {
			return err
		}
	}
	// The cookie secret has an owner reference and is garbage-collected with the
	// FeatureStore; nothing to delete explicitly here.
	return nil
}
