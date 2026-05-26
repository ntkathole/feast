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
	"fmt"
	"strconv"

	feastdevv1 "github.com/feast-dev/feast/infra/feast-operator/api/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	authProxyContainerName = "oauth2-proxy"
	authProxyDefaultImage  = "quay.io/openshift-release-dev/ocp-v4.0-art-dev@sha256:baa393768a6fd88e5c655b1cfdafcc145d0d8465ee301e630d935604da86baa6"
	authProxyPort          = 4180
	authProxyMetricsPort   = 44180
)

// setAuthProxySidecar automatically injects an oauth2-proxy sidecar when both the UI
// and authz are configured. The proxy configuration is derived from spec.authz:
// - spec.authz.kubernetes -> OpenShift oauth-proxy provider
// - spec.authz.oidc -> OIDC provider with issuer URL from the oidc config
func (feast *FeastServices) setAuthProxySidecar(podSpec *corev1.PodSpec) {
	if !feast.shouldInjectAuthProxy() {
		return
	}

	cr := feast.Handler.FeatureStore
	authzConfig := cr.Status.Applied.AuthzConfig

	// When auth proxy is enabled, the UI container should listen on plain HTTP.
	// The Route uses edge TLS termination, so external TLS is handled by the router.
	// Internal communication (oauth-proxy -> UI) uses HTTP within the same pod.
	uiPort := FeastServiceConstants[UIFeastType].TargetHttpPort

	provider, issuerURL, secretRef := deriveProxyConfig(authzConfig)
	sa := feast.initFeastSA()
	args := buildAuthProxyArgs(provider, issuerURL, uiPort, sa.Name)

	container := corev1.Container{
		Name:  authProxyContainerName,
		Image: authProxyDefaultImage,
		Args:  args,
		Ports: []corev1.ContainerPort{
			{
				Name:          "proxy",
				ContainerPort: int32(authProxyPort),
				Protocol:      corev1.ProtocolTCP,
			},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path:   "/oauth/healthz",
					Port:   intstr.FromInt(authProxyPort),
					Scheme: corev1.URISchemeHTTP,
				},
			},
			InitialDelaySeconds: 3,
			PeriodSeconds:       5,
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path:   "/oauth/healthz",
					Port:   intstr.FromInt(authProxyPort),
					Scheme: corev1.URISchemeHTTP,
				},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       10,
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("10m"),
				corev1.ResourceMemory: resource.MustParse("32Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("100m"),
				corev1.ResourceMemory: resource.MustParse("64Mi"),
			},
		},
	}

	// For OIDC auth, mount the secret containing client credentials as env vars
	if secretRef != nil { // pragma: allowlist secret
		container.EnvFrom = []corev1.EnvFromSource{
			{
				SecretRef: &corev1.SecretEnvSource{
					LocalObjectReference: corev1.LocalObjectReference{Name: secretRef.Name},
				},
			},
		}
	}

	// Mount a legacy SA token secret for OpenShift OAuth client authentication.
	// Bound tokens (projected in newer OCP) aren't accepted by the OAuth server.
	if provider == "openshift" {
		oauthTokenSecretName := cr.Name + "-oauth-token" // pragma: allowlist secret
		container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
			Name:      "oauth-token",
			MountPath: "/var/run/secrets/kubernetes.io/serviceaccount",
			ReadOnly:  true,
		})
		podSpec.Volumes = append(podSpec.Volumes, corev1.Volume{
			Name: "oauth-token",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: oauthTokenSecretName,
				},
			},
		})
	}

	// Set FEAST_OPERATOR_MANAGED env on the UI container so the backend enables management routes
	for i := range podSpec.Containers {
		if podSpec.Containers[i].Name == string(UIFeastType) {
			podSpec.Containers[i].Env = append(podSpec.Containers[i].Env, corev1.EnvVar{
				Name:  "FEAST_OPERATOR_MANAGED",
				Value: "true",
			})
			break
		}
	}

	// Add or replace the sidecar container
	found := false
	for i := range podSpec.Containers {
		if podSpec.Containers[i].Name == authProxyContainerName {
			podSpec.Containers[i] = container
			found = true
			break
		}
	}
	if !found {
		podSpec.Containers = append(podSpec.Containers, container)
	}
}

// shouldInjectAuthProxy returns true when both UI and authz are configured.
func (feast *FeastServices) shouldInjectAuthProxy() bool {
	cr := feast.Handler.FeatureStore
	return feast.isUiServer() && cr.Status.Applied.AuthzConfig != nil
}

// deriveProxyConfig extracts oauth2-proxy settings from the authz configuration.
func deriveProxyConfig(authz *feastdevv1.AuthzConfig) (provider string, issuerURL string, secretRef *corev1.LocalObjectReference) {
	if authz.KubernetesAuthz != nil {
		return "openshift", "", nil
	}
	if authz.OidcAuthz != nil {
		return "oidc", authz.OidcAuthz.IssuerUrl, authz.OidcAuthz.SecretRef
	}
	return "openshift", "", nil
}

func buildAuthProxyArgs(provider string, issuerURL string, upstreamPort int32, saName string) []string {
	// oauth-proxy listens on HTTP; Route edge termination handles external TLS.
	// Upstream is always HTTP since the UI container runs without TLS when behind the proxy.
	args := []string{
		"--http-address=0.0.0.0:" + strconv.Itoa(authProxyPort),
		"--https-address=",
		"--upstream=http://localhost:" + strconv.Itoa(int(upstreamPort)),
		"--provider=" + provider,
		"--pass-user-headers=true",
		"--set-xauthrequest=true",
		"--pass-access-token=false",
		"--skip-provider-button=true",
		"--cookie-secret=feast-operator-proxy-secret", // pragma: allowlist secret
		"--cookie-secure=true",
	}

	if issuerURL != "" {
		args = append(args, "--oidc-issuer-url="+issuerURL)
		args = append(args, "--email-domain=*")
	}

	// For OpenShift provider, delegate authorization check to the API server
	if provider == "openshift" {
		args = append(args,
			"--openshift-service-account="+saName,
			"--openshift-delegate-urls={\"/\":{\"resource\":\"featurestores\",\"verb\":\"list\",\"group\":\"feast.dev\"}}",
		)
	}

	return args
}

// DeployAuthProxyRBAC creates the ClusterRole and ClusterRoleBinding that grant the feast
// deployment's ServiceAccount permissions to impersonate users and create SubjectAccessReviews.
// These are required for the UI backend to perform per-user RBAC checks.
// Automatically triggered when both UI and authz are configured.
func (feast *FeastServices) DeployAuthProxyRBAC() error {
	if !feast.shouldInjectAuthProxy() {
		return nil
	}

	logger := log.FromContext(feast.Handler.Context)
	cr := feast.Handler.FeatureStore
	sa := feast.initFeastSA()

	clusterRoleName := fmt.Sprintf("feast-%s-ui-impersonator", cr.Name)
	clusterRole := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name:   clusterRoleName,
			Labels: feast.getLabels(),
		},
	}
	clusterRole.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("ClusterRole"))

	if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, clusterRole, func() error {
		clusterRole.Rules = []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"users", "groups"},
				Verbs:     []string{"impersonate"},
			},
			{
				APIGroups: []string{"authorization.k8s.io"},
				Resources: []string{"subjectaccessreviews"},
				Verbs:     []string{"create"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"namespaces"},
				Verbs:     []string{"list"},
			},
			{
				APIGroups: []string{"feast.dev"},
				Resources: []string{"featurestores"},
				Verbs:     []string{"list", "get", "create", "delete", "watch"},
			},
			{
				APIGroups: []string{""},
				Resources: []string{"pods", "secrets", "configmaps"},
				Verbs:     []string{"list", "get"},
			},
		}
		return nil
	}); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "ClusterRole", clusterRoleName, "operation", op)
	}

	// Create a legacy SA token secret for OAuth client authentication
	oauthTokenSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cr.Name + "-oauth-token",
			Namespace: cr.Namespace,
			Annotations: map[string]string{
				"kubernetes.io/service-account.name": sa.Name,
			},
		},
	}
	if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, oauthTokenSecret, func() error {
		oauthTokenSecret.Type = corev1.SecretTypeServiceAccountToken
		if oauthTokenSecret.Annotations == nil {
			oauthTokenSecret.Annotations = map[string]string{}
		}
		oauthTokenSecret.Annotations["kubernetes.io/service-account.name"] = sa.Name
		return controllerutil.SetControllerReference(cr, oauthTokenSecret, feast.Handler.Scheme)
	}); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "Secret", oauthTokenSecret.Name, "operation", op)
	}

	crbName := fmt.Sprintf("feast-%s-ui-impersonator", cr.Name)
	crb := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:   crbName,
			Labels: feast.getLabels(),
		},
	}
	crb.SetGroupVersionKind(rbacv1.SchemeGroupVersion.WithKind("ClusterRoleBinding"))

	if op, err := controllerutil.CreateOrUpdate(feast.Handler.Context, feast.Handler.Client, crb, func() error {
		crb.RoleRef = rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     clusterRoleName,
		}
		crb.Subjects = []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      sa.Name,
				Namespace: cr.Namespace,
			},
		}
		return nil
	}); err != nil {
		return err
	} else if op == controllerutil.OperationResultCreated || op == controllerutil.OperationResultUpdated {
		logger.Info("Successfully reconciled", "ClusterRoleBinding", crbName, "operation", op)
	}

	return nil
}
