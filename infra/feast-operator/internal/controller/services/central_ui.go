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
	"os"
	"strconv"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	routev1 "github.com/openshift/api/route/v1"
)

const (
	CentralUIName               = "feast-central-ui"
	centralUIPort               = 8888
	centralUIEnableEnvVar       = "FEAST_CENTRAL_UI_ENABLED"
	centralUIImageEnvVar        = "FEAST_CENTRAL_UI_IMAGE"
	centralUINamespaceEnvVar    = "FEAST_CENTRAL_UI_NAMESPACE"
	centralUIOAuthSecretSuffix  = "-oauth-token"
	centralUIClusterRoleName    = "feast-central-ui"
	centralUIClusterRBName      = "feast-central-ui"
)

// CentralUIDeployer handles deployment of the central management UI.
// It is independent of any FeatureStore CR and runs in the operator's namespace.
// Implements manager.Runnable so it can be added to the controller-runtime manager.
type CentralUIDeployer struct {
	Client    client.Client
	Namespace string
}

// Start implements manager.Runnable. It deploys the central UI once the manager starts.
func (d *CentralUIDeployer) Start(ctx context.Context) error {
	return d.Deploy(ctx)
}

// IsCentralUIEnabled checks if the central UI auto-deploy feature is enabled.
func IsCentralUIEnabled() bool {
	val, _ := os.LookupEnv(centralUIEnableEnvVar)
	return val == "true" || val == "1"
}

func getCentralUIImage() string {
	if img, exists := os.LookupEnv(centralUIImageEnvVar); exists {
		return img
	}
	return getFeatureServerImage()
}

func getCentralUINamespace() string {
	if ns, exists := os.LookupEnv(centralUINamespaceEnvVar); exists && ns != "" {
		return ns
	}
	if data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace"); err == nil {
		if ns := string(data); len(ns) > 0 {
			return ns
		}
	}
	return DefaultKubernetesNamespace
}

// Deploy creates or updates all resources needed for the central management UI.
func (d *CentralUIDeployer) Deploy(ctx context.Context) error {
	logger := log.FromContext(ctx)

	if d.Namespace == "" {
		d.Namespace = getCentralUINamespace()
	}

	logger.Info("Deploying central management UI", "namespace", d.Namespace)

	if err := d.ensureServiceAccount(ctx); err != nil {
		return fmt.Errorf("failed to create ServiceAccount: %w", err)
	}
	if err := d.ensureClusterRole(ctx); err != nil {
		return fmt.Errorf("failed to create ClusterRole: %w", err)
	}
	if err := d.ensureClusterRoleBinding(ctx); err != nil {
		return fmt.Errorf("failed to create ClusterRoleBinding: %w", err)
	}
	if err := d.ensureDeployment(ctx); err != nil {
		return fmt.Errorf("failed to create Deployment: %w", err)
	}
	if err := d.ensureService(ctx); err != nil {
		return fmt.Errorf("failed to create Service: %w", err)
	}
	if isOpenShift {
		if err := d.ensureOAuthTokenSecret(ctx); err != nil {
			return fmt.Errorf("failed to create OAuth token secret: %w", err)
		}
		if err := d.ensureRoute(ctx); err != nil {
			return fmt.Errorf("failed to create Route: %w", err)
		}
	}

	logger.Info("Central management UI deployed successfully", "namespace", d.Namespace)
	return nil
}

func (d *CentralUIDeployer) ensureServiceAccount(ctx context.Context) error {
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CentralUIName,
			Namespace: d.Namespace,
			Labels:    d.labels(),
			Annotations: map[string]string{
				"serviceaccounts.openshift.io/oauth-redirectreference.feast": `{"kind":"OAuthRedirectReference","apiVersion":"v1","reference":{"kind":"Route","name":"` + CentralUIName + `"}}`,
			},
		},
	}

	existing := &corev1.ServiceAccount{}
	err := d.Client.Get(ctx, types.NamespacedName{Name: sa.Name, Namespace: sa.Namespace}, existing)
	if errors.IsNotFound(err) {
		return d.Client.Create(ctx, sa)
	} else if err != nil {
		return err
	}
	existing.Labels = sa.Labels
	existing.Annotations = sa.Annotations
	return d.Client.Update(ctx, existing)
}

func (d *CentralUIDeployer) ensureClusterRole(ctx context.Context) error {
	cr := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name:   centralUIClusterRoleName,
			Labels: d.labels(),
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{"feast.dev"},
				Resources: []string{"featurestores"},
				Verbs:     []string{"get", "list", "watch"},
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
				APIGroups: []string{""},
				Resources: []string{"users", "groups", "serviceaccounts"},
				Verbs:     []string{"impersonate"},
			},
		},
	}

	existing := &rbacv1.ClusterRole{}
	err := d.Client.Get(ctx, types.NamespacedName{Name: cr.Name}, existing)
	if errors.IsNotFound(err) {
		return d.Client.Create(ctx, cr)
	} else if err != nil {
		return err
	}
	existing.Labels = cr.Labels
	existing.Rules = cr.Rules
	return d.Client.Update(ctx, existing)
}

func (d *CentralUIDeployer) ensureClusterRoleBinding(ctx context.Context) error {
	crb := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:   centralUIClusterRBName,
			Labels: d.labels(),
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     centralUIClusterRoleName,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      CentralUIName,
				Namespace: d.Namespace,
			},
		},
	}

	existing := &rbacv1.ClusterRoleBinding{}
	err := d.Client.Get(ctx, types.NamespacedName{Name: crb.Name}, existing)
	if errors.IsNotFound(err) {
		return d.Client.Create(ctx, crb)
	} else if err != nil {
		return err
	}
	existing.Labels = crb.Labels
	existing.RoleRef = crb.RoleRef
	existing.Subjects = crb.Subjects
	return d.Client.Update(ctx, existing)
}

func (d *CentralUIDeployer) ensureDeployment(ctx context.Context) error {
	replicas := int32(1)
	image := getCentralUIImage()

	uiContainer := corev1.Container{
		Name:  "ui",
		Image: image,
		Command: []string{
			"python", "-m", "feast.central_ui_server",
			"--host", "0.0.0.0",
			"--port", strconv.Itoa(centralUIPort),
		},
		Ports: []corev1.ContainerPort{
			{
				Name:          "http",
				ContainerPort: int32(centralUIPort),
				Protocol:      corev1.ProtocolTCP,
			},
		},
		Env: []corev1.EnvVar{
			{Name: "FEAST_OPERATOR_MANAGED", Value: "true"},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path:   "/healthz",
					Port:   intstr.FromInt(centralUIPort),
					Scheme: corev1.URISchemeHTTP,
				},
			},
			InitialDelaySeconds: 5,
			PeriodSeconds:       10,
		},
		LivenessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path:   "/healthz",
					Port:   intstr.FromInt(centralUIPort),
					Scheme: corev1.URISchemeHTTP,
				},
			},
			InitialDelaySeconds: 10,
			PeriodSeconds:       30,
		},
		Resources: corev1.ResourceRequirements{
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("50m"),
				corev1.ResourceMemory: resource.MustParse("128Mi"),
			},
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("500m"),
				corev1.ResourceMemory: resource.MustParse("512Mi"),
			},
		},
	}

	containers := []corev1.Container{uiContainer}

	// Add oauth-proxy sidecar on OpenShift
	if isOpenShift {
		proxyArgs := []string{
			"--http-address=0.0.0.0:" + strconv.Itoa(authProxyPort),
			"--https-address=",
			"--upstream=http://localhost:" + strconv.Itoa(centralUIPort),
			"--provider=openshift",
			"--pass-user-headers=true",
			"--set-xauthrequest=true",
			"--pass-access-token=false",
			"--skip-provider-button=true",
			"--cookie-secret=feast-central-ui-proxy-secret", // pragma: allowlist secret
			"--cookie-secure=true",
			"--openshift-service-account=" + CentralUIName,
		}
		proxyContainer := corev1.Container{
			Name:  authProxyContainerName,
			Image: authProxyDefaultImage,
			Args:  proxyArgs,
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
			VolumeMounts: []corev1.VolumeMount{
				{
					Name:      "oauth-token",
					MountPath: "/var/run/secrets/kubernetes.io/serviceaccount",
					ReadOnly:  true,
				},
			},
		}
		containers = append(containers, proxyContainer)
	}

	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CentralUIName,
			Namespace: d.Namespace,
			Labels:    d.labels(),
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: d.selectorLabels(),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: d.labels(),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: CentralUIName,
					Containers:         containers,
				},
			},
		},
	}

	// Add oauth-token volume for OpenShift
	if isOpenShift {
		dep.Spec.Template.Spec.Volumes = []corev1.Volume{
			{
				Name: "oauth-token",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: CentralUIName + centralUIOAuthSecretSuffix,
					},
				},
			},
		}
	}

	existing := &appsv1.Deployment{}
	err := d.Client.Get(ctx, types.NamespacedName{Name: dep.Name, Namespace: dep.Namespace}, existing)
	if errors.IsNotFound(err) {
		return d.Client.Create(ctx, dep)
	} else if err != nil {
		return err
	}
	existing.Spec = dep.Spec
	existing.Labels = dep.Labels
	return d.Client.Update(ctx, existing)
}

func (d *CentralUIDeployer) ensureService(ctx context.Context) error {
	targetPort := authProxyPort
	if !isOpenShift {
		targetPort = centralUIPort
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CentralUIName,
			Namespace: d.Namespace,
			Labels:    d.labels(),
			Annotations: map[string]string{
				"service.alpha.openshift.io/serving-cert-secret-name": CentralUIName + "-tls",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: d.selectorLabels(),
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       int32(HttpPort),
					TargetPort: intstr.FromInt(targetPort),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}

	existing := &corev1.Service{}
	err := d.Client.Get(ctx, types.NamespacedName{Name: svc.Name, Namespace: svc.Namespace}, existing)
	if errors.IsNotFound(err) {
		return d.Client.Create(ctx, svc)
	} else if err != nil {
		return err
	}
	existing.Spec.Ports = svc.Spec.Ports
	existing.Spec.Selector = svc.Spec.Selector
	existing.Labels = svc.Labels
	existing.Annotations = svc.Annotations
	return d.Client.Update(ctx, existing)
}

func (d *CentralUIDeployer) ensureOAuthTokenSecret(ctx context.Context) error {
	secretName := CentralUIName + centralUIOAuthSecretSuffix // pragma: allowlist secret
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: d.Namespace,
			Labels:    d.labels(),
			Annotations: map[string]string{
				"kubernetes.io/service-account.name": CentralUIName,
			},
		},
		Type: corev1.SecretTypeServiceAccountToken,
	}

	existing := &corev1.Secret{}
	err := d.Client.Get(ctx, types.NamespacedName{Name: secretName, Namespace: d.Namespace}, existing)
	if errors.IsNotFound(err) {
		return d.Client.Create(ctx, secret)
	}
	return err
}

func (d *CentralUIDeployer) ensureRoute(ctx context.Context) error {
	route := &routev1.Route{
		ObjectMeta: metav1.ObjectMeta{
			Name:      CentralUIName,
			Namespace: d.Namespace,
			Labels:    d.labels(),
		},
		Spec: routev1.RouteSpec{
			To: routev1.RouteTargetReference{
				Kind:   "Service",
				Name:   CentralUIName,
				Weight: ptr.To(int32(100)),
			},
			Port: &routev1.RoutePort{
				TargetPort: intstr.FromString("http"),
			},
			TLS: &routev1.TLSConfig{
				Termination:                   routev1.TLSTerminationEdge,
				InsecureEdgeTerminationPolicy: routev1.InsecureEdgeTerminationPolicyRedirect,
			},
		},
	}

	existing := &routev1.Route{}
	err := d.Client.Get(ctx, types.NamespacedName{Name: route.Name, Namespace: route.Namespace}, existing)
	if errors.IsNotFound(err) {
		return d.Client.Create(ctx, route)
	} else if err != nil {
		return err
	}
	existing.Spec = route.Spec
	existing.Labels = route.Labels
	return d.Client.Update(ctx, existing)
}

func (d *CentralUIDeployer) labels() map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":       CentralUIName,
		"app.kubernetes.io/managed-by": "feast-operator",
		"app.kubernetes.io/component":  "central-ui",
	}
}

func (d *CentralUIDeployer) selectorLabels() map[string]string {
	return map[string]string{
		"app.kubernetes.io/name":      CentralUIName,
		"app.kubernetes.io/component": "central-ui",
	}
}

