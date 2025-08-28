package controller

import (
	"context"
	"fmt"
	"math/rand"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	feastdevv1alpha1 "github.com/feast-dev/feast/infra/feast-operator/api/v1alpha1"
	feastservices "github.com/feast-dev/feast/infra/feast-operator/internal/controller/services"
	routev1 "github.com/openshift/api/route/v1"
)

// createOrUpdateOAuthConfig handles OAuth proxy configuration for all end-user services
func (r *FeatureStoreReconciler) createOrUpdateOAuthConfig(ctx context.Context, featureStore *feastdevv1alpha1.FeatureStore) error {
	// Handle OAuth proxy configuration for all end-user services
	if err := r.createOrUpdateOAuthConfigForAllServices(ctx, featureStore); err != nil {
		return err
	}

	return nil
}

func (r *FeatureStoreReconciler) createOrUpdateOAuthConfigForAllServices(ctx context.Context, featureStore *feastdevv1alpha1.FeatureStore) error {
	logger := log.FromContext(ctx)

	// Only create OAuth proxy resources if OAuthProxy is configured and enabled
	if featureStore.Spec.OAuthProxy == nil || !r.isOAuthProxyEnabled(featureStore) {
		// Remove all OAuth proxy resources if they exist
		if err := r.deleteAllOAuthResources(ctx, featureStore); err != nil {
			logger.Error(err, "Failed to delete OAuth proxy resources")
			return err
		}
		return nil
	}

	logger.Info("Creating/updating OAuth proxy resources for end-user services")

	// Create OAuth proxy role binding
	if err := r.createOrUpdateOAuthRoleBinding(ctx, featureStore); err != nil {
		logger.Error(err, "Failed to create/update OAuth proxy role binding")
		return err
	}

	// Create OAuth proxy cookie secrets for end-user services only
	services := []string{"online", "offline", "ui"}
	for _, service := range services {
		if err := r.createOrUpdateServiceOAuthCookieSecret(ctx, featureStore, service); err != nil {
			logger.Error(err, fmt.Sprintf("Failed to create/update %s OAuth proxy cookie secret", service))
			return err
		}
	}

	// Handle registry service separately for gRPC and REST
	if err := r.createOrUpdateRegistryOAuthCookieSecrets(ctx, featureStore); err != nil {
		logger.Error(err, "Failed to create/update registry OAuth proxy cookie secrets")
		return err
	}

	// Check if cluster is OpenShift for Route support
	if feastservices.IsOpenShift() {
		// Create OAuth proxy service routes if enabled
		if featureStore.Spec.OAuthProxy.ServiceRoute == "enabled" {
			// Create routes for online, offline, and ui services
			for _, service := range services {
				if err := r.createOrUpdateServiceOAuthRoute(ctx, featureStore, service); err != nil {
					logger.Error(err, fmt.Sprintf("Failed to create/update %s OAuth proxy route", service))
					return err
				}
			}

			// Create routes for registry gRPC and REST
			registryServices := []string{"registry-grpc", "registry-rest"}
			for _, service := range registryServices {
				if err := r.createOrUpdateServiceOAuthRoute(ctx, featureStore, service); err != nil {
					logger.Error(err, fmt.Sprintf("Failed to create/update %s OAuth proxy route", service))
					return err
				}
			}
		} else {
			// Remove all OAuth proxy routes if they exist
			for _, service := range services {
				if err := r.deleteServiceOAuthRoute(ctx, featureStore, service); err != nil {
					logger.Error(err, fmt.Sprintf("Failed to delete %s OAuth proxy route", service))
					return err
				}
			}

			// Remove registry OAuth proxy routes
			registryServices := []string{"registry-grpc", "registry-rest"}
			for _, service := range registryServices {
				if err := r.deleteServiceOAuthRoute(ctx, featureStore, service); err != nil {
					logger.Error(err, fmt.Sprintf("Failed to delete %s OAuth proxy route", service))
					return err
				}
			}
		}
	}

	return nil
}

func (r *FeatureStoreReconciler) isOAuthProxyEnabled(featureStore *feastdevv1alpha1.FeatureStore) bool {
	if featureStore.Spec.OAuthProxy == nil {
		return false
	}
	// If explicitly set, use that value
	if featureStore.Spec.OAuthProxy.Enabled != nil {
		return *featureStore.Spec.OAuthProxy.Enabled
	}
	// Default to true only in OpenShift environments
	return feastservices.IsOpenShift()
}

func (r *FeatureStoreReconciler) createOrUpdateOAuthRoleBinding(ctx context.Context, featureStore *feastdevv1alpha1.FeatureStore) error {
	roleBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-%s-auth-delegator", featureStore.Namespace, featureStore.Name),
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, roleBinding, func() error {
		roleBinding.RoleRef = rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     "system:auth-delegator",
		}
		roleBinding.Subjects = []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      fmt.Sprintf("%s-controller-manager", featureStore.Name),
				Namespace: featureStore.Namespace,
			},
		}
		return controllerutil.SetControllerReference(featureStore, roleBinding, r.Scheme)
	})

	return err
}

func (r *FeatureStoreReconciler) createOrUpdateServiceOAuthCookieSecret(ctx context.Context, featureStore *feastdevv1alpha1.FeatureStore, service string) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s-oauth-proxy-config", featureStore.Name, service),
			Namespace: featureStore.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		if secret.Data == nil {
			secret.Data = make(map[string][]byte)
		}

		// Generate cookie secret if not provided
		if featureStore.Spec.OAuthProxy.CookieSecret == nil {
			// Generate a random cookie secret
			cookieSecret := generateRandomString(32)
			secret.Data["cookie_secret"] = []byte(cookieSecret)
		} else {
			// Use provided cookie secret
			secret.Data["cookie_secret"] = []byte(featureStore.Spec.OAuthProxy.CookieSecret.Key)
		}

		secret.Type = corev1.SecretTypeOpaque
		return controllerutil.SetControllerReference(featureStore, secret, r.Scheme)
	})

	return err
}

func (r *FeatureStoreReconciler) createOrUpdateRegistryOAuthCookieSecrets(ctx context.Context, featureStore *feastdevv1alpha1.FeatureStore) error {
	// Create OAuth proxy cookie secrets for registry gRPC and REST
	registryServices := []string{"registry-grpc", "registry-rest"}
	for _, service := range registryServices {
		if err := r.createOrUpdateServiceOAuthCookieSecret(ctx, featureStore, service); err != nil {
			return err
		}
	}
	return nil
}

func (r *FeatureStoreReconciler) createOrUpdateServiceOAuthRoute(ctx context.Context, featureStore *feastdevv1alpha1.FeatureStore, service string) error {
	route := &routev1.Route{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s-oauth-proxy", featureStore.Name, service),
			Namespace: featureStore.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, route, func() error {
		route.Spec = routev1.RouteSpec{
			To: routev1.RouteTargetReference{
				Kind: "Service",
				Name: fmt.Sprintf("%s-%s", featureStore.Name, service),
			},
			Port: &routev1.RoutePort{
				TargetPort: intstr.FromString(fmt.Sprintf("%s-oauth-proxy", service)),
			},
			TLS: &routev1.TLSConfig{
				Termination:                   routev1.TLSTerminationReencrypt,
				InsecureEdgeTerminationPolicy: routev1.InsecureEdgeTerminationPolicyRedirect,
			},
		}

		// Set domain if provided
		if featureStore.Spec.OAuthProxy.Domain != "" {
			route.Spec.Host = fmt.Sprintf("%s-%s.%s", featureStore.Name, service, featureStore.Spec.OAuthProxy.Domain)
		}

		return controllerutil.SetControllerReference(featureStore, route, r.Scheme)
	})

	return err
}

func (r *FeatureStoreReconciler) deleteAllOAuthResources(ctx context.Context, featureStore *feastdevv1alpha1.FeatureStore) error {
	// Delete OAuth proxy role binding
	if err := r.deleteOAuthRoleBinding(ctx, featureStore); err != nil {
		return err
	}

	// Delete OAuth proxy routes for online, offline, and ui services
	services := []string{"online", "offline", "ui"}
	for _, service := range services {
		if err := r.deleteServiceOAuthRoute(ctx, featureStore, service); err != nil {
			return err
		}
	}

	// Delete OAuth proxy routes for registry gRPC and REST
	registryServices := []string{"registry-grpc", "registry-rest"}
	for _, service := range registryServices {
		if err := r.deleteServiceOAuthRoute(ctx, featureStore, service); err != nil {
			return err
		}
	}

	// Delete OAuth proxy cookie secrets for online, offline, and ui services
	for _, service := range services {
		if err := r.deleteServiceOAuthCookieSecret(ctx, featureStore, service); err != nil {
			return err
		}
	}

	// Delete OAuth proxy cookie secrets for registry gRPC and REST
	for _, service := range registryServices {
		if err := r.deleteServiceOAuthCookieSecret(ctx, featureStore, service); err != nil {
			return err
		}
	}

	return nil
}

func (r *FeatureStoreReconciler) deleteOAuthRoleBinding(ctx context.Context, featureStore *feastdevv1alpha1.FeatureStore) error {
	roleBinding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-%s-auth-delegator", featureStore.Namespace, featureStore.Name),
		},
	}
	return client.IgnoreNotFound(r.Client.Delete(ctx, roleBinding))
}

func (r *FeatureStoreReconciler) deleteServiceOAuthRoute(ctx context.Context, featureStore *feastdevv1alpha1.FeatureStore, service string) error {
	route := &routev1.Route{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s-oauth-proxy", featureStore.Name, service),
			Namespace: featureStore.Namespace,
		},
	}
	return client.IgnoreNotFound(r.Client.Delete(ctx, route))
}

func (r *FeatureStoreReconciler) deleteServiceOAuthCookieSecret(ctx context.Context, featureStore *feastdevv1alpha1.FeatureStore, service string) error {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%s-oauth-proxy-config", featureStore.Name, service),
			Namespace: featureStore.Namespace,
		},
	}
	return client.IgnoreNotFound(r.Client.Delete(ctx, secret))
}

// Helper function to generate random string for cookie secret
func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}
