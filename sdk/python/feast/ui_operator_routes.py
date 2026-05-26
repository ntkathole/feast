"""Operator management API routes for the Feast UI server.

This module is conditionally loaded only when:
1. FEAST_OPERATOR_MANAGED=true environment variable is set
2. The `kubernetes` Python package is installed (pip install feast[k8s])

All K8s operations use user impersonation so native RBAC applies.
The authenticated user's identity comes from X-Forwarded-User/Groups headers
set by the oauth2-proxy sidecar.
"""

import logging
import os
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from kubernetes import client
from kubernetes.client.rest import ApiException

from feast.ui_operator_rbac import RBACEnforcer, _get_user, _parse_groups

logger = logging.getLogger(__name__)

FEAST_GROUP = "feast.dev"
FEAST_VERSION = "v1"
FEAST_PLURAL = "featurestores"

_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"


def _make_k8s_configuration() -> client.Configuration:
    """Build a Kubernetes client Configuration using in-cluster credentials.

    Works around kubernetes-client v36+ where load_incluster_config() does not
    properly set the Bearer token for API authentication.
    """
    configuration = client.Configuration()
    configuration.host = f"https://{os.environ['KUBERNETES_SERVICE_HOST']}:{os.environ['KUBERNETES_SERVICE_PORT']}"
    configuration.ssl_ca_cert = _CA_PATH
    with open(_TOKEN_PATH) as f:
        token = f.read().strip()
    configuration.api_key = {"BearerToken": token}
    configuration.api_key_prefix = {"BearerToken": "Bearer"}
    return configuration


def _get_impersonated_custom_api(request: Request) -> client.CustomObjectsApi:
    """Create a CustomObjectsApi client that impersonates the authenticated user."""
    user = _get_user(request)
    groups = _parse_groups(request)

    configuration = _make_k8s_configuration()
    api_client = client.ApiClient(configuration)

    if user:
        api_client.set_default_header("Impersonate-User", user)
    for group in groups:
        api_client.set_default_header("Impersonate-Group", group)

    return client.CustomObjectsApi(api_client)


def _get_impersonated_core_api(request: Request) -> client.CoreV1Api:
    """Create a CoreV1Api client that impersonates the authenticated user."""
    user = _get_user(request)
    groups = _parse_groups(request)

    configuration = _make_k8s_configuration()
    api_client = client.ApiClient(configuration)

    if user:
        api_client.set_default_header("Impersonate-User", user)
    for group in groups:
        api_client.set_default_header("Impersonate-Group", group)

    return client.CoreV1Api(api_client)


def _pod_summary(pod) -> dict:
    """Extract minimal pod info for the frontend."""
    containers = []
    for c in pod.status.container_statuses or []:
        containers.append(
            {
                "name": c.name,
                "ready": c.ready,
                "state": _container_state(c.state),
            }
        )

    init_containers = []
    for c in pod.status.init_container_statuses or []:
        init_containers.append(
            {
                "name": c.name,
                "ready": c.ready,
                "state": _container_state(c.state),
            }
        )

    return {
        "name": pod.metadata.name,
        "namespace": pod.metadata.namespace,
        "phase": pod.status.phase,
        "creationTimestamp": (
            pod.metadata.creation_timestamp.isoformat()
            if pod.metadata.creation_timestamp
            else None
        ),
        "containers": containers,
        "initContainers": init_containers,
    }


def _container_state(state) -> str:
    """Convert container state to a human-readable string."""
    if state is None:
        return "Unknown"
    if state.running:
        return "Running"
    if state.waiting:
        return f"Waiting: {state.waiting.reason or 'Unknown'}"
    if state.terminated:
        return f"Terminated: {state.terminated.reason or 'Unknown'}"
    return "Unknown"


def mount_operator_routes(app: FastAPI):
    """Mount all /api/operator/* endpoints onto the FastAPI app.

    This function is the single entry point called by ui_server.py.
    If it raises ImportError, the caller handles it gracefully.
    """
    rbac = RBACEnforcer()
    configuration = _make_k8s_configuration()
    core_api = client.CoreV1Api(client.ApiClient(configuration))

    @app.get("/api/operator/permissions")
    async def get_permissions(request: Request):
        """Return the current user's permissions for UI gating."""
        user = _get_user(request)
        groups = _parse_groups(request)

        if not user:
            return {
                "canList": False,
                "canCreate": False,
                "canDelete": False,
                "canUpdate": False,
                "accessibleNamespaces": [],
            }

        perms = rbac.get_user_permissions(user, groups)
        perms["accessibleNamespaces"] = rbac.get_accessible_namespaces(user, groups)
        return perms

    @app.get("/api/operator/featurestores")
    async def list_featurestores(request: Request, namespace: Optional[str] = None):
        """List FeatureStore CRDs. Optionally filter by namespace."""
        rbac.enforce(request, "list", namespace=namespace or "")
        custom_api = _get_impersonated_custom_api(request)
        try:
            if namespace:
                result = custom_api.list_namespaced_custom_object(
                    FEAST_GROUP, FEAST_VERSION, namespace, FEAST_PLURAL
                )
            else:
                result = custom_api.list_cluster_custom_object(
                    FEAST_GROUP, FEAST_VERSION, FEAST_PLURAL
                )
            return result
        except ApiException as e:
            raise HTTPException(status_code=e.status or 500, detail=e.reason)

    @app.get("/api/operator/featurestores/{namespace}/{name}")
    async def get_featurestore(request: Request, namespace: str, name: str):
        """Get a specific FeatureStore CRD."""
        rbac.enforce(request, "get", namespace=namespace, name=name)
        custom_api = _get_impersonated_custom_api(request)
        try:
            return custom_api.get_namespaced_custom_object(
                FEAST_GROUP, FEAST_VERSION, namespace, FEAST_PLURAL, name
            )
        except ApiException as e:
            raise HTTPException(status_code=e.status or 500, detail=e.reason)

    @app.post("/api/operator/featurestores")
    async def create_featurestore(request: Request):
        """Create a FeatureStore CRD."""
        body = await request.json()
        namespace = body.get("metadata", {}).get("namespace", "default")
        rbac.enforce(request, "create", namespace=namespace)
        custom_api = _get_impersonated_custom_api(request)
        try:
            return custom_api.create_namespaced_custom_object(
                FEAST_GROUP, FEAST_VERSION, namespace, FEAST_PLURAL, body
            )
        except ApiException as e:
            raise HTTPException(status_code=e.status or 500, detail=e.reason)

    @app.delete("/api/operator/featurestores/{namespace}/{name}")
    async def delete_featurestore(request: Request, namespace: str, name: str):
        """Delete a FeatureStore CRD."""
        rbac.enforce(request, "delete", namespace=namespace, name=name)
        custom_api = _get_impersonated_custom_api(request)
        try:
            return custom_api.delete_namespaced_custom_object(
                FEAST_GROUP, FEAST_VERSION, namespace, FEAST_PLURAL, name
            )
        except ApiException as e:
            raise HTTPException(status_code=e.status or 500, detail=e.reason)

    @app.get("/api/operator/featurestores/{namespace}/{name}/pods")
    async def get_featurestore_pods(request: Request, namespace: str, name: str):
        """Get pods associated with a FeatureStore deployment."""
        rbac.enforce(request, "get", namespace=namespace, name=name)
        impersonated_core = _get_impersonated_core_api(request)
        try:
            pods = impersonated_core.list_namespaced_pod(
                namespace,
                label_selector=f"app.kubernetes.io/managed-by=feast,feast.dev/name={name}",
            )
            return {"items": [_pod_summary(p) for p in pods.items]}
        except ApiException as e:
            raise HTTPException(status_code=e.status or 500, detail=e.reason)

    @app.get("/api/operator/namespaces")
    async def list_namespaces(request: Request):
        """List namespaces using SA credentials (users rarely have cluster-wide ns list)."""
        user = _get_user(request)
        if not user:
            raise HTTPException(status_code=401, detail="Not authenticated")
        try:
            ns_list = core_api.list_namespace()
            return {
                "items": [
                    {"name": ns.metadata.name}
                    for ns in ns_list.items
                    if not ns.metadata.name.startswith("kube-")
                    and ns.metadata.name != "default"
                ]
            }
        except ApiException as e:
            raise HTTPException(status_code=e.status or 500, detail=e.reason)

    @app.get("/api/operator/secrets/{namespace}")
    async def list_secrets(request: Request, namespace: str):
        """List secret names in a namespace (no secret data exposed)."""
        rbac.enforce(request, "get", namespace=namespace)
        impersonated_core = _get_impersonated_core_api(request)
        try:
            secrets = impersonated_core.list_namespaced_secret(namespace)
            return {
                "items": [
                    {"name": s.metadata.name}
                    for s in secrets.items
                    if s.type != "kubernetes.io/service-account-token"
                ]
            }
        except ApiException as e:
            raise HTTPException(status_code=e.status or 500, detail=e.reason)

    @app.get("/api/operator/configmaps/{namespace}")
    async def list_configmaps(request: Request, namespace: str):
        """List ConfigMap names in a namespace."""
        rbac.enforce(request, "get", namespace=namespace)
        impersonated_core = _get_impersonated_core_api(request)
        try:
            cms = impersonated_core.list_namespaced_config_map(namespace)
            return {"items": [{"name": cm.metadata.name} for cm in cms.items]}
        except ApiException as e:
            raise HTTPException(status_code=e.status or 500, detail=e.reason)

    # --- Federated discovery endpoints ---
    from feast.ui_operator_federation import RegistryFederator

    federator = RegistryFederator()

    @app.get("/api/operator/discover/{resource_type}")
    async def discover_resources(
        request: Request,
        resource_type: str,
        tags: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "asc",
        page: int = 1,
        limit: int = 100,
    ):
        """Federated discovery: query all permitted registries for a resource type."""
        user = _get_user(request)
        groups = _parse_groups(request)
        if not user:
            raise HTTPException(status_code=401, detail="Not authenticated")

        valid_types = {
            "entities",
            "feature_views",
            "features",
            "data_sources",
            "feature_services",
            "saved_datasets",
        }
        if resource_type not in valid_types:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid resource_type. Must be one of: {', '.join(sorted(valid_types))}",
            )

        return await federator.federated_query(
            user=user,
            groups=groups,
            resource_type=resource_type,
            tags=tags,
            sort_by=sort_by,
            sort_order=sort_order,
            page=page,
            limit=limit,
        )

    @app.get("/api/operator/search")
    async def federated_search(
        request: Request,
        query: str = "",
        tags: Optional[str] = None,
        page: int = 1,
        limit: int = 50,
    ):
        """Federated search across all permitted registries."""
        user = _get_user(request)
        groups = _parse_groups(request)
        if not user:
            raise HTTPException(status_code=401, detail="Not authenticated")
        if not query:
            raise HTTPException(status_code=400, detail="query parameter is required")

        return await federator.federated_search(
            user=user,
            groups=groups,
            query=query,
            tags=tags,
            page=page,
            limit=limit,
        )

    logger.info("Operator management routes mounted successfully")
