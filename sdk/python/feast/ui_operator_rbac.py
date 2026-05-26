"""RBAC enforcement for Feast UI operator management endpoints.

This module is only imported when operator mode is active (FEAST_OPERATOR_MANAGED=true
and the `kubernetes` package is installed). It provides SubjectAccessReview-based
per-endpoint authorization using the authenticated user's identity from proxy headers.
"""

import logging
import os
from typing import List

from fastapi import HTTPException, Request
from kubernetes import client
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)

FEAST_GROUP = "feast.dev"
FEAST_RESOURCE = "featurestores"


class RBACEnforcer:
    """Performs Kubernetes SubjectAccessReview to check per-user permissions.

    Expects the oauth2-proxy (or equivalent) to set:
    - X-Forwarded-User: the authenticated username
    - X-Forwarded-Groups: comma-separated group memberships
    """

    def __init__(self):
        configuration = client.Configuration()
        configuration.host = f"https://{os.environ['KUBERNETES_SERVICE_HOST']}:{os.environ['KUBERNETES_SERVICE_PORT']}"
        configuration.ssl_ca_cert = (
            "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
        )
        with open("/var/run/secrets/kubernetes.io/serviceaccount/token") as f:
            token = f.read().strip()
        configuration.api_key = {"BearerToken": token}
        configuration.api_key_prefix = {"BearerToken": "Bearer"}
        api_client = client.ApiClient(configuration)
        self.authz_api = client.AuthorizationV1Api(api_client)
        self._core_api = client.CoreV1Api(api_client)

    def check_access(
        self,
        user: str,
        groups: List[str],
        verb: str,
        namespace: str = "",
        name: str = "",
    ) -> bool:
        """Perform a SubjectAccessReview for the given user and action."""
        try:
            sar = client.V1SubjectAccessReview(
                spec=client.V1SubjectAccessReviewSpec(
                    user=user,
                    groups=groups,
                    resource_attributes=client.V1ResourceAttributes(
                        group=FEAST_GROUP,
                        resource=FEAST_RESOURCE,
                        verb=verb,
                        namespace=namespace or None,
                        name=name or None,
                    ),
                )
            )
            response = self.authz_api.create_subject_access_review(sar)
            return response.status.allowed
        except ApiException as e:
            logger.warning(f"SSAR check failed for user={user} verb={verb}: {e.reason}")
            return False

    def enforce(self, request: Request, verb: str, namespace: str = "", name: str = ""):
        """Enforce RBAC for a given request. Raises HTTPException on denial."""
        user = _get_user(request)
        groups = _parse_groups(request)

        if not user:
            raise HTTPException(status_code=401, detail="Not authenticated")

        if not self.check_access(user, groups, verb, namespace, name):
            detail = f"User '{user}' cannot '{verb}' featurestores"
            if namespace:
                detail += f" in namespace '{namespace}'"
            raise HTTPException(status_code=403, detail=detail)

    def get_user_permissions(self, user: str, groups: List[str]) -> dict:
        """Return a summary of what the authenticated user can do."""
        return {
            "canList": self.check_access(user, groups, "list"),
            "canCreate": self.check_access(user, groups, "create"),
            "canDelete": self.check_access(user, groups, "delete"),
            "canUpdate": self.check_access(user, groups, "update"),
        }

    def get_accessible_namespaces(self, user: str, groups: List[str]) -> List[str]:
        """Return namespaces where the user can create featurestores."""
        try:
            ns_list = self._core_api.list_namespace()
            accessible = []
            for ns in ns_list.items:
                ns_name = ns.metadata.name
                if self.check_access(user, groups, "create", namespace=ns_name):
                    accessible.append(ns_name)
            return accessible
        except ApiException:
            return []


def _get_user(request: Request) -> str:
    """Extract authenticated username from proxy headers."""
    return request.headers.get("X-Forwarded-User", "") or request.headers.get(
        "X-Remote-User", ""
    )


def _parse_groups(request: Request) -> List[str]:
    """Parse group memberships from proxy headers."""
    raw = (
        request.headers.get("X-Forwarded-Groups", "")
        or request.headers.get("X-Remote-Group", "")
        or request.headers.get("X-Remote-Groups", "")
    )
    return [g.strip() for g in raw.split(",") if g.strip()]
