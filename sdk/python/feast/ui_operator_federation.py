"""Cross-project registry federation for Feast UI.

This module enables the centralized Feast UI to query multiple FeatureStore
registries in parallel, merge results, and return a unified view. It powers
the "Discover" page where data scientists search features across all projects.

Only loaded when operator mode is active (FEAST_OPERATOR_MANAGED=true).
"""

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import httpx
from kubernetes import client
from kubernetes.client.rest import ApiException

logger = logging.getLogger(__name__)

FEAST_GROUP = "feast.dev"
FEAST_VERSION = "v1"
FEAST_PLURAL = "featurestores"

_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

REGISTRY_QUERY_TIMEOUT = 5.0
CACHE_TTL_SECONDS = 60


@dataclass
class RegistryEndpoint:
    """Represents a discoverable Feast registry."""

    project: str
    namespace: str
    name: str
    rest_url: str


@dataclass
class CacheEntry:
    """TTL-based cache entry."""

    data: Any
    expires_at: float

    @property
    def is_valid(self) -> bool:
        return time.time() < self.expires_at


class RegistryFederator:
    """Fans out REST queries to multiple registry endpoints and merges results.

    Discovers registries from FeatureStore CRs, checks RBAC per-user,
    queries each permitted registry in parallel, and returns merged results.
    """

    def __init__(self):
        configuration = client.Configuration()
        configuration.host = f"https://{os.environ['KUBERNETES_SERVICE_HOST']}:{os.environ['KUBERNETES_SERVICE_PORT']}"
        configuration.ssl_ca_cert = _CA_PATH
        with open(_TOKEN_PATH) as f:
            self._sa_token = f.read().strip()
        configuration.api_key = {"BearerToken": self._sa_token}
        configuration.api_key_prefix = {"BearerToken": "Bearer"}
        api_client = client.ApiClient(configuration)
        self._custom_api = client.CustomObjectsApi(api_client)
        self._authz_api = client.AuthorizationV1Api(api_client)
        self._cache: Dict[str, CacheEntry] = {}

    def _get_cache(self, key: str) -> Optional[Any]:
        entry = self._cache.get(key)
        if entry and entry.is_valid:
            return entry.data
        return None

    def _set_cache(self, key: str, data: Any, ttl: float = CACHE_TTL_SECONDS):
        self._cache[key] = CacheEntry(data=data, expires_at=time.time() + ttl)

    def _check_access(
        self,
        user: str,
        groups: List[str],
        verb: str,
        namespace: str = "",
        name: str = "",
    ) -> bool:
        try:
            sar = client.V1SubjectAccessReview(
                spec=client.V1SubjectAccessReviewSpec(
                    user=user,
                    groups=groups,
                    resource_attributes=client.V1ResourceAttributes(
                        group=FEAST_GROUP,
                        resource=FEAST_PLURAL,
                        verb=verb,
                        namespace=namespace or None,
                        name=name or None,
                    ),
                )
            )
            response = self._authz_api.create_subject_access_review(sar)
            return response.status.allowed
        except ApiException as e:
            logger.warning(f"SSAR failed for user={user}: {e.reason}")
            return False

    def _list_all_featurestores(self) -> List[dict]:
        cache_key = "all_featurestores"
        cached = self._get_cache(cache_key)
        if cached is not None:
            return cached

        try:
            result = self._custom_api.list_cluster_custom_object(
                group=FEAST_GROUP, version=FEAST_VERSION, plural=FEAST_PLURAL
            )
            items = result.get("items", [])
            self._set_cache(cache_key, items, ttl=30)
            return items
        except ApiException as e:
            logger.error(f"Failed to list FeatureStores: {e.reason}")
            return []

    def get_permitted_registries(
        self, user: str, groups: List[str]
    ) -> List[RegistryEndpoint]:
        """Return registry endpoints for FeatureStores the user can access."""
        all_stores = self._list_all_featurestores()
        endpoints = []

        for store in all_stores:
            metadata = store.get("metadata", {})
            status = store.get("status", {})
            ns = metadata.get("namespace", "")
            name = metadata.get("name", "")

            if not self._check_access(user, groups, "get", namespace=ns, name=name):
                continue

            hostnames = status.get("serviceHostnames", {})
            registry_rest = hostnames.get("registryRest", "")
            project = status.get("applied", {}).get("feastProject", name)

            if not registry_rest:
                registry_svc = f"feast-{name}-registry-rest.{ns}.svc.cluster.local:80"
                registry_rest = registry_svc

            # Use http for in-cluster communication; TLS terminates at the service
            rest_url = f"http://{registry_rest}"
            if ":443" in registry_rest:
                rest_url = f"https://{registry_rest}"

            endpoints.append(
                RegistryEndpoint(
                    project=project, namespace=ns, name=name, rest_url=rest_url
                )
            )

        return endpoints

    def _get_registry_headers(self) -> Dict[str, str]:
        """Return auth headers for registry REST API calls."""
        try:
            with open(_TOKEN_PATH) as f:
                token = f.read().strip()
        except OSError:
            token = self._sa_token
        return {"Authorization": f"Bearer {token}"}

    async def _query_registry(
        self,
        http_client: httpx.AsyncClient,
        endpoint: RegistryEndpoint,
        path: str,
        params: Optional[Dict[str, str]] = None,
    ) -> Tuple[RegistryEndpoint, Optional[dict]]:
        """Query a single registry endpoint."""
        url = f"{endpoint.rest_url}{path}"
        if params is None:
            params = {}
        params.setdefault("project", endpoint.project)

        try:
            response = await http_client.get(
                url,
                params=params,
                timeout=REGISTRY_QUERY_TIMEOUT,
                headers=self._get_registry_headers(),
            )
            if response.status_code == 200:
                return endpoint, response.json()
            else:
                logger.warning(
                    f"Registry {endpoint.name} returned {response.status_code} for {path}"
                )
                return endpoint, None
        except Exception as e:
            logger.warning(f"Failed to query {endpoint.name} at {url}: {e}")
            return endpoint, None

    async def federated_query(
        self,
        user: str,
        groups: List[str],
        resource_type: str,
        tags: Optional[str] = None,
        sort_by: Optional[str] = None,
        sort_order: str = "asc",
        page: int = 1,
        limit: int = 100,
    ) -> dict:
        """Query all permitted registries for a resource type and merge results."""
        endpoints = self.get_permitted_registries(user, groups)

        if not endpoints:
            return {
                "items": [],
                "total": 0,
                "page": page,
                "limit": limit,
                "projects": [],
            }

        path = f"/api/v1/{resource_type}"
        params: Dict[str, str] = {"include_relationships": "true"}
        if tags:
            params["tags"] = tags

        # Check per-endpoint cache before making network calls
        results: List[Tuple[RegistryEndpoint, Optional[dict]]] = []
        uncached_endpoints: List[RegistryEndpoint] = []

        for ep in endpoints:
            cache_key = (
                f"registry:{ep.name}:{ep.namespace}:{resource_type}:{tags or ''}"
            )
            cached = self._get_cache(cache_key)
            if cached is not None:
                results.append((ep, cached))
            else:
                uncached_endpoints.append(ep)

        if uncached_endpoints:
            async with httpx.AsyncClient(verify=False) as http_client:
                tasks = [
                    self._query_registry(http_client, ep, path, dict(params))
                    for ep in uncached_endpoints
                ]
                fresh_results = await asyncio.gather(*tasks)

            for ep, data in fresh_results:
                if data is not None:
                    cache_key = f"registry:{ep.name}:{ep.namespace}:{resource_type}:{tags or ''}"
                    self._set_cache(cache_key, data)
                results.append((ep, data))

        merged_items = []
        projects_queried = []

        for endpoint, data in results:
            if data is None:
                continue
            projects_queried.append(
                {
                    "project": endpoint.project,
                    "namespace": endpoint.namespace,
                    "name": endpoint.name,
                }
            )
            items = self._extract_items(data, resource_type)
            for item in items:
                item["_source_project"] = endpoint.project
                item["_source_namespace"] = endpoint.namespace
                item["_source_featurestore"] = endpoint.name
                merged_items.append(item)

        if sort_by:
            reverse = sort_order.lower() == "desc"
            merged_items.sort(
                key=lambda x: str(x.get(sort_by, "")).lower(), reverse=reverse
            )

        total = len(merged_items)
        start = (page - 1) * limit
        end = start + limit
        paginated = merged_items[start:end]

        return {
            "items": paginated,
            "total": total,
            "page": page,
            "limit": limit,
            "projects": projects_queried,
        }

    async def federated_search(
        self,
        user: str,
        groups: List[str],
        query: str,
        tags: Optional[str] = None,
        page: int = 1,
        limit: int = 50,
    ) -> dict:
        """Search across all permitted registries."""
        endpoints = self.get_permitted_registries(user, groups)

        if not endpoints:
            return {"query": query, "results": [], "total": 0, "projects_searched": []}

        path = "/api/v1/search"

        async with httpx.AsyncClient(verify=False) as http_client:
            tasks = []
            for ep in endpoints:
                params = {"query": query, "projects": ep.project}
                if tags:
                    params["tags"] = tags
                tasks.append(self._query_registry(http_client, ep, path, params))
            results = await asyncio.gather(*tasks)

        merged_results = []
        projects_searched = []

        for endpoint, data in results:
            if data is None:
                continue
            projects_searched.append(endpoint.project)
            search_results = data.get("results", [])
            for item in search_results:
                item["_source_project"] = endpoint.project
                item["_source_namespace"] = endpoint.namespace
                item["_source_featurestore"] = endpoint.name
                merged_results.append(item)

        total = len(merged_results)
        start = (page - 1) * limit
        end = start + limit
        paginated = merged_results[start:end]

        return {
            "query": query,
            "results": paginated,
            "total": total,
            "page": page,
            "limit": limit,
            "projects_searched": projects_searched,
        }

    def _extract_items(self, data: dict, resource_type: str) -> List[dict]:
        """Extract items list from a registry response based on resource type."""
        key_map = {
            "entities": "entities",
            "feature_views": "featureViews",
            "features": "features",
            "data_sources": "dataSources",
            "feature_services": "featureServices",
            "saved_datasets": "savedDatasets",
        }
        key = key_map.get(resource_type, resource_type)
        return data.get(key, [])
