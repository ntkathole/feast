"""
Feast Server Discovery Module

This module provides functionality for discovering Feast FeatureStore instances
across namespaces in Kubernetes clusters using the namespace registry
(https://github.com/feast-dev/feast/blob/master/infra/feast-operator/docs/namespace-registry.md).
"""

import json
import logging
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

try:
    from kubernetes import client, config
    from kubernetes.client.rest import ApiException

    KUBERNETES_AVAILABLE = True
except ImportError:
    KUBERNETES_AVAILABLE = False

from feast.feature_store import FeatureStore
from feast.repo_config import load_repo_config

logger = logging.getLogger(__name__)

# Constants matching the operator implementation
NAMESPACE_REGISTRY_CONFIGMAP_NAME = "feast-configs-registry"
NAMESPACE_REGISTRY_DATA_KEY = "namespaces"
DEFAULT_KUBERNETES_NAMESPACE = "feast-operator-system"
DEFAULT_OPENSHIFT_NAMESPACE = "redhat-ods-applications"


@dataclass
class FeastServerInfo:
    """Information about a discovered Feast server instance."""

    name: str
    namespace: str
    project: str
    client_config_name: str
    service_hostnames: Dict[str, str]
    status: str
    age: str

    def get_registry_endpoint(self) -> Optional[str]:
        """Get the registry endpoint for this server."""
        return self.service_hostnames.get("registry")

    def get_online_store_endpoint(self) -> Optional[str]:
        """Get the online store endpoint for this server."""
        return self.service_hostnames.get("onlineStore")

    def get_offline_store_endpoint(self) -> Optional[str]:
        """Get the offline store endpoint for this server."""
        return self.service_hostnames.get("offlineStore")

    def get_ui_endpoint(self) -> Optional[str]:
        """Get the UI endpoint for this server."""
        return self.service_hostnames.get("ui")


class FeastServerDiscovery:
    """
    Discovers Feast FeatureStore instances across Kubernetes namespaces.

    This class provides methods to discover available Feast servers that clients
    can connect to, even when the servers are deployed in different namespaces
    than the client.
    """

    def __init__(self, kubeconfig_path: Optional[str] = None):
        """
        Initialize the discovery client.
        Args:
            kubeconfig_path: Path to kubeconfig file. If None, uses default config.
        """
        if not KUBERNETES_AVAILABLE:
            raise ImportError(
                "kubernetes library is required for server discovery. "
                "Install with: pip install feast[k8s]"
            )

        try:
            if kubeconfig_path:
                config.load_kube_config(config_file=kubeconfig_path)
            else:
                try:
                    config.load_incluster_config()
                except config.ConfigException:
                    config.load_kube_config()

            self.k8s_client = client.ApiClient()
            self.core_v1 = client.CoreV1Api()
            self.custom_objects_api = client.CustomObjectsApi()

        except Exception as e:
            raise RuntimeError(f"Failed to initialize Kubernetes client: {e}")

    def _get_namespace_registry_location(self) -> tuple[str, str]:
        """
        Determine the namespace and name of the registry ConfigMap.
        Returns:
            Tuple of (namespace, configmap_name)
        """
        try:
            # Check for DSCInitialization
            self.custom_objects_api.get_namespaced_custom_object(
                group="dscinitialization.opendatahub.io",
                version="v1",
                namespace=DEFAULT_OPENSHIFT_NAMESPACE,
                plural="dscinitializations",
                name="default-dsci",
            )
            return DEFAULT_OPENSHIFT_NAMESPACE, NAMESPACE_REGISTRY_CONFIGMAP_NAME
        except ApiException:
            pass

        # Default to Kubernetes mode
        return DEFAULT_KUBERNETES_NAMESPACE, NAMESPACE_REGISTRY_CONFIGMAP_NAME

    def get_servers(
        self, namespace_filter: Optional[str] = None
    ) -> List[FeastServerInfo]:
        """
        Discover available Feast servers.
        Args:
            namespace_filter: Optional namespace to filter servers by
        Returns:
            List of FeastServerInfo objects representing available servers
        """
        servers = []

        try:
            # First, try to get servers from the namespace registry
            registry_namespace, registry_configmap = (
                self._get_namespace_registry_location()
            )

            try:
                registry_cm = self.core_v1.read_namespaced_config_map(
                    name=registry_configmap, namespace=registry_namespace
                )

                if registry_cm.data and NAMESPACE_REGISTRY_DATA_KEY in registry_cm.data:
                    namespace_data = json.loads(
                        registry_cm.data[NAMESPACE_REGISTRY_DATA_KEY]
                    )

                    for ns, client_configs in namespace_data.get(
                        "namespaces", {}
                    ).items():
                        if namespace_filter and ns != namespace_filter:
                            continue

                        for config_name in client_configs:
                            server_info = self._get_server_from_client_config(
                                ns, config_name
                            )
                            if server_info:
                                servers.append(server_info)

            except ApiException as e:
                if e.status == 404:
                    logger.info(
                        f"Namespace registry ConfigMap not found in {registry_namespace}. "
                        f"This usually means no Feast instances have been deployed yet, "
                        f"or the Feast operator hasn't created the registry."
                    )
                else:
                    logger.warning(f"Could not read namespace registry: {e}")

        except Exception as e:
            logger.warning(f"Error accessing namespace registry: {e}")

        # Fallback: directly discover FeatureStore CRs (requires broader permissions)
        if not servers:
            servers.extend(self._discover_featurestore_crs(namespace_filter))

        return servers

    def _get_server_from_client_config(
        self, namespace: str, config_name: str
    ) -> Optional[FeastServerInfo]:
        """
        Get server information from a client ConfigMap.
        Args:
            namespace: Namespace containing the ConfigMap
            config_name: Name of the client ConfigMap
        Returns:
            FeastServerInfo if successful, None otherwise
        """
        try:
            config_cm = self.core_v1.read_namespaced_config_map(
                name=config_name, namespace=namespace
            )

            if not config_cm.data or "feature_store.yaml" not in config_cm.data:
                return None

            # Parse the feature store config
            config_yaml = config_cm.data["feature_store.yaml"]

            # Create a temporary file to load the config
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".yaml", delete=False
            ) as f:
                f.write(config_yaml)
                temp_path = f.name

            try:
                repo_config = load_repo_config(Path.cwd(), Path(temp_path))

                # Get the corresponding FeatureStore CR for additional info
                cr_info = self._get_featurestore_cr_info(namespace, config_name)

                return FeastServerInfo(
                    name=cr_info.get("name", config_name.replace("-config", "")),
                    namespace=namespace,
                    project=repo_config.project,
                    client_config_name=config_name,
                    service_hostnames=cr_info.get("service_hostnames", {}),
                    status=cr_info.get("status", "Unknown"),
                    age=cr_info.get("age", "Unknown"),
                )
            finally:
                import os

                os.unlink(temp_path)

        except Exception as e:
            logger.warning(
                f"Could not read client config {config_name} in {namespace}: {e}"
            )
            return None

    def _get_featurestore_cr_info(self, namespace: str, config_name: str) -> Dict:
        """Get additional info from the FeatureStore CR."""
        try:
            # Try to find the FeatureStore CR that owns this ConfigMap
            crs = self.custom_objects_api.list_namespaced_custom_object(
                group="feast.dev",
                version="v1alpha1",
                namespace=namespace,
                plural="featurestores",
            )

            for cr in crs.get("items", []):
                if cr.get("status", {}).get("clientConfigMap") == config_name:
                    return {
                        "name": cr["metadata"]["name"],
                        "service_hostnames": cr.get("status", {}).get(
                            "serviceHostnames", {}
                        ),
                        "status": cr.get("status", {}).get("phase", "Unknown"),
                        "age": cr["metadata"].get("creationTimestamp", "Unknown"),
                    }
        except Exception as e:
            logger.debug(f"Could not get FeatureStore CR info: {e}")

        return {}

    def _discover_featurestore_crs(
        self, namespace_filter: Optional[str] = None
    ) -> List[FeastServerInfo]:
        """
        Directly discover FeatureStore CRs (requires broader permissions).
        Args:
            namespace_filter: Optional namespace to filter by
        Returns:
            List of discovered servers
        """
        servers = []

        try:
            if namespace_filter:
                crs = self.custom_objects_api.list_namespaced_custom_object(
                    group="feast.dev",
                    version="v1alpha1",
                    namespace=namespace_filter,
                    plural="featurestores",
                )
            else:
                crs = self.custom_objects_api.list_cluster_custom_object(
                    group="feast.dev", version="v1alpha1", plural="featurestores"
                )

            for cr in crs.get("items", []):
                status = cr.get("status", {})
                spec = cr.get("spec", {})
                metadata = cr.get("metadata", {})

                if status.get("clientConfigMap"):
                    server_info = FeastServerInfo(
                        name=metadata.get("name", "Unknown"),
                        namespace=metadata.get("namespace", "Unknown"),
                        project=spec.get("feastProject", "Unknown"),
                        client_config_name=status.get("clientConfigMap", ""),
                        service_hostnames=status.get("serviceHostnames", {}),
                        status=status.get("phase", "Unknown"),
                        age=metadata.get("creationTimestamp", "Unknown"),
                    )
                    servers.append(server_info)

        except ApiException as e:
            logger.warning(f"Could not list FeatureStore CRs: {e}")

        return servers

    def connect_to_server(self, server_info: FeastServerInfo) -> FeatureStore:
        """
        Create a FeatureStore client connected to the specified server.
        Args:
            server_info: Information about the server to connect to
        Returns:
            Configured FeatureStore instance
        """
        try:
            # Get the client configuration from the ConfigMap
            config_cm = self.core_v1.read_namespaced_config_map(
                name=server_info.client_config_name, namespace=server_info.namespace
            )

            if not config_cm.data or "feature_store.yaml" not in config_cm.data:
                raise ValueError(
                    f"Invalid client configuration in {server_info.client_config_name}"
                )

            # Create a temporary directory with the configuration
            temp_dir = tempfile.mkdtemp()
            config_path = os.path.join(temp_dir, "feature_store.yaml")

            with open(config_path, "w") as f:
                f.write(config_cm.data["feature_store.yaml"])

            # Create and return the FeatureStore instance
            return FeatureStore(repo_path=temp_dir)

        except Exception as e:
            raise RuntimeError(f"Failed to connect to server {server_info.name}: {e}")


# Convenience functions for the desired API
def get_servers(
    namespace_filter: Optional[str] = None, kubeconfig_path: Optional[str] = None
) -> List[FeastServerInfo]:
    """
    Discover available Feast servers.
    Args:
        namespace_filter: Optional namespace to filter servers by
        kubeconfig_path: Path to kubeconfig file
    Returns:
        List of available servers
    """
    discovery = FeastServerDiscovery(kubeconfig_path)
    return discovery.get_servers(namespace_filter)


def connect_to_server(
    server_info: FeastServerInfo, kubeconfig_path: Optional[str] = None
) -> FeatureStore:
    """
    Connect to a specific Feast server.
    Args:
        server_info: Server to connect to
        kubeconfig_path: Path to kubeconfig file
    Returns:
        Connected FeatureStore instance
    """
    discovery = FeastServerDiscovery(kubeconfig_path)
    return discovery.connect_to_server(server_info)


# Extend FeatureStore class with discovery methods
def _get_servers(
    cls, namespace_filter: Optional[str] = None, kubeconfig_path: Optional[str] = None
) -> List[FeastServerInfo]:
    """Class method to discover available Feast servers."""
    return get_servers(namespace_filter, kubeconfig_path)


def _connect_to_discovered_server(self, server_info: FeastServerInfo) -> "FeatureStore":
    """Connect this FeatureStore instance to a discovered server."""
    discovery = FeastServerDiscovery()
    connected_store = discovery.connect_to_server(server_info)

    # Copy the configuration and registry to this instance
    self.config = connected_store.config
    self.repo_path = connected_store.repo_path
    self._registry = connected_store._registry
    self._provider = connected_store._provider

    return self


# Monkey patch the FeatureStore class to add discovery methods
FeatureStore.get_servers = classmethod(_get_servers)
FeatureStore.connect_to_server = _connect_to_discovered_server
