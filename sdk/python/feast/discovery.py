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
from typing import TYPE_CHECKING, Dict, List, Optional

try:
    import urllib3
    from kubernetes import client, config
    from kubernetes.client.rest import ApiException

    KUBERNETES_AVAILABLE = True
except ImportError:
    KUBERNETES_AVAILABLE = False
    urllib3 = None  # type: ignore

from feast.repo_config import load_repo_config

if TYPE_CHECKING:
    from feast.feature_store import FeatureStore

logger = logging.getLogger(__name__)

if urllib3 is not None:
    urllib3.disable_warnings()

# Constants matching the operator implementation
NAMESPACE_REGISTRY_CONFIGMAP_NAME = "feast-configs-registry"
NAMESPACE_REGISTRY_DATA_KEY = "namespaces"
DEFAULT_KUBERNETES_NAMESPACE = "feast-operator-system"
DEFAULT_OPENSHIFT_NAMESPACE = "redhat-ods-applications"

_current_auth = None


class ClusterTokenAuthentication:
    """
    Manages authentication session for cluster access.

    This class provides a session-based authentication approach,
    where you authenticate once and then use discovery functions without passing credentials.
    """

    def __init__(
        self,
        token: str,
        server_url: str,
        ca_cert: Optional[str] = None,
        verify_ssl: bool = True,
    ):
        """
        Initialize cluster authentication session.

        Args:
            token: Bearer token for authentication
            server_url: Kubernetes API server URL
            ca_cert: CA certificate for server verification (optional)
            verify_ssl: Whether to verify SSL certificates (default: True)
        """
        self.token = token
        self.server_url = server_url
        self.ca_cert = ca_cert
        self.verify_ssl = verify_ssl
        self._discovery: Optional["FeastServerDiscovery"] = None
        self._authenticated = False

        self.login()

    def login(self) -> None:
        """Authenticate with the cluster and establish session."""
        try:
            self._discovery = FeastServerDiscovery(
                token=self.token,
                server_url=self.server_url,
                ca_cert=self.ca_cert,
                verify_ssl=self.verify_ssl,
            )

            self._discovery.get_servers()

            global _current_auth
            _current_auth = self

            self._authenticated = True
            logger.info("Successfully authenticated with cluster")

        except Exception as e:
            self._authenticated = False
            raise RuntimeError(f"Failed to authenticate with cluster: {e}")

    def logout(self) -> None:
        """Logout and clear the authentication session."""
        global _current_auth

        self._authenticated = False
        self._discovery = None
        _current_auth = None

        logger.info("Logged out from cluster")

    def is_authenticated(self) -> bool:
        """Check if currently authenticated."""
        return self._authenticated

    def get_discovery(self) -> "FeastServerDiscovery":
        """Get the discovery client for this session."""
        if not self._authenticated or not self._discovery:
            raise RuntimeError("Not authenticated. Call login() first.")
        assert self._discovery is not None
        return self._discovery

    def get_servers(
        self, namespace_filter: Optional[str] = None
    ) -> List["FeastServerInfo"]:
        """Get servers using this authentication session."""
        return self.get_discovery().get_servers(namespace_filter)

    def connect_to_server(self, server_info: "FeastServerInfo") -> "FeatureStore":
        """Connect to a server using this authentication session."""
        return self.get_discovery().connect_to_server(server_info)


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


class FeastServerDiscovery:
    """
    Discovers Feast FeatureStore instances across Kubernetes namespaces.

    This class provides methods to discover available Feast servers that clients
    can connect to, even when the servers are deployed in different namespaces
    than the client.
    """

    def __init__(
        self,
        kubeconfig_path: Optional[str] = None,
        token: Optional[str] = None,
        server_url: Optional[str] = None,
        ca_cert: Optional[str] = None,
        verify_ssl: bool = True,
    ):
        """
        Initialize the discovery client.
        Args:
            kubeconfig_path: Path to kubeconfig file. If None, uses default config.
            token: Bearer token for authentication (alternative to kubeconfig)
            server_url: Kubernetes API server URL (required with token)
            ca_cert: CA certificate for server verification (optional)
            verify_ssl: Whether to verify SSL certificates (default: True)
        """
        if not KUBERNETES_AVAILABLE:
            raise ImportError(
                "kubernetes library is required for server discovery. "
                "Install with: pip install feast[k8s]"
            )

        try:
            if token and server_url:
                self._init_with_token(token, server_url, ca_cert, verify_ssl)
            elif kubeconfig_path:
                config.load_kube_config(config_file=kubeconfig_path)
                self._init_clients()
            else:
                try:
                    config.load_incluster_config()
                except config.ConfigException:
                    config.load_kube_config()
                self._init_clients()

        except Exception as e:
            raise RuntimeError(f"Failed to initialize Kubernetes client: {e}")

    def _init_with_token(
        self,
        token: str,
        server_url: str,
        ca_cert: Optional[str] = None,
        verify_ssl: bool = True,
    ):
        """Initialize client with token-based authentication."""
        configuration = client.Configuration()
        configuration.host = server_url
        configuration.api_key_prefix["authorization"] = "Bearer"
        configuration.api_key["authorization"] = token

        if ca_cert:
            configuration.ssl_ca_cert = ca_cert
        configuration.verify_ssl = verify_ssl

        self.k8s_client = client.ApiClient(configuration)
        self.core_v1 = client.CoreV1Api(self.k8s_client)
        self.custom_objects_api = client.CustomObjectsApi(self.k8s_client)

    def _init_clients(self):
        """Initialize standard Kubernetes clients."""
        self.k8s_client = client.ApiClient()
        self.core_v1 = client.CoreV1Api()
        self.custom_objects_api = client.CustomObjectsApi()

    def _get_namespace_registry_location(self) -> tuple[str, str]:
        """
        Determine the namespace and name of the registry ConfigMap.
        Returns:
            Tuple of (namespace, configmap_name)
        """
        try:
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

    def connect_to_server(self, server_info: FeastServerInfo) -> "FeatureStore":
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
            from feast.feature_store import FeatureStore

            return FeatureStore(repo_path=temp_dir)

        except Exception as e:
            raise RuntimeError(f"Failed to connect to server {server_info.name}: {e}")


def get_servers(
    namespace_filter: Optional[str] = None,
    kubeconfig_path: Optional[str] = None,
    token: Optional[str] = None,
    server_url: Optional[str] = None,
    ca_cert: Optional[str] = None,
    verify_ssl: bool = True,
) -> List[FeastServerInfo]:
    """
    Discover available Feast servers.

    If authentication parameters are provided, they will be used for this call only.
    If no authentication parameters are provided, the global authentication session
    will be used (if available).

    Args:
        namespace_filter: Optional namespace to filter servers by
        kubeconfig_path: Path to kubeconfig file (alternative to token auth)
        token: Bearer token for authentication (alternative to kubeconfig)
        server_url: Kubernetes API server URL (required with token)
        ca_cert: CA certificate for server verification (optional)
        verify_ssl: Whether to verify SSL certificates (default: True)
    Returns:
        List of available servers
    """
    if token and server_url:
        discovery = FeastServerDiscovery(
            kubeconfig_path=kubeconfig_path,
            token=token,
            server_url=server_url,
            ca_cert=ca_cert,
            verify_ssl=verify_ssl,
        )
        return discovery.get_servers(namespace_filter)

    if kubeconfig_path:
        discovery = FeastServerDiscovery(kubeconfig_path=kubeconfig_path)
        return discovery.get_servers(namespace_filter)

    global _current_auth
    if _current_auth and _current_auth.is_authenticated():
        return _current_auth.get_servers(namespace_filter)

    discovery = FeastServerDiscovery()
    return discovery.get_servers(namespace_filter)


def connect_to_server(
    server_info: FeastServerInfo,
    kubeconfig_path: Optional[str] = None,
    token: Optional[str] = None,
    server_url: Optional[str] = None,
    ca_cert: Optional[str] = None,
    verify_ssl: bool = True,
) -> "FeatureStore":
    """
    Connect to a specific Feast server.

    If authentication parameters are provided, they will be used for this call only.
    If no authentication parameters are provided, the global authentication session
    will be used (if available).

    Args:
        server_info: Server to connect to
        kubeconfig_path: Path to kubeconfig file (alternative to token auth)
        token: Bearer token for authentication (alternative to kubeconfig)
        server_url: Kubernetes API server URL (required with token)
        ca_cert: CA certificate for server verification (optional)
        verify_ssl: Whether to verify SSL certificates (default: True)
    Returns:
        Connected FeatureStore instance
    """
    if token and server_url:
        discovery = FeastServerDiscovery(
            kubeconfig_path=kubeconfig_path,
            token=token,
            server_url=server_url,
            ca_cert=ca_cert,
            verify_ssl=verify_ssl,
        )
        return discovery.connect_to_server(server_info)

    if kubeconfig_path:
        discovery = FeastServerDiscovery(kubeconfig_path=kubeconfig_path)
        return discovery.connect_to_server(server_info)

    global _current_auth
    if _current_auth and _current_auth.is_authenticated():
        return _current_auth.connect_to_server(server_info)

    discovery = FeastServerDiscovery()
    return discovery.connect_to_server(server_info)
