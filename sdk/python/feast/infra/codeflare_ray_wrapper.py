"""
Efficient Ray Data wrapper for Feast operations.

This module provides a unified interface for Ray Data operations that can work
with local Ray clusters and KubeRay clusters via CodeFlare SDK. The key design
principle is efficiency: Ray connection is established once during initialization,
and all subsequent operations use ray.data directly without repeated initialization.
"""

import logging
import time
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import pyarrow as pa
import ray
import ray.data

from feast.infra.ray_config_manager import RayConfigManager, RayExecutionMode

logger = logging.getLogger(__name__)

try:
    from codeflare_sdk import Cluster, TokenAuthentication, get_cluster

    CODEFLARE_AVAILABLE = True
except ImportError:
    CODEFLARE_AVAILABLE = False
    logger.warning("CodeFlare SDK not available. KubeRay functionality disabled.")


class CodeFlareRayWrapper:
    """
    Efficient wrapper for Ray Data operations.

    This wrapper establishes the Ray connection once during initialization and then
    uses ray.data operations directly. For KubeRay mode, it connects to the cluster
    via CodeFlare SDK during __init__, and all subsequent operations automatically
    use the cluster connection without repeated ray.init() calls.
    """

    def __init__(self, config: Optional[Union[Dict[str, Any], object]] = None):
        """
        Initialize the Ray wrapper.

        Args:
            config: Ray configuration object (RayOfflineStoreConfig, RayComputeEngineConfig, or dict)
        """
        self.config_manager = RayConfigManager(config or {})
        self.execution_mode = self.config_manager.determine_execution_mode()
        self.use_kuberay = self.execution_mode == RayExecutionMode.KUBERAY

        # Get configuration from manager for KubeRay connections
        kuberay_config = self.config_manager.get_kuberay_config()
        self.cluster_name = kuberay_config.get("cluster_name")
        self.namespace = kuberay_config.get("namespace", "default")
        self.auth_token = kuberay_config.get("auth_token")
        self.auth_server = kuberay_config.get("auth_server")
        self.skip_tls = kuberay_config.get("skip_tls", False)

        # Connection timeout and retry settings
        self.connection_timeout = kuberay_config.get(
            "connection_timeout", 60
        )  # seconds
        self.max_retries = kuberay_config.get("max_retries", 3)
        self.retry_delay = kuberay_config.get("retry_delay", 5)  # seconds

        self.cluster = None
        self._ray_initialized = False
        self._authenticated = False  # Track authentication status

        logger.info(
            f"Ray wrapper initialized in {'KubeRay' if self.use_kuberay else 'client-side'} mode"
        )

        # Log configuration details for debugging
        if self.use_kuberay:
            logger.info("KubeRay configuration:")
            logger.info(f"  - Cluster name: {self.cluster_name}")
            logger.info(f"  - Namespace: {self.namespace}")
            logger.info(f"  - Connection timeout: {self.connection_timeout}s")
            logger.info(f"  - Max retries: {self.max_retries}")
            logger.info(f"  - Retry delay: {self.retry_delay}s")
            logger.info(f"  - Auth configured: {'Yes' if self.auth_token else 'No'}")
            logger.info(f"  - Skip TLS: {self.skip_tls}")

            # Log Ray version for debugging compatibility issues
            try:
                logger.info(f"  - Ray version: {ray.__version__}")
            except Exception:
                logger.debug("Could not determine Ray version")

        # Initialize Ray connection once if using KubeRay
        if self.use_kuberay:
            self._ensure_ray_connection()

    def _get_cluster(self) -> Optional[Cluster]:
        """Get or connect to existing cluster."""
        if not self.use_kuberay:
            return None

        if self.cluster is None:
            try:
                auth_result = self._authenticate_codeflare()
                if auth_result is False:  # Explicitly failed authentication
                    logger.warning("Authentication failed, cannot get cluster")
                    return None

                # Now try to get existing cluster after authentication
                self.cluster = get_cluster(
                    cluster_name=self.cluster_name, namespace=self.namespace
                )
                logger.info(
                    f"✓ Connected to existing KubeRay cluster: {self.cluster_name}"
                )

            except Exception as e:
                logger.warning(f"Could not connect to existing cluster: {e}")
                return None

        return self.cluster

    def _authenticate_codeflare(self) -> Optional[bool]:
        """
        Authenticate with CodeFlare SDK using token authentication.
        Returns:
            Optional[bool]: True if authentication is successful, False if failed, None if no auth config
        """
        if not self.use_kuberay or not CODEFLARE_AVAILABLE:
            return True  # No authentication needed for non-KubeRay mode

        # Check if already authenticated
        if self._authenticated:
            logger.debug("CodeFlare SDK already authenticated")
            return True

        if not self.auth_token:
            logger.info(
                "No authentication token provided for KubeRay mode - attempting without auth"
            )
            return None

        if not self.auth_server:
            logger.info(
                "No authentication server provided for KubeRay mode - attempting without auth"
            )
            return None

        try:
            logger.info("Authenticating with CodeFlare SDK using token authentication")
            logger.info(f"Auth server: {self.auth_server}")
            logger.info(f"Skip TLS: {self.skip_tls}")

            auth = TokenAuthentication(
                token=self.auth_token, server=self.auth_server, skip_tls=self.skip_tls
            )

            auth.login()
            logger.info("✓ CodeFlare SDK authentication successful")
            self._authenticated = True  # Mark as authenticated
            return True

        except Exception as e:
            logger.error(f"✗ CodeFlare SDK authentication failed: {e}")
            return False

    def _ensure_ray_connection(self) -> bool:
        """
        Ensure Ray is connected to the appropriate cluster.
        This is called once during initialization for KubeRay mode.

        Returns:
            bool: True if connection is successful, False otherwise
        """
        if self._ray_initialized:
            return True

        if not self.use_kuberay:
            # For client-side mode, let Ray handle initialization
            self._ray_initialized = True
            return True

        logger.info(f"Attempting to connect to KubeRay cluster: {self.cluster_name}")

        # Attempt connection with retry logic
        for attempt in range(self.max_retries):
            try:
                cluster = self._get_cluster()
                if cluster:
                    cluster_uri = cluster.cluster_uri()
                    logger.info(
                        f"Connecting to KubeRay cluster at: {cluster_uri} (attempt {attempt + 1}/{self.max_retries})"
                    )

                    # Test cluster connectivity before Ray connection
                    if not self._test_cluster_connectivity(cluster_uri):
                        if attempt < self.max_retries - 1:
                            logger.warning(
                                f"Cluster connectivity test failed, retrying in {self.retry_delay} seconds..."
                            )
                            time.sleep(self.retry_delay)
                            continue
                        else:
                            logger.warning(
                                "All connectivity tests failed, falling back to local Ray cluster"
                            )
                            self._ray_initialized = True
                            return False

                    # Prepare Ray init kwargs with timeout settings
                    ray_kwargs = {
                        "address": cluster_uri,
                        "ignore_reinit_error": True,
                        "log_to_driver": True,
                    }

                    # Add authentication token if available
                    if self.auth_token:
                        logger.info("Using authentication token for Ray connection")
                        ray_kwargs["_redis_password"] = self.auth_token

                    # Initialize Ray with timeout
                    success = self._init_ray_with_timeout(ray_kwargs)
                    if success:
                        logger.info(
                            f"✓ Successfully connected to KubeRay cluster: {self.cluster_name}"
                        )
                        self._ray_initialized = True
                        return True
                    else:
                        if attempt < self.max_retries - 1:
                            logger.warning(
                                f"Ray connection failed, retrying in {self.retry_delay} seconds..."
                            )
                            time.sleep(self.retry_delay)
                            continue
                        else:
                            logger.warning(
                                "All Ray connection attempts failed, falling back to local Ray cluster"
                            )
                            self._ray_initialized = True
                            return False
                else:
                    logger.warning(
                        f"✗ KubeRay cluster '{self.cluster_name}' not found in namespace '{self.namespace}'"
                    )
                    logger.warning("Falling back to local Ray cluster")
                    self._ray_initialized = True
                    return False
            except Exception as e:
                if attempt < self.max_retries - 1:
                    logger.warning(
                        f"Failed to connect to KubeRay cluster '{self.cluster_name}' (attempt {attempt + 1}): {e}"
                    )
                    logger.warning(f"Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    logger.warning(
                        f"✗ All attempts failed to connect to KubeRay cluster '{self.cluster_name}': {e}"
                    )
                    logger.warning("Falling back to local Ray cluster")
                    self._ray_initialized = True
                    return False

        return False

    def from_pandas(self, df: pd.DataFrame) -> Any:
        """Create Ray Dataset from pandas DataFrame."""
        return ray.data.from_pandas(df)

    def from_arrow(self, table: pa.Table) -> Any:
        """Create Ray Dataset from PyArrow Table."""
        return ray.data.from_arrow(table)

    def read_parquet(self, path: Union[str, List[str]]) -> Any:
        """Read parquet files into Ray Dataset."""
        return ray.data.read_parquet(path)

    def read_csv(self, path: Union[str, List[str]]) -> Any:
        """Read CSV files into Ray Dataset."""
        return ray.data.read_csv(path)

    def to_pandas(self, dataset: Any) -> pd.DataFrame:
        """Convert Ray Dataset to pandas DataFrame."""
        return dataset.to_pandas()

    def to_arrow(self, dataset: Any) -> pa.Table:
        """Convert Ray Dataset to PyArrow Table."""
        if hasattr(dataset, "to_arrow"):
            return dataset.to_arrow()
        else:
            df = dataset.to_pandas()
            return pa.Table.from_pandas(df)

    def _test_cluster_connectivity(self, cluster_uri: str) -> bool:
        """
        Test cluster connectivity before attempting Ray connection.
        Args:
            cluster_uri: The cluster URI to test
        Returns:
            bool: True if cluster is reachable, False otherwise
        """
        try:
            import socket
            import urllib.parse

            # Parse the cluster URI to get host and port
            parsed = urllib.parse.urlparse(cluster_uri)
            if not parsed.hostname or not parsed.port:
                logger.warning(f"Invalid cluster URI format: {cluster_uri}")
                return False

            # Test socket connection with timeout
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)  # 10 second timeout for connectivity test

            try:
                result = sock.connect_ex((parsed.hostname, parsed.port))
                return result == 0
            finally:
                sock.close()

        except Exception as e:
            logger.debug(f"Cluster connectivity test failed: {e}")
            return False

    def _init_ray_with_timeout(self, ray_kwargs: Dict[str, Any]) -> bool:
        """
        Initialize Ray with timeout handling.
        Args:
            ray_kwargs: Ray initialization arguments
        Returns:
            bool: True if initialization successful, False otherwise
        """
        import threading

        success = False
        exception = None

        def init_ray():
            nonlocal success, exception
            try:
                # Clean up any existing Ray instance first
                if ray.is_initialized():
                    ray.shutdown()

                ray.init(**ray_kwargs)
                success = True
            except Exception as e:
                exception = e

        # Start Ray initialization in a separate thread
        init_thread = threading.Thread(target=init_ray)
        init_thread.daemon = True
        init_thread.start()

        # Wait for the thread to complete or timeout
        init_thread.join(timeout=self.connection_timeout)

        if init_thread.is_alive():
            logger.warning(
                f"Ray initialization timed out after {self.connection_timeout} seconds"
            )
            # Force cleanup of any partial Ray state
            try:
                if ray.is_initialized():
                    ray.shutdown()
            except Exception as e:
                logger.warning(f"Error shutting down Ray: {e}")
            return False

        if exception:
            # Check if it's a version compatibility issue
            if "unexpected kwargs" in str(exception):
                logger.warning(f"Ray version compatibility issue: {exception}")
                logger.info("Attempting connection with basic parameters...")

                # Try with minimal parameters for better compatibility
                basic_kwargs = {
                    "address": ray_kwargs["address"],
                    "ignore_reinit_error": True,
                }

                # Add authentication if it was in the original request and seems safe
                if "_redis_password" in ray_kwargs and "redis_password" not in str(
                    exception
                ):
                    basic_kwargs["_redis_password"] = ray_kwargs["_redis_password"]

                try:
                    if ray.is_initialized():
                        ray.shutdown()
                    ray.init(**basic_kwargs)
                    logger.info("✓ Ray connection successful with basic parameters")
                    return True
                except Exception as basic_exception:
                    logger.warning(
                        f"Basic Ray connection also failed: {basic_exception}"
                    )

                    # Try one more time without any authentication
                    if "_redis_password" in basic_kwargs:
                        logger.info("Trying connection without authentication...")
                        minimal_kwargs = {
                            "address": ray_kwargs["address"],
                            "ignore_reinit_error": True,
                        }
                        try:
                            if ray.is_initialized():
                                ray.shutdown()
                            ray.init(**minimal_kwargs)
                            logger.info(
                                "✓ Ray connection successful without authentication"
                            )
                            return True
                        except Exception as minimal_exception:
                            logger.warning(
                                f"Minimal Ray connection also failed: {minimal_exception}"
                            )

                    return False
            else:
                logger.warning(f"Ray initialization failed: {exception}")
                return False

        return success

    def cleanup(self):
        """Clean up resources."""
        if self.cluster:
            try:
                self.cluster.down()
                logger.info("Cluster connection closed")
            except Exception as e:
                logger.warning(f"Error closing cluster connection: {e}")


# Global instance for easy access
_ray_wrapper = None


def get_ray_wrapper() -> CodeFlareRayWrapper:
    """
    Get the global CodeFlare Ray wrapper instance.
    This wrapper should be initialized during Ray offline store or compute engine
    initialization using initialize_ray_wrapper().

    Returns:
        CodeFlareRayWrapper instance

    Raises:
        RuntimeError: If wrapper hasn't been initialized
    """
    global _ray_wrapper

    if _ray_wrapper is None:
        # Fallback to default configuration if not initialized
        logger.warning("Ray wrapper not initialized, using default configuration")
        _ray_wrapper = CodeFlareRayWrapper()

    return _ray_wrapper


def initialize_ray_wrapper_from_config(config: Any) -> CodeFlareRayWrapper:
    """
    Initialize the global CodeFlare Ray wrapper from Ray store/engine config.

    Args:
        config: RayOfflineStoreConfig or RayComputeEngineConfig instance

    Returns:
        CodeFlareRayWrapper instance
    """
    global _ray_wrapper

    # Use the new configuration manager approach
    _ray_wrapper = CodeFlareRayWrapper(config=config)

    return _ray_wrapper
