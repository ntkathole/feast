# Copyright 2025 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Centralized Ray Initialization Module for Feast.

This module combines configuration management and initialization logic for a
complete, self-contained Ray setup system.
"""

import logging
import os
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import ray
from ray.data.context import DatasetContext

logger = logging.getLogger(__name__)

# Well-known paths for Kubernetes in-cluster service account credentials
_K8S_SA_TOKEN_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/token"
_K8S_SA_NAMESPACE_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/namespace"
_K8S_SA_CA_PATH = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"


def _is_running_in_k8s() -> bool:
    """Check if the current process is running inside a Kubernetes pod."""
    return os.path.exists(_K8S_SA_TOKEN_PATH)


def _read_k8s_sa_token() -> Optional[str]:
    """Read the Kubernetes service account token from the projected volume."""
    try:
        return Path(_K8S_SA_TOKEN_PATH).read_text().strip()
    except (OSError, IOError):
        return None


def _read_k8s_namespace() -> Optional[str]:
    """Read the Kubernetes namespace from the projected volume."""
    try:
        return Path(_K8S_SA_NAMESPACE_PATH).read_text().strip()
    except (OSError, IOError):
        return None


def _detect_k8s_api_server() -> Optional[str]:
    """
    Detect the Kubernetes API server URL from in-cluster environment variables.
    These are automatically set by Kubernetes in every pod.
    """
    k8s_host = os.getenv("KUBERNETES_SERVICE_HOST")
    k8s_port = os.getenv("KUBERNETES_SERVICE_PORT_HTTPS", "443")
    if k8s_host:
        # IPv6 addresses need brackets
        if ":" in k8s_host and not k8s_host.startswith("["):
            k8s_host = f"[{k8s_host}]"
        return f"https://{k8s_host}:{k8s_port}"
    return None


class RayExecutionMode(Enum):
    """Ray execution modes supported by Feast."""

    LOCAL = "local"
    REMOTE = "remote"
    KUBERAY = "kuberay"


class RayConfigManager:
    """
    Manages Ray configuration and execution mode determination.

    Supports three main scenarios:
    1. Local Ray: Single-machine development and testing
    2. Remote Ray: Connect to existing Ray standalone cluster
    3. KubeRay: Ray on Kubernetes with CodeFlare SDK

    The manager determines execution mode based on configuration precedence:
    1. Environment variable FEAST_RAY_EXECUTION_MODE (highest)
    2. KubeRay mode (use_kuberay=True or cluster_name specified)
    3. Remote mode (ray_address specified)
    4. Local mode (default fallback)
    """

    def __init__(self, config: Optional[Union[Dict[str, Any], object]] = None):
        """
        Initialize Ray configuration manager.

        Args:
            config: Ray configuration (RayOfflineStoreConfig, RayComputeEngineConfig, or dict)
        """
        self.config = config or {}
        self._execution_mode: Optional[RayExecutionMode] = None
        self._codeflare_config: Optional[Dict[str, Any]] = None

    def determine_execution_mode(self) -> RayExecutionMode:
        """
        Determine the appropriate Ray execution mode based on configuration.

        Precedence (highest to lowest):
        1. Environment variable FEAST_RAY_EXECUTION_MODE (explicit override)
        2. KubeRay mode (use_kuberay=True or cluster_name specified)
        3. Remote mode (ray_address specified)
        4. Local mode (default fallback)

        Returns:
            RayExecutionMode enum value
        """
        if self._execution_mode is not None:
            return self._execution_mode

        # 1. Check environment variable override first (highest precedence)
        env_mode = os.getenv("FEAST_RAY_EXECUTION_MODE", "").lower()
        if env_mode in ["local", "remote", "kuberay"]:
            self._execution_mode = RayExecutionMode(env_mode)
            logger.info(
                f"Ray execution mode set via FEAST_RAY_EXECUTION_MODE: {env_mode}"
            )
            return self._execution_mode

        # 2. Check for KubeRay configuration (second highest precedence)
        use_kuberay = self._get_config_value("use_kuberay")

        # Check for cluster_name in kuberay_conf
        kuberay_conf = self._get_config_value("kuberay_conf", {}) or {}
        cluster_name = kuberay_conf.get("cluster_name")

        # Environment variables can enable KubeRay
        if os.getenv("FEAST_USE_KUBERAY", "").lower() == "true":
            use_kuberay = True
        if os.getenv("FEAST_RAY_CLUSTER_NAME"):
            cluster_name = os.getenv("FEAST_RAY_CLUSTER_NAME")

        # KubeRay takes precedence over remote/local if configured
        if use_kuberay or cluster_name:
            self._execution_mode = RayExecutionMode.KUBERAY
            reason = []
            if use_kuberay:
                reason.append("use_kuberay=True")
            if cluster_name:
                reason.append(f"cluster_name='{cluster_name}'")
            logger.info(f"Ray execution mode: KubeRay ({', '.join(reason)})")
            return self._execution_mode

        # 3. Check for remote Ray configuration (third precedence)
        ray_address = self._get_config_value("ray_address") or os.getenv("RAY_ADDRESS")

        if ray_address:
            self._execution_mode = RayExecutionMode.REMOTE
            logger.info(f"Ray execution mode: Remote (ray_address='{ray_address}')")
            return self._execution_mode

        # 4. Default to local Ray (lowest precedence - fallback)
        self._execution_mode = RayExecutionMode.LOCAL
        logger.info(
            "Ray execution mode: Local (default - no KubeRay or remote configuration found)"
        )
        return self._execution_mode

    def get_kuberay_config(self) -> Dict[str, Any]:
        """
        Get KubeRay/CodeFlare SDK configuration.

        Configuration is resolved with the following precedence (highest first):
        1. Environment variables (FEAST_RAY_AUTH_TOKEN, FEAST_RAY_AUTH_SERVER, etc.)
        2. kuberay_conf in feature_store.yaml
        3. Automatic in-cluster detection (SA token, K8s API server, namespace)

        When running inside a Kubernetes pod, auth_token and auth_server are
        automatically detected from the pod's service account credentials,
        so users don't need to specify them explicitly.

        Returns:
            Dictionary of KubeRay configuration with passthrough settings
        """
        if self._codeflare_config is not None:
            return self._codeflare_config

        # Get passthrough configuration from kuberay_conf first
        kuberay_conf = self._get_config_value("kuberay_conf", {}) or {}

        # Resolve namespace: env var > yaml > in-cluster detection > raise error
        namespace = os.getenv("FEAST_RAY_NAMESPACE") or kuberay_conf.get("namespace")
        if not namespace:
            # Auto-detect from in-cluster service account
            namespace = _read_k8s_namespace()
            if namespace:
                logger.info(
                    f"Auto-detected Kubernetes namespace from service account: {namespace}"
                )
            else:
                raise ValueError(
                    "namespace is required for KubeRay mode but could not be "
                    "determined. Set it in kuberay_conf.namespace in "
                    "feature_store.yaml, via FEAST_RAY_NAMESPACE environment "
                    "variable, or run from inside a Kubernetes pod (namespace "
                    "is detected automatically from the service account)."
                )

        config = {
            "use_kuberay": (
                os.getenv("FEAST_USE_KUBERAY", "").lower() == "true"
                or self._get_config_value("use_kuberay", False)
            ),
            "cluster_name": (
                os.getenv("FEAST_RAY_CLUSTER_NAME") or kuberay_conf.get("cluster_name")
            ),
            "namespace": namespace,
        }

        # Resolve auth_token: env var > yaml > in-cluster SA token
        auth_token = (
            os.getenv("FEAST_RAY_AUTH_TOKEN")
            or os.getenv("RAY_AUTH_TOKEN")
            or kuberay_conf.get("auth_token")
        )
        if not auth_token and _is_running_in_k8s():
            auth_token = _read_k8s_sa_token()
            if auth_token:
                logger.info("Auto-detected auth token from Kubernetes service account")
        if auth_token:
            config["auth_token"] = auth_token

        # Resolve auth_server: env var > yaml > in-cluster K8s API server
        auth_server = (
            os.getenv("FEAST_RAY_AUTH_SERVER")
            or os.getenv("RAY_AUTH_SERVER")
            or kuberay_conf.get("auth_server")
        )
        if not auth_server and _is_running_in_k8s():
            auth_server = _detect_k8s_api_server()
            if auth_server:
                logger.info(f"Auto-detected Kubernetes API server: {auth_server}")
        if auth_server:
            config["auth_server"] = auth_server

        # Resolve skip_tls: env var > yaml > auto (True for in-cluster with auto-detected server)
        skip_tls_env = os.getenv("FEAST_RAY_SKIP_TLS", "").lower()
        if skip_tls_env:
            skip_tls = skip_tls_env == "true"
        elif "skip_tls" in kuberay_conf:
            skip_tls = kuberay_conf["skip_tls"]
        elif (
            not (
                os.getenv("FEAST_RAY_AUTH_SERVER")
                or os.getenv("RAY_AUTH_SERVER")
                or kuberay_conf.get("auth_server")
            )
            and _is_running_in_k8s()
        ):
            # Auto-detected in-cluster API server: default to skip_tls=True
            # because the in-cluster API uses an internal CA that CodeFlare SDK
            # may not trust by default
            skip_tls = True
            logger.info(
                "Auto-detected in-cluster environment, defaulting skip_tls=True"
            )
        else:
            skip_tls = False
        config["skip_tls"] = skip_tls

        # Add any additional configuration from kuberay_conf
        for key, value in kuberay_conf.items():
            if key not in config:  # Don't override already processed keys
                config[key] = value

        self._codeflare_config = config
        return config

    def _get_config_value(self, key: str, default: Any = None) -> Any:
        """
        Get configuration value from config object or dictionary.

        Args:
            key: Configuration key
            default: Default value if key not found

        Returns:
            Configuration value
        """
        if hasattr(self.config, key):
            return getattr(self.config, key)
        elif isinstance(self.config, dict):
            return self.config.get(key, default)
        else:
            return default


class StandardRayWrapper:
    """Wrapper for Ray Native operations."""

    def read_parquet(self, path: Union[str, List[str]], **kwargs) -> Any:
        """Read parquet files using standard Ray."""
        return ray.data.read_parquet(path, **kwargs)

    def read_csv(self, path: Union[str, List[str]], **kwargs) -> Any:
        """Read CSV files using standard Ray."""
        return ray.data.read_csv(path, **kwargs)

    def from_pandas(self, df: Any) -> Any:
        """Create dataset from pandas DataFrame using standard Ray."""
        return ray.data.from_pandas(df)

    def from_arrow(self, table: Any) -> Any:
        """Create dataset from Arrow table using standard Ray."""
        return ray.data.from_arrow(table)


class CodeFlareRayWrapper:
    """Wrapper for Ray operations on KubeRay clusters using CodeFlare SDK."""

    def __init__(
        self,
        cluster_name: str,
        namespace: str,
        auth_token: Optional[str] = None,
        auth_server: Optional[str] = None,
        skip_tls: bool = False,
        enable_logging: bool = False,
    ):
        """
        Initialize CodeFlare Ray wrapper with cluster connection parameters.

        When running inside a Kubernetes pod, auth_token and auth_server are
        automatically resolved from the pod's service account if not provided.

        Args:
            cluster_name: Name of the KubeRay RayCluster resource
            namespace: Kubernetes namespace where the RayCluster is deployed
            auth_token: Authentication token (auto-detected from SA if not provided)
            auth_server: Kubernetes API server URL (auto-detected if not provided)
            skip_tls: Whether to skip TLS verification for K8s API
            enable_logging: Whether to enable verbose Ray logging
        """
        self.cluster_name = cluster_name
        self.namespace = namespace
        self.skip_tls = skip_tls
        self.enable_logging = enable_logging
        self.cluster = None

        # Auto-detect auth_token from in-cluster service account if not provided
        self.auth_token = auth_token
        if not self.auth_token and _is_running_in_k8s():
            self.auth_token = _read_k8s_sa_token()
            logger.info("Using auto-detected Kubernetes service account token")
        if not self.auth_token:
            raise ValueError(
                "auth_token is required for KubeRay mode. Either provide it in "
                "kuberay_conf, set FEAST_RAY_AUTH_TOKEN environment variable, or "
                "run from inside a Kubernetes pod (service account token is used "
                "automatically)."
            )

        # Auto-detect auth_server from in-cluster environment if not provided
        self.auth_server = auth_server
        if not self.auth_server and _is_running_in_k8s():
            self.auth_server = _detect_k8s_api_server()
            logger.info(
                f"Using auto-detected Kubernetes API server: {self.auth_server}"
            )
        if not self.auth_server:
            raise ValueError(
                "auth_server is required for KubeRay mode. Either provide it in "
                "kuberay_conf, set FEAST_RAY_AUTH_SERVER environment variable, or "
                "run from inside a Kubernetes pod (API server is detected "
                "automatically)."
            )

        # Authenticate and setup Ray connection
        self._authenticate_codeflare()
        self._setup_ray_connection()

    def _authenticate_codeflare(self):
        """Authenticate with CodeFlare SDK."""
        try:
            from codeflare_sdk import TokenAuthentication

            auth = TokenAuthentication(
                token=self.auth_token,
                server=self.auth_server,
                skip_tls=self.skip_tls,
            )
            auth.login()
        except Exception as e:
            logger.error(f"CodeFlare authentication failed: {e}")
            raise

    def _setup_ray_connection(self):
        """Setup Ray connection to KubeRay cluster using TLS certificates."""
        try:
            from codeflare_sdk import generate_cert, get_cluster

            self.cluster = get_cluster(
                cluster_name=self.cluster_name, namespace=self.namespace
            )
            if self.cluster is None:
                raise RuntimeError(
                    f"Failed to find KubeRay cluster '{self.cluster_name}' in namespace '{self.namespace}'"
                )
            generate_cert.generate_tls_cert(self.cluster_name, self.namespace)
            generate_cert.export_env(self.cluster_name, self.namespace)

            cluster_uri = self.cluster.cluster_uri()
            runtime_env = {
                "pip": ["feast"],
                "env_vars": {"RAY_DISABLE_IMPORT_WARNING": "1"},
            }

            ray.shutdown()

            logging_level = "INFO" if self.enable_logging else "ERROR"

            ray.init(
                address=cluster_uri,
                ignore_reinit_error=True,
                logging_level=logging_level,
                log_to_driver=self.enable_logging,
                runtime_env=runtime_env,
            )

            logger.info(f"Ray connected successfully to cluster: {self.cluster_name}")

        except Exception as e:
            logger.error(f"Ray connection failed: {e}")
            raise

    # Ray Data API methods - wrapped in @ray.remote to execute on cluster workers
    def read_parquet(self, path: Union[str, List[str]], **kwargs) -> Any:
        """Read parquet files - runs remotely on KubeRay cluster workers."""
        from feast.infra.ray_shared_utils import RemoteDatasetProxy

        @ray.remote
        def _remote_read_parquet(file_path, read_kwargs):
            import ray

            return ray.data.read_parquet(file_path, **read_kwargs)

        return RemoteDatasetProxy(_remote_read_parquet.remote(path, kwargs))

    def read_csv(self, path: Union[str, List[str]], **kwargs) -> Any:
        """Read CSV files - runs remotely on KubeRay cluster workers."""
        from feast.infra.ray_shared_utils import RemoteDatasetProxy

        @ray.remote
        def _remote_read_csv(file_path, read_kwargs):
            import ray

            return ray.data.read_csv(file_path, **read_kwargs)

        return RemoteDatasetProxy(_remote_read_csv.remote(path, kwargs))

    def from_pandas(self, df: Any) -> Any:
        """Create dataset from pandas DataFrame - runs remotely on KubeRay cluster workers."""
        from feast.infra.ray_shared_utils import RemoteDatasetProxy

        @ray.remote
        def _remote_from_pandas(dataframe):
            import ray

            return ray.data.from_pandas(dataframe)

        return RemoteDatasetProxy(_remote_from_pandas.remote(df))

    def from_arrow(self, table: Any) -> Any:
        """Create dataset from Arrow table - runs remotely on KubeRay cluster workers."""
        from feast.infra.ray_shared_utils import RemoteDatasetProxy

        @ray.remote
        def _remote_from_arrow(arrow_table):
            import ray

            return ray.data.from_arrow(arrow_table)

        return RemoteDatasetProxy(_remote_from_arrow.remote(table))


# Global state tracking
_ray_initialized = False
_ray_wrapper: Optional[Union[StandardRayWrapper, CodeFlareRayWrapper]] = None


def _suppress_ray_logging() -> None:
    """Suppress Ray and Ray Data logging completely."""
    import warnings

    # Suppress Ray warnings
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="ray")
    warnings.filterwarnings("ignore", category=UserWarning, module="ray")

    # Set environment variables to suppress Ray output
    os.environ["RAY_DISABLE_IMPORT_WARNING"] = "1"
    os.environ["RAY_SUPPRESS_UNVERIFIED_TLS_WARNING"] = "1"
    os.environ["RAY_LOG_LEVEL"] = "ERROR"
    os.environ["RAY_DATA_LOG_LEVEL"] = "ERROR"
    os.environ["RAY_DISABLE_PROGRESS_BARS"] = "1"

    # Suppress all Ray-related loggers
    ray_loggers = [
        "ray",
        "ray.data",
        "ray.data.dataset",
        "ray.data.context",
        "ray.data._internal.streaming_executor",
        "ray.data._internal.execution",
        "ray.data._internal",
        "ray.tune",
        "ray.serve",
        "ray.util",
        "ray._private",
    ]
    for logger_name in ray_loggers:
        logging.getLogger(logger_name).setLevel(logging.ERROR)

    # Configure DatasetContext to disable progress bars
    try:
        ctx = DatasetContext.get_current()
        ctx.enable_progress_bars = False
        if hasattr(ctx, "verbose_progress"):
            ctx.verbose_progress = False
    except Exception:
        pass  # Ignore if Ray Data is not available


def _initialize_local_ray(config: Any, enable_logging: bool = False) -> None:
    """
    Initialize Ray in local mode.

    Args:
        config: Configuration object (RayOfflineStoreConfig or RayComputeEngineConfig)
        enable_logging: Whether to enable Ray logging
    """
    logger.info("Initializing Ray in LOCAL mode")

    ray_init_kwargs: Dict[str, Any] = {
        "ignore_reinit_error": True,
        "include_dashboard": False,
    }

    if enable_logging:
        ray_init_kwargs.update(
            {
                "log_to_driver": True,
                "logging_level": "INFO",
            }
        )
    else:
        ray_init_kwargs.update(
            {
                "log_to_driver": False,
                "logging_level": "ERROR",
            }
        )
        _suppress_ray_logging()

    # Add local configuration
    ray_init_kwargs.update(
        {
            "_node_ip_address": os.getenv("RAY_NODE_IP", "127.0.0.1"),
            "num_cpus": os.cpu_count() or 4,
        }
    )

    # Merge with user-provided ray_conf if available
    if hasattr(config, "ray_conf") and config.ray_conf:
        ray_init_kwargs.update(config.ray_conf)

    # Initialize Ray
    ray.init(**ray_init_kwargs)

    # Configure DatasetContext
    ctx = DatasetContext.get_current()
    ctx.shuffle_strategy = "sort"  # type: ignore
    ctx.enable_tensor_extension_casting = False

    # Log cluster info
    if enable_logging:
        cluster_resources = ray.cluster_resources()
        logger.info(
            f"Ray local cluster initialized with {cluster_resources.get('CPU', 0)} CPUs, "
            f"{cluster_resources.get('memory', 0) / (1024**3):.1f}GB memory"
        )


def _initialize_remote_ray(config: Any, enable_logging: bool = False) -> None:
    """
    Initialize Ray in remote mode (connect to existing Ray cluster).

    Args:
        config: Configuration object with ray_address
        enable_logging: Whether to enable Ray logging
    """
    ray_address = getattr(config, "ray_address", None)
    if not ray_address:
        ray_address = os.getenv("RAY_ADDRESS")

    if not ray_address:
        raise ValueError("ray_address must be specified for remote Ray mode")

    logger.info(f"Initializing Ray in REMOTE mode, connecting to: {ray_address}")

    ray_init_kwargs: Dict[str, Any] = {
        "address": ray_address,
        "ignore_reinit_error": True,
        "include_dashboard": False,
    }

    if enable_logging:
        ray_init_kwargs.update(
            {
                "log_to_driver": True,
                "logging_level": "INFO",
            }
        )
    else:
        ray_init_kwargs.update(
            {
                "log_to_driver": False,
                "logging_level": "ERROR",
            }
        )
        _suppress_ray_logging()

    # Merge with user-provided ray_conf if available
    if hasattr(config, "ray_conf") and config.ray_conf:
        ray_init_kwargs.update(config.ray_conf)

    # Initialize Ray
    ray.init(**ray_init_kwargs)

    # Configure DatasetContext
    ctx = DatasetContext.get_current()
    ctx.shuffle_strategy = "sort"  # type: ignore
    ctx.enable_tensor_extension_casting = False

    # Log cluster info
    if enable_logging:
        cluster_resources = ray.cluster_resources()
        logger.info(
            f"Ray remote cluster initialized with {cluster_resources.get('CPU', 0)} CPUs, "
            f"{cluster_resources.get('memory', 0) / (1024**3):.1f}GB memory"
        )


def _initialize_kuberay(config: Any, enable_logging: bool = False) -> None:
    """
    Initialize Ray in KubeRay mode using CodeFlare SDK.

    Args:
        config: Configuration object with KubeRay settings
        enable_logging: Whether to enable Ray logging
    """
    global _ray_wrapper

    logger.info("Initializing Ray in KUBERAY mode using CodeFlare SDK")

    if not enable_logging:
        _suppress_ray_logging()

    # Get KubeRay configuration
    config_manager = RayConfigManager(config)
    kuberay_config = config_manager.get_kuberay_config()

    # Validate that cluster_name is provided
    if not kuberay_config.get("cluster_name"):
        raise ValueError(
            "cluster_name is required for KubeRay mode. Set it in "
            "kuberay_conf.cluster_name in feature_store.yaml or via "
            "FEAST_RAY_CLUSTER_NAME environment variable."
        )

    # Initialize CodeFlare Ray wrapper - auth_token and auth_server will be
    # auto-detected from in-cluster SA if not explicitly provided
    _ray_wrapper = CodeFlareRayWrapper(
        cluster_name=kuberay_config["cluster_name"],
        namespace=kuberay_config["namespace"],
        auth_token=kuberay_config.get("auth_token"),
        auth_server=kuberay_config.get("auth_server"),
        skip_tls=kuberay_config.get("skip_tls", False),
        enable_logging=enable_logging,
    )

    logger.info("KubeRay cluster connection established via CodeFlare SDK")


def ensure_ray_initialized(
    config: Optional[Any] = None, force_reinit: bool = False
) -> None:
    """
    Ensure Ray is initialized with appropriate configuration.

    This is the main entry point for Ray initialization across all Feast components.
    It automatically detects the execution mode and initializes Ray accordingly.

    Args:
        config: Configuration object (RayOfflineStoreConfig, RayComputeEngineConfig, or RepoConfig)
        force_reinit: If True, reinitialize Ray even if already initialized

    Raises:
        ValueError: If configuration is invalid or required parameters are missing
    """
    global _ray_initialized

    # Check if already initialized
    if _ray_initialized and not force_reinit:
        logger.debug("Ray already initialized, skipping initialization")
        return

    # Extract Ray-specific config if RepoConfig is provided
    ray_config = config
    if config and hasattr(config, "offline_store"):
        ray_config = config.offline_store
    elif config and hasattr(config, "batch_engine"):
        ray_config = config.batch_engine

    # Determine enable_logging setting
    enable_logging = (
        getattr(ray_config, "enable_ray_logging", False) if ray_config else False
    )

    # Use RayConfigManager to determine execution mode
    config_manager = RayConfigManager(ray_config)
    execution_mode = config_manager.determine_execution_mode()

    logger.info(f"Ray execution mode detected: {execution_mode.value}")

    # Check if Ray is already initialized (from external source)
    if ray.is_initialized() and not force_reinit:
        logger.info("Ray is already initialized externally, using existing cluster")
        # Configure DatasetContext even if Ray is already initialized
        ctx = DatasetContext.get_current()
        ctx.shuffle_strategy = "sort"  # type: ignore
        ctx.enable_tensor_extension_casting = False
        if not enable_logging:
            _suppress_ray_logging()
        _ray_initialized = True
        return

    # Initialize based on execution mode
    try:
        if execution_mode == RayExecutionMode.KUBERAY:
            _initialize_kuberay(ray_config, enable_logging)
        elif execution_mode == RayExecutionMode.REMOTE:
            _initialize_remote_ray(ray_config, enable_logging)
        else:  # LOCAL
            _initialize_local_ray(ray_config, enable_logging)

        _ray_initialized = True
        logger.info(f"Ray initialized successfully in {execution_mode.value} mode")

    except Exception as e:
        logger.error(f"Failed to initialize Ray in {execution_mode.value} mode: {e}")
        raise


def get_ray_wrapper() -> Union[StandardRayWrapper, CodeFlareRayWrapper]:
    """
    Get the appropriate Ray wrapper based on current initialization mode.

    Returns:
        StandardRayWrapper for local/remote modes, CodeFlareRayWrapper for KubeRay mode
    """
    global _ray_wrapper

    if _ray_wrapper is None:
        # Return a standard Ray wrapper for local/remote modes
        _ray_wrapper = StandardRayWrapper()

    return _ray_wrapper


def is_ray_initialized() -> bool:
    """Check if Ray has been initialized via this module."""
    return _ray_initialized


def shutdown_ray() -> None:
    """Shutdown Ray and reset initialization state."""
    global _ray_initialized, _ray_wrapper

    if ray.is_initialized():
        logger.info("Shutting down Ray")
        ray.shutdown()

    _ray_initialized = False
    _ray_wrapper = None
    logger.info("Ray shutdown complete")
