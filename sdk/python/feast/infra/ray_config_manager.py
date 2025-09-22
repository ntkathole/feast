"""
Comprehensive Ray Configuration Manager for Feast.

This module provides a unified configuration system for Ray execution modes,
handling all scenarios from local Ray to remote clusters to CodeFlare SDK integration.
"""

import logging
import os
from enum import Enum
from typing import Any, Dict, Optional, Union

logger = logging.getLogger(__name__)


class RayExecutionMode(Enum):
    """Ray execution modes supported by Feast."""

    LOCAL = "local"
    REMOTE = "remote"
    KUBERAY = "kuberay"


class RayConfigManager:
    """
    Manages Ray configuration and execution mode determination.

    Supports three main scenarios:
    1. Local Ray: Only ray is defined
    2. Remote Ray: ray is defined with ray_address, existing ray standalone
    3. KubeRay: ray is defined with KubeRay settings
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

        Returns:
            Dictionary of KubeRay configuration with passthrough settings
        """
        if self._codeflare_config is not None:
            return self._codeflare_config

        # Get passthrough configuration from kuberay_conf first
        kuberay_conf = self._get_config_value("kuberay_conf", {}) or {}

        config = {
            "use_kuberay": (
                os.getenv("FEAST_USE_KUBERAY", "").lower() == "true"
                or self._get_config_value("use_kuberay", False)
            ),
            # Get values from kuberay_conf or environment variables
            "cluster_name": (
                os.getenv("FEAST_RAY_CLUSTER_NAME") or kuberay_conf.get("cluster_name")
            ),
            "namespace": (
                os.getenv("FEAST_RAY_NAMESPACE")
                or kuberay_conf.get("namespace", "default")
            ),
        }

        # Add authentication configuration from kuberay_conf or environment variables
        auth_token = (
            os.getenv("FEAST_RAY_AUTH_TOKEN")
            or os.getenv("RAY_AUTH_TOKEN")
            or kuberay_conf.get("auth_token")
        )
        if auth_token:
            config["auth_token"] = auth_token

        # Add authentication server URL
        auth_server = (
            os.getenv("FEAST_RAY_AUTH_SERVER")
            or os.getenv("RAY_AUTH_SERVER")
            or kuberay_conf.get("auth_server")
        )
        if auth_server:
            config["auth_server"] = auth_server

        # Add skip TLS verification setting
        skip_tls = os.getenv(
            "FEAST_RAY_SKIP_TLS", ""
        ).lower() == "true" or kuberay_conf.get("skip_tls", False)
        config["skip_tls"] = skip_tls

        # Add connection timeout and retry settings
        config["connection_timeout"] = int(
            os.getenv("FEAST_RAY_CONNECTION_TIMEOUT", "60")
        ) or kuberay_conf.get("connection_timeout", 60)
        config["max_retries"] = int(
            os.getenv("FEAST_RAY_MAX_RETRIES", "3")
        ) or kuberay_conf.get("max_retries", 3)
        config["retry_delay"] = int(
            os.getenv("FEAST_RAY_RETRY_DELAY", "5")
        ) or kuberay_conf.get("retry_delay", 5)

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
