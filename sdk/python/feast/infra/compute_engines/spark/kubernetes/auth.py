import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


def get_k8s_api_client(kubeconfig_path: Optional[str] = None):
    """Resolve Kubernetes API client using kubeconfig or in-cluster service account.

    Args:
        kubeconfig_path: Path to kubeconfig file. If None, uses in-cluster auth.

    Returns:
        kubernetes.client.ApiClient instance.

    Raises:
        FeastSparkClusterError: If authentication fails.
    """
    from kubernetes import client, config

    from feast.errors import FeastSparkClusterError

    try:
        if kubeconfig_path:
            kubeconfig_path = os.path.expanduser(kubeconfig_path)
            if not os.path.isfile(kubeconfig_path):
                raise FeastSparkClusterError(
                    f"kubeconfig file not found: {kubeconfig_path}"
                )
            config.load_kube_config(config_file=kubeconfig_path)
            logger.info(
                "Authenticated with Kubernetes using kubeconfig: %s",
                kubeconfig_path,
            )
        else:
            config.load_incluster_config()
            logger.info(
                "Authenticated with Kubernetes using in-cluster service account"
            )
        return client.ApiClient()
    except Exception as e:
        if isinstance(e, FeastSparkClusterError):
            raise
        raise FeastSparkClusterError(
            f"Failed to authenticate with Kubernetes: {e}"
        ) from e
