import logging
from typing import TYPE_CHECKING

from pyspark import SparkConf
from pyspark.sql import SparkSession

if TYPE_CHECKING:
    from feast.infra.compute_engines.spark.compute import SparkComputeEngineConfig

logger = logging.getLogger(__name__)


class SparkKubernetesConnector:
    """Manages connection to external Spark clusters on Kubernetes."""

    def validate_connectivity(self, config: "SparkComputeEngineConfig") -> bool:
        """Test cluster reachability via the Kubernetes API.

        Raises:
            FeastSparkClusterError: If the cluster is unreachable or auth fails.
        """
        from feast.errors import FeastSparkClusterError
        from feast.infra.compute_engines.spark.kubernetes.auth import (
            get_k8s_api_client,
        )

        cluster_address = config.master_url or "unknown"
        logger.info(
            "Validating Spark Kubernetes cluster connectivity",
            extra={
                "cluster_address": cluster_address,
                "namespace": config.namespace,
                "auth_method": "kubeconfig" if config.kubeconfig_path else "in-cluster",
            },
        )

        try:
            api_client = get_k8s_api_client(config.kubeconfig_path)
            from kubernetes import client as k8s_client

            v1 = k8s_client.CoreV1Api(api_client)
            v1.list_namespaced_pod(
                namespace=config.namespace,
                limit=1,
            )

            logger.info(
                "Spark Kubernetes cluster connectivity validated",
                extra={
                    "cluster_address": cluster_address,
                    "namespace": config.namespace,
                },
            )

            self._update_connectivity_metric(cluster_address, connected=True)
            return True

        except FeastSparkClusterError:
            self._update_connectivity_metric(cluster_address, connected=False)
            raise
        except Exception as e:
            self._update_connectivity_metric(cluster_address, connected=False)
            raise FeastSparkClusterError(
                f"Cannot connect to Spark cluster at {cluster_address}: {e}"
            ) from e

    def create_spark_session(self, config: "SparkComputeEngineConfig") -> SparkSession:
        """Create a SparkSession connected to an external Kubernetes Spark cluster.

        Maps compute engine config fields to spark.kubernetes.* configuration keys.
        """
        spark_conf_dict = {}

        # Core cluster settings
        spark_conf_dict["spark.master"] = config.master_url
        spark_conf_dict["spark.kubernetes.namespace"] = config.namespace

        # Container images
        image = config.image
        if image:
            spark_conf_dict["spark.kubernetes.container.image"] = image
        if config.driver_image:
            spark_conf_dict["spark.kubernetes.driver.container.image"] = config.driver_image
        if config.executor_image:
            spark_conf_dict["spark.kubernetes.executor.container.image"] = config.executor_image

        # Image pull secrets
        if config.image_pull_secrets:
            spark_conf_dict["spark.kubernetes.container.image.pullSecrets"] = ",".join(
                config.image_pull_secrets
            )

        # Service account
        if config.service_account_name:
            spark_conf_dict["spark.kubernetes.authenticate.driver.serviceAccountName"] = (
                config.service_account_name
            )
            spark_conf_dict["spark.kubernetes.authenticate.executor.serviceAccountName"] = (
                config.service_account_name
            )

        # Kubeconfig auth
        if config.kubeconfig_path:
            import os

            expanded = os.path.expanduser(config.kubeconfig_path)
            spark_conf_dict["spark.kubernetes.authenticate.driver.oauthTokenFile"] = expanded

        # Resource allocation
        spark_conf_dict["spark.executor.instances"] = str(config.executor_instances)
        spark_conf_dict["spark.executor.memory"] = config.executor_memory
        spark_conf_dict["spark.executor.cores"] = str(config.executor_cores)
        spark_conf_dict["spark.driver.memory"] = config.driver_memory
        spark_conf_dict["spark.driver.cores"] = str(config.driver_cores)

        # Deploy mode
        spark_conf_dict["spark.submit.deployMode"] = "client"

        # Mount secrets as volumes
        for i, secret in enumerate(config.secrets):
            prefix = f"spark.kubernetes.driver.secrets.{secret.name}"
            spark_conf_dict[prefix] = secret.mount_path
            exec_prefix = f"spark.kubernetes.executor.secrets.{secret.name}"
            spark_conf_dict[exec_prefix] = secret.mount_path

        # User-provided spark_conf overlay (takes precedence)
        if config.spark_conf:
            spark_conf_dict.update(config.spark_conf)

        logger.info(
            "Creating Spark Kubernetes session",
            extra={
                "cluster_address": config.master_url,
                "namespace": config.namespace,
                "executor_instances": config.executor_instances,
                "image": config.image,
            },
        )

        spark_builder = SparkSession.builder.appName("feast-spark-k8s")
        spark_builder = spark_builder.config(
            conf=SparkConf().setAll(list(spark_conf_dict.items()))
        )
        session = spark_builder.getOrCreate()
        session.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        return session

    def _update_connectivity_metric(self, cluster_address: str, connected: bool):
        """Update the Prometheus connectivity gauge."""
        try:
            from feast.infra.compute_engines.spark.kubernetes.metrics import (
                SPARK_K8S_CLUSTER_CONNECTIVITY,
            )

            SPARK_K8S_CLUSTER_CONNECTIVITY.labels(
                cluster_address=cluster_address,
            ).set(1 if connected else 0)
        except Exception:
            logger.debug("Failed to update connectivity metric", exc_info=True)
