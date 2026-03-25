import os
import tempfile

import pytest
from pydantic import ValidationError

from feast.infra.compute_engines.spark.kubernetes.config import (
    ConfigMapRef,
    SecretRef,
)
from feast.infra.compute_engines.spark.compute import SparkComputeEngineConfig


def test_secret_ref_valid():
    ref = SecretRef(name="my-secret", mount_path="/mnt/secrets")
    assert ref.name == "my-secret"
    assert ref.mount_path == "/mnt/secrets"


def test_secret_ref_requires_name():
    with pytest.raises(ValidationError):
        SecretRef(mount_path="/mnt/secrets")


def test_secret_ref_requires_mount_path():
    with pytest.raises(ValidationError):
        SecretRef(name="my-secret")


def test_config_map_ref_valid():
    ref = ConfigMapRef(name="my-config", mount_path="/mnt/config")
    assert ref.name == "my-config"
    assert ref.mount_path == "/mnt/config"


def test_config_map_ref_requires_name():
    with pytest.raises(ValidationError):
        ConfigMapRef(mount_path="/mnt/config")


def test_config_map_ref_requires_mount_path():
    with pytest.raises(ValidationError):
        ConfigMapRef(name="my-config")


def test_default_execution_mode_is_local():
    config = SparkComputeEngineConfig()
    assert config.execution_mode == "local"


def test_default_namespace():
    config = SparkComputeEngineConfig()
    assert config.namespace == "default"


def test_default_executor_instances():
    config = SparkComputeEngineConfig()
    assert config.executor_instances == 2


def test_default_job_timeout():
    config = SparkComputeEngineConfig()
    assert config.job_timeout_seconds == 3600


def test_default_metrics_enabled():
    config = SparkComputeEngineConfig()
    assert config.metrics_enabled is True


def test_local_mode_no_master_url_required():
    config = SparkComputeEngineConfig(execution_mode="local")
    assert config.master_url is None


def test_remote_requires_master_url():
    with pytest.raises(ValidationError, match="master_url is required"):
        SparkComputeEngineConfig(execution_mode="remote")


def test_remote_requires_k8s_scheme():
    with pytest.raises(ValidationError, match="k8s://"):
        SparkComputeEngineConfig(
            execution_mode="remote",
            master_url="spark://my-cluster:7077",
        )


def test_remote_valid_config():
    config = SparkComputeEngineConfig(
        execution_mode="remote",
        master_url="k8s://https://kubernetes.default.svc",
    )
    assert config.execution_mode == "remote"
    assert config.master_url == "k8s://https://kubernetes.default.svc"


def test_remote_requires_executor_instances_gte_1():
    with pytest.raises(
        ValidationError, match="executor_instances must be >= 1"
    ):
        SparkComputeEngineConfig(
            execution_mode="remote",
            master_url="k8s://https://kubernetes.default.svc",
            executor_instances=0,
        )


def test_operator_requires_image():
    with pytest.raises(ValidationError, match="image is required"):
        SparkComputeEngineConfig(execution_mode="operator")


def test_operator_valid_config():
    config = SparkComputeEngineConfig(
        execution_mode="operator",
        image="feast/spark:latest",
    )
    assert config.execution_mode == "operator"
    assert config.image == "feast/spark:latest"


def test_operator_requires_executor_instances_gte_1():
    with pytest.raises(
        ValidationError, match="executor_instances must be >= 1"
    ):
        SparkComputeEngineConfig(
            execution_mode="operator",
            image="feast/spark:latest",
            executor_instances=0,
        )


def test_kubeconfig_nonexistent_raises():
    with pytest.raises(
        ValidationError, match="kubeconfig file not found"
    ):
        SparkComputeEngineConfig(
            execution_mode="local",
            kubeconfig_path="/nonexistent/path/kubeconfig",
        )


def test_kubeconfig_valid_path():
    with tempfile.NamedTemporaryFile(
        suffix=".kubeconfig", delete=False
    ) as f:
        f.write(b"apiVersion: v1\n")
        kubeconfig_path = f.name
    try:
        config = SparkComputeEngineConfig(
            execution_mode="local",
            kubeconfig_path=kubeconfig_path,
        )
        assert config.kubeconfig_path == kubeconfig_path
    finally:
        os.unlink(kubeconfig_path)


def test_timeout_must_be_positive():
    with pytest.raises(
        ValidationError, match="job_timeout_seconds must be > 0"
    ):
        SparkComputeEngineConfig(job_timeout_seconds=0)


def test_negative_timeout_rejected():
    with pytest.raises(
        ValidationError, match="job_timeout_seconds must be > 0"
    ):
        SparkComputeEngineConfig(job_timeout_seconds=-10)


def test_secrets_and_configmaps():
    config = SparkComputeEngineConfig(
        execution_mode="remote",
        master_url="k8s://https://kubernetes.default.svc",
        secrets=[SecretRef(name="s3-creds", mount_path="/mnt/s3")],
        config_maps=[
            ConfigMapRef(name="spark-conf", mount_path="/mnt/conf")
        ],
    )
    assert len(config.secrets) == 1
    assert config.secrets[0].name == "s3-creds"
    assert len(config.config_maps) == 1
    assert config.config_maps[0].name == "spark-conf"


def test_image_overrides():
    config = SparkComputeEngineConfig(
        execution_mode="remote",
        master_url="k8s://https://kubernetes.default.svc",
        image="feast/spark:base",
        driver_image="feast/spark:driver",
        executor_image="feast/spark:executor",
    )
    assert config.image == "feast/spark:base"
    assert config.driver_image == "feast/spark:driver"
    assert config.executor_image == "feast/spark:executor"


def test_resource_config():
    config = SparkComputeEngineConfig(
        execution_mode="remote",
        master_url="k8s://https://kubernetes.default.svc",
        executor_instances=4,
        executor_memory="4g",
        executor_cores=2,
        driver_memory="2g",
        driver_cores=2,
    )
    assert config.executor_instances == 4
    assert config.executor_memory == "4g"
    assert config.executor_cores == 2
    assert config.driver_memory == "2g"
    assert config.driver_cores == 2
