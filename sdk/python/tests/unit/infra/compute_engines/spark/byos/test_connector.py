import tempfile
from unittest.mock import MagicMock, patch

import pytest

from feast.errors import FeastSparkClusterError
from feast.infra.compute_engines.spark.byos.config import (
    ConfigMapRef,
    SecretRef,
)
from feast.infra.compute_engines.spark.compute import (
    SparkComputeEngineConfig,
)


def _make_remote_config(**overrides):
    defaults = {
        "execution_mode": "remote",
        "master_url": "k8s://https://kubernetes.default.svc",
    }
    defaults.update(overrides)
    return SparkComputeEngineConfig(**defaults)


@patch("kubernetes.client.CoreV1Api")
@patch(
    "feast.infra.compute_engines.spark.byos.auth.get_k8s_api_client"
)
def test_validate_connectivity_success(
    mock_get_client, mock_core_v1
):
    from feast.infra.compute_engines.spark.byos.connector import (
        BYOSConnector,
    )

    mock_api_client = MagicMock()
    mock_get_client.return_value = mock_api_client
    mock_v1 = MagicMock()
    mock_core_v1.return_value = mock_v1

    config = _make_remote_config()
    connector = BYOSConnector()
    result = connector.validate_connectivity(config)

    assert result is True
    mock_get_client.assert_called_once_with(None)
    mock_v1.list_namespaced_pod.assert_called_once_with(
        namespace="default", limit=1
    )


@patch(
    "feast.infra.compute_engines.spark.byos.auth.get_k8s_api_client"
)
def test_validate_connectivity_auth_failure(mock_get_client):
    from feast.infra.compute_engines.spark.byos.connector import (
        BYOSConnector,
    )

    mock_get_client.side_effect = FeastSparkClusterError(
        "Failed to authenticate with Kubernetes"
    )

    config = _make_remote_config()
    connector = BYOSConnector()

    with pytest.raises(
        FeastSparkClusterError, match="Failed to authenticate"
    ):
        connector.validate_connectivity(config)


@patch("kubernetes.client.CoreV1Api")
@patch(
    "feast.infra.compute_engines.spark.byos.auth.get_k8s_api_client"
)
def test_validate_connectivity_unreachable(
    mock_get_client, mock_core_v1
):
    from feast.infra.compute_engines.spark.byos.connector import (
        BYOSConnector,
    )

    mock_api_client = MagicMock()
    mock_get_client.return_value = mock_api_client
    mock_v1 = MagicMock()
    mock_core_v1.return_value = mock_v1
    mock_v1.list_namespaced_pod.side_effect = Exception(
        "Connection refused"
    )

    config = _make_remote_config()
    connector = BYOSConnector()

    with pytest.raises(FeastSparkClusterError, match="Cannot connect"):
        connector.validate_connectivity(config)


@patch("kubernetes.client.CoreV1Api")
@patch(
    "feast.infra.compute_engines.spark.byos.auth.get_k8s_api_client"
)
def test_validate_connectivity_uses_kubeconfig(
    mock_get_client, mock_core_v1
):
    from feast.infra.compute_engines.spark.byos.connector import (
        BYOSConnector,
    )

    mock_api_client = MagicMock()
    mock_get_client.return_value = mock_api_client
    mock_v1 = MagicMock()
    mock_core_v1.return_value = mock_v1

    with tempfile.NamedTemporaryFile(
        suffix=".kubeconfig", delete=False
    ) as f:
        f.write(b"apiVersion: v1\n")
        kubeconfig_path = f.name

    config = _make_remote_config(kubeconfig_path=kubeconfig_path)
    connector = BYOSConnector()
    connector.validate_connectivity(config)

    mock_get_client.assert_called_once_with(kubeconfig_path)

    import os

    os.unlink(kubeconfig_path)


@patch("kubernetes.client.CoreV1Api")
@patch(
    "feast.infra.compute_engines.spark.byos.auth.get_k8s_api_client"
)
def test_validate_connectivity_custom_namespace(
    mock_get_client, mock_core_v1
):
    from feast.infra.compute_engines.spark.byos.connector import (
        BYOSConnector,
    )

    mock_api_client = MagicMock()
    mock_get_client.return_value = mock_api_client
    mock_v1 = MagicMock()
    mock_core_v1.return_value = mock_v1

    config = _make_remote_config(namespace="feast-jobs")
    connector = BYOSConnector()
    connector.validate_connectivity(config)

    mock_v1.list_namespaced_pod.assert_called_once_with(
        namespace="feast-jobs", limit=1
    )


@patch(
    "feast.infra.compute_engines.spark.byos.connector.SparkSession"
)
def test_create_spark_session_basic(mock_spark_session):
    from feast.infra.compute_engines.spark.byos.connector import (
        BYOSConnector,
    )

    mock_builder = MagicMock()
    mock_spark_session.builder = mock_builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_session = MagicMock()
    mock_builder.getOrCreate.return_value = mock_session

    config = _make_remote_config()
    connector = BYOSConnector()
    result = connector.create_spark_session(config)

    assert result == mock_session
    mock_builder.appName.assert_called_once_with("feast-byos")
    mock_builder.config.assert_called_once()
    call_args = mock_builder.config.call_args
    spark_conf = (
        call_args.kwargs.get("conf")
        or call_args[1].get("conf")
        or call_args[0][0]
    )
    conf_dict = dict(spark_conf.getAll())
    assert (
        conf_dict["spark.master"]
        == "k8s://https://kubernetes.default.svc"
    )
    assert conf_dict["spark.kubernetes.namespace"] == "default"


@patch(
    "feast.infra.compute_engines.spark.byos.connector.SparkSession"
)
def test_create_spark_session_with_images(mock_spark_session):
    from feast.infra.compute_engines.spark.byos.connector import (
        BYOSConnector,
    )

    mock_builder = MagicMock()
    mock_spark_session.builder = mock_builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = MagicMock()

    config = _make_remote_config(
        image="feast/spark:base",
        driver_image="feast/spark:driver",
        executor_image="feast/spark:executor",
    )
    connector = BYOSConnector()
    connector.create_spark_session(config)

    call_args = mock_builder.config.call_args
    spark_conf = (
        call_args.kwargs.get("conf")
        or call_args[1].get("conf")
        or call_args[0][0]
    )
    conf_dict = dict(spark_conf.getAll())
    assert (
        conf_dict["spark.kubernetes.container.image"]
        == "feast/spark:base"
    )
    assert (
        conf_dict["spark.kubernetes.driver.container.image"]
        == "feast/spark:driver"
    )
    assert (
        conf_dict["spark.kubernetes.executor.container.image"]
        == "feast/spark:executor"
    )


@patch(
    "feast.infra.compute_engines.spark.byos.connector.SparkSession"
)
def test_create_spark_session_with_service_account(
    mock_spark_session,
):
    from feast.infra.compute_engines.spark.byos.connector import (
        BYOSConnector,
    )

    mock_builder = MagicMock()
    mock_spark_session.builder = mock_builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = MagicMock()

    config = _make_remote_config(service_account_name="spark-sa")
    connector = BYOSConnector()
    connector.create_spark_session(config)

    call_args = mock_builder.config.call_args
    spark_conf = (
        call_args.kwargs.get("conf")
        or call_args[1].get("conf")
        or call_args[0][0]
    )
    conf_dict = dict(spark_conf.getAll())
    key = "spark.kubernetes.authenticate.driver.serviceAccountName"
    assert conf_dict[key] == "spark-sa"


@patch(
    "feast.infra.compute_engines.spark.byos.connector.SparkSession"
)
def test_create_spark_session_with_secrets(mock_spark_session):
    from feast.infra.compute_engines.spark.byos.connector import (
        BYOSConnector,
    )

    mock_builder = MagicMock()
    mock_spark_session.builder = mock_builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = MagicMock()

    config = _make_remote_config(
        secrets=[SecretRef(name="s3-creds", mount_path="/mnt/s3")]
    )
    connector = BYOSConnector()
    connector.create_spark_session(config)

    call_args = mock_builder.config.call_args
    spark_conf = (
        call_args.kwargs.get("conf")
        or call_args[1].get("conf")
        or call_args[0][0]
    )
    conf_dict = dict(spark_conf.getAll())
    assert (
        conf_dict["spark.kubernetes.driver.secrets.s3-creds"]
        == "/mnt/s3"
    )
    assert (
        conf_dict["spark.kubernetes.executor.secrets.s3-creds"]
        == "/mnt/s3"
    )


@patch(
    "feast.infra.compute_engines.spark.byos.connector.SparkSession"
)
def test_create_spark_session_resource_config(mock_spark_session):
    from feast.infra.compute_engines.spark.byos.connector import (
        BYOSConnector,
    )

    mock_builder = MagicMock()
    mock_spark_session.builder = mock_builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = MagicMock()

    config = _make_remote_config(
        executor_instances=4,
        executor_memory="4g",
        executor_cores=2,
        driver_memory="2g",
        driver_cores=2,
    )
    connector = BYOSConnector()
    connector.create_spark_session(config)

    call_args = mock_builder.config.call_args
    spark_conf = (
        call_args.kwargs.get("conf")
        or call_args[1].get("conf")
        or call_args[0][0]
    )
    conf_dict = dict(spark_conf.getAll())
    assert conf_dict["spark.executor.instances"] == "4"
    assert conf_dict["spark.executor.memory"] == "4g"
    assert conf_dict["spark.executor.cores"] == "2"
    assert conf_dict["spark.driver.memory"] == "2g"
    assert conf_dict["spark.driver.cores"] == "2"


@patch(
    "feast.infra.compute_engines.spark.byos.connector.SparkSession"
)
def test_create_spark_session_user_overrides(mock_spark_session):
    from feast.infra.compute_engines.spark.byos.connector import (
        BYOSConnector,
    )

    mock_builder = MagicMock()
    mock_spark_session.builder = mock_builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = MagicMock()

    config = _make_remote_config(
        spark_conf={
            "spark.executor.instances": "8",
            "spark.custom.key": "value",
        },
    )
    connector = BYOSConnector()
    connector.create_spark_session(config)

    call_args = mock_builder.config.call_args
    spark_conf = (
        call_args.kwargs.get("conf")
        or call_args[1].get("conf")
        or call_args[0][0]
    )
    conf_dict = dict(spark_conf.getAll())
    # User override should take precedence
    assert conf_dict["spark.executor.instances"] == "8"
    assert conf_dict["spark.custom.key"] == "value"


@patch(
    "feast.infra.compute_engines.spark.byos.connector.SparkSession"
)
def test_create_spark_session_image_pull_secrets(mock_spark_session):
    from feast.infra.compute_engines.spark.byos.connector import (
        BYOSConnector,
    )

    mock_builder = MagicMock()
    mock_spark_session.builder = mock_builder
    mock_builder.appName.return_value = mock_builder
    mock_builder.config.return_value = mock_builder
    mock_builder.getOrCreate.return_value = MagicMock()

    config = _make_remote_config(
        image_pull_secrets=["regcred", "ghcr-secret"]
    )
    connector = BYOSConnector()
    connector.create_spark_session(config)

    call_args = mock_builder.config.call_args
    spark_conf = (
        call_args.kwargs.get("conf")
        or call_args[1].get("conf")
        or call_args[0][0]
    )
    conf_dict = dict(spark_conf.getAll())
    assert (
        conf_dict["spark.kubernetes.container.image.pullSecrets"]
        == "regcred,ghcr-secret"
    )
