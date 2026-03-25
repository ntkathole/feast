from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from feast.errors import (
    FeastSparkOperatorError,
    FeastSparkTimeoutError,
)
from feast.infra.compute_engines.spark.kubernetes.operator import (
    SparkApplicationJob,
    SparkApplicationStatus,
    SparkOperatorJobSubmitter,
)
from feast.infra.compute_engines.spark.compute import (
    SparkComputeEngineConfig,
)


def _make_operator_config(**overrides):
    defaults = {
        "execution_mode": "operator",
        "image": "feast/spark:latest",
        "namespace": "feast-jobs",
        "service_account_name": "spark-sa",
    }
    defaults.update(overrides)
    return SparkComputeEngineConfig(**defaults)


def _make_mock_task():
    task = MagicMock()
    task.feature_view.name = "driver_hourly_stats"
    task.start_time = datetime(2025, 1, 1)
    task.end_time = datetime(2025, 12, 31)
    task.project = "test_project"
    return task


def _make_job(**overrides):
    defaults = {
        "job_id": "test-job",
        "application_name": "feast-materialize-test",
        "namespace": "feast-jobs",
        "feature_view_name": "test_fv",
    }
    defaults.update(overrides)
    return SparkApplicationJob(**defaults)


_SUBMIT_PATCH = (
    "feast.infra.compute_engines.spark.kubernetes.operator"
    ".SparkOperatorJobSubmitter._get_custom_objects_api"
)

_CLIENT_PATCH = (
    "feast.infra.compute_engines.spark.kubernetes.operator"
    ".SparkOperatorJobSubmitter._get_api_client"
)

_STATUS_PATCH = (
    "feast.infra.compute_engines.spark.kubernetes.operator"
    ".SparkOperatorJobSubmitter.get_status"
)

_CORE_PATCH = (
    "feast.infra.compute_engines.spark.kubernetes.operator"
    ".SparkOperatorJobSubmitter._get_core_v1_api"
)


@patch(_SUBMIT_PATCH)
@patch(_CLIENT_PATCH)
def test_submit_creates_crd(mock_get_client, mock_get_api):
    mock_api = MagicMock()
    mock_get_api.return_value = mock_api

    config = _make_operator_config()
    repo_config = MagicMock()
    repo_config.model_dump.return_value = {"project": "test"}

    submitter = SparkOperatorJobSubmitter(config)
    task = _make_mock_task()
    job = submitter.submit(task, repo_config)

    assert job.feature_view_name == "driver_hourly_stats"
    assert job.namespace == "feast-jobs"
    assert job.status == SparkApplicationStatus.SUBMITTED
    assert job.submit_time is not None

    mock_api.create_namespaced_custom_object.assert_called_once()
    call_kwargs = (
        mock_api.create_namespaced_custom_object.call_args
    )
    body = call_kwargs.kwargs.get("body") or call_kwargs[1].get(
        "body"
    )
    assert body["kind"] == "SparkApplication"
    assert body["metadata"]["namespace"] == "feast-jobs"
    assert body["spec"]["image"] == "feast/spark:latest"
    assert body["spec"]["type"] == "Python"
    assert body["spec"]["mode"] == "cluster"
    assert body["spec"]["driver"]["serviceAccount"] == "spark-sa"


@patch(_SUBMIT_PATCH)
@patch(_CLIENT_PATCH)
def test_submit_failure_raises_operator_error(
    mock_get_client, mock_get_api
):
    mock_api = MagicMock()
    mock_get_api.return_value = mock_api
    mock_api.create_namespaced_custom_object.side_effect = Exception(
        "API error"
    )

    config = _make_operator_config()
    repo_config = MagicMock()
    repo_config.model_dump.return_value = {"project": "test"}

    submitter = SparkOperatorJobSubmitter(config)
    task = _make_mock_task()

    with pytest.raises(
        FeastSparkOperatorError, match="Failed to submit"
    ):
        submitter.submit(task, repo_config)


@patch(_SUBMIT_PATCH)
@patch(_CLIENT_PATCH)
def test_crd_includes_secrets_and_configmaps(
    mock_get_client, mock_get_api
):
    from feast.infra.compute_engines.spark.kubernetes.config import (
        ConfigMapRef,
        SecretRef,
    )

    mock_api = MagicMock()
    mock_get_api.return_value = mock_api

    config = _make_operator_config(
        secrets=[
            SecretRef(name="s3-creds", mount_path="/mnt/s3")
        ],
        config_maps=[
            ConfigMapRef(
                name="spark-defaults", mount_path="/opt/conf"
            )
        ],
    )
    repo_config = MagicMock()
    repo_config.model_dump.return_value = {"project": "test"}

    submitter = SparkOperatorJobSubmitter(config)
    task = _make_mock_task()
    submitter.submit(task, repo_config)

    call_kwargs = (
        mock_api.create_namespaced_custom_object.call_args
    )
    body = call_kwargs.kwargs.get("body") or call_kwargs[1].get(
        "body"
    )
    driver = body["spec"]["driver"]
    executor = body["spec"]["executor"]

    assert len(driver["secrets"]) == 1
    assert driver["secrets"][0]["name"] == "s3-creds"
    assert len(driver["configMaps"]) == 1
    assert driver["configMaps"][0]["name"] == "spark-defaults"
    assert len(executor["secrets"]) == 1
    assert len(executor["configMaps"]) == 1


@patch(_SUBMIT_PATCH)
@patch(_CLIENT_PATCH)
def test_get_status_completed(mock_get_client, mock_get_api):
    mock_api = MagicMock()
    mock_get_api.return_value = mock_api
    mock_api.get_namespaced_custom_object.return_value = {
        "status": {
            "applicationState": {"state": "COMPLETED"},
            "sparkApplicationId": "spark-abc123",
        }
    }

    config = _make_operator_config()
    submitter = SparkOperatorJobSubmitter(config)
    job = _make_job()

    status = submitter.get_status(job)
    assert status == SparkApplicationStatus.COMPLETED
    assert job.spark_application_id == "spark-abc123"


@patch(_SUBMIT_PATCH)
@patch(_CLIENT_PATCH)
def test_get_status_failed_with_error(
    mock_get_client, mock_get_api
):
    mock_api = MagicMock()
    mock_get_api.return_value = mock_api
    mock_api.get_namespaced_custom_object.return_value = {
        "status": {
            "applicationState": {
                "state": "FAILED",
                "errorMessage": "OOM killed",
            }
        }
    }

    config = _make_operator_config()
    submitter = SparkOperatorJobSubmitter(config)
    job = _make_job()

    status = submitter.get_status(job)
    assert status == SparkApplicationStatus.FAILED
    assert job.error_message == "OOM killed"


@patch(_STATUS_PATCH)
@patch(
    "feast.infra.compute_engines.spark.kubernetes.operator.time.sleep"
)
def test_wait_for_completion_completed(
    mock_sleep, mock_get_status
):
    mock_get_status.side_effect = [
        SparkApplicationStatus.RUNNING,
        SparkApplicationStatus.RUNNING,
        SparkApplicationStatus.COMPLETED,
    ]

    config = _make_operator_config()
    submitter = SparkOperatorJobSubmitter(config)
    job = _make_job(submit_time=datetime.utcnow())

    result = submitter.wait_for_completion(
        job, timeout_seconds=300
    )
    assert result.status == "COMPLETED"
    assert job.completion_time is not None


@patch(_STATUS_PATCH)
@patch(
    "feast.infra.compute_engines.spark.kubernetes.operator.time.sleep"
)
def test_wait_for_completion_failed(
    mock_sleep, mock_get_status
):
    mock_get_status.side_effect = [
        SparkApplicationStatus.RUNNING,
        SparkApplicationStatus.FAILED,
    ]

    config = _make_operator_config()
    submitter = SparkOperatorJobSubmitter(config)
    job = _make_job(
        submit_time=datetime.utcnow(),
        error_message="Driver OOM",
    )

    result = submitter.wait_for_completion(
        job, timeout_seconds=300
    )
    assert result.status == "FAILED"
    assert result.error_message == "Driver OOM"


@patch(_STATUS_PATCH)
@patch(
    "feast.infra.compute_engines.spark.kubernetes.operator.time.time"
)
@patch(
    "feast.infra.compute_engines.spark.kubernetes.operator.time.sleep"
)
def test_wait_for_completion_timeout(
    mock_sleep, mock_time, mock_get_status
):
    mock_time.side_effect = [0, 0, 100, 200, 400]
    mock_get_status.return_value = SparkApplicationStatus.RUNNING

    config = _make_operator_config()
    submitter = SparkOperatorJobSubmitter(config)
    job = _make_job(submit_time=datetime.utcnow())

    with pytest.raises(
        FeastSparkTimeoutError, match="exceeded timeout"
    ):
        submitter.wait_for_completion(job, timeout_seconds=300)


@patch(_CORE_PATCH)
@patch(_CLIENT_PATCH)
def test_get_logs_success(mock_get_client, mock_get_core):
    mock_v1 = MagicMock()
    mock_get_core.return_value = mock_v1
    mock_v1.read_namespaced_pod_log.return_value = (
        "Driver log output"
    )

    config = _make_operator_config()
    submitter = SparkOperatorJobSubmitter(config)
    job = _make_job()

    logs = submitter.get_logs(job)
    assert logs == "Driver log output"
    mock_v1.read_namespaced_pod_log.assert_called_once_with(
        name="feast-materialize-test-driver",
        namespace="feast-jobs",
    )


@patch(_CORE_PATCH)
@patch(_CLIENT_PATCH)
def test_get_logs_failure(mock_get_client, mock_get_core):
    mock_v1 = MagicMock()
    mock_get_core.return_value = mock_v1
    mock_v1.read_namespaced_pod_log.side_effect = Exception(
        "Pod not found"
    )

    config = _make_operator_config()
    submitter = SparkOperatorJobSubmitter(config)
    job = _make_job()

    logs = submitter.get_logs(job)
    assert "Failed to retrieve logs" in logs
