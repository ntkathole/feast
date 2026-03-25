import base64
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from feast.infra.common.materialization_job import MaterializationTask
    from feast.infra.compute_engines.spark.compute import SparkComputeEngineConfig
    from feast.repo_config import RepoConfig

logger = logging.getLogger(__name__)

SPARK_OPERATOR_API_GROUP = "sparkoperator.k8s.io"
SPARK_OPERATOR_API_VERSION = "v1beta2"
SPARK_OPERATOR_PLURAL = "sparkapplications"


class SparkApplicationStatus(str, Enum):
    PENDING = ""
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SUBMISSION_FAILED = "SUBMISSION_FAILED"
    TIMED_OUT = "TIMED_OUT"


@dataclass
class SparkApplicationJob:
    job_id: str
    application_name: str
    namespace: str
    feature_view_name: str
    status: SparkApplicationStatus = SparkApplicationStatus.PENDING
    submit_time: Optional[datetime] = None
    completion_time: Optional[datetime] = None
    error_message: Optional[str] = None
    spark_application_id: Optional[str] = None


@dataclass
class SparkApplicationJobResult:
    status: str
    error_message: Optional[str] = None


class SparkOperatorJobSubmitter:
    """Submits and monitors SparkApplication CRDs via the Kubernetes API."""

    def __init__(self, config: "SparkComputeEngineConfig"):
        self._config = config
        self._api_client = None

    def _get_api_client(self):
        if self._api_client is None:
            from feast.infra.compute_engines.spark.kubernetes.auth import (
                get_k8s_api_client,
            )

            self._api_client = get_k8s_api_client(self._config.kubeconfig_path)
        return self._api_client

    def _get_custom_objects_api(self):
        from kubernetes import client as k8s_client

        return k8s_client.CustomObjectsApi(self._get_api_client())

    def _get_core_v1_api(self):
        from kubernetes import client as k8s_client

        return k8s_client.CoreV1Api(self._get_api_client())

    def submit(
        self,
        task: "MaterializationTask",
        repo_config: "RepoConfig",
    ) -> SparkApplicationJob:
        """Create a SparkApplication CRD for the materialization task."""
        import json

        short_uuid = uuid.uuid4().hex[:8]
        app_name = f"feast-materialize-{task.feature_view.name}-{short_uuid}"
        job_id = f"{task.feature_view.name}-{task.start_time}-{task.end_time}"

        config = self._config
        serialized_config = base64.b64encode(
            json.dumps(repo_config.model_dump(mode="json")).encode()
        ).decode()

        crd_body = self._build_crd(
            app_name=app_name,
            task=task,
            serialized_config=serialized_config,
        )

        logger.info(
            "Submitting SparkApplication CRD",
            extra={
                "application_name": app_name,
                "namespace": config.namespace,
                "feature_view": task.feature_view.name,
                "image": config.image,
            },
        )

        api = self._get_custom_objects_api()
        try:
            api.create_namespaced_custom_object(
                group=SPARK_OPERATOR_API_GROUP,
                version=SPARK_OPERATOR_API_VERSION,
                namespace=config.namespace,
                plural=SPARK_OPERATOR_PLURAL,
                body=crd_body,
            )
        except Exception as e:
            from feast.errors import FeastSparkOperatorError

            raise FeastSparkOperatorError(
                f"Failed to submit SparkApplication '{app_name}': {e}"
            ) from e

        self._update_active_jobs_metric(config.namespace, delta=1)

        return SparkApplicationJob(
            job_id=job_id,
            application_name=app_name,
            namespace=config.namespace,
            feature_view_name=task.feature_view.name,
            status=SparkApplicationStatus.SUBMITTED,
            submit_time=datetime.utcnow(),
        )

    def get_status(self, job: SparkApplicationJob) -> SparkApplicationStatus:
        """Read SparkApplication status from the Kubernetes API."""
        api = self._get_custom_objects_api()
        try:
            result = api.get_namespaced_custom_object(
                group=SPARK_OPERATOR_API_GROUP,
                version=SPARK_OPERATOR_API_VERSION,
                namespace=job.namespace,
                plural=SPARK_OPERATOR_PLURAL,
                name=job.application_name,
            )
            state = (
                result.get("status", {})
                .get("applicationState", {})
                .get("state", "")
            )
            error_msg = (
                result.get("status", {})
                .get("applicationState", {})
                .get("errorMessage", "")
            )

            if error_msg:
                job.error_message = error_msg

            spark_app_id = result.get("status", {}).get("sparkApplicationId", "")
            if spark_app_id:
                job.spark_application_id = spark_app_id

            return SparkApplicationStatus(state)

        except Exception as e:
            logger.error(
                "Failed to get SparkApplication status",
                extra={
                    "application_name": job.application_name,
                    "error": str(e),
                },
            )
            return SparkApplicationStatus.FAILED

    def wait_for_completion(
        self, job: SparkApplicationJob, timeout_seconds: int
    ) -> SparkApplicationJobResult:
        """Poll until the job completes, fails, or times out."""
        start = time.time()
        poll_interval = 5
        max_poll_interval = 60

        while True:
            elapsed = time.time() - start
            if elapsed > timeout_seconds:
                from feast.errors import FeastSparkTimeoutError

                self._update_active_jobs_metric(job.namespace, delta=-1)
                job.status = SparkApplicationStatus.TIMED_OUT
                raise FeastSparkTimeoutError(job.job_id, timeout_seconds)

            status = self.get_status(job)
            job.status = status

            logger.debug(
                "SparkApplication status",
                extra={
                    "application_name": job.application_name,
                    "status": status.value,
                    "elapsed_seconds": int(elapsed),
                },
            )

            if status == SparkApplicationStatus.COMPLETED:
                job.completion_time = datetime.utcnow()
                self._update_active_jobs_metric(job.namespace, delta=-1)
                self._record_duration(job)
                return SparkApplicationJobResult(status="COMPLETED")

            if status in (
                SparkApplicationStatus.FAILED,
                SparkApplicationStatus.SUBMISSION_FAILED,
            ):
                job.completion_time = datetime.utcnow()
                self._update_active_jobs_metric(job.namespace, delta=-1)
                self._record_duration(job)
                return SparkApplicationJobResult(
                    status="FAILED",
                    error_message=job.error_message or "Unknown error",
                )

            time.sleep(min(poll_interval, max_poll_interval))
            poll_interval = min(poll_interval * 1.5, max_poll_interval)

    def get_logs(self, job: SparkApplicationJob) -> str:
        """Retrieve driver pod logs for debugging."""
        api = self._get_core_v1_api()
        driver_pod_name = f"{job.application_name}-driver"
        try:
            return api.read_namespaced_pod_log(
                name=driver_pod_name,
                namespace=job.namespace,
            )
        except Exception as e:
            logger.warning(
                "Failed to retrieve driver logs",
                extra={
                    "driver_pod": driver_pod_name,
                    "error": str(e),
                },
            )
            return f"Failed to retrieve logs: {e}"

    def _build_crd(
        self,
        app_name: str,
        task: "MaterializationTask",
        serialized_config: str,
    ) -> Dict[str, Any]:
        """Build the SparkApplication CRD dict."""
        config = self._config

        # Secrets list for driver/executor
        secrets = [
            {"name": s.name, "path": s.mount_path, "secretType": "Generic"}
            for s in config.secrets
        ]
        config_maps = [
            {"name": cm.name, "path": cm.mount_path}
            for cm in config.config_maps
        ]

        driver_spec: Dict[str, Any] = {
            "cores": config.driver_cores,
            "memory": config.driver_memory,
            "labels": {"feast.dev/role": "driver"},
        }
        if config.service_account_name:
            driver_spec["serviceAccount"] = config.service_account_name
        if secrets:
            driver_spec["secrets"] = secrets
        if config_maps:
            driver_spec["configMaps"] = config_maps
        if config.driver_image:
            driver_spec["image"] = config.driver_image

        executor_spec: Dict[str, Any] = {
            "cores": config.executor_cores,
            "memory": config.executor_memory,
            "instances": config.executor_instances,
            "labels": {"feast.dev/role": "executor"},
        }
        if secrets:
            executor_spec["secrets"] = secrets
        if config_maps:
            executor_spec["configMaps"] = config_maps
        if config.executor_image:
            executor_spec["image"] = config.executor_image

        crd: Dict[str, Any] = {
            "apiVersion": f"{SPARK_OPERATOR_API_GROUP}/{SPARK_OPERATOR_API_VERSION}",
            "kind": "SparkApplication",
            "metadata": {
                "name": app_name,
                "namespace": config.namespace,
                "labels": {
                    "app.kubernetes.io/managed-by": "feast",
                    "feast.dev/feature-view": task.feature_view.name,
                },
            },
            "spec": {
                "type": "Python",
                "pythonVersion": "3",
                "mode": "cluster",
                "image": config.image,
                "imagePullPolicy": "IfNotPresent",
                "mainApplicationFile": "local:///opt/feast/kubernetes/main.py",
                "arguments": [
                    f"--feature-view={task.feature_view.name}",
                    f"--start-date={task.start_time.isoformat()}",
                    f"--end-date={task.end_time.isoformat()}",
                    f"--config-base64={serialized_config}",
                ],
                "sparkVersion": "3.5",
                "restartPolicy": {
                    "type": "OnFailure",
                    "onFailureRetries": 3,
                    "onFailureRetryInterval": 30,
                    "onSubmissionFailureRetries": 3,
                    "onSubmissionFailureRetryInterval": 30,
                },
                "timeToLiveSeconds": 300,
                "driver": driver_spec,
                "executor": executor_spec,
            },
        }

        if config.spark_conf:
            crd["spec"]["sparkConf"] = config.spark_conf

        if config.image_pull_secrets:
            crd["spec"]["imagePullSecrets"] = config.image_pull_secrets

        return crd

    def _update_active_jobs_metric(self, namespace: str, delta: int):
        try:
            from feast.infra.compute_engines.spark.kubernetes.metrics import (
                SPARK_K8S_ACTIVE_JOBS,
            )

            SPARK_K8S_ACTIVE_JOBS.labels(namespace=namespace).inc(delta)
        except Exception:
            logger.debug("Failed to update active jobs metric", exc_info=True)

    def _record_duration(self, job: SparkApplicationJob):
        if job.submit_time and job.completion_time:
            duration = (job.completion_time - job.submit_time).total_seconds()
            try:
                from feast.infra.compute_engines.spark.kubernetes.metrics import (
                    SPARK_K8S_JOB_DURATION,
                    SPARK_K8S_JOBS_TOTAL,
                )

                SPARK_K8S_JOBS_TOTAL.labels(
                    status=job.status.value.lower() or "unknown",
                    feature_view=job.feature_view_name,
                    execution_mode="operator",
                ).inc()
                SPARK_K8S_JOB_DURATION.labels(
                    feature_view=job.feature_view_name,
                    operation="materialize",
                ).observe(duration)
            except Exception:
                logger.debug("Failed to record operator metrics", exc_info=True)
