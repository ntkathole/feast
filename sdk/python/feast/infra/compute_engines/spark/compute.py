import logging
import os
import time
from datetime import datetime
from typing import Dict, List, Literal, Optional, Sequence, Union, cast

from pydantic import StrictStr, model_validator
from pyspark.sql import SparkSession

from feast import (
    BatchFeatureView,
    Entity,
    FeatureView,
    OnDemandFeatureView,
    StreamFeatureView,
)
from feast.infra.common.materialization_job import (
    MaterializationJob,
    MaterializationJobStatus,
    MaterializationTask,
)
from feast.infra.common.retrieval_task import HistoricalRetrievalTask
from feast.infra.common.serde import SerializedArtifacts
from feast.infra.compute_engines.base import ComputeEngine
from feast.infra.compute_engines.spark.byos.config import ConfigMapRef, SecretRef
from feast.infra.compute_engines.spark.feature_builder import SparkFeatureBuilder
from feast.infra.compute_engines.spark.job import (
    SparkDAGRetrievalJob,
    SparkMaterializationJob,
)
from feast.infra.compute_engines.spark.utils import (
    get_or_create_new_spark_session,
    map_in_pandas,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkRetrievalJob,
)
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.infra.registry.base_registry import BaseRegistry
from feast.repo_config import FeastConfigBaseModel
from feast.utils import _get_column_names

logger = logging.getLogger(__name__)


class SparkComputeEngineConfig(FeastConfigBaseModel):
    type: Literal["spark.engine"] = "spark.engine"
    """ Spark Compute type selector"""

    spark_conf: Optional[Dict[str, str]] = None
    """ Configuration overlay for the spark session """

    staging_location: Optional[StrictStr] = None
    """ Remote path for batch materialization jobs"""

    region: Optional[StrictStr] = None
    """ AWS Region if applicable for s3-based staging locations"""

    partitions: int = 0
    """Number of partitions to use when writing data to online store. If 0, no repartitioning is done"""

    # --- BYOS fields ---

    execution_mode: Literal["local", "remote", "operator"] = "local"
    """Execution mode: 'local' (default), 'remote' (direct SparkSession to K8s), or 'operator' (SparkApplication CRDs)"""

    master_url: Optional[StrictStr] = None
    """Spark master URL (e.g., k8s://https://kubernetes.default.svc). Required when execution_mode is 'remote'."""

    namespace: StrictStr = "default"
    """Kubernetes namespace for Spark jobs"""

    kubeconfig_path: Optional[StrictStr] = None
    """Path to kubeconfig file for remote auth. If not set, uses in-cluster service account."""

    image: Optional[StrictStr] = None
    """Default Spark container image for driver and executor"""

    driver_image: Optional[StrictStr] = None
    """Override image for driver pod (falls back to image)"""

    executor_image: Optional[StrictStr] = None
    """Override image for executor pods (falls back to image)"""

    image_pull_secrets: List[str] = []
    """Kubernetes image pull secret names"""

    service_account_name: StrictStr = ""
    """Kubernetes service account for Spark pods"""

    secrets: List[SecretRef] = []
    """Kubernetes secrets to mount on driver/executor"""

    config_maps: List[ConfigMapRef] = []
    """ConfigMaps to mount on driver/executor"""

    executor_instances: int = 2
    """Number of executor instances"""

    executor_memory: StrictStr = "1g"
    """Memory per executor"""

    executor_cores: int = 1
    """CPU cores per executor"""

    driver_memory: StrictStr = "1g"
    """Memory for driver"""

    driver_cores: int = 1
    """CPU cores for driver"""

    job_timeout_seconds: int = 3600
    """Maximum job execution time before timeout"""

    metrics_enabled: bool = True
    """Enable Prometheus metrics exposition"""

    @model_validator(mode="after")
    def _validate_byos_config(self):
        if self.execution_mode == "remote":
            if not self.master_url:
                raise ValueError(
                    "master_url is required when execution_mode is 'remote'"
                )
            if not self.master_url.startswith("k8s://"):
                raise ValueError(
                    "master_url must use k8s:// scheme for Kubernetes clusters"
                )
        if self.execution_mode == "operator":
            if not self.image:
                raise ValueError(
                    "image is required when execution_mode is 'operator'"
                )
        if self.kubeconfig_path:
            expanded = os.path.expanduser(self.kubeconfig_path)
            if not os.path.isfile(expanded):
                raise ValueError(f"kubeconfig file not found: {expanded}")
        if self.execution_mode != "local" and self.executor_instances < 1:
            raise ValueError(
                "executor_instances must be >= 1 when execution_mode is not 'local'"
            )
        if self.job_timeout_seconds <= 0:
            raise ValueError("job_timeout_seconds must be > 0")
        return self


class SparkComputeEngine(ComputeEngine):
    def update(
        self,
        project: str,
        views_to_delete: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView]
        ],
        views_to_keep: Sequence[
            Union[BatchFeatureView, StreamFeatureView, FeatureView, OnDemandFeatureView]
        ],
        entities_to_delete: Sequence[Entity],
        entities_to_keep: Sequence[Entity],
    ):
        config = self.repo_config.batch_engine
        if isinstance(config, SparkComputeEngineConfig) and config.execution_mode != "local":
            from feast.infra.compute_engines.spark.byos.connector import BYOSConnector

            connector = BYOSConnector()
            connector.validate_connectivity(config)

    def teardown_infra(
        self,
        project: str,
        fvs: Sequence[Union[BatchFeatureView, StreamFeatureView, FeatureView]],
        entities: Sequence[Entity],
    ):
        pass

    def _get_spark_session(
        self,
        config: SparkComputeEngineConfig,
        spark_conf: Optional[Dict[str, str]] = None,
    ) -> SparkSession:
        if config.execution_mode == "remote":
            from feast.infra.compute_engines.spark.byos.connector import BYOSConnector

            connector = BYOSConnector()
            return connector.create_spark_session(config)
        return get_or_create_new_spark_session(spark_conf)

    def _get_feature_view_spark_session(
        self, feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView]
    ) -> SparkSession:
        spark_conf = self._get_feature_view_engine_config(feature_view)
        config = self.repo_config.batch_engine
        if isinstance(config, SparkComputeEngineConfig) and config.execution_mode != "local":
            return self._get_spark_session(config, spark_conf)
        return get_or_create_new_spark_session(spark_conf)

    def _materialize_one(
        self,
        registry: BaseRegistry,
        task: MaterializationTask,
        from_offline_store: bool = False,
        **kwargs,
    ) -> MaterializationJob:
        if from_offline_store:
            return self._materialize_from_offline_store(
                registry=registry,
                feature_view=task.feature_view,
                start_date=task.start_time,
                end_date=task.end_time,
                project=task.project,
            )

        config = self.repo_config.batch_engine
        job_id = f"{task.feature_view.name}-{task.start_time}-{task.end_time}"

        # Operator mode: delegate to SparkOperatorJobSubmitter
        if isinstance(config, SparkComputeEngineConfig) and config.execution_mode == "operator":
            return self._materialize_via_operator(config, task, job_id)

        # Local or remote mode: use SparkSession directly
        start_time = time.time()
        context = self.get_execution_context(registry, task)
        spark_session = self._get_feature_view_spark_session(task.feature_view)

        try:
            builder = SparkFeatureBuilder(
                registry=registry,
                spark_session=spark_session,
                task=task,
            )
            plan = builder.build()
            plan.execute(context)

            duration = time.time() - start_time
            cluster_addr = None
            ns = None
            if isinstance(config, SparkComputeEngineConfig) and config.execution_mode != "local":
                cluster_addr = config.master_url
                ns = config.namespace

                # Check timeout
                if duration > config.job_timeout_seconds:
                    from feast.errors import FeastSparkTimeoutError

                    self._record_job_metrics(config, task.feature_view.name, "materialize", "timeout", duration)
                    raise FeastSparkTimeoutError(job_id, config.job_timeout_seconds)

                self._record_job_metrics(config, task.feature_view.name, "materialize", "succeeded", duration)
                logger.info(
                    "BYOS materialization completed",
                    extra={
                        "job_id": job_id,
                        "feature_view": task.feature_view.name,
                        "cluster": config.master_url,
                        "duration_ms": int(duration * 1000),
                    },
                )

            return SparkMaterializationJob(
                job_id=job_id,
                status=MaterializationJobStatus.SUCCEEDED,
                cluster_address=cluster_addr,
                namespace=ns,
            )

        except Exception as e:
            duration = time.time() - start_time
            if isinstance(config, SparkComputeEngineConfig) and config.execution_mode != "local":
                self._record_job_metrics(config, task.feature_view.name, "materialize", "failed", duration)
                from feast.errors import FeastSparkClusterError

                logger.error(
                    "BYOS materialization failed",
                    extra={
                        "job_id": job_id,
                        "feature_view": task.feature_view.name,
                        "cluster": config.master_url,
                        "error": str(e),
                    },
                )
                wrapped = FeastSparkClusterError(
                    f"Materialization failed for '{task.feature_view.name}' "
                    f"on cluster {config.master_url}: {e}"
                )
                return SparkMaterializationJob(
                    job_id=job_id, status=MaterializationJobStatus.ERROR, error=wrapped
                )
            return SparkMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.ERROR, error=e
            )

    def _materialize_via_operator(
        self,
        config: SparkComputeEngineConfig,
        task: MaterializationTask,
        job_id: str,
    ) -> MaterializationJob:
        from feast.infra.compute_engines.spark.byos.operator import (
            SparkOperatorJobSubmitter,
        )

        submitter = SparkOperatorJobSubmitter(config)
        try:
            job = submitter.submit(task, self.repo_config)
            result = submitter.wait_for_completion(job, config.job_timeout_seconds)

            if result.status in ("COMPLETED",):
                return SparkMaterializationJob(
                    job_id=job_id, status=MaterializationJobStatus.SUCCEEDED
                )
            else:
                from feast.errors import FeastSparkOperatorError

                error = FeastSparkOperatorError(
                    f"SparkApplication '{job.application_name}' failed: {result.error_message}"
                )
                return SparkMaterializationJob(
                    job_id=job_id, status=MaterializationJobStatus.ERROR, error=error
                )
        except Exception as e:
            return SparkMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.ERROR, error=e
            )

    def _materialize_from_offline_store(
        self,
        registry: BaseRegistry,
        feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        start_date: datetime,
        end_date: datetime,
        project: str,
    ):
        logging.warning(
            "Materializing from offline store will be deprecated in the future. Please use the new "
            "materialization API."
        )
        entities = []
        for entity_name in feature_view.entities:
            entities.append(registry.get_entity(entity_name, project))

        (
            join_key_columns,
            feature_name_columns,
            timestamp_field,
            created_timestamp_column,
        ) = _get_column_names(feature_view, entities)

        job_id = f"{feature_view.name}-{start_date}-{end_date}"

        try:
            offline_job = cast(
                SparkRetrievalJob,
                self.offline_store.pull_latest_from_table_or_query(
                    config=self.repo_config,
                    data_source=feature_view.batch_source,
                    join_key_columns=join_key_columns,
                    feature_name_columns=feature_name_columns,
                    timestamp_field=timestamp_field,
                    created_timestamp_column=created_timestamp_column,
                    start_date=start_date,
                    end_date=end_date,
                ),
            )

            serialized_artifacts = SerializedArtifacts.serialize(
                feature_view=feature_view, repo_config=self.repo_config
            )

            spark_df = offline_job.to_spark_df()
            if self.repo_config.batch_engine.partitions != 0:
                spark_df = spark_df.repartition(
                    self.repo_config.batch_engine.partitions
                )

            spark_df.mapInPandas(
                lambda x: map_in_pandas(x, serialized_artifacts), "status int"
            ).count()

            return SparkMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.SUCCEEDED
            )
        except BaseException as e:
            return SparkMaterializationJob(
                job_id=job_id, status=MaterializationJobStatus.ERROR, error=e
            )

    def get_historical_features(
        self, registry: BaseRegistry, task: HistoricalRetrievalTask
    ) -> RetrievalJob:
        if isinstance(task.entity_df, str):
            raise NotImplementedError("SQL-based entity_df is not yet supported in DAG")

        start_time = time.time()
        context = self.get_execution_context(registry, task)
        spark_session = self._get_feature_view_spark_session(task.feature_view)
        config = self.repo_config.batch_engine

        try:
            builder = SparkFeatureBuilder(
                registry=registry,
                spark_session=spark_session,
                task=task,
            )
            plan = builder.build()

            if isinstance(config, SparkComputeEngineConfig) and config.execution_mode != "local":
                duration = time.time() - start_time
                self._record_job_metrics(config, task.feature_view.name, "historical_retrieval", "succeeded", duration)

            return SparkDAGRetrievalJob(
                plan=plan,
                spark_session=spark_session,
                context=context,
                config=self.repo_config,
                full_feature_names=task.full_feature_name,
            )
        except Exception as e:
            if isinstance(config, SparkComputeEngineConfig) and config.execution_mode != "local":
                from feast.errors import FeastSparkClusterError

                logger.error(
                    "BYOS historical retrieval failed",
                    extra={
                        "feature_view": task.feature_view.name,
                        "cluster": config.master_url,
                        "error": str(e),
                    },
                )
                wrapped = FeastSparkClusterError(
                    f"Historical retrieval failed for '{task.feature_view.name}' "
                    f"on cluster {config.master_url}: {e}"
                )
                return SparkDAGRetrievalJob(
                    plan=None,
                    spark_session=spark_session,
                    context=context,
                    config=self.repo_config,
                    full_feature_names=task.full_feature_name,
                    error=wrapped,
                )
            return SparkDAGRetrievalJob(
                plan=None,
                spark_session=spark_session,
                context=context,
                config=self.repo_config,
                full_feature_names=task.full_feature_name,
                error=e,
            )

    def _record_job_metrics(
        self,
        config: SparkComputeEngineConfig,
        feature_view: str,
        operation: str,
        status: str,
        duration: float,
    ):
        if not config.metrics_enabled:
            return
        try:
            from feast.infra.compute_engines.spark.byos.metrics import (
                BYOS_JOB_DURATION,
                BYOS_JOBS_TOTAL,
            )

            BYOS_JOBS_TOTAL.labels(
                status=status,
                feature_view=feature_view,
                execution_mode=config.execution_mode,
            ).inc()
            BYOS_JOB_DURATION.labels(
                feature_view=feature_view,
                operation=operation,
            ).observe(duration)
        except Exception:
            logger.debug("Failed to record BYOS metrics", exc_info=True)
