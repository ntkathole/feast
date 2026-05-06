import logging
from datetime import datetime
from typing import Sequence, Union

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
from feast.infra.compute_engines.base import ComputeEngine
from feast.infra.compute_engines.ray.config import RayComputeEngineConfig
from feast.infra.compute_engines.ray.feature_builder import RayFeatureBuilder
from feast.infra.compute_engines.ray.job import (
    KubeRayRetrievalJob,
    RayDAGRetrievalJob,
    RayMaterializationJob,
)
from feast.infra.compute_engines.ray.utils import (
    write_to_online_store,
    write_to_online_store_from_ray_ds,
)
from feast.infra.offline_stores.offline_store import RetrievalJob
from feast.infra.ray_initializer import (
    RayConfigManager,
    RayExecutionMode,
    ensure_ray_initialized,
    get_ray_wrapper,
)
from feast.infra.registry.base_registry import BaseRegistry

logger = logging.getLogger(__name__)


class RayComputeEngine(ComputeEngine):
    """
    Ray-based compute engine for distributed feature computation.

    Supports three execution paths:

    1. **Local** – ``ray.init()`` on the calling machine (default, no K8s needed).
    2. **Remote** – connect to an existing Ray cluster via ``ray_address``.
    3. **KubeRay (existing cluster)** – ``use_kuberay=True`` + ``cluster_name``
       set; connects to a pre-existing RayCluster via CodeFlare SDK.
    4. **KubeRay Job (ephemeral cluster)** – ``use_kuberay=True``, no
       ``cluster_name``; Feast creates a temporary RayCluster via a KubeRay
       ``RayJob`` CR, runs materialisation, and tears the cluster down
       automatically.  Kueue queue integration is available via ``local_queue``.
    """

    def __init__(
        self,
        offline_store,
        online_store,
        repo_config,
        **kwargs,
    ):
        super().__init__(
            offline_store=offline_store,
            online_store=online_store,
            repo_config=repo_config,
            **kwargs,
        )
        self.config = repo_config.batch_engine
        assert isinstance(self.config, RayComputeEngineConfig)

        self._execution_mode: RayExecutionMode = RayConfigManager(
            self.config
        ).determine_execution_mode()

        # For KUBERAY_JOB mode the job runs entirely inside the remote cluster –
        # no local ray.init() is needed on the driver side.
        if self._execution_mode != RayExecutionMode.KUBERAY_JOB:
            self._ensure_ray_initialized()

    def _ensure_ray_initialized(self):
        """Ensure Ray is initialized with proper configuration."""
        ensure_ray_initialized(self.config)

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
        """Ray compute engine doesn't require infrastructure updates."""
        pass

    def teardown_infra(
        self,
        project: str,
        fvs: Sequence[Union[BatchFeatureView, StreamFeatureView, FeatureView]],
        entities: Sequence[Entity],
    ):
        """Ray compute engine doesn't require infrastructure teardown."""
        pass

    def _materialize_one(
        self,
        registry: BaseRegistry,
        task: MaterializationTask,
        from_offline_store: bool = False,
        **kwargs,
    ) -> MaterializationJob:
        """Materialize features for a single feature view."""
        job_id = f"{task.feature_view.name}-{task.start_time}-{task.end_time}"

        # ------------------------------------------------------------------ #
        # Ephemeral KubeRay Job path: submit a RayJob CR and wait.
        # ------------------------------------------------------------------ #
        if self._execution_mode == RayExecutionMode.KUBERAY_JOB:
            return self._materialize_one_via_kuberay_job(registry, task, job_id)

        # ------------------------------------------------------------------ #
        # Standard (local / remote / existing-cluster) paths below.
        # ------------------------------------------------------------------ #
        if from_offline_store:
            logger.warning(
                "Materializing from offline store will be deprecated. "
                "Please use the new materialization API."
            )
            return self._materialize_from_offline_store(
                registry=registry,
                feature_view=task.feature_view,
                start_date=task.start_time,
                end_date=task.end_time,
                project=task.project,
            )

        try:
            # Build typed execution context
            context = self.get_execution_context(registry, task)

            # Construct Feature Builder and execute
            builder = RayFeatureBuilder(registry, task.feature_view, task, self.config)
            plan = builder.build()
            result = plan.execute(context)

            # Log execution results
            logger.info(f"Materialization completed for {task.feature_view.name}")

            return RayMaterializationJob(
                job_id=job_id,
                status=MaterializationJobStatus.SUCCEEDED,
                result=result,
            )

        except Exception as e:
            logger.error(f"Materialization failed for {task.feature_view.name}: {e}")
            return RayMaterializationJob(
                job_id=job_id,
                status=MaterializationJobStatus.ERROR,
                error=e,
            )

    def _materialize_one_via_kuberay_job(
        self,
        registry: BaseRegistry,
        task: MaterializationTask,
        job_id: str,
    ) -> MaterializationJob:
        """
        Materialise a single feature view on an ephemeral KubeRay cluster.

        The ``ExecutionPlan`` is built on the driver (same code path as local
        mode) and cloudpickle-serialised into ``working_dir``.  Task parameters
        (feature view name, time window) are passed as plain env vars.

        On the cluster the runner:
        1. Loads the plan via cloudpickle.
        2. Rebuilds ``ExecutionContext`` from ``feature_store.yaml`` + env vars.
        3. Calls ``plan.execute(context)`` — Ray Data distributes to workers.

        No dill, no context pkl, no FeatureStore reconstruction complexity.
        """
        from feast.infra.compute_engines.ray.kuberay_job import (
            KubeRayJobSubmitter,
            build_safe_job_name,
        )

        submitter = KubeRayJobSubmitter(self.config)
        k8s_job_name = build_safe_job_name(task.feature_view.name, task.start_time)

        # Build plan locally — cloudpickle handles any Python callables (UDFs).
        builder = RayFeatureBuilder(registry, task.feature_view, task, self.config)
        execution_plan = builder.build()
        logger.debug(
            "ExecutionPlan built for '%s' (%d nodes).",
            task.feature_view.name,
            len(execution_plan.nodes),
        )

        try:
            logger.info(
                "KubeRay Job mode: submitting RayJob '%s' for feature view '%s'",
                k8s_job_name,
                task.feature_view.name,
            )

            submitter.submit_materialization_job(
                job_name=k8s_job_name,
                execution_plan=execution_plan,
                feature_view_name=task.feature_view.name,
                start_time=task.start_time,
                end_time=task.end_time,
            )

            timeout = self.config.ray_job_active_deadline_seconds
            submitter.wait_for_job(k8s_job_name, timeout=timeout)

            logger.info(
                "KubeRay Job '%s' completed for feature view '%s'.",
                k8s_job_name,
                task.feature_view.name,
            )
            return RayMaterializationJob(
                job_id=job_id,
                status=MaterializationJobStatus.SUCCEEDED,
            )

        except Exception as e:
            logger.error(
                "KubeRay Job '%s' failed for feature view '%s': %s",
                k8s_job_name,
                task.feature_view.name,
                e,
            )
            return RayMaterializationJob(
                job_id=job_id,
                status=MaterializationJobStatus.ERROR,
                error=e,
            )

    def _materialize_from_offline_store(
        self,
        registry: BaseRegistry,
        feature_view: Union[BatchFeatureView, StreamFeatureView, FeatureView],
        start_date: datetime,
        end_date: datetime,
        project: str,
    ) -> MaterializationJob:
        """Legacy materialization method for backward compatibility."""
        from feast.utils import _get_column_names

        job_id = f"{feature_view.name}-{start_date}-{end_date}"

        try:
            # Get column information
            entities = [
                registry.get_entity(name, project) for name in feature_view.entities
            ]
            (
                join_key_columns,
                feature_name_columns,
                timestamp_field,
                created_timestamp_column,
            ) = _get_column_names(feature_view, entities)

            # Pull data from offline store
            retrieval_job = self.offline_store.pull_latest_from_table_or_query(
                config=self.repo_config,
                data_source=feature_view.batch_source,  # type: ignore[arg-type]
                join_key_columns=join_key_columns,
                feature_name_columns=feature_name_columns,
                timestamp_field=timestamp_field,
                created_timestamp_column=created_timestamp_column,
                start_date=start_date,
                end_date=end_date,
            )

            # Prefer the distributed Ray path: keep data on the cluster and
            # write each partition in parallel.  Fall back to the Arrow driver
            # path for non-Ray retrieval jobs (e.g. Dask, DuckDB offline stores).
            from feast.infra.offline_stores.contrib.ray_offline_store.ray import (
                RayRetrievalJob,
            )

            if isinstance(retrieval_job, RayRetrievalJob):
                ray_ds = retrieval_job.to_ray_dataset()

                needs_offline = getattr(feature_view, "offline", False)
                sink_source = getattr(feature_view, "sink_source", None)

                # Materialise the lazy pipeline ONCE into Ray object-store memory
                # when more than one write target will consume it.
                # Without this, each use of ray_ds (online write via map_batches,
                # to_arrow_refs for offline_write_batch, write_parquet for
                # sink_source) independently re-executes the full source-read
                # pipeline, producing up to three separate data snapshots that can
                # diverge when the source changes between executions.
                if needs_offline or sink_source is not None:
                    ray_ds = ray_ds.materialize()

                # Distributed online store write — each Ray worker writes its shard
                write_to_online_store_from_ray_ds(
                    ray_ds=ray_ds,
                    feature_view=feature_view,
                    online_store=self.online_store,
                    repo_config=self.repo_config,
                )

                # offline_write_batch and sink_source are independent — both can
                # apply when a feature view has offline=True AND a sink_source.
                if needs_offline:
                    import pyarrow as pa
                    import ray as _ray

                    arrow_table = pa.concat_tables(_ray.get(ray_ds.to_arrow_refs()))
                    self.offline_store.offline_write_batch(
                        config=self.repo_config,
                        feature_view=feature_view,
                        table=arrow_table,
                        progress=lambda x: None,
                    )

                if sink_source is not None:
                    logger.debug(
                        f"Writing derived view {feature_view.name} to sink_source: {sink_source.path}"
                    )
                    try:
                        ray_ds.write_parquet(sink_source.path)
                    except Exception as e:
                        logger.error(
                            f"Failed to write to sink_source {sink_source.path}: {e}"
                        )
            else:
                # Non-Ray offline store: collect on driver and write sequentially
                arrow_table = retrieval_job.to_arrow()

                write_to_online_store(
                    arrow_table=arrow_table,
                    feature_view=feature_view,
                    online_store=self.online_store,
                    repo_config=self.repo_config,
                )

                if getattr(feature_view, "offline", False):
                    self.offline_store.offline_write_batch(
                        config=self.repo_config,
                        feature_view=feature_view,
                        table=arrow_table,
                        progress=lambda x: None,
                    )

                sink_source = getattr(feature_view, "sink_source", None)
                if sink_source is not None:
                    logger.debug(
                        f"Writing derived view {feature_view.name} to sink_source: {sink_source.path}"
                    )
                    try:
                        ray_wrapper = get_ray_wrapper()
                        ray_dataset = ray_wrapper.from_arrow(arrow_table)
                        ray_dataset.write_parquet(sink_source.path)
                    except Exception as e:
                        logger.error(
                            f"Failed to write to sink_source {sink_source.path}: {e}"
                        )
            return RayMaterializationJob(
                job_id=job_id,
                status=MaterializationJobStatus.SUCCEEDED,
            )

        except Exception as e:
            logger.error(f"Legacy materialization failed: {e}")
            return RayMaterializationJob(
                job_id=job_id,
                status=MaterializationJobStatus.ERROR,
                error=e,
            )

    def get_historical_features(
        self, registry: BaseRegistry, task: HistoricalRetrievalTask
    ) -> RetrievalJob:
        """
        Get historical features using Ray DAG execution.

        In **KUBERAY_JOB** mode the work is dispatched to an ephemeral
        RayCluster via a KubeRay ``RayJob`` CR and a ``KubeRayRetrievalJob``
        is returned.  The actual cluster work (and blocking poll) is deferred
        until the caller consumes the job (``to_df()`` / ``to_arrow()``).

        In all other modes the plan is built locally and executed on whatever
        Ray cluster is currently initialised (lazy ``RayDAGRetrievalJob``).
        """
        if isinstance(task.entity_df, str):
            raise NotImplementedError(
                "SQL-based entity_df is not yet supported in Ray DAG"
            )

        # ── KUBERAY_JOB mode: submit to ephemeral cluster ──────────────
        if self._execution_mode == RayExecutionMode.KUBERAY_JOB:
            return self._get_historical_features_via_kuberay_job(registry, task)

        # ── Standard (local / remote / existing-cluster) path ──────────
        try:
            context = self.get_execution_context(registry, task)
            builder = RayFeatureBuilder(registry, task.feature_view, task, self.config)
            plan = builder.build()

            return RayDAGRetrievalJob(
                plan=plan,
                context=context,
                config=self.repo_config,
                full_feature_names=task.full_feature_name,
                on_demand_feature_views=getattr(task, "on_demand_feature_views", None),
                feature_refs=getattr(task, "feature_refs", None),
            )

        except Exception as e:
            logger.error(f"Historical feature retrieval failed: {e}")
            return RayDAGRetrievalJob(
                plan=None,
                context=None,
                config=self.repo_config,
                full_feature_names=task.full_feature_name,
                on_demand_feature_views=getattr(task, "on_demand_feature_views", None),
                feature_refs=getattr(task, "feature_refs", None),
                error=e,
            )

    def _get_historical_features_via_kuberay_job(
        self,
        registry: BaseRegistry,
        task: HistoricalRetrievalTask,
    ) -> RetrievalJob:
        """
        Run ``get_historical_features`` on an ephemeral KubeRay cluster.

        Builds the ``ExecutionPlan`` locally (same code path as local/remote
        mode), dill-serialises it, and ships it to a ``RayJob`` CR.  The
        cluster executes the plan and writes the result parquet to
        ``staging_location``.

        Returns a ``KubeRayRetrievalJob`` — cluster execution is **lazy**:
        it starts only when the caller calls ``to_df()`` / ``to_arrow()``.

        Requires:
        * ``staging_location`` configured in batch_engine (for entity DataFrame
          transfer and result retrieval).
        * Feast available in the Ray runtime environment.
        """
        from feast.infra.compute_engines.ray.kuberay_job import (
            KubeRayJobSubmitter,
            build_safe_job_name,
        )

        submitter = KubeRayJobSubmitter(self.config)
        job_name = build_safe_job_name(
            task.feature_view.name, timestamp=task.start_time, prefix="fh"
        )

        feature_refs: list = getattr(task, "feature_refs", None) or [
            f"{task.feature_view.name}:{f.name}" for f in task.feature_view.features
        ]
        full_feature_names: bool = getattr(task, "full_feature_name", False)

        # Build plan locally — cloudpickle handles any Python callables (UDFs).
        builder = RayFeatureBuilder(registry, task.feature_view, task, self.config)
        execution_plan = builder.build()
        logger.debug(
            "ExecutionPlan built for '%s' (%d nodes).",
            task.feature_view.name,
            len(execution_plan.nodes),
        )

        logger.info(
            "KubeRay Job mode: submitting historical features RayJob '%s' "
            "for feature view '%s' (%d refs).",
            job_name,
            task.feature_view.name,
            len(feature_refs),
        )

        submitted_name, output_path = submitter.submit_historical_features_job(
            job_name=job_name,
            execution_plan=execution_plan,
            feature_refs=feature_refs,
            full_feature_names=full_feature_names,
            entity_df=task.entity_df,
        )

        logger.info(
            "KubeRay historical features job '%s' submitted. Results → '%s'.",
            submitted_name,
            output_path,
        )

        return KubeRayRetrievalJob(
            job_name=submitted_name,
            output_path=output_path,
            submitter=submitter,
            config=self.repo_config,
            full_feature_names=task.full_feature_name,
            on_demand_feature_views=getattr(task, "on_demand_feature_views", None),
            feature_refs=feature_refs,
            timeout=self.config.ray_job_active_deadline_seconds,
        )
