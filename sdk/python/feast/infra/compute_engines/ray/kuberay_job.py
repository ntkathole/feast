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
KubeRay Job submitter for Feast – ephemeral cluster lifecycle management.

Design
------
**Orchestration stays on the driver.  The cluster only does distributed compute.**

When ``use_kuberay: true`` and no ``cluster_name`` is set, Feast:

  1. Builds the full ``ExecutionPlan`` (DAG) and ``ExecutionContext`` on the
     driver — same code path as local/connected-cluster mode.
  2. dill-serialises both to ``_feast_plan.pkl`` / ``_feast_context.pkl``
     and stages them into ``working_dir``.
  3. Submits a ``RayJob`` CR whose entrypoint is ``_feast_runner.py``.

On the cluster, the runner does exactly two things:

  1. ``plan  = dill.load("_feast_plan.pkl")``
  2. ``context = dill.load("_feast_context.pkl")``
  3. ``plan.execute(context)``   ← Ray Data distributes tasks to workers

No ``FeatureStore`` reconstruction, no JSON parsing, no protobuf decoding.
The cluster is purely a distributed compute resource.

Prerequisites
~~~~~~~~~~~~~
Feast must be available in the Ray runtime environment (pre-installed in the
image or listed in ``extra_pip_packages``).

``staging_location`` is required for ``get_historical_features`` (entity
DataFrame transfer and result retrieval).
"""

import importlib
import logging
import time
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from feast.infra.compute_engines.ray.config import RayComputeEngineConfig

if TYPE_CHECKING:
    from feast.infra.compute_engines.dag.plan import ExecutionPlan

logger = logging.getLogger(__name__)

# ── Timing constants ───────────────────────────────────────────────────────────
_POLL_INTERVAL_SECONDS = 15
_DEFAULT_TIMEOUT_SECONDS = 3600  # 1 hour

# ── Staged file names (placed inside working_dir zip) ─────────────────────────
_STAGED_RUNNER_FILENAME = "_feast_runner.py"
_STAGED_PLAN_FILENAME = "_feast_plan.pkl"
# No separate context file — the context is rebuilt on the cluster from
# feature_store.yaml (already in working_dir) plus env-var task parameters.


class KubeRayJobSubmitter:
    """
    Manages the lifecycle of an ephemeral KubeRay RayJob for Feast operations.

    Public API:

    * :meth:`submit_materialization_job` — materialise a feature view.
    * :meth:`submit_historical_features_job` — run ``get_historical_features``
      on the cluster and write results to ``staging_location``.
    * :meth:`wait_for_job` — poll a submitted job until completion.

    Usage (materialization)::

        submitter = KubeRayJobSubmitter(config)
        job_name = submitter.submit_materialization_job(
            job_name="fm-driver-2501010000",
            execution_plan=plan,
            execution_context=context,
            repo_config=repo_config,
        )
        submitter.wait_for_job(job_name, timeout=1800)

    Usage (historical features)::

        job_name, output_path = submitter.submit_historical_features_job(
            job_name="fh-driver-2501010000",
            execution_plan=plan,
            execution_context=context,
            repo_config=repo_config,
            entity_df=entity_df,
        )
        submitter.wait_for_job(job_name, timeout=1800)
        result = pyarrow.parquet.read_table(output_path)
    """

    def __init__(self, config: RayComputeEngineConfig):
        self.config = config
        self._validate_codeflare_sdk()

    # ------------------------------------------------------------------ #
    # Public API – materialization
    # ------------------------------------------------------------------ #

    def submit_materialization_job(
        self,
        job_name: str,
        execution_plan: "ExecutionPlan",
        feature_view_name: str,
        start_time: Any,  # datetime
        end_time: Any,  # datetime
    ) -> str:
        """
        Submit a materialisation RayJob.

        The ``ExecutionPlan`` is cloudpickle-serialised (for any Python
        callables it contains) and staged into ``working_dir``.  Task
        parameters are passed as plain env vars — no context serialisation.

        On the cluster the runner:
        1. Loads the plan via cloudpickle.
        2. Reconstructs ``ExecutionContext`` from ``feature_store.yaml``
           (already in ``working_dir``) and the task env vars.
        3. Calls ``plan.execute(context)`` — Ray Data distributes to workers.

        Returns the RayJob CR name.
        """
        task_env_vars = {
            "FEAST_FEATURE_VIEW": feature_view_name,
            "FEAST_START_TIME": start_time.isoformat(),
            "FEAST_END_TIME": end_time.isoformat(),
        }
        return self._submit_job(
            job_name=job_name,
            op="materialize",
            execution_plan=execution_plan,
            task_env_vars=task_env_vars,
        )

    # ------------------------------------------------------------------ #
    # Public API – historical features
    # ------------------------------------------------------------------ #

    def submit_historical_features_job(
        self,
        job_name: str,
        execution_plan: "ExecutionPlan",
        feature_refs: List[str],
        full_feature_names: bool,
        entity_df: Any,  # pandas.DataFrame
    ) -> Tuple[str, str]:
        """
        Submit a historical feature retrieval RayJob.

        The entity DataFrame is staged to ``staging_location`` as parquet.
        Feature refs and options are passed as plain env vars.

        Returns ``(job_name, output_path)`` — poll with :meth:`wait_for_job`,
        then read ``output_path`` with ``pyarrow.parquet.read_table``.

        Raises:
            ValueError: If ``staging_location`` is not set in config.
        """
        if not self.config.staging_location:
            raise ValueError(
                "staging_location must be configured in batch_engine to use "
                "get_historical_features with KubeRay Job mode.  "
                "Example: staging_location: s3://my-bucket/feast-staging"
            )

        output_path = self._make_output_path(job_name)
        entity_df_path = self._stage_entity_df(entity_df, job_name)

        task_env_vars = {
            "FEAST_FEATURE_REFS": ",".join(feature_refs),
            "FEAST_FULL_FEATURE_NAMES": "true" if full_feature_names else "false",
        }

        self._submit_job(
            job_name=job_name,
            op="historical_features",
            execution_plan=execution_plan,
            task_env_vars=task_env_vars,
            entity_df_path=entity_df_path,
            output_path=output_path,
        )

        return job_name, output_path

    # ------------------------------------------------------------------ #
    # Public API – polling
    # ------------------------------------------------------------------ #

    def wait_for_job(
        self,
        job_name: str,
        timeout: Optional[int] = None,
    ) -> None:
        """
        Poll the RayJob status until it completes or times out.

        Raises:
            RuntimeError: If the job fails or times out.
        """
        from codeflare_sdk import RayJob
        from codeflare_sdk.ray.rayjobs.status import CodeflareRayJobStatus

        namespace = self._resolve_namespace()
        deadline = time.monotonic() + (timeout or _DEFAULT_TIMEOUT_SECONDS)

        job = RayJob(
            job_name=job_name,
            entrypoint="",
            cluster_name=job_name,
            namespace=namespace,
        )

        while True:
            status, _ = job.status(print_to_console=False)

            if status == CodeflareRayJobStatus.COMPLETE:
                logger.info("RayJob '%s' completed successfully.", job_name)
                return

            if status == CodeflareRayJobStatus.FAILED:
                raise RuntimeError(
                    f"RayJob '{job_name}' failed.  Check the KubeRay operator "
                    f"logs and RayJob events in namespace '{namespace}'."
                )

            if status == CodeflareRayJobStatus.SUSPENDED:
                logger.info(
                    "RayJob '%s' is suspended (queue='%s'). Waiting…",
                    job_name,
                    self.config.local_queue or "N/A",
                )

            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Timed out waiting for RayJob '{job_name}' after "
                    f"{timeout or _DEFAULT_TIMEOUT_SECONDS}s."
                )

            time.sleep(_POLL_INTERVAL_SECONDS)

    # ------------------------------------------------------------------ #
    # Internal – core submission
    # ------------------------------------------------------------------ #

    def _submit_job(
        self,
        job_name: str,
        op: str,
        execution_plan: "ExecutionPlan",
        task_env_vars: Dict[str, str],
        entity_df_path: Optional[str] = None,
        output_path: Optional[str] = None,
    ) -> str:
        """
        Serialise the plan, stage it into working_dir, submit a RayJob.

        The ``ExecutionContext`` is NOT serialised.  The cluster already has
        everything it needs: ``feature_store.yaml`` (via ``working_dir``) and
        task parameters (via ``task_env_vars``).  cloudpickle is used for the
        plan — it is a Ray core dependency, always present on the cluster.

        ``task_env_vars`` carries operation-specific parameters:
          - ``FEAST_FEATURE_VIEW``, ``FEAST_START_TIME``, ``FEAST_END_TIME``
            for materialisation.
          - ``FEAST_FEATURE_REFS``, ``FEAST_FULL_FEATURE_NAMES`` for
            historical features.
        """
        import os
        import shutil

        from codeflare_sdk import RayJob

        namespace = self._resolve_namespace()
        working_dir = self._resolve_working_dir()

        if not working_dir:
            raise ValueError(
                "feature_repo_dir must be set in batch_engine config.  "
                "It becomes the working_dir that packages the feature repo "
                "(registry, feature definitions) and the serialised plan."
            )

        # ── Serialise the execution plan (cloudpickle, Ray's own serialiser) ─
        plan_path = os.path.join(working_dir, _STAGED_PLAN_FILENAME)
        self._stage_plan(execution_plan, plan_path)
        logger.info("ExecutionPlan serialised → '%s'.", _STAGED_PLAN_FILENAME)

        # ── Stage the runner script ──────────────────────────────────────────
        runner_src = os.path.join(os.path.dirname(__file__), "_kuberay_dag_runner.py")
        runner_dst = os.path.join(working_dir, _STAGED_RUNNER_FILENAME)
        shutil.copy2(runner_src, runner_dst)

        # ── Build runtime environment ────────────────────────────────────────
        env_vars: Dict[str, str] = {
            "FEAST_RUNNER_OP": op,
            "FEAST_PLAN_FILE": _STAGED_PLAN_FILENAME,
            # Force local Ray mode — prevents recursive KubeRay job submission.
            "FEAST_RAY_EXECUTION_MODE": "local",
            "RAY_DISABLE_IMPORT_WARNING": "1",
            # Merge runtime_env instead of erroring on duplicate env vars.
            "RAY_OVERRIDE_JOB_RUNTIME_ENV": "1",
        }
        env_vars.update(task_env_vars)

        if entity_df_path:
            env_vars["FEAST_ENTITY_DF_PATH"] = entity_df_path
        if output_path:
            env_vars["FEAST_OUTPUT_PATH"] = output_path

        extra_envs = (self.config.rayjob_cluster_config or {}).get("envs", {})
        if extra_envs:
            env_vars.update(extra_envs)

        pip_packages: List[str] = list(self.config.extra_pip_packages or [])
        runtime_env: Dict[str, Any] = {
            "pip": pip_packages,
            "env_vars": env_vars,
            "working_dir": os.path.abspath(working_dir),
        }

        logger.info(
            "Submitting RayJob '%s' (op=%s, namespace=%s, "
            "working_dir=%s, extra_pip=%s).",
            job_name,
            op,
            namespace,
            os.path.abspath(working_dir),
            pip_packages or "[]",
        )

        staged_files = [plan_path, runner_dst]
        try:
            job = RayJob(
                job_name=job_name,
                entrypoint=f"python {_STAGED_RUNNER_FILENAME}",
                cluster_config=self._build_managed_cluster_config(),
                namespace=namespace,
                runtime_env=runtime_env,
                ttl_seconds_after_finished=self.config.ray_job_ttl_seconds,
                active_deadline_seconds=self.config.ray_job_active_deadline_seconds,
                local_queue=self.config.local_queue,
            )
            job.submit()
        finally:
            for f in staged_files:
                if os.path.exists(f):
                    os.remove(f)
                    logger.debug("Removed staged file: %s", f)

        logger.info(
            "RayJob '%s' submitted%s.",
            job_name,
            f" (queue: '{self.config.local_queue}')" if self.config.local_queue else "",
        )
        self._maybe_unsuspend(job_name, namespace)
        return job_name

    # ------------------------------------------------------------------ #
    # Internal – plan serialisation
    # ------------------------------------------------------------------ #

    @staticmethod
    def _stage_plan(plan: "ExecutionPlan", plan_path: str) -> None:
        """
        Serialise the ``ExecutionPlan`` to *plan_path* using cloudpickle.

        cloudpickle is a Ray dependency — it is always available wherever
        Ray is installed.  It handles closures, lambdas, and dynamically-
        defined functions (on-demand feature view UDFs, transformations),
        which are the only objects in the plan that cannot be reimported.

        The ``ExecutionContext`` is NOT serialised here.  The cluster already
        has everything it needs to reconstruct it:
        - ``feature_store.yaml`` is shipped in ``working_dir`` by the SDK.
        - Task parameters (feature_view, start_time, end_time) are env vars.
        - ``FEAST_RAY_EXECUTION_MODE=local`` prevents recursive job submission.
        """
        import cloudpickle

        with open(plan_path, "wb") as fh:
            cloudpickle.dump(plan, fh)

    # ------------------------------------------------------------------ #
    # Internal – entity DataFrame staging
    # ------------------------------------------------------------------ #

    def _stage_entity_df(self, entity_df: Any, job_name: str) -> str:
        """Write entity DataFrame to ``staging_location`` as parquet and return the path."""
        import os

        staging = self.config.staging_location
        path = os.path.join(staging, f"entities_{job_name}.parquet").replace("\\", "/")

        if path.startswith(("s3://", "gs://", "abfss://")):
            import pyarrow as pa
            import pyarrow.parquet as pq

            pq.write_table(pa.Table.from_pandas(entity_df, preserve_index=False), path)
        else:
            import io

            os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            buf = io.BytesIO()
            entity_df.to_parquet(buf, index=False)
            buf.seek(0)
            with open(path, "wb") as fh:
                fh.write(buf.read())

        logger.info("Entity DataFrame staged to '%s'.", path)
        return path

    def _make_output_path(self, job_name: str) -> str:
        """Return the staging path for the job's result parquet."""
        import os

        return os.path.join(
            self.config.staging_location, f"output_{job_name}.parquet"
        ).replace("\\", "/")

    # ------------------------------------------------------------------ #
    # Internal – Kueue auto-unsuspend
    # ------------------------------------------------------------------ #

    def _maybe_unsuspend(self, job_name: str, namespace: str) -> None:
        """
        Unsuspend a RayJob when Kueue CRDs exist but no controller is running.

        CodeFlare SDK auto-adds ``spec.suspend=true`` when Kueue CRDs are
        detected.  If no Kueue controller is running the job hangs indefinitely.
        This method detects that condition and patches ``spec.suspend=false``.
        """
        _CHECK_WAIT = 8
        _CHECK_TIMEOUT = 30

        try:
            import kubernetes as k8s

            k8s.config.load_kube_config()
            custom = k8s.client.CustomObjectsApi()

            deadline = time.monotonic() + _CHECK_TIMEOUT
            while time.monotonic() < deadline:
                time.sleep(_CHECK_WAIT)

                try:
                    job = custom.get_namespaced_custom_object(
                        group="ray.io",
                        version="v1",
                        namespace=namespace,
                        plural="rayjobs",
                        name=job_name,
                    )
                except Exception:
                    return

                if not job.get("spec", {}).get("suspend", False):
                    return  # already running

                if self._kueue_workload_exists(custom, namespace, job_name):
                    logger.info(
                        "Kueue Workload found for '%s' — deferring to Kueue.", job_name
                    )
                    return

                logger.warning(
                    "RayJob '%s' is suspended but no Kueue Workload exists. "
                    "Auto-unsuspending (Kueue controller appears inactive).",
                    job_name,
                )
                custom.patch_namespaced_custom_object(
                    group="ray.io",
                    version="v1",
                    namespace=namespace,
                    plural="rayjobs",
                    name=job_name,
                    body={"spec": {"suspend": False}},
                )
                logger.info("RayJob '%s' unsuspended.", job_name)
                return

        except Exception as exc:
            logger.debug("Could not check/unsuspend '%s': %s", job_name, exc)

    @staticmethod
    def _kueue_workload_exists(custom_api: Any, namespace: str, job_name: str) -> bool:
        try:
            workloads = custom_api.list_namespaced_custom_object(
                group="kueue.x-k8s.io",
                version="v1beta1",
                namespace=namespace,
                plural="workloads",
            )
            for wl in workloads.get("items", []):
                for ref in wl.get("metadata", {}).get("ownerReferences", []):
                    if ref.get("name") == job_name:
                        return True
        except Exception:
            pass
        return False

    # ------------------------------------------------------------------ #
    # Internal – cluster config helpers
    # ------------------------------------------------------------------ #

    def _validate_codeflare_sdk(self) -> None:
        try:
            importlib.import_module("codeflare_sdk")
        except ImportError:
            raise ImportError(
                "The 'codeflare_sdk' package is required for ephemeral KubeRay "
                "job execution (use_kuberay=True without cluster_name).  "
                "Install with: pip install codeflare-sdk"
            ) from None

    def _resolve_namespace(self) -> str:
        import os

        return (
            os.getenv("FEAST_RAY_NAMESPACE")
            or self.config.namespace
            or (self.config.kuberay_conf or {}).get("namespace")
            or "default"
        )

    def _resolve_working_dir(self) -> Optional[str]:
        import os

        if not self.config.feature_repo_dir:
            return None
        return os.path.abspath(self.config.feature_repo_dir)

    def _resolve_image(self) -> str:
        c = self.config.rayjob_cluster_config or {}
        return c.get("image") or self.config.ray_image or ""

    def _build_managed_cluster_config(self) -> Any:
        from codeflare_sdk import ManagedClusterConfig

        c = self.config.rayjob_cluster_config or {}
        image = self._resolve_image()

        worker_accelerators: Dict[str, Any] = dict(
            c.get("worker_accelerators", {}) or {}
        )
        if not worker_accelerators and self.config.num_gpus:
            worker_accelerators["nvidia.com/gpu"] = int(self.config.num_gpus)

        kwargs: Dict[str, Any] = dict(
            num_workers=c.get("num_workers", 1),
            worker_cpu_requests=c.get("worker_cpu_requests", 2),
            worker_cpu_limits=c.get("worker_cpu_limits", 4),
            worker_memory_requests=c.get("worker_memory_requests", "4Gi"),
            worker_memory_limits=c.get("worker_memory_limits", "8Gi"),
            head_cpu_requests=c.get("head_cpu_requests", 1),
            head_cpu_limits=c.get("head_cpu_limits", 2),
            head_memory_requests=c.get("head_memory_requests", "4Gi"),
            head_memory_limits=c.get("head_memory_limits", "8Gi"),
            worker_accelerators=worker_accelerators,
            head_accelerators=c.get("head_accelerators", {}),
            envs=c.get("envs", {}),
            labels=c.get("labels", {}),
            annotations=c.get("annotations", {}),
        )
        if image:
            kwargs["image"] = image
        if c.get("image_pull_secrets"):
            kwargs["image_pull_secrets"] = c["image_pull_secrets"]

        return ManagedClusterConfig(**kwargs)


# ---------------------------------------------------------------------------
# Job name helper
# ---------------------------------------------------------------------------


def build_safe_job_name(
    feature_view_name: str,
    timestamp: Optional[datetime] = None,
    prefix: str = "fm",
) -> str:
    """
    Build a Kubernetes-safe RayJob name.

    Constraints:
    * Lowercase alphanumeric and ``-`` only.
    * Max **34 characters** (KubeRay internal volume naming limit).

    Format: ``{prefix}-{8-char-fv}-{YYMMDDHHmm}``  (e.g. ``fm-cheque-e-2501010000``).

    Args:
        feature_view_name: Feast feature view name.
        timestamp: Optional timestamp (defaults to UTC now).
        prefix: ``"fm"`` for materialization, ``"fh"`` for historical features.
    """
    import re

    ts = (timestamp or datetime.utcnow()).strftime("%y%m%d%H%M")
    sanitised = re.sub(r"[^a-z0-9-]", "-", feature_view_name.lower())
    sanitised = re.sub(r"-+", "-", sanitised).strip("-")[:8]
    return f"{prefix}-{sanitised}-{ts}"[:34]
