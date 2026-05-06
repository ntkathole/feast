"""Configuration for Ray compute engine."""

from datetime import timedelta
from typing import Any, Dict, List, Literal, Optional

from pydantic import StrictStr

from feast.repo_config import FeastConfigBaseModel


class RayComputeEngineConfig(FeastConfigBaseModel):
    """Configuration for Ray Compute Engine."""

    type: Literal["ray.engine"] = "ray.engine"
    """Ray Compute Engine type selector"""

    ray_address: Optional[str] = None
    """Ray cluster address. If None, uses local Ray cluster."""

    staging_location: Optional[StrictStr] = None
    """Remote path for batch materialization jobs"""

    # Ray-specific performance configurations
    broadcast_join_threshold_mb: int = 100
    """Threshold for using broadcast joins (in MB)"""

    enable_distributed_joins: bool = True
    """Whether to enable distributed joins for large datasets"""

    max_parallelism_multiplier: int = 2
    """Multiplier for max parallelism based on available CPUs"""

    target_partition_size_mb: int = 64
    """Target partition size in MB"""

    window_size_for_joins: str = "1H"
    """Window size for windowed temporal joins"""

    ray_conf: Optional[Dict[str, Any]] = None
    """Ray configuration parameters"""

    # Additional configuration options
    max_workers: Optional[int] = None
    """Maximum number of Ray workers for transformation and join nodes.
    If None, Ray uses all available cores."""

    write_concurrency: Optional[int] = None
    """Concurrency for the RayWriteNode's map_batches call (online-store writes).
    If None, falls back to max_workers, then 1 (safe default
    for single-file stores).

    Example - SQLite online store (default for local deployments):
      write_concurrency: 1

    Example - Redis / DynamoDB online store (supports parallel writes):
      write_concurrency: 8
    """

    enable_optimization: bool = True
    """Enable automatic performance optimizations."""

    # Worker task resource configuration
    num_gpus: Optional[float] = None
    """Number of GPUs to request per worker task. Requires GPU nodes in the
    Ray cluster. Fractional values (e.g. 0.5) are supported by Ray for GPU
    sharing. Supported in all modes: local, remote, and KubeRay."""

    gpu_batch_format: str = "pandas"
    """Batch format for map_batches when num_gpus is set. Use 'numpy' or
    'pyarrow' for GPU-native libraries (e.g. cuDF, PyTorch). Defaults to
    'pandas'."""

    worker_task_options: Optional[Dict[str, Any]] = None
    """Arbitrary Ray task options passed verbatim to @ray.remote .options()
    and map_batches for every worker task Feast dispatches. This is the
    escape hatch for any Ray or CodeFlare SDK scheduling parameter not
    covered by the dedicated fields above.

    Pairs with ray_conf (which configures ray.init) — worker_task_options
    targets the individual worker tasks rather than the cluster connection.

    Common keys (see https://docs.ray.io/en/latest/ray-core/api/doc/ray.remote_function.RemoteFunction.options.html):
      num_cpus          (float)  – CPUs per task (default: 1)
      memory            (int)    – Heap memory in bytes (e.g. 8 * 1024**3 for 8 GB)
      accelerator_type  (str)    – Specific GPU model, e.g. 'A100', 'T4', 'V100'.
                                   Pins tasks to nodes advertising that type. Useful
                                   on KubeRay clusters with mixed GPU pools.
      resources         (dict)   – Custom/extended resource labels, e.g.
                                   {'intel.com/gpu': 1} for Kubernetes extended resources.
      runtime_env       (dict)   – Per-task runtime environment (pip, conda, env_vars,
                                   working_dir, …). For KubeRay use this to install
                                   extra packages on workers without rebuilding images.
      max_retries       (int)    – Task retry count on worker failure (default: 3).
      scheduling_strategy (str)  – 'DEFAULT', 'SPREAD', or a placement group strategy.

    Example:
      worker_task_options:
        num_cpus: 4
        memory: 8589934592       # 8 GB
        accelerator_type: "A100"
        max_retries: 5
        runtime_env:
          pip: ["cudf-cu12==24.10.0"]
          env_vars: {CUDA_VISIBLE_DEVICES: "0"}
    """

    @property
    def window_size_timedelta(self) -> timedelta:
        """Convert window size string to timedelta."""
        if self.window_size_for_joins.endswith("H"):
            hours = int(self.window_size_for_joins[:-1])
            return timedelta(hours=hours)
        elif self.window_size_for_joins.endswith("min"):
            minutes = int(self.window_size_for_joins[:-3])
            return timedelta(minutes=minutes)
        elif self.window_size_for_joins.endswith("s"):
            seconds = int(self.window_size_for_joins[:-1])
            return timedelta(seconds=seconds)
        else:
            # Default to 1 hour
            return timedelta(hours=1)

    # KubeRay/CodeFlare SDK configurations
    use_kuberay: Optional[bool] = None
    """Whether to use KubeRay/CodeFlare SDK for Ray cluster management.

    Behaviour depends on whether `cluster_name` is also set:
    - use_kuberay=True + cluster_name set  → connect to an existing RayCluster
      (requires auth_token / auth_server, as before).
    - use_kuberay=True + no cluster_name   → **ephemeral RayJob mode**: Feast
      automatically creates a temporary RayCluster via a KubeRay RayJob CR,
      runs the materialisation job on it, and tears it down afterwards.
      No pre-existing cluster or auth credentials are required.
    """

    cluster_name: Optional[str] = None
    """Name of the KubeRay cluster to connect to.

    Required only when connecting to an existing cluster (use_kuberay=True with
    a pre-provisioned cluster).  Leave unset to enable automatic ephemeral
    cluster creation via RayJob.
    """

    auth_token: Optional[str] = None
    """Authentication token for Ray cluster connection (for secure clusters).
    Only required when connecting to an existing cluster."""

    kuberay_conf: Optional[Dict[str, Any]] = None
    """KubeRay/CodeFlare configuration parameters (passed to CodeFlare SDK)"""

    # ------------------------------------------------------------------ #
    # Ephemeral RayJob cluster settings (use_kuberay=True, no cluster_name)
    # ------------------------------------------------------------------ #

    namespace: Optional[str] = None
    """Kubernetes namespace in which to create the RayJob / connect to an
    existing cluster.  Falls back to kuberay_conf['namespace'] and then
    'default'."""

    local_queue: Optional[str] = None
    """Kueue LocalQueue name.  When set, the RayJob CR is annotated with the
    queue label so that Kueue can schedule it according to quota policies.
    Example: 'feast-materialization-queue'"""

    rayjob_cluster_config: Optional[Dict[str, Any]] = None
    """Resource sizing for the ephemeral RayCluster created by the RayJob.
    Keys map directly to codeflare_sdk.ManagedClusterConfig fields:

      image                   (str)       – Container image for head + workers.
                                            Example: 'quay.io/rhoai/ray:2.35.0-py311'
      num_workers             (int)       – Number of worker replicas (default: 1).
      worker_cpu_requests     (int|str)   – e.g. 2  or "2000m"
      worker_cpu_limits       (int|str)   – e.g. 4
      worker_memory_requests  (int|str)   – e.g. "4Gi"
      worker_memory_limits    (int|str)   – e.g. "8Gi"
      head_cpu_requests       (int|str)   – e.g. 1
      head_cpu_limits         (int|str)   – e.g. 2
      head_memory_requests    (int|str)   – e.g. "4Gi"
      head_memory_limits      (int|str)   – e.g. "8Gi"
      worker_accelerators     (dict)      – e.g. {"nvidia.com/gpu": 1}
      head_accelerators       (dict)      – e.g. {}
      envs                    (dict)      – extra env vars for cluster pods.
      labels                  (dict)      – extra K8s labels for the cluster.
      annotations             (dict)      – extra K8s annotations.

    If omitted, sensible defaults are applied (1 worker, 2 CPUs, 4 Gi memory).
    """

    ray_image: Optional[str] = None
    """Container image shorthand.  Equivalent to rayjob_cluster_config['image'].
    When both are set, rayjob_cluster_config['image'] takes precedence."""

    ray_job_ttl_seconds: int = 300
    """Seconds to wait before the KubeRay operator deletes the RayJob CR
    (and its child RayCluster) after the job finishes.  Set to 0 to delete
    immediately on completion."""

    ray_job_active_deadline_seconds: Optional[int] = None
    """Hard upper bound (seconds) on how long the RayJob may run before the
    operator forcibly terminates it.  Useful as a safety net for stuck jobs."""

    extra_pip_packages: Optional[List[str]] = None
    """Pip packages to install in the ephemeral RayJob runtime environment.

    Feast is a **prerequisite** and must be available in the Ray runtime:

    * **Pre-installed image** (recommended): build a custom image that
      includes feast, e.g. ``RUN pip install feast==<version>`` in a
      Dockerfile derived from the RHOAI Ray base image.
    * **Runtime install**: add feast to this list, pinned to the exact
      version running on the driver to guarantee dill compatibility::

          extra_pip_packages:
            - "feast==0.50.0"   # must match driver version exactly

    Additional domain packages (OCR, ML, etc.) are listed here regardless
    of how feast itself is provided::

        extra_pip_packages:
          - "docling"
          - "transformers"
          - "datasets"
    """

    feature_repo_dir: Optional[str] = None
    """Local path to the feature repo directory.

    When set, the entire directory is packaged by the CodeFlare SDK and
    uploaded to the ephemeral RayCluster as the ``working_dir`` of the
    RayJob ``runtime_env``.  This makes the following resources available
    inside every pod without any manual upload:

    * The feature registry file (e.g. ``data/registry.db``)
    * Feature-definition modules (e.g. ``cheque_features.py``)
    * Any other local artifacts referenced by relative paths in
      ``feature_store.yaml``

    Ray automatically adds ``working_dir`` to ``PYTHONPATH``, so feature
    definition modules are importable by the materialisation runner.

    Use ``"."`` to package the current working directory (i.e. the directory
    from which ``feast materialize`` is run), or supply an explicit path:

    .. code-block:: yaml

        batch_engine:
          type: ray.engine
          use_kuberay: true
          feature_repo_dir: "."          # package cwd (feature_repo/)

    Note: any local absolute paths in ``ray_conf.runtime_env.env_vars``
    (e.g. a PYTHONPATH pointing to the local machine) are automatically
    stripped from the in-job runtime_env to avoid breakage on the cluster.
    """

    entity_df_inline_threshold_mb: float = 0.5
    """Maximum entity DataFrame size (MiB) to embed inline in the RayJob
    environment.  DataFrames larger than this threshold are written to
    ``staging_location`` and referenced by path.

    Applies to ``get_historical_features`` in KubeRay Job mode only.
    """

    enable_ray_logging: bool = False
    """Enable Ray progress bars and verbose logging."""
