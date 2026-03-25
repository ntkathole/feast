import logging

from prometheus_client import Counter, Gauge, Histogram

logger = logging.getLogger(__name__)

BYOS_JOBS_TOTAL = Counter(
    "feast_byos_spark_jobs_total",
    "Total BYOS Spark jobs submitted",
    ["status", "feature_view", "execution_mode"],
)

BYOS_JOB_DURATION = Histogram(
    "feast_byos_spark_job_duration_seconds",
    "BYOS Spark job execution duration in seconds",
    ["feature_view", "operation"],
    buckets=(1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600),
)

BYOS_ACTIVE_JOBS = Gauge(
    "feast_byos_spark_active_jobs",
    "Currently running BYOS Spark jobs",
    ["namespace"],
)

BYOS_CLUSTER_CONNECTIVITY = Gauge(
    "feast_byos_spark_cluster_connectivity",
    "BYOS Spark cluster connectivity status (1=connected, 0=disconnected)",
    ["cluster_address"],
)
