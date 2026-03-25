import logging

from prometheus_client import Counter, Gauge, Histogram

logger = logging.getLogger(__name__)

SPARK_K8S_JOBS_TOTAL = Counter(
    "feast_spark_k8s_jobs_total",
    "Total Spark Kubernetes jobs submitted",
    ["status", "feature_view", "execution_mode"],
)

SPARK_K8S_JOB_DURATION = Histogram(
    "feast_spark_k8s_job_duration_seconds",
    "Spark Kubernetes job execution duration in seconds",
    ["feature_view", "operation"],
    buckets=(1, 5, 10, 30, 60, 120, 300, 600, 1800, 3600),
)

SPARK_K8S_ACTIVE_JOBS = Gauge(
    "feast_spark_k8s_active_jobs",
    "Currently running Spark Kubernetes jobs",
    ["namespace"],
)

SPARK_K8S_CLUSTER_CONNECTIVITY = Gauge(
    "feast_spark_k8s_cluster_connectivity",
    "Spark Kubernetes cluster connectivity status (1=connected, 0=disconnected)",
    ["cluster_address"],
)
