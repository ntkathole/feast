from feast.infra.compute_engines.spark.byos.metrics import (
    BYOS_ACTIVE_JOBS,
    BYOS_CLUSTER_CONNECTIVITY,
    BYOS_JOB_DURATION,
    BYOS_JOBS_TOTAL,
)


def test_jobs_total_counter_exists():
    assert BYOS_JOBS_TOTAL._name == "feast_byos_spark_jobs_total"


def test_jobs_total_has_correct_labels():
    assert sorted(BYOS_JOBS_TOTAL._labelnames) == sorted(
        ["status", "feature_view", "execution_mode"]
    )


def test_job_duration_histogram_exists():
    assert (
        BYOS_JOB_DURATION._name
        == "feast_byos_spark_job_duration_seconds"
    )


def test_job_duration_has_correct_labels():
    assert sorted(BYOS_JOB_DURATION._labelnames) == sorted(
        ["feature_view", "operation"]
    )


def test_job_duration_has_custom_buckets():
    assert BYOS_JOB_DURATION._kwargs["upper_bound"] == 3600


def test_active_jobs_gauge_exists():
    assert BYOS_ACTIVE_JOBS._name == "feast_byos_spark_active_jobs"


def test_active_jobs_has_namespace_label():
    assert BYOS_ACTIVE_JOBS._labelnames == ("namespace",)


def test_cluster_connectivity_gauge_exists():
    assert (
        BYOS_CLUSTER_CONNECTIVITY._name
        == "feast_byos_spark_cluster_connectivity"
    )


def test_cluster_connectivity_has_correct_label():
    assert BYOS_CLUSTER_CONNECTIVITY._labelnames == (
        "cluster_address",
    )


def test_jobs_total_can_increment():
    BYOS_JOBS_TOTAL.labels(
        status="succeeded",
        feature_view="test_view",
        execution_mode="remote",
    ).inc()


def test_job_duration_can_observe():
    BYOS_JOB_DURATION.labels(
        feature_view="test_view",
        operation="materialize",
    ).observe(1.5)


def test_active_jobs_can_set():
    BYOS_ACTIVE_JOBS.labels(namespace="default").set(3)


def test_cluster_connectivity_can_set():
    BYOS_CLUSTER_CONNECTIVITY.labels(
        cluster_address="k8s://https://my-cluster"
    ).set(1)
