# Spark

## Description

Spark Compute Engine provides a distributed execution engine for batch materialization operations (`materialize` and `materialize-incremental`) and historical retrieval operations (`get_historical_features`).

It is designed to handle large-scale data processing and can be used with various offline stores, such as Snowflake, BigQuery, and Spark SQL.

### Design
The Spark Compute engine is implemented as a subclass of `feast.infra.compute_engine.ComputeEngine`.
Offline store is used to read and write data, while the Spark engine is used to perform transformations and aggregations on the data.
The engine supports the following features:
- Support for reading different data sources, such as Spark SQL, BigQuery, and Snowflake.
- Distributed execution of feature transformations and aggregations.
- Support for custom transformations using Spark SQL or UDFs.


## Example

{% code title="feature_store.yaml" %}
```yaml
...
offline_store:
  type: snowflake.offline
...
batch_engine:
  type: spark.engine
  partitions: 10 # number of partitions when writing to the online or offline store
  spark_conf:
    spark.master: "local[*]"
    spark.app.name: "Feast Spark Engine"
    spark.sql.shuffle.partitions: 100
    spark.executor.memory: "4g"
```
{% endcode %}

## Spark on Kubernetes Configuration

The Spark engine supports connecting to external Spark clusters on Kubernetes. This allows Feast to execute transformations, materialization, and historical retrieval on your own Spark infrastructure.

When Kubernetes mode is configured on the compute engine, the Spark offline store will automatically share the same remote SparkSession — no separate configuration is needed.

### Execution Modes

| Mode | Description |
|------|-------------|
| `local` | Default. Uses local SparkSession (existing behavior). |
| `remote` | Connects to an external Spark cluster via `k8s://` master URL. |
| `kubernetes` | Submits SparkApplication CRDs to the Spark Operator on Kubernetes. |

### Remote Mode Example

{% code title="feature_store.yaml" %}
```yaml
batch_engine:
  type: spark.engine
  execution_mode: remote
  master_url: k8s://https://kubernetes.default.svc
  namespace: feast-spark
  image: feast/spark:latest
  service_account_name: feast-spark
  executor_instances: 4
  executor_memory: 4g
  executor_cores: 2
  driver_memory: 2g
  driver_cores: 1
  job_timeout_seconds: 3600
  metrics_enabled: true
```
{% endcode %}

### Kubernetes Mode Example

{% code title="feature_store.yaml" %}
```yaml
batch_engine:
  type: spark.engine
  execution_mode: kubernetes
  namespace: feast-spark
  image: feast/spark:latest
  service_account_name: feast-spark
  executor_instances: 3
  executor_memory: 2g
  job_timeout_seconds: 1800
```
{% endcode %}

### Kubernetes Configuration Fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `execution_mode` | string | `local` | Execution mode: `local`, `remote`, or `kubernetes` |
| `master_url` | string | - | Spark master URL (`k8s://...`). Required for `remote` mode. |
| `namespace` | string | `default` | Kubernetes namespace for Spark jobs |
| `kubeconfig_path` | string | - | Path to kubeconfig file. Uses in-cluster auth if not set. |
| `image` | string | - | Default Spark container image. Required for `kubernetes` mode. |
| `driver_image` | string | - | Override image for the driver pod |
| `executor_image` | string | - | Override image for executor pods |
| `image_pull_secrets` | list | `[]` | Kubernetes image pull secret names |
| `service_account_name` | string | `""` | Kubernetes service account for Spark pods |
| `secrets` | list | `[]` | Kubernetes secrets to mount (name + mount_path) |
| `config_maps` | list | `[]` | ConfigMaps to mount (name + mount_path) |
| `executor_instances` | int | `2` | Number of executor instances |
| `executor_memory` | string | `1g` | Memory per executor |
| `executor_cores` | int | `1` | CPU cores per executor |
| `driver_memory` | string | `1g` | Memory for driver |
| `driver_cores` | int | `1` | CPU cores for driver |
| `job_timeout_seconds` | int | `3600` | Maximum job execution time |
| `metrics_enabled` | bool | `true` | Enable Prometheus metrics |

### Authentication

Spark on Kubernetes supports two authentication methods:

1. **In-cluster service account** (default when running inside K8s): Automatically uses the pod's mounted service account token.
2. **Kubeconfig file**: Set `kubeconfig_path` to your kubeconfig file for remote development and CI/CD.

### Offline Store Integration

When `execution_mode` is set to `remote`, the Spark offline store (`offline_store: spark`) automatically shares the same Kubernetes-connected SparkSession. This means both `batch_engine` and `offline_store` operations run on the same remote cluster without any additional configuration.

### Observability

When `metrics_enabled: true`, Spark on Kubernetes exposes Prometheus metrics:

- `feast_spark_k8s_jobs_total`: Total jobs submitted (counter)
- `feast_spark_k8s_job_duration_seconds`: Job duration (histogram)
- `feast_spark_k8s_active_jobs`: Currently running jobs (gauge)
- `feast_spark_k8s_cluster_connectivity`: Cluster connectivity status (gauge)

## Example in Python

{% code title="feature_store.py" %}
```python
from feast import FeatureStore, RepoConfig
from feast.repo_config import RegistryConfig
from feast.infra.online_stores.dynamodb import DynamoDBOnlineStoreConfig
from feast.infra.offline_stores.contrib.spark_offline_store.spark import SparkOfflineStoreConfig

repo_config = RepoConfig(
    registry="s3://[YOUR_BUCKET]/feast-registry.db",
    project="feast_repo",
    provider="aws",
    offline_store=SparkOfflineStoreConfig(
      spark_conf={
        "spark.ui.enabled": "false",
        "spark.eventLog.enabled": "false",
        "spark.sql.catalogImplementation": "hive",
        "spark.sql.parser.quotedRegexColumnNames": "true",
        "spark.sql.session.timeZone": "UTC"
      }
    ),
    batch_engine={
      "type": "spark.engine",
      "partitions": 10
    },
    online_store=DynamoDBOnlineStoreConfig(region="us-west-1"),
    entity_key_serialization_version=3
)

store = FeatureStore(config=repo_config)
```
{% endcode %}
