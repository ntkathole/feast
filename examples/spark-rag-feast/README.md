# Spark + RAG + Feast Pipeline Example

This example demonstrates an end-to-end Retrieval-Augmented Generation (RAG) pipeline using:
- **Feast** for feature management and serving
- **Spark** (via BYOS) for distributed embedding computation
- **Kubernetes** for orchestration via the Spark Operator

## Architecture

```
Documents → [Spark Ingest] → Chunks → [Spark Embed] → Embeddings → [Feast Materialize] → Online Store → [RAG Query]
```

1. **Ingest**: Read documents, chunk them into overlapping segments
2. **Embed**: Distribute sentence-transformer inference across Spark executors
3. **Materialize**: Store embeddings in Feast online store
4. **Query**: Retrieve embeddings, compute similarity, return relevant chunks

## Prerequisites

- Kubernetes cluster (Kind, Minikube, or managed K8s)
- [Spark Operator](https://github.com/kubeflow/spark-operator) installed
- Python 3.9+
- `feast[spark]` installed: `pip install 'feast[spark]'`
- `sentence-transformers` installed: `pip install sentence-transformers`

## Quick Start

### Option A: Remote SparkSession Mode

Uses `execution_mode: remote` — Feast connects directly to Spark on K8s.

```bash
# 1. Set up the Feast project
cd examples/spark-rag-feast
feast apply

# 2. Ingest documents
python pipeline/ingest.py \
  --input-dir /path/to/documents \
  --output-path data/document_chunks.parquet

# 3. Compute embeddings via Spark
python pipeline/embed.py \
  --input-path data/document_chunks.parquet \
  --output-path data/document_embeddings.parquet

# 4. Materialize to online store
feast materialize 2024-01-01T00:00:00 2025-12-31T23:59:59

# 5. Query
python pipeline/query.py --query "How does feature serving work?"
```

### Option B: Spark Operator Mode

Uses `execution_mode: operator` — Feast submits SparkApplication CRDs.

```bash
# 1. Create namespace and RBAC
kubectl apply -f k8s/feast-config.yaml

# 2. Update feature_store.yaml to use operator mode
# Set execution_mode: operator in feature_store.yaml

# 3. Apply Feast project
feast apply

# 4. Submit embedding job via Spark Operator
kubectl apply -f k8s/spark-application.yaml

# 5. Monitor job
kubectl get sparkapplications -n feast-rag
kubectl logs feast-rag-embed-driver -n feast-rag

# 6. Materialize (uses operator mode)
feast materialize 2024-01-01T00:00:00 2025-12-31T23:59:59

# 7. Query
python pipeline/query.py --query "How does feature serving work?"
```

## Configuration Reference

### feature_store.yaml

| Field | Description | Default |
|-------|-------------|---------|
| `execution_mode` | `local`, `remote`, or `operator` | `local` |
| `master_url` | K8s API server URL (required for remote) | - |
| `namespace` | K8s namespace for Spark jobs | `default` |
| `image` | Spark container image | - |
| `executor_instances` | Number of Spark executors | `2` |
| `executor_memory` | Memory per executor | `1g` |
| `job_timeout_seconds` | Max job runtime | `3600` |

See the [Spark Engine documentation](../../docs/reference/compute-engine/spark.md) for all options.

## Troubleshooting

**Connection refused**: Verify `master_url` points to your K8s API server. Run `kubectl cluster-info`.

**Pod not starting**: Check image pull secrets and service account permissions:
```bash
kubectl describe pod <pod-name> -n feast-rag
```

**Timeout errors**: Increase `job_timeout_seconds` or add more executor resources.

**OOM errors**: Increase `executor_memory` or reduce batch size in the embedding script.
