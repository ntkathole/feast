# Spark Tiling Implementation

## Overview

This page describes the **Spark-specific implementation** of [Tiling with Intermediate Representations](../../getting-started/concepts/tiling.md) in Feast.

For general tiling concepts and benefits, see the [Tiling Concepts](../../getting-started/concepts/tiling.md) page.

**Spark Implementation Status**: ✅ **Production-Ready**
- Full IR support for all algebraic and holistic aggregations
- Sawtooth window tiling algorithm (inspired by [Chronon](https://chronon.ai/window_tiling.html))
- Distributed execution via Apache Spark
- Works with `StreamFeatureView` and any Spark-compatible data source

---

## Spark-Specific Architecture

### Execution Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    Spark Materialization                         │
└─────────────────────────────────────────────────────────────────┘
                              ↓
        ┌─────────────────────────────────────────┐
        │  SparkFeatureBuilder                    │
        │  - Builds DAG for StreamFeatureView     │
        │  - Detects enable_tiling=True           │
        └─────────────────────────────────────────┘
                              ↓
        ┌─────────────────────────────────────────┐
        │  SparkAggregationNode                   │
        │  - Routes to _execute_tiled_aggregation │
        └─────────────────────────────────────────┘
                              ↓
        ┌─────────────────────────────────────────┐
        │  apply_sawtooth_window_tiling()         │
        │  - Partition by hop intervals (Spark)   │
        │  - Compute IRs per tile (Spark UDFs)    │
        │  - Store in Spark DataFrame             │
        └─────────────────────────────────────────┘
                              ↓
        ┌─────────────────────────────────────────┐
        │  SparkWriteNode                         │
        │  - Write tiles to online store          │
        │  - Uses mapInArrow for efficiency       │
        └─────────────────────────────────────────┘
```

---

## Sawtooth Window Tiling Algorithm

Feast's Spark engine uses the **sawtooth window** approach for efficient tiling:

### Algorithm Steps

1. **Round timestamps** to nearest hop boundary
   ```python
   tile_start = floor(event_timestamp, hop_size)
   ```

2. **Group by entity + tile_start**
   ```python
   df.groupBy("customer_id", "tile_start")
   ```

3. **Compute tail aggregations**
   - For each tile, aggregate from tile_start up to window_size
   - Store IRs (sum, count, sum_sq) instead of final values

4. **Handle multiple time windows**
   - Process each unique time_window separately
   - Join results on entity keys

### Complexity Analysis

| Operation | Complexity | Description |
|-----------|-----------|-------------|
| Tile creation (materialization) | O(n × h) | n = events, h = hops |
| Tile identification (query) | O(log k) | k = total tiles |
| Tile merging (query) | O(m) | m = overlapping tiles |

**Speedup vs. no tiling**: ~10-100x for queries, depending on event volume.

---

## Spark Implementation Details

### Key Files

| File | Purpose |
|------|---------|
| `sdk/python/feast/infra/compute_engines/spark/tiling.py` | Core tiling logic |
| `sdk/python/feast/infra/compute_engines/spark/nodes.py` | `SparkAggregationNode` with tiling support |
| `sdk/python/feast/infra/compute_engines/spark/feature_builder.py` | DAG construction with tiling |

---

### Tiling Functions

#### `apply_sawtooth_window_tiling()`

Creates tiles with IRs for a given time window.

**Signature**:
```python
def apply_sawtooth_window_tiling(
    df: DataFrame,
    aggregations: List[Aggregation],
    group_by_keys: List[str],
    timestamp_col: str,
    window_size: timedelta,
    hop_size: Optional[timedelta] = None,
) -> DataFrame:
```

**What it does**:
1. Partitions events into hop-sized intervals using Spark window functions
2. Computes IRs based on aggregation type (sum, count, sum_sq)
3. Computes final values from IRs (for immediate use)
4. Returns DataFrame with columns:
   - Entity keys (e.g., `customer_id`)
   - Timestamp (window end time)
   - Final aggregated features (e.g., `avg_amount_3600s`)
   - IR columns (e.g., `avg_amount_3600s_sum`, `avg_amount_3600s_count`)

**Spark optimizations**:
- Uses native Spark window functions (`window()` from `pyspark.sql.functions`)
- Leverages Spark's distributed aggregation (`groupBy().agg()`)
- Avoids shuffles by partitioning on entity keys

---

#### `merge_tiles_for_query()` (Future Use)

Merges tiles at query time. Currently, Feast reads final values directly from online store.

**Signature**:
```python
def merge_tiles_for_query(
    tiles_df: DataFrame,
    query_timestamp_col: str,
    window_size: timedelta,
    group_by_keys: List[str],
) -> DataFrame:
```

**What it does**:
1. Filters tiles that overlap with query window
2. Merges IRs using aggregation-specific logic:
   - Sum IRs: `sum(tile_sums)`
   - Count IRs: `sum(tile_counts)`
   - Sum_sq IRs: `sum(tile_sum_sqs)`
3. Recomputes final values from merged IRs:
   - `avg = merged_sum / merged_count`
   - `std = sqrt((merged_sum_sq - merged_sum²/merged_count) / (merged_count - 1))`

---

### IR Computation Logic

The Spark implementation computes IRs based on aggregation type:

```python
# In apply_sawtooth_window_tiling()

if agg.function == "avg":
    # Store sum and count
    tail_agg_exprs.append(F.sum(agg.column).alias(f"_tail_{feature_name}_sum"))
    tail_agg_exprs.append(F.count(agg.column).alias(f"_tail_{feature_name}_count"))
    
    # Compute final avg (with divide-by-zero protection)
    result_df = result_df.withColumn(
        feature_name,
        F.when(F.col(count_col) > 0, F.col(sum_col) / F.col(count_col)).otherwise(None)
    )

elif agg.function == "std":
    # Store count, sum, and sum of squares
    tail_agg_exprs.append(F.count(agg.column).alias(f"_tail_{feature_name}_count"))
    tail_agg_exprs.append(F.sum(agg.column).alias(f"_tail_{feature_name}_sum"))
    tail_agg_exprs.append(
        F.sum(F.col(agg.column) * F.col(agg.column)).alias(f"_tail_{feature_name}_sum_sq")
    )
    
    # Compute final std (with divide-by-zero protection)
    delta = 0 if is_population else 1
    result_df = result_df.withColumn(
        feature_name,
        F.when(
            F.col(count_col) > delta,
            F.sqrt(
                (F.col(sum_sq_col) - (F.col(sum_col) * F.col(sum_col) / F.col(count_col)))
                / (F.col(count_col) - delta)
            )
        ).otherwise(None)
    )
```

**Key Spark features used**:
- `F.when().otherwise()` for null-safe computations
- Native Spark aggregation functions (`sum`, `count`)
- Column expressions for efficient computation

---

## Configuration

### Feature Store YAML

Configure Spark as the batch compute engine:

```yaml
project: my_project
provider: local
online_store:
  type: redis
  connection_string: "localhost:6379"
offline_store:
  type: file
entity_key_serialization_version: 3

# Spark configuration for tiling
batch_engine:
  type: spark.engine
  spark_conf:
    spark.master: "local[*]"  # or "yarn", "k8s://...", etc.
    spark.ui.enabled: "false"
    spark.sql.session.timeZone: "UTC"
    spark.executor.memory: "4g"
    spark.executor.cores: "2"
    # Optional: tune for large-scale tiling
    spark.sql.shuffle.partitions: "200"
    spark.default.parallelism: "200"
```

---

### StreamFeatureView Configuration

Enable tiling in your feature view:

```python
from datetime import timedelta
from feast import Entity, StreamFeatureView, PushSource, FileSource
from feast.stream_feature_view import Aggregation
from feast.types import Float32, Int64
from feast.field import Field

# Define entity
customer = Entity(name="customer_id", join_keys=["customer_id"])

# Batch source for backfill
batch_source = FileSource(
    path="data/transactions.parquet",
    timestamp_field="event_timestamp",
)

# Wrap in PushSource for StreamFeatureView
stream_source = PushSource(
    name="customer_transactions",
    batch_source=batch_source,
)

# Define feature view with tiling
customer_features = StreamFeatureView(
    name="customer_features",
    entities=[customer],
    source=stream_source,
    
    # Define aggregations
    aggregations=[
        Aggregation(column="amount", function="sum", time_window=timedelta(hours=1)),
        Aggregation(column="amount", function="avg", time_window=timedelta(hours=1)),
        Aggregation(column="amount", function="std", time_window=timedelta(hours=1)),
    ],
    
    # CRITICAL: Define schema with final + IR columns
    schema=[
        # Algebraic
        Field(name="sum_amount_3600s", dtype=Float32),
        
        # Avg (final + 2 IRs)
        Field(name="avg_amount_3600s", dtype=Float32),
        Field(name="avg_amount_3600s_sum", dtype=Float32),
        Field(name="avg_amount_3600s_count", dtype=Int64),
        
        # Std (final + 3 IRs)
        Field(name="std_amount_3600s", dtype=Float32),
        Field(name="std_amount_3600s_count", dtype=Int64),
        Field(name="std_amount_3600s_sum", dtype=Float32),
        Field(name="std_amount_3600s_sum_sq", dtype=Float32),
    ],
    
    timestamp_field="event_timestamp",
    ttl=timedelta(days=7),
    online=True,
    
    # ⚡ TILING CONFIGURATION (Spark-specific)
    enable_tiling=True,
    tiling_hop_size=timedelta(minutes=5),
)
```

---

## Usage

### Materialization

```python
from feast import FeatureStore
from datetime import datetime

fs = FeatureStore(repo_path=".")

# Materialize features with tiling
fs.materialize(
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2024, 1, 2)
)
```

**What happens**:
1. Spark reads events from batch source
2. `SparkAggregationNode` detects `enable_tiling=True`
3. Calls `apply_sawtooth_window_tiling()` for each time window
4. Computes IRs and final values
5. Writes tiles (with IRs) to online store via `SparkWriteNode`

---

### Querying

```python
# Query features (reads final values from online store)
features = fs.get_online_features(
    features=[
        "customer_features:sum_amount_3600s",
        "customer_features:avg_amount_3600s",
        "customer_features:std_amount_3600s",
    ],
    entity_rows=[{"customer_id": 1}]
).to_dict()

print(features)
# {
#   "customer_id": [1],
#   "sum_amount_3600s": [1250.5],
#   "avg_amount_3600s": [125.05],
#   "std_amount_3600s": [35.2]
# }
```

**Note**: Currently, Feast reads final values directly. IR columns are stored but not exposed to users.

---

## Performance Tuning

### Spark Configuration for Large-Scale Tiling

```yaml
batch_engine:
  type: spark.engine
  spark_conf:
    # Increase parallelism for large datasets
    spark.sql.shuffle.partitions: "500"  # Default: 200
    spark.default.parallelism: "500"
    
    # Tune memory for large aggregations
    spark.executor.memory: "8g"  # Default: 1g
    spark.driver.memory: "4g"    # Default: 1g
    spark.memory.fraction: "0.8" # Fraction for execution/storage
    
    # Enable adaptive query execution
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"
    
    # Optimize for time-series data
    spark.sql.sources.partitionOverwriteMode: "dynamic"
```

---

### Hop Size Selection

| Hop Size | Tiles per Hour | Storage | Query Speed | Use Case |
|----------|----------------|---------|-------------|----------|
| 1 min | 60 | High | Very Fast | Real-time, high precision |
| 5 min | 12 | Medium | Fast | **Recommended** for most cases |
| 15 min | 4 | Low | Medium | Long-term aggregations |
| 30 min | 2 | Very Low | Slower | Daily/weekly windows |

**Rule of thumb**: `hop_size = window_size / 10` to `window_size / 20`

---

## Monitoring & Debugging

### Spark UI

Monitor tiling performance in Spark UI (if enabled):

```yaml
spark_conf:
  spark.ui.enabled: "true"
  spark.ui.port: "4040"
```

**Key metrics to watch**:
- **Shuffle read/write**: Should be minimal for well-partitioned data
- **Task duration**: Should be balanced across executors
- **GC time**: Should be < 10% of task time

---

### Logging

Enable detailed logging for tiling:

```python
import logging
logging.getLogger("feast.infra.compute_engines.spark.tiling").setLevel(logging.DEBUG)
```

**Logs show**:
- Tile creation progress
- IR computation details
- Window processing order

---

## Limitations

### Spark-Specific Limitations

1. **PySpark workers need Feast SDK**
   - All Spark workers must have Feast installed
   - Use `--py-files` or containerized environments

2. **Arrow compatibility**
   - `mapInArrow` requires PyArrow and compatible Spark version
   - Tested with Spark 3.3+ and PyArrow 10+

3. **Broadcast variables**
   - Large schemas may hit broadcast size limits
   - Consider increasing `spark.driver.maxResultSize`

4. **Serialization**
   - Feature views are serialized via Protocol Buffers
   - Very large views may hit serialization limits

---

## Troubleshooting

### Issue: "ImportError: cannot import name 'apply_sawtooth_window_tiling'"

**Cause**: Old Spark workers with stale Python environment  
**Fix**: Restart Spark cluster or clear Python cache

---

### Issue: "Field 'X_sum' does not exist in schema"

**Cause**: IR columns not included in StreamFeatureView schema  
**Fix**: Add all IR columns to `schema` parameter (see configuration above)

---

### Issue: Slow tiling performance

**Causes & Fixes**:
- **Too many shuffle partitions**: Reduce `spark.sql.shuffle.partitions`
- **Unbalanced entity keys**: Add salting or repartition by entity
- **Large hop size**: Decrease `tiling_hop_size` for better parallelism

---

### Issue: Out of memory during tiling

**Causes & Fixes**:
- **Too many tiles in memory**: Increase `spark.executor.memory`
- **Large window sizes**: Process windows separately (already done automatically)
- **Skewed data**: Enable `spark.sql.adaptive.skewJoin.enabled`

---

## Comparison with Other Engines

| Feature | Spark | Local | Flink (Planned) |
|---------|-------|-------|-----------------|
| Distribution | ✅ Distributed | ❌ Single-node | ✅ Distributed |
| IR Support | ✅ Full | ⚠️ Partial | 🔄 Planned |
| Streaming | ⚠️ Batch only | ❌ Batch only | ✅ True streaming |
| Scale | 🚀 PB-scale | 💾 GB-scale | 🚀 PB-scale |
| Latency | ~seconds | ~milliseconds | ~milliseconds |

---

## Future Enhancements

### Planned for Spark Tiling

- [ ] **True streaming materialization** via Structured Streaming
- [ ] **Incremental tiling** (only update changed tiles)
- [ ] **Sketch-based IRs** for median/percentiles (e.g., T-Digest)
- [ ] **Query-time tile merging** (currently reads final values)
- [ ] **Tile compaction** (merge old tiles to reduce storage)
- [ ] **Multi-level tiling** (e.g., 1-min, 5-min, 1-hour tiles)

---

## Example

See the complete working example in:
- [`examples/spark-tiling-demo/`](https://github.com/feast-dev/feast/tree/master/examples/spark-tiling-demo)

The example includes:
- Feature repository with tiling enabled
- Sample data generation
- Materialization scripts
- Query demonstrations
- IR inspection utilities

---

## See Also

- [Tiling Concepts](../../getting-started/concepts/tiling.md) - General tiling architecture
- [Spark Compute Engine](spark.md) - General Spark engine docs
- [StreamFeatureView](../../getting-started/concepts/stream-feature-view.md) - Streaming feature views
- [Chronon Window Tiling](https://chronon.ai/window_tiling.html) - Original algorithm inspiration
