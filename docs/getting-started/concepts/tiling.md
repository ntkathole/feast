# Tiling with Intermediate Representations

## Overview

**Tiling** is an optimization technique for time-windowed aggregations that enables efficient feature computation by pre-aggregating data into smaller time intervals (tiles) and storing **Intermediate Representations (IRs)** that can be correctly merged at query time.

**Key Benefits:**
- 🚀 **Faster queries**: Query-time aggregations merge pre-computed tiles instead of scanning raw events
- ✅ **Correct results**: IRs ensure mathematically accurate merging for all aggregation types
- 💾 **Efficient storage**: Reuse tiles across multiple queries and time windows
- 📊 **Scalable**: Handle high-throughput streaming and large-scale batch workloads

**Currently Supported:**
- ✅ [SparkComputeEngine](../../reference/compute-engine/spark-tiling.md) with `StreamFeatureView`
- ✅ LocalComputeEngine with `StreamFeatureView` (basic support)

**Planned Support:**
- 🔄 FlinkComputeEngine with `StreamFeatureView`
- 🔄 RayComputeEngine with `StreamFeatureView`

---

## The Problem: Why Intermediate Representations?

Traditional approaches to time-windowed aggregations either:
1. **Recompute from raw data** every time → Slow, expensive
2. **Store final aggregated values** per tile → Fast but often **incorrect** when merging

### The Merging Problem

You **cannot correctly merge** many common aggregations:

```
❌ WRONG: avg(tile1, tile2) ≠ (avg_tile1 + avg_tile2) / 2

Example:
  tile1: [10, 20, 30] → avg = 20
  tile2: [100]        → avg = 100
  
  ✅ Correct merged avg: (10+20+30+100) / 4 = 40
  ❌ Wrong merged avg:   (20 + 100) / 2     = 60
```

**The same problem exists for:**
- Standard deviation (`std`)
- Variance (`var`)
- Median and percentiles
- Any "holistic" aggregation that requires knowledge of all values

---

## The Solution: Intermediate Representations (IRs)

Instead of storing **final aggregated values**, store **intermediate data** that preserves the mathematical properties needed for correct merging.

### Example: Average

**Traditional (Incorrect)**:
```
Tile 1: avg = 20
Tile 2: avg = 20
Merged avg = (20 + 20) / 2 = 20 ❌ WRONG
```

**With IRs (Correct)**:
```
Tile 1: sum = 60, count = 3
Tile 2: sum = 100, count = 1
Merged: sum = 160, count = 4
Merged avg = 160 / 4 = 40 ✅ CORRECT
```

---

## Aggregation Categories

### Algebraic Aggregations

These can be merged by applying the same aggregation function to tiles:

| Aggregation | Stored Value | Merge Strategy | Storage |
|-------------|--------------|----------------|---------|
| `sum` | sum | `sum(tile_sums)` | 1 column |
| `count` | count | `sum(tile_counts)` | 1 column |
| `max` | max | `max(tile_maxes)` | 1 column |
| `min` | min | `min(tile_mins)` | 1 column |

**No IRs needed** - the final value is the IR!

---

### Holistic Aggregations

These require storing multiple intermediate values:

#### Average (`avg`, `mean`)

**Stored IRs**: `sum`, `count`  
**Final computation**: `avg = sum / count`  
**Merge strategy**: Sum the sums and counts, then divide

**Storage**: 3 columns (final + 2 IRs)

---

#### Standard Deviation (`std`, `stddev`)

**Stored IRs**: `count`, `sum`, `sum_of_squares`  
**Final computation**: 
```python
variance = (sum_sq - sum²/count) / (count - δ)
std = sqrt(variance)
# δ = 1 for sample, 0 for population
```

**Merge strategy**: Sum all three IRs, then apply formula

**Storage**: 4 columns (final + 3 IRs)

---

#### Variance (`var`, `variance`)

**Stored IRs**: `count`, `sum`, `sum_of_squares`  
**Final computation**: Same as std but without `sqrt()`

**Storage**: 4 columns (final + 3 IRs)

---

#### Median, Percentiles (Not Yet Supported)

These are **distributive aggregations** that require the full data distribution:
- Cannot be accurately computed from summary statistics
- Would require storing histograms or sketches (e.g., T-Digest)
- Currently approximated (may not merge correctly)

**Future work**: Implement sketch-based IRs for these aggregations.

---

## How Tiling Works

### 1. Tile Creation (Materialization)

```
Raw Events → Partition by Hop Intervals → Compute IRs → Store Tiles
  |                  |                         |              |
  |                  |                         |              └─> Online Store (Redis, etc.)
  |                  |                         └─> avg_sum, avg_count, std_sum_sq, etc.
  |                  └─> 5-min windows: [00:00-00:05], [00:05-00:10], ...
  └─> customer_id=1: [txn1, txn2, txn3, ...]
```

**Process**:
1. Events are partitioned into small time buckets (e.g., 5 minutes)
2. For each tile, compute IRs based on aggregation type
3. Compute final values from IRs (for immediate use)
4. Store both IRs and final values in online store

---

### 2. Query-Time Merging

```
Query (1-hour window) → Identify Overlapping Tiles → Merge IRs → Compute Final
  |                            |                         |              |
  |                            |                         |              └─> avg = sum/count
  |                            |                         └─> merged_sum, merged_count
  |                            └─> 12 tiles (5-min each) overlap with 1-hour window
  └─> 2024-01-01 11:00:00
```

**Process**:
1. Identify which tiles overlap with the query time window
2. Read IR columns for those tiles
3. Merge IRs using aggregation-specific logic
4. Compute final values from merged IRs

---

## Column Naming Convention

For a feature with aggregation `avg` on column `amount` with time window `3600s` (1 hour):

| Column Name | Purpose | Visible to User |
|-------------|---------|-----------------|
| `avg_amount_3600s` | Final aggregated value | ✅ Yes (query result) |
| `avg_amount_3600s_sum` | IR: Sum of amounts | ❌ No (internal only) |
| `avg_amount_3600s_count` | IR: Count of amounts | ❌ No (internal only) |

For std/var, add:
- `std_amount_3600s_sum_sq` (IR: Sum of squares)

**Important**: IR columns are stored in the online store but are **not exposed** to users during queries. They're only used internally for merging.

---

## Storage Overhead

| Aggregation Type | Columns Stored | Overhead Factor |
|-----------------|----------------|-----------------|
| sum/count/max/min | 1 | None (baseline) |
| avg/mean | 3 (final + sum + count) | 2x |
| std/var | 4 (final + count + sum + sum_sq) | 3x |

**Note**: This overhead is minimal compared to:
- **Compute savings**: Reusing tiles instead of recomputing from raw events
- **Query speedup**: 10-100x faster by merging 10-20 tiles instead of scanning 1000+ events

---

## Performance Characteristics

### Materialization

| Without Tiling | With Tiling (5-min hops) |
|---------------|-------------------------|
| Compute full window for each entity | Compute small tiles for each entity |
| Time: O(n × w) | Time: O(n × h) where h << w |

**Example**: For a 1-hour window:
- Without tiling: Aggregate 60 minutes of data
- With tiling (5-min hops): Aggregate 5 minutes × 12 tiles = same work, but **reusable**

**Speedup**: Typically **5-10x faster** for large windows, and tiles can be reused for multiple queries.

---

### Query Time

| Without Tiling | With Tiling |
|---------------|-------------|
| Scan all events in window | Merge k pre-computed tiles |
| Time: O(events_in_window) | Time: O(k) where k = number of tiles |

**Example**: For a 1-hour query:
- Without tiling: Scan 1000+ events per customer
- With tiling (5-min hops): Merge 12 tiles per customer

**Speedup**: **10-100x faster** depending on event volume.

---

## Tiling Algorithms

Different compute engines may use different tiling algorithms:

### Sawtooth Window Tiling

Used by **SparkComputeEngine** (inspired by [Chronon](https://chronon.ai/window_tiling.html)):

1. **Partition events** into hop-sized intervals (e.g., 5 minutes)
2. **Compute tail aggregations** for each hop from the start of the window
3. **Store IRs** for correct merging
4. **At query time**, identify overlapping tiles and merge IRs

**Benefits**:
- O(log n) query-time complexity for tile identification
- Minimal storage overhead (only hop-sized tiles)
- Point-in-time correct semantics

---

### Future Algorithms

Other compute engines may implement different approaches:
- **Flink**: Leverage Flink's native windowing with custom IR accumulators
- **Ray**: Distributed tiling with Ray's task-based execution
- **Snowflake**: SQL-based CTEs for tile creation and merging

The key is that **all approaches must store IRs** for correct merging.

---

## Configuration

Tiling is enabled per `StreamFeatureView`:

```python
from feast import StreamFeatureView
from feast.stream_feature_view import Aggregation
from datetime import timedelta

customer_features = StreamFeatureView(
    name="customer_features",
    entities=[customer],
    source=stream_source,
    aggregations=[
        Aggregation(column="amount", function="sum", time_window=timedelta(hours=1)),
        Aggregation(column="amount", function="avg", time_window=timedelta(hours=1)),
        Aggregation(column="amount", function="std", time_window=timedelta(hours=1)),
    ],
    # Define schema with BOTH final values AND IR columns
    schema=[
        Field(name="sum_amount_3600s", dtype=Float32),
        Field(name="avg_amount_3600s", dtype=Float32),
        Field(name="avg_amount_3600s_sum", dtype=Float32),
        Field(name="avg_amount_3600s_count", dtype=Int64),
        Field(name="std_amount_3600s", dtype=Float32),
        Field(name="std_amount_3600s_count", dtype=Int64),
        Field(name="std_amount_3600s_sum", dtype=Float32),
        Field(name="std_amount_3600s_sum_sq", dtype=Float32),
    ],
    timestamp_field="event_timestamp",
    online=True,
    
    # Tiling configuration (Spark-specific)
    enable_tiling=True,  # Enable IR-based tiling
    tiling_hop_size=timedelta(minutes=5),  # Tile size
)
```

**Note**: Configuration parameters may vary by compute engine. See engine-specific documentation for details.

---

## Hop Size Selection

The **hop size** (tile size) affects:
- **Storage overhead**: Smaller hops = more tiles = more storage
- **Query granularity**: Smaller hops = more precise time alignment
- **Merge cost**: Smaller hops = more tiles to merge at query time

**Recommendations**:
- **5 minutes**: Good balance for hourly/daily aggregations
- **1 minute**: High precision, higher storage overhead
- **15 minutes**: Lower overhead, less granular

**Rule of thumb**: Hop size should be **1/10 to 1/20** of your smallest time window.

---

## Limitations & Considerations

### Schema Requirements

**Critical**: You **must** explicitly define the `schema` parameter in `StreamFeatureView` to include:
1. All final aggregated feature names (e.g., `avg_amount_3600s`)
2. All IR columns (e.g., `avg_amount_3600s_sum`, `avg_amount_3600s_count`)

Schema inference is **not supported** for StreamFeatureViews with aggregations and tiling.

---

### Multiple Time Windows

When defining aggregations with **different time windows** (e.g., hourly and daily):
1. Each window is processed separately with its own tiling
2. Results are joined on entity keys
3. Each window creates its own set of tiles

**Example**:
```python
aggregations=[
    Aggregation(column="amount", function="sum", time_window=timedelta(hours=1)),
    Aggregation(column="amount", function="sum", time_window=timedelta(days=1)),
]
```
This creates two separate tile sets with different hop sizes.

---

### Unsupported Aggregations

Currently **approximated** (may not merge correctly):
- ⚠️ `median`
- ⚠️ `percentile_*` (e.g., p50, p90, p99)

**Workaround**: Use supported aggregations or wait for sketch-based IR implementation.

---

## Compute Engine Support

| Compute Engine | Status | Implementation |
|---------------|--------|----------------|
| **SparkComputeEngine** | ✅ **Full Support** | [Spark Tiling](../../reference/compute-engine/spark-tiling.md) |
| **LocalComputeEngine** | ✅ Basic Support | Time-windowed aggregations (no IRs) |
| **FlinkComputeEngine** | 🔄 Planned | Flink windowing + IR accumulators |
| **RayComputeEngine** | 🔄 Planned | Distributed tiling with Ray |
| **SnowflakeComputeEngine** | 🔄 Planned | SQL-based CTEs for tiling |

Each engine implements tiling differently but follows the same **IR principle** for correct merging.

---

## See Also

- [StreamFeatureView](stream-feature-view.md) - Feature views for streaming data
- [Spark Tiling Implementation](../../reference/compute-engine/spark-tiling.md) - Spark-specific details
- [Compute Engines](../../reference/compute-engine/README.md) - Overview of all compute engines
- [Chronon Window Tiling](https://chronon.ai/window_tiling.html) - Inspiration for sawtooth algorithm


