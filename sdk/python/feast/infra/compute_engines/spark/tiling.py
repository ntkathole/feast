"""
Spark tiling implementation using Intermediate Representations (IRs).

Key Concepts:
-------------
1. **Intermediate Representations (IRs)**: Instead of storing final aggregation values
   in tiles, we store intermediate data that can be correctly merged:

   - sum, count, max, min: Store the value directly (algebraic aggregations)
   - avg: Store sum and count separately, compute avg = sum/count
   - std/var: Store count, sum, and sum_of_squares, compute using formulas

2. **Sawtooth Windows**: Combine hopping (tail) and sliding (head) windows:
   - Tail: Historical data pre-aggregated into hop-sized tiles (reusable)
   - Head: Most recent hop window (changes with each query)

3. **Correct Merging**: IRs enable correct aggregation across tiles:
   - avg(tile1, tile2) = (sum1 + sum2) / (count1 + count2)
   - std(tile1, tile2) = sqrt(variance(merged count, sum, sum_sq))

Supported Aggregations:
-----------------------
All Spark aggregation functions are supported with correct merging:
- sum, count, max, min: Direct merging (algebraic)
- avg, mean: IR-based (sum + count)
- std, stddev, stddev_samp, stddev_pop: IR-based (count + sum + sum_sq)
- var, variance, var_samp, var_pop: IR-based (count + sum + sum_sq)
- Others: Stored directly (may not merge correctly)
"""

from datetime import timedelta
from typing import Callable, List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

from feast.aggregation import Aggregation


def generate_tiles(
    left: int, right: int, tile_consumer: Callable[[int, int], None]
) -> int:
    """
    Generate tiles using interval tree algorithm.

    Breaks any time window [left, right] into log(n) non-overlapping tiles
    that can be reused across queries.

    Args:
        left: Left boundary timestamp (milliseconds)
        right: Right boundary timestamp (milliseconds)
        tile_consumer: Function that processes each tile (left, right)

    Returns:
        Split point timestamp
    """
    if left >= right:
        return left

    # Find power-of-two boundary between left and right
    xor = left ^ right
    if xor == 0:
        # Same value, single tile
        tile_consumer(left, right)
        return left

    # Find highest bit that differs
    power_of_two = 1 << (31 - _number_of_leading_zeros(xor))
    split_point = (right // power_of_two) * power_of_two

    # Generate tiles on left side (decreasing)
    left_distance = split_point - left
    right_boundary = split_point
    while left_distance > 0:
        max_power = _highest_one_bit(left_distance)
        tile_consumer(right_boundary - max_power, right_boundary)
        right_boundary -= max_power
        left_distance -= max_power

    # Generate tiles on right side (increasing)
    right_distance = right - split_point
    left_boundary = split_point
    while right_distance > 0:
        max_power = _highest_one_bit(right_distance)
        tile_consumer(left_boundary, left_boundary + max_power)
        left_boundary += max_power
        right_distance -= max_power

    return split_point


def _number_of_leading_zeros(x: int) -> int:
    """Count leading zeros in 32-bit integer."""
    if x == 0:
        return 32
    return 31 - (x.bit_length() - 1)


def _highest_one_bit(x: int) -> int:
    """Get highest one bit in integer."""
    if x == 0:
        return 0
    return 1 << (x.bit_length() - 1)


def nearest_multiple(ts: int, factor_ms: int) -> int:
    """
    Round timestamp to nearest multiple of factor.

    Args:
        ts: Timestamp in milliseconds
        factor_ms: Factor in milliseconds

    Returns:
        Rounded timestamp
    """
    return (ts // factor_ms) * factor_ms


def apply_interval_tree_tiling(
    df: DataFrame,
    aggregations: List[Aggregation],
    group_by_keys: List[str],
    timestamp_col: str,
    window_size: timedelta,
) -> DataFrame:
    """
    Apply interval tree tiling to Spark DataFrame for efficient window aggregation.

    Args:
        df: Input Spark DataFrame
        aggregations: List of aggregations to apply
        group_by_keys: Keys to group by (entity keys)
        timestamp_col: Timestamp column name
        window_size: Size of the time window

    Returns:
        DataFrame with tiled aggregations
    """
    # Convert timestamp to milliseconds for tiling
    window_ms = int(window_size.total_seconds() * 1000)

    # Create UDF for generating tiles
    def generate_tiles_for_window(query_ts_ms: int) -> List[Tuple[int, int]]:
        """Generate tile boundaries for a query timestamp."""
        tiles = []
        left = query_ts_ms - window_ms
        right = query_ts_ms

        def tile_consumer(tile_left: int, tile_right: int):
            tiles.append((tile_left, tile_right))

        generate_tiles(left, right, tile_consumer)
        return tiles

    # For now, use sawtooth window approach (simpler for initial implementation)
    # Full interval tree implementation can be added later
    return apply_sawtooth_window_tiling(
        df, aggregations, group_by_keys, timestamp_col, window_size
    )


def apply_sawtooth_window_tiling(
    df: DataFrame,
    aggregations: List[Aggregation],
    group_by_keys: List[str],
    timestamp_col: str,
    window_size: timedelta,
    hop_size: Optional[timedelta] = None,
) -> DataFrame:
    """
    Apply sawtooth window tiling for streaming efficiency.

    Sawtooth windows combine:
    - Sliding head: Most recent data (changes with each query)
    - Hopping tail: Historical data (reusable across queries)

    This approach is significantly more distributable and handles skew better.

    Args:
        df: Input Spark DataFrame
        aggregations: List of aggregations to apply
        group_by_keys: Keys to group by (entity keys)
        timestamp_col: Timestamp column name
        window_size: Size of the time window
        hop_size: Size of hop intervals (defaults to 5 minutes)

    Returns:
        DataFrame with tiled aggregations
    """
    if hop_size is None:
        # Default to 5 minutes for hop size
        hop_size = timedelta(minutes=5)

    hop_size_ms = int(hop_size.total_seconds() * 1000)

    # Step 1: Add hop interval column
    # Use Spark's unix_timestamp to get milliseconds, then floor to hop boundaries
    df_with_hop = df.withColumn(
        "_hop_interval",
        (
            F.floor((F.unix_timestamp(F.col(timestamp_col)) * 1000) / hop_size_ms)
            * hop_size_ms
        ).cast(LongType()),
    )

    # Step 2: Pre-aggregate by hop intervals (tail computation)
    # This creates reusable tiles for historical data
    # Store intermediate representations (IRs) for correct merging
    tail_agg_exprs = []
    ir_metadata = {}  # Track which IRs are needed for each feature

    for agg in aggregations:
        feature_name = (
            f"{agg.function}_{agg.column}_{int(window_size.total_seconds())}s"
        )

        # Store IRs based on aggregation type
        if agg.function in ["sum", "count", "max", "min"]:
            # Algebraic aggregations: store the aggregation directly
            func = getattr(F, agg.function)
            tail_agg_exprs.append(func(agg.column).alias(f"_tail_{feature_name}"))
            ir_metadata[feature_name] = {"type": "algebraic", "function": agg.function}

        elif agg.function in ["avg", "mean"]:
            # Average: store sum and count
            tail_agg_exprs.append(F.sum(agg.column).alias(f"_tail_{feature_name}_sum"))
            tail_agg_exprs.append(
                F.count(agg.column).alias(f"_tail_{feature_name}_count")
            )
            ir_metadata[feature_name] = {"type": "avg", "function": agg.function}

        elif agg.function in ["std", "stddev", "stddev_samp", "stddev_pop"]:
            # Standard deviation: store count, sum, and sum of squares
            tail_agg_exprs.append(
                F.count(agg.column).alias(f"_tail_{feature_name}_count")
            )
            tail_agg_exprs.append(F.sum(agg.column).alias(f"_tail_{feature_name}_sum"))
            tail_agg_exprs.append(
                F.sum(F.col(agg.column) * F.col(agg.column)).alias(
                    f"_tail_{feature_name}_sum_sq"
                )
            )
            population = "pop" in agg.function
            ir_metadata[feature_name] = {
                "type": "std",
                "function": agg.function,
                "population": str(population),
            }

        elif agg.function in ["var", "variance", "var_samp", "var_pop"]:
            # Variance: store count, sum, and sum of squares
            tail_agg_exprs.append(
                F.count(agg.column).alias(f"_tail_{feature_name}_count")
            )
            tail_agg_exprs.append(F.sum(agg.column).alias(f"_tail_{feature_name}_sum"))
            tail_agg_exprs.append(
                F.sum(F.col(agg.column) * F.col(agg.column)).alias(
                    f"_tail_{feature_name}_sum_sq"
                )
            )
            population = "pop" in agg.function
            ir_metadata[feature_name] = {
                "type": "var",
                "function": agg.function,
                "population": str(population),
            }

        else:
            # Fallback: store the aggregation directly (may not merge correctly)
            func = getattr(F, agg.function)
            tail_agg_exprs.append(func(agg.column).alias(f"_tail_{feature_name}"))
            ir_metadata[feature_name] = {"type": "unknown", "function": agg.function}

    # Group by entity keys and hop interval
    tail_group_cols = group_by_keys + ["_hop_interval"]
    tail_aggregated = df_with_hop.groupBy(*tail_group_cols).agg(*tail_agg_exprs)

    # Step 3: For head computation (sliding window), we use standard windowing
    # but optimized for smaller time ranges
    # The head is the most recent hop_size window, which is small and efficient

    # For materialization, we'll create tiles for each entity-window combination
    # The actual query-time merging will happen in the online store or retrieval

    # Create final aggregation with window start/end times
    # For now, we'll use the hop interval as the tile boundary
    result_df = tail_aggregated.withColumnRenamed("_hop_interval", "_tile_start")

    # Add tile end time (tile_start + hop_size)
    result_df = result_df.withColumn("_tile_end", F.col("_tile_start") + hop_size_ms)

    # Compute final values from IRs
    for agg in aggregations:
        feature_name = (
            f"{agg.function}_{agg.column}_{int(window_size.total_seconds())}s"
        )
        metadata = ir_metadata[feature_name]

        if metadata["type"] == "algebraic":
            # Simple rename for algebraic aggregations
            result_df = result_df.withColumnRenamed(
                f"_tail_{feature_name}", feature_name
            )

        elif metadata["type"] == "avg":
            # Compute average from sum and count
            sum_col = f"_tail_{feature_name}_sum"
            count_col = f"_tail_{feature_name}_count"
            result_df = result_df.withColumn(
                feature_name,
                F.when(
                    F.col(count_col) > 0, F.col(sum_col) / F.col(count_col)
                ).otherwise(None),
            )
            # Keep IRs for merging (don't drop them)
            result_df = result_df.withColumnRenamed(sum_col, f"{feature_name}_sum")
            result_df = result_df.withColumnRenamed(count_col, f"{feature_name}_count")

        elif metadata["type"] == "std":
            # Compute standard deviation from IRs
            count_col = f"_tail_{feature_name}_count"
            sum_col = f"_tail_{feature_name}_sum"
            sum_sq_col = f"_tail_{feature_name}_sum_sq"

            # Variance = (sum_sq - (sum²/count)) / (count - delta)
            # delta = 0 for population, 1 for sample
            delta = 0 if metadata["population"] else 1

            result_df = result_df.withColumn(
                feature_name,
                F.when(
                    F.col(count_col) > delta,
                    F.sqrt(
                        (
                            F.col(sum_sq_col)
                            - (F.col(sum_col) * F.col(sum_col) / F.col(count_col))
                        )
                        / (F.col(count_col) - delta)
                    ),
                ).otherwise(None),
            )
            # Keep IRs for merging
            result_df = result_df.withColumnRenamed(count_col, f"{feature_name}_count")
            result_df = result_df.withColumnRenamed(sum_col, f"{feature_name}_sum")
            result_df = result_df.withColumnRenamed(
                sum_sq_col, f"{feature_name}_sum_sq"
            )

        elif metadata["type"] == "var":
            # Compute variance from IRs
            count_col = f"_tail_{feature_name}_count"
            sum_col = f"_tail_{feature_name}_sum"
            sum_sq_col = f"_tail_{feature_name}_sum_sq"

            delta = 0 if metadata["population"] else 1

            result_df = result_df.withColumn(
                feature_name,
                F.when(
                    F.col(count_col) > delta,
                    (
                        F.col(sum_sq_col)
                        - (F.col(sum_col) * F.col(sum_col) / F.col(count_col))
                    )
                    / (F.col(count_col) - delta),
                ).otherwise(None),
            )
            # Keep IRs for merging
            result_df = result_df.withColumnRenamed(count_col, f"{feature_name}_count")
            result_df = result_df.withColumnRenamed(sum_col, f"{feature_name}_sum")
            result_df = result_df.withColumnRenamed(
                sum_sq_col, f"{feature_name}_sum_sq"
            )

        else:
            # Unknown aggregation: just rename
            result_df = result_df.withColumnRenamed(
                f"_tail_{feature_name}", feature_name
            )

    return result_df


def merge_tiles_for_query(
    tiles_df: DataFrame,
    query_timestamp_col: str,
    window_size: timedelta,
    group_by_keys: List[str],
) -> DataFrame:
    """
    Merge tiles to compute final aggregation for a query timestamp.

    This implements the merging logic for sawtooth windows:
    - Computes tail from pre-aggregated hop tiles
    - Computes head from raw events in recent hop
    - Merges head + tail

    Args:
        tiles_df: DataFrame containing pre-aggregated tiles
        query_timestamp_col: Column containing query timestamps
        window_size: Size of the time window
        group_by_keys: Keys to group by (entity keys)

    Returns:
        DataFrame with merged aggregations
    """
    window_size_ms = int(window_size.total_seconds() * 1000)

    # Filter tiles that overlap with query window
    # Query window: [query_ts - window_size, query_ts]
    tiles_df = tiles_df.withColumn(
        "_query_window_start",
        F.col(query_timestamp_col) - window_size_ms,
    )

    # Filter tiles that overlap: tile_end > window_start AND tile_start < query_ts
    overlapping_tiles = tiles_df.filter(
        (F.col("_tile_end") > F.col("_query_window_start"))
        & (F.col("_tile_start") < F.col(query_timestamp_col))
    )

    # Merge overlapping tiles for each entity
    # This is the tail computation (reusable part)
    aggregation_cols = [
        col
        for col in overlapping_tiles.columns
        if col not in group_by_keys
        and col
        not in ["_tile_start", "_tile_end", "_query_window_start", query_timestamp_col]
    ]

    # Merge tiles based on aggregation type
    # Extract aggregation function from column name (format: {function}_{column}_{window}s)
    agg_exprs = []
    final_computations: list[tuple[str, str]] = []

    for col in aggregation_cols:
        # Check if this is an IR column (ends with _sum, _count, _sum_sq)
        if col.endswith("_sum") or col.endswith("_count") or col.endswith("_sum_sq"):
            # This is an IR component - sum it
            agg_exprs.append(F.sum(col).alias(col))

            # Track base feature name for final computation
            if col.endswith("_count"):
                base_feature = col[:-6]  # Remove "_count"
                if base_feature not in [fc[0] for fc in final_computations]:
                    # Determine aggregation type from other IR columns present
                    if f"{base_feature}_sum_sq" in aggregation_cols:
                        # Has sum_sq -> std or var
                        agg_type = "std_or_var"
                    else:
                        # Only sum and count -> avg
                        agg_type = "avg"
                    final_computations.append((base_feature, agg_type))
        else:
            # Parse column name to determine aggregation function
            # Format: {function}_{column}_{window}s
            parts = col.split("_")
            if len(parts) >= 2:
                agg_function = parts[0]  # e.g., "sum", "count", "max", "min"

                # Choose merge strategy based on aggregation type
                if agg_function in ["sum", "count"]:
                    # Algebraic: sum of sums/counts
                    agg_exprs.append(F.sum(col).alias(col))
                elif agg_function == "max":
                    # Max of maxes
                    agg_exprs.append(F.max(col).alias(col))
                elif agg_function == "min":
                    # Min of mins
                    agg_exprs.append(F.min(col).alias(col))
                elif agg_function in [
                    "avg",
                    "mean",
                    "std",
                    "stddev",
                    "var",
                    "variance",
                ]:
                    # These should have IR columns, but if they don't (old data), use approximation
                    agg_exprs.append(F.avg(col).alias(col))
                else:
                    # Default: sum
                    agg_exprs.append(F.sum(col).alias(col))
            else:
                # Fallback: sum if can't parse
                agg_exprs.append(F.sum(col).alias(col))

    if agg_exprs:
        merged = overlapping_tiles.groupBy(*group_by_keys, query_timestamp_col).agg(
            *agg_exprs
        )

        # Compute final values from merged IRs
        for base_feature, agg_type in final_computations:
            if agg_type == "avg":
                # Recompute average from merged sum and count
                sum_col = f"{base_feature}_sum"
                count_col = f"{base_feature}_count"
                if sum_col in merged.columns and count_col in merged.columns:
                    merged = merged.withColumn(
                        base_feature, F.col(sum_col) / F.col(count_col)
                    )
            elif agg_type == "std_or_var":
                # Determine if it's std or var from the base feature name
                count_col = f"{base_feature}_count"
                sum_col = f"{base_feature}_sum"
                sum_sq_col = f"{base_feature}_sum_sq"

                if all(c in merged.columns for c in [count_col, sum_col, sum_sq_col]):
                    # Determine if population or sample (default to sample)
                    is_population = "pop" in base_feature
                    delta = 0 if is_population else 1

                    # Compute variance
                    variance_expr = (
                        F.col(sum_sq_col)
                        - (F.col(sum_col) * F.col(sum_col) / F.col(count_col))
                    ) / (F.col(count_col) - delta)

                    # Check if it's std or var
                    if any(x in base_feature for x in ["std", "stddev"]):
                        merged = merged.withColumn(base_feature, F.sqrt(variance_expr))
                    else:
                        merged = merged.withColumn(base_feature, variance_expr)
    else:
        merged = overlapping_tiles.select(
            *group_by_keys, query_timestamp_col
        ).distinct()

    return merged
