from datetime import datetime, timedelta
from typing import Callable, List, Optional, Union, cast

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from feast import BatchFeatureView, StreamFeatureView
from feast.aggregation import Aggregation
from feast.data_source import DataSource
from feast.infra.common.serde import SerializedArtifacts
from feast.infra.compute_engines.dag.context import ColumnInfo, ExecutionContext
from feast.infra.compute_engines.dag.model import DAGFormat
from feast.infra.compute_engines.dag.node import DAGNode
from feast.infra.compute_engines.dag.value import DAGValue
from feast.infra.compute_engines.spark.tiling import (
    apply_sawtooth_window_tiling,
)
from feast.infra.compute_engines.spark.utils import map_in_arrow
from feast.infra.compute_engines.utils import (
    create_offline_store_retrieval_job,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark import (
    SparkRetrievalJob,
    _get_entity_schema,
)
from feast.infra.offline_stores.contrib.spark_offline_store.spark_source import (
    SparkSource,
)
from feast.infra.offline_stores.offline_utils import (
    infer_event_timestamp_from_entity_df,
)

ENTITY_TS_ALIAS = "__entity_event_timestamp"


# Rename entity_df event_timestamp_col to match feature_df
def rename_entity_ts_column(
    spark_session: SparkSession, entity_df: DataFrame
) -> DataFrame:
    # check if entity_ts_alias already exists
    if ENTITY_TS_ALIAS in entity_df.columns:
        return entity_df

    entity_schema = _get_entity_schema(
        spark_session=spark_session,
        entity_df=entity_df,
    )
    event_timestamp_col = infer_event_timestamp_from_entity_df(
        entity_schema=entity_schema,
    )
    if not isinstance(entity_df, DataFrame):
        entity_df = spark_session.createDataFrame(entity_df)
    entity_df = entity_df.withColumnRenamed(event_timestamp_col, ENTITY_TS_ALIAS)
    return entity_df


class SparkReadNode(DAGNode):
    def __init__(
        self,
        name: str,
        source: DataSource,
        column_info: ColumnInfo,
        spark_session: SparkSession,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
    ):
        super().__init__(name)
        self.source = source
        self.column_info = column_info
        self.spark_session = spark_session
        self.start_time = start_time
        self.end_time = end_time

    def execute(self, context: ExecutionContext) -> DAGValue:
        retrieval_job = create_offline_store_retrieval_job(
            data_source=self.source,
            column_info=self.column_info,
            context=context,
            start_time=self.start_time,
            end_time=self.end_time,
        )
        if isinstance(retrieval_job, SparkRetrievalJob):
            spark_df = cast(SparkRetrievalJob, retrieval_job).to_spark_df()
        else:
            spark_df = self.spark_session.createDataFrame(retrieval_job.to_arrow())

        return DAGValue(
            data=spark_df,
            format=DAGFormat.SPARK,
            metadata={
                "source": "feature_view_batch_source",
                "timestamp_field": self.column_info.timestamp_column,
                "created_timestamp_column": self.column_info.created_timestamp_column,
                "start_date": self.start_time,
                "end_date": self.end_time,
            },
        )


class SparkAggregationNode(DAGNode):
    def __init__(
        self,
        name: str,
        aggregations: List[Aggregation],
        group_by_keys: List[str],
        timestamp_col: str,
        inputs=None,
        enable_tiling: bool = False,
        hop_size: Optional[timedelta] = None,
    ):
        super().__init__(name, inputs=inputs)
        self.aggregations = aggregations
        self.group_by_keys = group_by_keys
        self.timestamp_col = timestamp_col
        self.enable_tiling = enable_tiling
        self.hop_size = hop_size

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.SPARK)
        input_df: DataFrame = input_value.data

        # Check if tiling is enabled and we have time-windowed aggregations
        has_time_windows = any(agg.time_window for agg in self.aggregations)

        if self.enable_tiling and has_time_windows:
            # Use transformation-based tiling (Chronon-inspired)
            return self._execute_tiled_aggregation(input_df)
        else:
            # Use standard aggregation (existing logic)
            return self._execute_standard_aggregation(input_df)

    def _execute_tiled_aggregation(self, input_df: DataFrame) -> DAGValue:
        """
        Execute aggregation using transformation-based tiling.

        Processes each unique time window separately to avoid column name conflicts.
        """
        # Extract entity join keys (convert Entity objects to strings)
        entity_keys = []
        for key in self.group_by_keys:
            if hasattr(key, "join_key"):
                entity_keys.append(key.join_key)
            elif isinstance(key, str):
                entity_keys.append(key)
            else:
                # Fallback: try to get name
                entity_keys.append(str(key))

        # Group aggregations by time window to process separately
        from collections import defaultdict

        aggs_by_window = defaultdict(list)
        for agg in self.aggregations:
            if agg.time_window is None:
                raise ValueError(
                    f"Aggregation on {agg.column} requires time_window for tiling."
                )
            aggs_by_window[agg.time_window].append(agg)

        # Process each time window separately and collect results
        result_dfs = []
        for time_window, window_aggs in aggs_by_window.items():
            tiled_df = apply_sawtooth_window_tiling(
                df=input_df,
                aggregations=window_aggs,
                group_by_keys=entity_keys,
                timestamp_col=self.timestamp_col,
                window_size=time_window,
                hop_size=self.hop_size,
            )

            # Drop internal tile metadata columns before collecting
            # These are only needed during tiling, not in the final output
            cols_to_drop = ["_tile_start", "_tile_end"]
            for col in cols_to_drop:
                if col in tiled_df.columns:
                    tiled_df = tiled_df.drop(col)

            result_dfs.append(tiled_df)

        # Merge all windowed results by joining on entity keys
        if len(result_dfs) == 1:
            final_df = result_dfs[0]
        else:
            # Join on entity keys - each window contributes different aggregated features
            final_df = result_dfs[0]
            for df in result_dfs[1:]:
                # Outer join to preserve all entity combinations
                final_df = final_df.join(df, on=entity_keys, how="outer")

        return DAGValue(
            data=final_df,
            format=DAGFormat.SPARK,
            metadata={
                "aggregated": True,
                "tiled": True,
                "window_sizes": [
                    int(tw.total_seconds()) for tw in aggs_by_window.keys()
                ],
            },
        )

    def _execute_standard_aggregation(self, input_df: DataFrame) -> DAGValue:
        """Execute standard Spark aggregation (existing logic)."""
        agg_exprs = []
        for agg in self.aggregations:
            func = getattr(F, agg.function)
            expr = func(agg.column).alias(
                f"{agg.function}_{agg.column}_{int(agg.time_window.total_seconds())}s"
                if agg.time_window
                else f"{agg.function}_{agg.column}"
            )
            agg_exprs.append(expr)

        if any(agg.time_window for agg in self.aggregations):
            # 🕒 Use Spark's `window` function
            time_window = self.aggregations[
                0
            ].time_window  # assume consistent window size for now
            if time_window is None:
                raise ValueError("Aggregation requires time_window but got None.")
            window_duration_str = f"{int(time_window.total_seconds())} seconds"

            # Extract entity join keys
            entity_keys = []
            for key in self.group_by_keys:
                if hasattr(key, "join_key"):
                    entity_keys.append(key.join_key)
                elif isinstance(key, str):
                    entity_keys.append(key)
                else:
                    entity_keys.append(str(key))

            grouped = input_df.groupBy(
                *entity_keys,
                F.window(F.col(self.timestamp_col), window_duration_str),
            ).agg(*agg_exprs)
        else:
            # Simple aggregation
            entity_keys = []
            for key in self.group_by_keys:
                if hasattr(key, "join_key"):
                    entity_keys.append(key.join_key)
                elif isinstance(key, str):
                    entity_keys.append(key)
                else:
                    entity_keys.append(str(key))

            grouped = input_df.groupBy(*entity_keys).agg(*agg_exprs)

        return DAGValue(
            data=grouped, format=DAGFormat.SPARK, metadata={"aggregated": True}
        )


class SparkJoinNode(DAGNode):
    def __init__(
        self,
        name: str,
        column_info: ColumnInfo,
        spark_session: SparkSession,
        inputs: Optional[List[DAGNode]] = None,
        how: str = "inner",
    ):
        super().__init__(name, inputs=inputs or [])
        self.column_info = column_info
        self.spark_session = spark_session
        self.how = how

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_values = self.get_input_values(context)
        for val in input_values:
            val.assert_format(DAGFormat.SPARK)

        # Join all input DataFrames on join_keys
        joined_df = None
        for i, dag_value in enumerate(input_values):
            df = dag_value.data

            # Use original FeatureView name if available
            fv_name = self.inputs[i].name.split(":")[0]
            prefix = fv_name + "__"

            # Skip renaming join keys to preserve join compatibility
            renamed_cols = [
                F.col(c).alias(f"{prefix}{c}")
                if c not in self.column_info.join_keys
                else F.col(c)
                for c in df.columns
            ]
            df = df.select(*renamed_cols)
            if joined_df is None:
                joined_df = df
            else:
                joined_df = joined_df.join(
                    df, on=self.column_info.join_keys, how=self.how
                )

        # If entity_df is provided, join it in last
        entity_df = context.entity_df
        if entity_df is not None:
            entity_df = rename_entity_ts_column(
                spark_session=self.spark_session,
                entity_df=entity_df,
            )
            if joined_df is None:
                raise RuntimeError("No input features available to join with entity_df")

            joined_df = entity_df.join(
                joined_df, on=self.column_info.join_keys, how="left"
            )

        return DAGValue(
            data=joined_df,
            format=DAGFormat.SPARK,
            metadata={"joined_on": self.column_info.join_keys, "join_type": self.how},
        )


class SparkFilterNode(DAGNode):
    def __init__(
        self,
        name: str,
        column_info: ColumnInfo,
        spark_session: SparkSession,
        ttl: Optional[timedelta] = None,
        filter_condition: Optional[str] = None,
        inputs=None,
    ):
        super().__init__(name, inputs=inputs)
        self.column_info = column_info
        self.spark_session = spark_session
        self.ttl = ttl
        self.filter_condition = filter_condition

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.SPARK)
        input_df: DataFrame = input_value.data

        # Get timestamp fields from feature view
        timestamp_column = self.column_info.timestamp_column

        # Optional filter: feature.ts <= entity.event_timestamp
        filtered_df = input_df
        if ENTITY_TS_ALIAS in input_df.columns:
            filtered_df = filtered_df.filter(
                F.col(timestamp_column) <= F.col(ENTITY_TS_ALIAS)
            )

            # Optional TTL filter: feature.ts >= entity.event_timestamp - ttl
            if self.ttl:
                ttl_seconds = int(self.ttl.total_seconds())
                lower_bound = F.col(ENTITY_TS_ALIAS) - F.expr(
                    f"INTERVAL {ttl_seconds} seconds"
                )
                filtered_df = filtered_df.filter(F.col(timestamp_column) >= lower_bound)

        # Optional custom filter condition
        if self.filter_condition:
            filtered_df = filtered_df.filter(self.filter_condition)

        return DAGValue(
            data=filtered_df,
            format=DAGFormat.SPARK,
            metadata={"filter_applied": True},
        )


class SparkDedupNode(DAGNode):
    def __init__(
        self,
        name: str,
        column_info: ColumnInfo,
        spark_session: SparkSession,
        inputs=None,
    ):
        super().__init__(name, inputs=inputs)
        self.column_info = column_info
        self.spark_session = spark_session

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_value = self.get_single_input_value(context)
        input_value.assert_format(DAGFormat.SPARK)
        input_df: DataFrame = input_value.data

        # Dedup based on join keys and event timestamp column
        # Dedup with row_number
        partition_cols = self.column_info.join_keys
        deduped_df = input_df
        if partition_cols:
            ordering = [F.col(self.column_info.timestamp_column).desc()]
            if self.column_info.created_timestamp_column:
                ordering.append(F.col(self.column_info.created_timestamp_column).desc())

            window = Window.partitionBy(*partition_cols).orderBy(*ordering)
            deduped_df = (
                input_df.withColumn("row_num", F.row_number().over(window))
                .filter("row_num = 1")
                .drop("row_num")
            )

        return DAGValue(
            data=deduped_df,
            format=DAGFormat.SPARK,
            metadata={"deduped": True},
        )


class SparkWriteNode(DAGNode):
    def __init__(
        self,
        name: str,
        feature_view: Union[BatchFeatureView, StreamFeatureView],
        inputs=None,
    ):
        super().__init__(name, inputs=inputs)
        self.feature_view = feature_view

    def execute(self, context: ExecutionContext) -> DAGValue:
        spark_df: DataFrame = self.get_single_input_value(context).data
        serialized_artifacts = SerializedArtifacts.serialize(
            feature_view=self.feature_view, repo_config=context.repo_config
        )

        # ✅ 1. Write to online store if online enabled
        if self.feature_view.online:
            spark_df.mapInArrow(
                lambda x: map_in_arrow(x, serialized_artifacts, mode="online"),
                spark_df.schema,
            ).count()

        # ✅ 2. Write to offline store if offline enabled
        if self.feature_view.offline:
            if not isinstance(self.feature_view.batch_source, SparkSource):
                spark_df.mapInArrow(
                    lambda x: map_in_arrow(x, serialized_artifacts, mode="offline"),
                    spark_df.schema,
                ).count()
            # Directly write spark df to spark offline store without using mapInArrow
            else:
                dest_path = self.feature_view.batch_source.path
                file_format = self.feature_view.batch_source.file_format
                if not dest_path or not file_format:
                    raise ValueError(
                        "Destination path and file format must be specified for SparkSource."
                    )
                spark_df.write.format(file_format).mode("append").save(dest_path)

        return DAGValue(
            data=spark_df,
            format=DAGFormat.SPARK,
            metadata={
                "feature_view": self.feature_view.name,
                "write_to_online": self.feature_view.online,
                "write_to_offline": self.feature_view.offline,
            },
        )


class SparkTransformationNode(DAGNode):
    def __init__(self, name: str, udf: Callable, inputs: List[DAGNode]):
        super().__init__(name, inputs)
        self.udf = udf

    def execute(self, context: ExecutionContext) -> DAGValue:
        input_values = self.get_input_values(context)
        for val in input_values:
            val.assert_format(DAGFormat.SPARK)

        input_dfs: List[DataFrame] = [val.data for val in input_values]

        transformed_df = self.udf(*input_dfs)

        return DAGValue(
            data=transformed_df, format=DAGFormat.SPARK, metadata={"transformed": True}
        )
