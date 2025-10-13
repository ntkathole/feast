from datetime import timedelta

import pandas as pd
import pyarrow as pa

from feast.infra.compute_engines.backends.base import DataFrameBackend


class PandasBackend(DataFrameBackend):
    def columns(self, df):
        return df.columns.tolist()

    def from_arrow(self, table):
        return table.to_pandas()

    def join(self, left, right, on, how):
        return left.merge(right, on=on, how=how)

    def groupby_agg(self, df, group_keys, agg_ops):
        # Extract only function and column for simple aggregations
        simple_agg_ops = {alias: (op[0], op[1]) for alias, op in agg_ops.items()}
        return (
            df.groupby(group_keys)
            .agg(
                **{
                    alias: pd.NamedAgg(column=col, aggfunc=func)
                    for alias, (func, col) in simple_agg_ops.items()
                }
            )
            .reset_index()
        )

    def groupby_agg_with_time_window(self, df, group_keys, agg_ops, timestamp_col):
        """
        Perform time-windowed aggregation using pandas

        Args:
            df: DataFrame to aggregate
            group_keys: Entity keys to group by
            agg_ops: Dictionary of {alias: (function, column, time_window)}
            timestamp_col: Name of timestamp column for windowing
        """
        # Convert timestamp to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(df[timestamp_col]):
            df[timestamp_col] = pd.to_datetime(df[timestamp_col])

        # Group aggregations by time window
        from collections import defaultdict

        aggs_by_window = defaultdict(dict)
        for alias, op in agg_ops.items():
            if len(op) > 2 and op[2] is not None:
                window_seconds = int(op[2].total_seconds())
                aggs_by_window[window_seconds][alias] = op
            else:
                # No time window - simple aggregation
                aggs_by_window[None][alias] = op

        if not aggs_by_window or (len(aggs_by_window) == 1 and None in aggs_by_window):
            # Fall back to simple aggregation
            return self.groupby_agg(df, group_keys, agg_ops)

        # Process each window separately and join results
        results = []
        df = df.sort_values(by=timestamp_col)

        for window_seconds, window_aggs in aggs_by_window.items():
            if window_seconds is None:
                continue

            # Create window bins
            df_windowed = df.copy()
            df_windowed["_window_start"] = df_windowed[timestamp_col].dt.floor(
                f"{window_seconds}s"
            )

            group_by_cols = group_keys + ["_window_start"]

            # Build aggregation dict for this window
            agg_dict = {}
            for alias, op in window_aggs.items():
                func, col = op[0], op[1]
                agg_dict[alias] = pd.NamedAgg(column=col, aggfunc=func)

            window_result = (
                df_windowed.groupby(group_by_cols).agg(**agg_dict).reset_index()
            )

            # Use window END time instead of START to avoid Redis rejecting "old" timestamps
            # Tiles represent aggregated data UP TO this timestamp
            window_result[timestamp_col] = window_result[
                "_window_start"
            ] + pd.Timedelta(seconds=window_seconds)
            window_result = window_result.drop(columns=["_window_start"])

            results.append(window_result)

        # Merge all window results
        if len(results) == 1:
            return results[0]
        else:
            # Join all results on entity keys + timestamp
            final_result = results[0]
            for result in results[1:]:
                final_result = final_result.merge(
                    result, on=group_keys + [timestamp_col], how="outer"
                )
            return final_result

    def filter(self, df, expr):
        return df.query(expr)

    def to_arrow(self, df):
        return pa.Table.from_pandas(df)

    def to_timedelta_value(self, delta: timedelta):
        return pd.to_timedelta(delta)

    def drop_duplicates(self, df, keys, sort_by, ascending: bool = False):
        return df.sort_values(by=sort_by, ascending=ascending).drop_duplicates(
            subset=keys
        )

    def rename_columns(self, df, columns: dict[str, str]):
        return df.rename(columns=columns)
