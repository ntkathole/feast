"""
This module implements the core aggregation logic for tiling support,
managing intermediate representations and their merging.
"""

from typing import Any, Dict, List, Optional

from feast.aggregation import Aggregation
from feast.feature_view import FeatureView
from feast.infra.tiling.tiling_config import TilingConfig
from feast.protos.feast.types.Value_pb2 import Value as ValueProto


class TiledAggregator:
    """
    Manages intermediate representations (IRs) for feature aggregations.

    This class handles the creation, updating, and merging of intermediate
    representations that can be used to compute final feature values.
    """

    def __init__(self, feature_view: FeatureView, tiling_config: "TilingConfig"):
        self.feature_view = feature_view
        self.tiling_config = tiling_config
        self.aggregation_specs = self._build_aggregation_specs()

    def _build_aggregation_specs(self) -> Dict[str, Dict[str, Any]]:
        """Build aggregation specifications for each feature based on user-defined aggregations"""
        specs = {}

        # Only process features that have user-defined aggregations
        if (
            hasattr(self.feature_view, "aggregations")
            and self.feature_view.aggregations
        ):
            # Group aggregations by column/feature name
            aggregations_by_feature: Dict[str, List[Aggregation]] = {}
            for agg in self.feature_view.aggregations:
                if agg.column not in aggregations_by_feature:
                    aggregations_by_feature[agg.column] = []
                aggregations_by_feature[agg.column].append(agg)

            # Build specs for features with aggregations
            for feature_name, aggregations in aggregations_by_feature.items():
                # Get feature dtype from the feature view schema
                feature_dtype = self._get_feature_dtype(feature_name)
                specs[feature_name] = {
                    "dtype": feature_dtype,
                    "aggregations": [agg.function for agg in aggregations],
                    "time_windows": [agg.time_window for agg in aggregations],
                    "slide_intervals": [agg.slide_interval for agg in aggregations],
                }

        return specs

    def _get_feature_dtype(self, feature_name: str) -> str:
        """Get the data type of a feature from the feature view schema"""
        if hasattr(self.feature_view, "schema") and self.feature_view.schema:
            for field in self.feature_view.schema:
                if field.name == feature_name:
                    return str(field.dtype)

        # Default to float64 if not found
        return "float64"

    def _get_feature_aggregations(self, feature_name: str) -> List[str]:
        """Get the aggregation functions for a feature from user-defined aggregations"""
        # Check if the feature view has user-defined aggregations
        if (
            hasattr(self.feature_view, "aggregations")
            and self.feature_view.aggregations
        ):
            # Extract aggregations for this specific feature
            feature_aggregations = []
            for agg in self.feature_view.aggregations:
                if agg.column == feature_name:
                    feature_aggregations.append(agg.function)
            return feature_aggregations if feature_aggregations else []

        # If no user-defined aggregations, return empty list
        # The tiling system should only work with explicitly defined aggregations
        return []

    def create_initial_ir(self) -> Dict[str, Any]:
        """Create an initial intermediate representation"""
        ir = {}

        for feature_name, spec in self.aggregation_specs.items():
            for agg_func in spec["aggregations"]:
                key = f"{feature_name}_{agg_func}"
                ir[key] = self._get_initial_value(agg_func)

        return ir

    def _get_initial_value(self, aggregation: str) -> Any:
        """Get the initial value for an aggregation function"""
        initial_values = {
            "sum": 0,
            "count": 0,
            "max": float("-inf"),
            "min": float("inf"),
            "avg": 0,  # Will be calculated from sum/count
            "std": 0,
            "var": 0,
            "first": None,
            "last": None,
            "median": 0,
            "p25": 0,  # 25th percentile
            "p75": 0,  # 75th percentile
            "p90": 0,  # 90th percentile
            "p95": 0,  # 95th percentile
            "p99": 0,  # 99th percentile
            "mode": None,
            "distinct_count": 0,
            "null_count": 0,
        }
        return initial_values.get(aggregation, 0)

    def update_ir(
        self,
        ir: Dict[str, Any],
        feature_name: str,
        value: Any,
        timestamp: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Update an intermediate representation with a new value"""
        updated_ir = ir.copy()

        if feature_name not in self.aggregation_specs:
            # If no aggregations are defined for this feature, return unchanged IR
            return updated_ir

        feature_spec = self.aggregation_specs[feature_name]
        aggregations = feature_spec["aggregations"]
        time_windows = feature_spec.get("time_windows", [])
        slide_intervals = feature_spec.get("slide_intervals", [])

        for i, agg_func in enumerate(aggregations):
            # Create key with time window info if applicable
            time_window = time_windows[i] if i < len(time_windows) else None
            slide_interval = slide_intervals[i] if i < len(slide_intervals) else None

            key = self._create_aggregation_key(
                feature_name, agg_func, time_window, slide_interval
            )
            current_value = updated_ir.get(key, self._get_initial_value(agg_func))
            updated_ir[key] = self._update_aggregation(
                current_value, value, agg_func, timestamp, updated_ir, time_window
            )

        return updated_ir

    def _create_aggregation_key(
        self,
        feature_name: str,
        agg_func: str,
        time_window: Optional[Any] = None,
        slide_interval: Optional[Any] = None,
    ) -> str:
        """Create a unique key for an aggregation considering time windows"""
        key = f"{feature_name}_{agg_func}"

        if time_window:
            # Add time window info to the key
            if hasattr(time_window, "total_seconds"):
                window_seconds = int(time_window.total_seconds())
                key += f"_{window_seconds}s"
            else:
                key += f"_{time_window}"

        if slide_interval and slide_interval != time_window:
            # Add slide interval info if different from time window
            if hasattr(slide_interval, "total_seconds"):
                slide_seconds = int(slide_interval.total_seconds())
                key += f"_slide_{slide_seconds}s"
            else:
                key += f"_slide_{slide_interval}"

        return key

    def _update_aggregation(
        self,
        current_value: Any,
        new_value: Any,
        aggregation: str,
        timestamp: Optional[Any] = None,
        ir: Optional[Dict] = None,
        time_window: Optional[Any] = None,
    ) -> Any:
        """Update a single aggregation with a new value - supports only predefined functions"""

        # Only support predefined aggregation functions (following Chronon's approach)
        if aggregation == "sum":
            return current_value + new_value
        elif aggregation == "count":
            return current_value + 1
        elif aggregation == "max":
            return max(current_value, new_value)
        elif aggregation == "min":
            return min(current_value, new_value)
        elif aggregation == "first":
            return current_value if current_value is not None else new_value
        elif aggregation == "last":
            return new_value
        elif aggregation in ["avg", "mean"]:
            # For average, we maintain sum and count separately
            return current_value  # This would need special handling
        elif aggregation == "std":
            # For standard deviation, we need to maintain sum, sum_squares, and count
            if ir is None:
                ir = {}
            return self._update_std_aggregation(current_value, new_value, ir)
        elif aggregation == "var":
            # For variance, we need to maintain sum, sum_squares, and count
            if ir is None:
                ir = {}
            return self._update_var_aggregation(current_value, new_value, ir)
        elif aggregation == "median":
            # For median, we maintain a list of values (memory intensive)
            return self._update_median_aggregation(current_value, new_value)
        elif aggregation.startswith("p"):
            # Percentile aggregations
            percentile = int(aggregation[1:])
            return self._update_percentile_aggregation(
                current_value, new_value, percentile
            )
        elif aggregation == "mode":
            # For mode, we maintain a frequency count
            return self._update_mode_aggregation(current_value, new_value)
        elif aggregation == "distinct_count":
            # For distinct count, we maintain a set of unique values
            return self._update_distinct_count_aggregation(current_value, new_value)
        elif aggregation == "null_count":
            return current_value + (1 if new_value is None else 0)
        else:
            # Only support predefined functions - raise error for unknown functions
            raise ValueError(
                f"Unsupported aggregation function: {aggregation}. "
                f"Supported functions: sum, count, max, min, first, last, avg, mean, std, var, "
                f"median, mode, distinct_count, null_count, p25, p50, p75, p90, p95, p99"
            )

    def merge_irs(self, ir1: Dict[str, Any], ir2: Dict[str, Any]) -> Dict[str, Any]:
        """Merge two intermediate representations"""
        merged = {}

        # Get all keys from both IRs
        all_keys = set(ir1.keys()) | set(ir2.keys())

        for key in all_keys:
            if key.endswith("_sum"):
                merged[key] = ir1.get(key, 0) + ir2.get(key, 0)
            elif key.endswith("_count"):
                merged[key] = ir1.get(key, 0) + ir2.get(key, 0)
            elif key.endswith("_max"):
                merged[key] = max(
                    ir1.get(key, float("-inf")), ir2.get(key, float("-inf"))
                )
            elif key.endswith("_min"):
                merged[key] = min(
                    ir1.get(key, float("inf")), ir2.get(key, float("inf"))
                )
            else:
                # For other aggregations, use the latest value
                merged[key] = ir2.get(key, ir1.get(key))

        return merged

    def _update_std_aggregation(
        self, current_value: Any, new_value: Any, ir: Dict[str, Any]
    ) -> Any:
        """Update standard deviation aggregation using Welford's algorithm"""
        # We need to maintain sum, sum_squares, and count for std calculation
        # This is a simplified implementation - in production, use Welford's algorithm
        return current_value  # Placeholder for now

    def _update_var_aggregation(
        self, current_value: Any, new_value: Any, ir: Dict[str, Any]
    ) -> Any:
        """Update variance aggregation"""
        # Similar to std, we need sum, sum_squares, and count
        return current_value  # Placeholder for now

    def _update_median_aggregation(self, current_value: Any, new_value: Any) -> Any:
        """Update median aggregation (memory intensive)"""
        if current_value is None:
            return [new_value]
        if isinstance(current_value, list):
            current_value.append(new_value)
            return current_value
        return [current_value, new_value]

    def _update_percentile_aggregation(
        self, current_value: Any, new_value: Any, percentile: int
    ) -> Any:
        """Update percentile aggregation"""
        if current_value is None:
            return [new_value]
        if isinstance(current_value, list):
            current_value.append(new_value)
            return current_value
        return [current_value, new_value]

    def _update_mode_aggregation(self, current_value: Any, new_value: Any) -> Any:
        """Update mode aggregation"""
        if current_value is None:
            return {new_value: 1}
        if isinstance(current_value, dict):
            current_value[new_value] = current_value.get(new_value, 0) + 1
            return current_value
        return {current_value: 1, new_value: 1}

    def _update_distinct_count_aggregation(
        self, current_value: Any, new_value: Any
    ) -> Any:
        """Update distinct count aggregation"""
        if current_value is None:
            return {new_value}
        if isinstance(current_value, set):
            current_value.add(new_value)
            return current_value
        return {current_value, new_value}

    def finalize_ir(self, ir: Dict[str, Any]) -> Dict[str, ValueProto]:
        """Convert intermediate representation to final feature values"""
        import statistics

        features = {}

        for feature_name, spec in self.aggregation_specs.items():
            for agg_func in spec["aggregations"]:
                key = f"{feature_name}_{agg_func}"

                if key in ir:
                    if agg_func == "avg":
                        # Calculate average from sum and count
                        sum_key = f"{feature_name}_sum"
                        count_key = f"{feature_name}_count"
                        if sum_key in ir and count_key in ir and ir[count_key] > 0:
                            avg_value = ir[sum_key] / ir[count_key]
                            features[f"{feature_name}_avg"] = self._create_value_proto(
                                avg_value, spec["dtype"]
                            )
                    elif agg_func == "std":
                        # Calculate standard deviation from sum, sum_squares, and count
                        sum_key = f"{feature_name}_sum"
                        sum_squares_key = f"{feature_name}_sum_squares"
                        count_key = f"{feature_name}_count"
                        if (
                            all(k in ir for k in [sum_key, sum_squares_key, count_key])
                            and ir[count_key] > 1
                        ):
                            n = ir[count_key]
                            mean = ir[sum_key] / n
                            variance = (ir[sum_squares_key] / n) - (mean * mean)
                            std_value = variance**0.5 if variance > 0 else 0
                            features[f"{feature_name}_std"] = self._create_value_proto(
                                std_value, spec["dtype"]
                            )
                    elif agg_func == "var":
                        # Calculate variance from sum, sum_squares, and count
                        sum_key = f"{feature_name}_sum"
                        sum_squares_key = f"{feature_name}_sum_squares"
                        count_key = f"{feature_name}_count"
                        if (
                            all(k in ir for k in [sum_key, sum_squares_key, count_key])
                            and ir[count_key] > 1
                        ):
                            n = ir[count_key]
                            mean = ir[sum_key] / n
                            variance = (ir[sum_squares_key] / n) - (mean * mean)
                            features[f"{feature_name}_var"] = self._create_value_proto(
                                variance, spec["dtype"]
                            )
                    elif agg_func == "median":
                        # Calculate median from list of values
                        if isinstance(ir[key], list) and len(ir[key]) > 0:
                            median_value = statistics.median(ir[key])
                            features[f"{feature_name}_median"] = (
                                self._create_value_proto(median_value, spec["dtype"])
                            )
                    elif agg_func.startswith("p"):
                        # Calculate percentile from list of values
                        percentile = int(agg_func[1:])
                        if isinstance(ir[key], list) and len(ir[key]) > 0:
                            pct_value = (
                                statistics.quantiles(ir[key], n=100)[percentile - 1]
                                if percentile <= 100
                                else max(ir[key])
                            )
                            features[f"{feature_name}_{agg_func}"] = (
                                self._create_value_proto(pct_value, spec["dtype"])
                            )
                    elif agg_func == "mode":
                        # Calculate mode from frequency dictionary
                        if isinstance(ir[key], dict) and ir[key]:
                            mode_value = max(ir[key], key=ir[key].get)
                            features[f"{feature_name}_mode"] = self._create_value_proto(
                                mode_value, spec["dtype"]
                            )
                    elif agg_func == "distinct_count":
                        # Count distinct values
                        if isinstance(ir[key], set):
                            features[f"{feature_name}_distinct_count"] = (
                                self._create_value_proto(len(ir[key]), spec["dtype"])
                            )
                    else:
                        # Use the aggregation value directly (for supported functions)
                        features[f"{feature_name}_{agg_func}"] = (
                            self._create_value_proto(ir[key], spec["dtype"])
                        )

        return features

    def _create_value_proto(self, value: Any, dtype) -> ValueProto:
        """Create a ValueProto from a value and data type"""
        # This is a simplified implementation
        # In practice, you'd need to handle different data types properly
        if isinstance(value, (int, float)):
            return ValueProto(double_val=float(value))
        elif isinstance(value, str):
            return ValueProto(string_val=value)
        elif isinstance(value, bool):
            return ValueProto(bool_val=value)
        else:
            return ValueProto(string_val=str(value))

    def get_aggregation_functions(self) -> Dict[str, str]:
        """Get aggregation functions for Spark SQL"""
        functions = {}

        for feature_name, spec in self.aggregation_specs.items():
            for agg_func in spec["aggregations"]:
                key = f"{feature_name}_{agg_func}"
                if agg_func == "sum":
                    functions[key] = f"sum({feature_name})"
                elif agg_func == "count":
                    functions[key] = f"count({feature_name})"
                elif agg_func == "max":
                    functions[key] = f"max({feature_name})"
                elif agg_func == "min":
                    functions[key] = f"min({feature_name})"
                elif agg_func == "avg":
                    functions[key] = f"avg({feature_name})"

        return functions
