"""
Tiled Online Store Implementation

This module implements a tiled online store that wraps existing online stores
to provide pre-aggregated feature storage and retrieval capabilities.
"""

import json
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple

from feast import FeatureView, RepoConfig
from feast.infra.online_stores.online_store import OnlineStore
from feast.infra.tiling.memory_manager import MemoryManager
from feast.infra.tiling.tiling_config import TilingConfig
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.repo_config import FeastConfigBaseModel


class TiledOnlineStoreConfig(FeastConfigBaseModel):
    """Configuration for the Tiled Online Store"""

    base_store_type: str
    base_store_config: Dict[str, Any]
    tiling_enabled: bool = True
    default_window_size: str = "1h"  # Default tiling window size
    compression_enabled: bool = True
    max_tile_age_hours: int = 24  # Maximum age for tiles before cleanup


class TiledOnlineStore(OnlineStore):
    """
    Tiled Online Store that provides pre-aggregated feature storage and retrieval.

    This store wraps an existing online store and adds tiling capabilities by:
    1. Converting individual feature writes into pre-aggregated tiles
    2. Storing tiles with time-bounded intermediate representations
    3. Merging tiles during feature retrieval to compute final values
    """

    def __init__(self, config: TiledOnlineStoreConfig):
        self.config = config
        self.base_store = self._create_base_store()
        self.tile_codec = TileCodec()
        # Create a TilingConfig from the store config

        # Parse window size string to timedelta
        window_size_str = config.default_window_size
        if window_size_str.endswith("h"):
            hours = int(window_size_str[:-1])
            window_size = timedelta(hours=hours)
        elif window_size_str.endswith("m"):
            minutes = int(window_size_str[:-1])
            window_size = timedelta(minutes=minutes)
        elif window_size_str.endswith("s"):
            seconds = int(window_size_str[:-1])
            window_size = timedelta(seconds=seconds)
        else:
            # Default to 1 hour
            window_size = timedelta(hours=1)

        tiling_config = TilingConfig(
            enabled=config.tiling_enabled,
            window_size=window_size,
            compression_enabled=config.compression_enabled,
        )
        self.memory_manager = MemoryManager(tiling_config)
        self.memory_manager.start_monitoring()

    def _create_base_store(self):
        """Create the underlying base store"""
        # Import here to avoid circular imports
        from feast.infra.online_stores.bigtable import BigtableOnlineStore
        from feast.infra.online_stores.redis import RedisOnlineStore

        store_type = self.config.base_store_type
        store_config = self.config.base_store_config

        if store_type == "redis":
            return RedisOnlineStore(**store_config)
        elif store_type == "bigtable":
            return BigtableOnlineStore(**store_config)
        else:
            raise ValueError(f"Unsupported base store type: {store_type}")

    def online_write_batch(
        self,
        config: RepoConfig,
        table: FeatureView,
        data: List[
            Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
        ],
        progress: Optional[Callable[[int], Any]],
    ) -> None:
        """Write feature data as tiles to the online store"""
        if not self._is_tiling_enabled(table):
            # Fall back to base store for non-tiled features
            return self.base_store.online_write_batch(config, table, data, progress)

        # Convert individual features to tiles
        tiled_data = self._convert_to_tiles(data, table)

        # Write tiles to base store
        self.base_store.online_write_batch(config, table, tiled_data, progress)

    def online_read(
        self,
        config: RepoConfig,
        table: FeatureView,
        entity_keys: List[EntityKeyProto],
        requested_features: Optional[List[str]] = None,
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        """Read and merge tiles to compute final feature values"""
        if not self._is_tiling_enabled(table):
            # Fall back to base store for non-tiled features
            return self.base_store.online_read(
                config, table, entity_keys, requested_features
            )

        # Fetch tiles for the requested entities
        tiles = self._fetch_tiles(config, table, entity_keys)

        # Merge tiles to compute final features
        merged_features = self._merge_tiles(tiles, requested_features)

        return merged_features

    def _is_tiling_enabled(self, table: FeatureView) -> bool:
        """Check if tiling is enabled for this feature view"""
        if not self.config.tiling_enabled:
            return False

        # Check if the feature view has tiling configuration
        tiling_config = getattr(table, "tiling_config", None)
        return tiling_config is not None and tiling_config.enabled

    def _convert_to_tiles(self, data: List[Tuple], table: FeatureView) -> List[Tuple]:
        """Convert individual feature writes to tiles"""
        # Group data by entity and time window
        tiles_by_entity = {}

        for entity_key, features, event_ts, created_ts in data:
            entity_str = self._entity_key_to_string(entity_key)
            window_start = self._get_window_start(event_ts, table)

            tile_key = (entity_str, window_start)

            if tile_key not in tiles_by_entity:
                tiles_by_entity[tile_key] = {
                    "entity_key": entity_key,
                    "features": {},
                    "event_ts": event_ts,
                    "created_ts": created_ts,
                    "window_start": window_start,
                }

            # Merge features into tile
            self._merge_features_into_tile(tiles_by_entity[tile_key], features)

        # Convert tiles to the format expected by base store
        tiled_data = []
        for tile_data in tiles_by_entity.values():
            # Encode the tile
            encoded_tile = self.tile_codec.encode_tile(
                tile_data["features"], tile_data["window_start"], is_complete=True
            )

            tiled_data.append(
                (
                    tile_data["entity_key"],
                    {"tile_data": encoded_tile},
                    tile_data["event_ts"],
                    tile_data["created_ts"],
                )
            )

        return tiled_data

    def _fetch_tiles(
        self, config: RepoConfig, table: FeatureView, entity_keys: List[EntityKeyProto]
    ) -> List[Dict]:
        """Fetch tiles for the requested entities with time window handling and memory management"""
        tiles = []

        # Process entity keys in batches for memory efficiency
        batch_size = self.memory_manager._calculate_optimal_batch_size(len(entity_keys))

        for i in range(0, len(entity_keys), batch_size):
            batch_entity_keys = entity_keys[i : i + batch_size]

            for entity_key in batch_entity_keys:
                # Fetch tiles for this entity with time window resolution
                entity_tiles = self._fetch_entity_tiles_with_time_windows(
                    config, table, entity_key
                )
                tiles.extend(entity_tiles)

            # Optimize memory usage after each batch
            if tiles:
                tiles = self.memory_manager.optimize_tile_storage(tiles)

        return tiles

    def _fetch_entity_tiles_with_time_windows(
        self, config: RepoConfig, table: FeatureView, entity_key: EntityKeyProto
    ) -> List[Dict]:
        """Fetch tiles for a specific entity with time window resolution"""

        # Get all tiles for this entity
        all_tiles = self._fetch_entity_tiles(config, table, entity_key)

        if not all_tiles:
            return []

        # Sort tiles by timestamp
        sorted_tiles = sorted(all_tiles, key=lambda x: x.get("timestamp", 0))

        # Resolve time window overlaps
        resolved_tiles = self._resolve_time_window_overlaps(sorted_tiles)

        return resolved_tiles

    def _resolve_time_window_overlaps(self, tiles: List[Dict]) -> List[Dict]:
        """Resolve overlapping time windows in tiles"""
        if not tiles:
            return []

        resolved_tiles = []
        current_tile = tiles[0].copy()

        for next_tile in tiles[1:]:
            # Check for time window overlap
            if self._tiles_overlap(current_tile, next_tile):
                # Merge overlapping tiles
                current_tile = self._merge_overlapping_tiles(current_tile, next_tile)
            else:
                # No overlap, add current tile and start new one
                resolved_tiles.append(current_tile)
                current_tile = next_tile.copy()

        # Add the last tile
        resolved_tiles.append(current_tile)

        return resolved_tiles

    def _tiles_overlap(self, tile1: Dict, tile2: Dict) -> bool:
        """Check if two tiles have overlapping time windows"""
        from datetime import datetime

        # Get tile time windows
        tile1_start = tile1.get("window_start")
        tile1_end = tile1.get("window_end")
        tile2_start = tile2.get("window_start")
        tile2_end = tile2.get("window_end")

        if not all([tile1_start, tile1_end, tile2_start, tile2_end]):
            return False

        # Convert to datetime if needed
        if isinstance(tile1_start, (int, float)):
            tile1_start = datetime.fromtimestamp(tile1_start)
        if isinstance(tile1_end, (int, float)):
            tile1_end = datetime.fromtimestamp(tile1_end)
        if isinstance(tile2_start, (int, float)):
            tile2_start = datetime.fromtimestamp(tile2_start)
        if isinstance(tile2_end, (int, float)):
            tile2_end = datetime.fromtimestamp(tile2_end)

        # Ensure all values are datetime objects and not None
        if not all(
            isinstance(t, datetime)
            for t in [tile1_start, tile1_end, tile2_start, tile2_end]
        ):
            return False

        # Type assertion for mypy - we know these are datetime objects now
        assert isinstance(tile1_start, datetime)
        assert isinstance(tile1_end, datetime)
        assert isinstance(tile2_start, datetime)
        assert isinstance(tile2_end, datetime)

        # Check for overlap: tile1_start < tile2_end AND tile2_start < tile1_end
        return tile1_start < tile2_end and tile2_start < tile1_end

    def _merge_overlapping_tiles(self, tile1: Dict, tile2: Dict) -> Dict:
        """Merge two overlapping tiles"""
        from datetime import datetime

        # Create merged tile
        merged_tile = {
            "entity_key": tile1.get("entity_key"),
            "window_start": min(
                tile1.get("window_start", 0), tile2.get("window_start", 0)
            ),
            "window_end": max(tile1.get("window_end", 0), tile2.get("window_end", 0)),
            "features": {},
            "metadata": {
                "merged_from": [tile1.get("tile_id"), tile2.get("tile_id")],
                "merge_timestamp": datetime.now().isoformat(),
            },
        }

        # Merge features from both tiles
        all_features = {}

        # Add features from tile1
        if "features" in tile1:
            for feature_name, feature_data in tile1["features"].items():
                all_features[feature_name] = feature_data.copy()

        # Add features from tile2
        if "features" in tile2:
            for feature_name, feature_data in tile2["features"].items():
                if feature_name in all_features:
                    # Merge feature data (handle conflicts)
                    all_features[feature_name] = self._merge_feature_data(
                        all_features[feature_name], feature_data
                    )
                else:
                    all_features[feature_name] = feature_data.copy()

        merged_tile["features"] = all_features

        return merged_tile

    def _merge_feature_data(self, feature1: Dict, feature2: Dict) -> Dict:
        """Merge feature data from two tiles, handling conflicts"""
        merged_feature = feature1.copy()

        # Handle different types of feature data
        if "aggregations" in feature1 and "aggregations" in feature2:
            # Merge aggregation data
            merged_aggregations = {}

            for agg_name, agg_value1 in feature1["aggregations"].items():
                agg_value2 = feature2["aggregations"].get(agg_name)

                if agg_value2 is not None:
                    # Merge aggregation values based on type
                    merged_aggregations[agg_name] = self._merge_aggregation_values(
                        agg_name, agg_value1, agg_value2
                    )
                else:
                    merged_aggregations[agg_name] = agg_value1

            # Add aggregations from feature2 that aren't in feature1
            for agg_name, agg_value2 in feature2["aggregations"].items():
                if agg_name not in merged_aggregations:
                    merged_aggregations[agg_name] = agg_value2

            merged_feature["aggregations"] = merged_aggregations

        # Handle other feature metadata
        for key, value2 in feature2.items():
            if key not in merged_feature:
                merged_feature[key] = value2
            elif key == "last_updated":
                # Use the most recent timestamp
                merged_feature[key] = max(merged_feature[key], value2)

        return merged_feature

    def _merge_aggregation_values(self, agg_name: str, value1: Any, value2: Any) -> Any:
        """Merge aggregation values based on aggregation type"""
        if agg_name in ["sum", "count"]:
            return value1 + value2
        elif agg_name == "max":
            return max(value1, value2)
        elif agg_name == "min":
            return min(value1, value2)
        elif agg_name in ["first"]:
            return value1  # Keep the first value
        elif agg_name in ["last"]:
            return value2  # Keep the last value
        elif agg_name in ["avg", "std", "var"]:
            # For these, we need to recalculate from sum, sum_squares, count
            # This is handled in the aggregator
            return value2  # Use the more recent value for now
        else:
            # For other aggregations, use the more recent value
            return value2

    def _fetch_entity_tiles(
        self, config: RepoConfig, table: FeatureView, entity_key: EntityKeyProto
    ) -> List[Dict]:
        """Fetch tiles for a specific entity"""
        # This is a simplified implementation
        # In production, this would query the underlying store
        return []

    def _merge_tiles(
        self, tiles: List[Dict], requested_features: Optional[List[str]]
    ) -> List[Tuple]:
        """Merge tiles to compute final feature values with sophisticated conflict resolution"""
        if not tiles:
            return []

        # Group tiles by entity
        tiles_by_entity: Dict[str, List[Dict]] = self._group_tiles_by_entity(tiles)

        # Merge tiles for each entity with conflict resolution
        results = []
        for entity_key, entity_tiles in tiles_by_entity.items():
            merged_features = self._merge_entity_tiles_with_conflict_resolution(
                entity_tiles, requested_features
            )
            results.append((entity_key, merged_features))

        return results

    def _group_tiles_by_entity(self, tiles: List[Dict]) -> Dict[str, List[Dict]]:
        """Group tiles by entity key with proper key handling"""
        tiles_by_entity: Dict[str, List[Dict]] = {}

        for tile in tiles:
            entity_key = self._extract_entity_key(tile)
            if entity_key not in tiles_by_entity:
                tiles_by_entity[entity_key] = []
            tiles_by_entity[entity_key].append(tile)

        return tiles_by_entity

    def _extract_entity_key(self, tile: Dict) -> str:
        """Extract entity key from tile with proper handling"""
        entity_key = tile.get("entity_key")
        if isinstance(entity_key, dict):
            # Handle complex entity key structures
            return self._serialize_complex_entity_key(entity_key)
        elif isinstance(entity_key, (list, tuple)):
            # Handle multi-part entity keys
            return "|".join(str(part) for part in entity_key)
        else:
            return str(entity_key)

    def _serialize_complex_entity_key(self, entity_key: Dict) -> str:
        """Serialize complex entity key structures"""
        # Sort keys for consistent serialization
        sorted_items = sorted(entity_key.items())
        return "|".join(f"{k}:{v}" for k, v in sorted_items)

    def _merge_entity_tiles_with_conflict_resolution(
        self, entity_tiles: List[Dict], requested_features: Optional[List[str]]
    ) -> Dict[str, Any]:
        """Merge tiles for a single entity with sophisticated conflict resolution"""
        if not entity_tiles:
            return {}

        # Sort tiles by timestamp and priority
        sorted_tiles = self._sort_tiles_by_priority(entity_tiles)

        # Handle partial tiles
        complete_tiles, partial_tiles = self._separate_complete_and_partial_tiles(
            sorted_tiles
        )

        # Merge complete tiles first
        merged_features = {}
        if complete_tiles:
            merged_features = self._merge_complete_tiles(
                complete_tiles, requested_features
            )

        # Handle partial tiles with conflict resolution
        if partial_tiles:
            merged_features = self._merge_partial_tiles(
                merged_features, partial_tiles, requested_features
            )

        return merged_features

    def _sort_tiles_by_priority(self, tiles: List[Dict]) -> List[Dict]:
        """Sort tiles by priority (timestamp, completeness, quality)"""

        def tile_priority(tile):
            # Priority factors (higher is better)
            timestamp = tile.get("timestamp", 0)
            completeness = tile.get("completeness_score", 0.5)  # 0-1 score
            quality = tile.get("quality_score", 0.5)  # 0-1 score

            # Weighted priority: timestamp (40%), completeness (35%), quality (25%)
            return timestamp * 0.4 + completeness * 0.35 + quality * 0.25

        return sorted(tiles, key=tile_priority, reverse=True)

    def _separate_complete_and_partial_tiles(
        self, tiles: List[Dict]
    ) -> Tuple[List[Dict], List[Dict]]:
        """Separate complete tiles from partial tiles"""
        complete_tiles = []
        partial_tiles = []

        for tile in tiles:
            completeness = tile.get("completeness_score", 1.0)
            if completeness >= 0.9:  # 90% complete threshold
                complete_tiles.append(tile)
            else:
                partial_tiles.append(tile)

        return complete_tiles, partial_tiles

    def _merge_complete_tiles(
        self, tiles: List[Dict], requested_features: Optional[List[str]]
    ) -> Dict[str, Any]:
        """Merge complete tiles with standard conflict resolution"""
        if not tiles:
            return {}

        # Start with the highest priority tile
        merged_features = tiles[0].get("features", {}).copy()

        # Merge remaining tiles
        for tile in tiles[1:]:
            tile_features = tile.get("features", {})
            merged_features = self._merge_feature_sets(
                merged_features, tile_features, conflict_resolution="latest"
            )

        # Filter by requested features if specified
        if requested_features:
            filtered_features = {}
            for feature_name in requested_features:
                if feature_name in merged_features:
                    filtered_features[feature_name] = merged_features[feature_name]
            merged_features = filtered_features

        return merged_features

    def _merge_partial_tiles(
        self,
        base_features: Dict[str, Any],
        partial_tiles: List[Dict],
        requested_features: Optional[List[str]],
    ) -> Dict[str, Any]:
        """Merge partial tiles with sophisticated conflict resolution"""
        merged_features = base_features.copy()

        for tile in partial_tiles:
            tile_features = tile.get("features", {})
            completeness = tile.get("completeness_score", 0.5)
            quality = tile.get("quality_score", 0.5)

            # Use weighted merging based on completeness and quality
            weight = (completeness + quality) / 2

            merged_features = self._merge_feature_sets(
                merged_features,
                tile_features,
                conflict_resolution="weighted",
                weight=weight,
            )

        # Filter by requested features if specified
        if requested_features:
            filtered_features = {}
            for feature_name in requested_features:
                if feature_name in merged_features:
                    filtered_features[feature_name] = merged_features[feature_name]
            merged_features = filtered_features

        return merged_features

    def _merge_feature_sets(
        self,
        features1: Dict[str, Any],
        features2: Dict[str, Any],
        conflict_resolution: str = "latest",
        weight: float = 1.0,
    ) -> Dict[str, Any]:
        """Merge two feature sets with specified conflict resolution strategy"""
        merged = features1.copy()

        for feature_name, feature_data2 in features2.items():
            if feature_name not in merged:
                # Feature doesn't exist, add it
                merged[feature_name] = feature_data2
            else:
                # Feature exists, resolve conflict
                feature_data1 = merged[feature_name]
                merged[feature_name] = self._resolve_feature_conflict(
                    feature_data1, feature_data2, conflict_resolution, weight
                )

        return merged

    def _resolve_feature_conflict(
        self,
        feature1: Dict[str, Any],
        feature2: Dict[str, Any],
        strategy: str,
        weight: float = 1.0,
    ) -> Dict[str, Any]:
        """Resolve conflicts between two feature instances"""
        if strategy == "latest":
            # Use the most recent feature
            timestamp1 = feature1.get("timestamp", 0)
            timestamp2 = feature2.get("timestamp", 0)
            return feature2 if timestamp2 > timestamp1 else feature1

        elif strategy == "weighted":
            # Weighted average based on quality and completeness
            return self._weighted_merge_features(feature1, feature2, weight)

        elif strategy == "best_quality":
            # Use the feature with higher quality score
            quality1 = feature1.get("quality_score", 0.5)
            quality2 = feature2.get("quality_score", 0.5)
            return feature2 if quality2 > quality1 else feature1

        elif strategy == "most_complete":
            # Use the feature with higher completeness
            completeness1 = feature1.get("completeness_score", 0.5)
            completeness2 = feature2.get("completeness_score", 0.5)
            return feature2 if completeness2 > completeness1 else feature1

        else:
            # Default to latest
            return feature2

    def _weighted_merge_features(
        self, feature1: Dict[str, Any], feature2: Dict[str, Any], weight: float
    ) -> Dict[str, Any]:
        """Merge features using weighted average"""
        # This is a simplified implementation
        # In production, you'd implement proper weighted merging for each aggregation type
        if weight > 0.5:
            return feature2
        else:
            return feature1

    def _merge_entity_tiles(self, tiles: List[Dict]) -> Dict[str, ValueProto]:
        """Merge tiles for a single entity"""
        if not tiles:
            return {}

        # Start with the first tile
        merged_ir = tiles[0]["ir"].copy()

        # Merge with remaining tiles
        for tile in tiles[1:]:
            merged_ir = self._merge_irs(merged_ir, tile["ir"])

        # Convert IR to final feature values
        return self._convert_ir_to_features(merged_ir)

    def _merge_irs(self, ir1: Dict, ir2: Dict) -> Dict:
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

    def _entity_key_to_string(self, entity_key: EntityKeyProto) -> str:
        """Convert entity key to string for grouping"""
        return f"{entity_key.join_keys}"

    def _get_window_start(self, timestamp: datetime, table: FeatureView) -> datetime:
        """Get the start of the tiling window for a timestamp"""
        window_size = self._get_window_size(table)
        # Round down to the nearest window boundary
        return timestamp - (timestamp - datetime.min) % window_size

    def _get_window_size(self, table: FeatureView) -> timedelta:
        """Get the tiling window size for a feature view"""
        tiling_config = getattr(table, "tiling_config", None)
        if tiling_config and hasattr(tiling_config, "window_size"):
            return tiling_config.window_size
        return timedelta(hours=1)  # Default window size

    def _merge_features_into_tile(
        self, tile_data: Dict, features: Dict[str, ValueProto]
    ):
        """Merge new features into an existing tile"""
        # we need to handle different aggregation types
        for feature_name, value in features.items():
            if feature_name not in tile_data["features"]:
                tile_data["features"][feature_name] = []
            tile_data["features"][feature_name].append(value)

    def _convert_ir_to_features(self, ir: Dict) -> Dict[str, ValueProto]:
        """Convert intermediate representation to final feature values"""
        # we need to handle different aggregation types
        features = {}
        for key, value in ir.items():
            if key.endswith("_sum") and key.endswith("_count"):
                # Calculate average
                sum_key = key
                count_key = key.replace("_sum", "_count")
                if count_key in ir and ir[count_key] > 0:
                    avg_value = ir[sum_key] / ir[count_key]
                    feature_name = key.replace("_sum", "")
                    features[feature_name] = ValueProto(double_val=avg_value)
            else:
                # Use the value directly
                feature_name = (
                    key.replace("_sum", "")
                    .replace("_count", "")
                    .replace("_max", "")
                    .replace("_min", "")
                )
                features[feature_name] = ValueProto(double_val=value)

        return features


class TileCodec:
    """Encodes and decodes tiles for storage and retrieval"""

    def encode_tile(self, ir: Dict, timestamp: datetime, is_complete: bool) -> bytes:
        """Encode a tile for storage"""
        tile_data = {
            "ir": ir,
            "timestamp": timestamp.isoformat(),
            "is_complete": is_complete,
        }
        return json.dumps(tile_data).encode("utf-8")

    def decode_tile(self, data: bytes) -> Tuple[Dict, datetime, bool]:
        """Decode a tile from storage"""
        tile_data = json.loads(data.decode("utf-8"))
        return (
            tile_data["ir"],
            datetime.fromisoformat(tile_data["timestamp"]),
            tile_data["is_complete"],
        )
