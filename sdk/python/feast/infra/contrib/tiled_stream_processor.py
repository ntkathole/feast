"""
This module implements a stream processor that creates pre-aggregated tiles
instead of individual events.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from feast import FeatureStore
from feast.data_source import DataSource, PushMode
from feast.infra.contrib.stream_processor import StreamProcessor
from feast.infra.tiling import TileCodec, TiledAggregator, TilingConfig
from feast.stream_feature_view import StreamFeatureView


class TiledStreamProcessor(StreamProcessor):
    """
    Stream processor that creates pre-aggregated tiles instead of individual events.

    This processor implements the tiled architecture pattern where:
    1. Events are grouped by entity and time windows
    2. Intermediate representations (IRs) are maintained for each window
    3. Tiles containing IRs are written to the online store
    4. Feature serving merges tiles to compute final values
    """

    def __init__(
        self,
        fs: "FeatureStore",
        sfv: StreamFeatureView,
        data_source: DataSource,
        tiling_config: Optional[TilingConfig] = None,
    ):
        super().__init__(fs, sfv, data_source)
        self.tiling_config = tiling_config or TilingConfig()
        self.aggregator = TiledAggregator(sfv, self.tiling_config)
        self.tile_codec = TileCodec(self.tiling_config)

    def ingest_stream_feature_view(
        self, to: PushMode = PushMode.ONLINE
    ) -> Optional[Any]:
        """
        Ingest data from the stream source and create tiles.

        Args:
            to: Where to write the data (online, offline, or both)

        Returns:
            Handle for the streaming job
        """
        # Get the stream data
        stream_table = self._ingest_stream_data()

        # Apply transformations
        transformed_table = self._construct_transformation_plan(stream_table)

        # Create tiles and write to stores
        return self._write_tiled_data(transformed_table, to)

    def _write_tiled_data(self, df: Any, to: PushMode) -> Any:
        """
        Create tiles from stream data and write to stores.

        Args:
            df: Stream table with transformed data
            to: Where to write the data

        Returns:
            Handle for the streaming job
        """

        def create_tiles_batch(batch_df: Any, batch_id: int):
            """Create tiles from a batch of data"""
            # Convert to pandas for processing
            if hasattr(batch_df, "toPandas"):
                df_pandas = batch_df.toPandas()
            else:
                df_pandas = batch_df

            if df_pandas.empty:
                return

            # Group by entity and time window
            tiles = self._create_tiles_from_batch(df_pandas)

            # Write tiles to stores
            if tiles:
                if to in [PushMode.ONLINE, PushMode.ONLINE_AND_OFFLINE]:
                    self._write_tiles_to_online_store(tiles)
                if to in [PushMode.OFFLINE, PushMode.ONLINE_AND_OFFLINE]:
                    self._write_tiles_to_offline_store(tiles)

        # Start streaming job
        query = (
            df.writeStream.outputMode("update")
            .option("checkpointLocation", f"/tmp/checkpoint/{self.sfv.name}")
            .trigger(processingTime=self._get_processing_time())
            .foreachBatch(create_tiles_batch)
            .start()
        )

        return query

    def _create_tiles_from_batch(self, df_pandas) -> List[Dict[str, Any]]:
        """
        Create tiles from a batch of data.

        Args:
            df_pandas: Pandas DataFrame with batch data

        Returns:
            List of tile dictionaries
        """
        tiles = []

        # Group by entity and time window
        entity_columns = [col for col in self.sfv.entity_columns]
        time_column = self.sfv.timestamp_field

        # Calculate time windows
        df_pandas["window_start"] = df_pandas[time_column].apply(
            lambda ts: self._get_window_start(ts)
        )

        # Group by entity and window
        grouped = df_pandas.groupby(entity_columns + ["window_start"])

        for (entity_values, window_start), group in grouped:
            # Create tile for this entity and window
            tile = self._create_tile_for_group(entity_values, window_start, group)
            if tile:
                tiles.append(tile)

        return tiles

    def _create_tile_for_group(
        self, entity_values: tuple, window_start: datetime, group_df
    ) -> Optional[Dict[str, Any]]:
        """
        Create a tile for a specific entity and time window.

        Args:
            entity_values: Entity key values
            window_start: Start of the time window
            group_df: DataFrame with data for this entity/window

        Returns:
            Tile dictionary or None if no data
        """
        if group_df.empty:
            return None

        # Initialize intermediate representation
        ir = self.aggregator.create_initial_ir()

        # Update IR with each row in the group
        for _, row in group_df.iterrows():
            for feature in self.sfv.features:
                feature_name = feature.name
                if feature_name in row and pd.notna(row[feature_name]):
                    ir = self.aggregator.update_ir(ir, feature_name, row[feature_name])

        # Create tile
        tile = {
            "entity_key": self._create_entity_key(entity_values),
            "ir": ir,
            "window_start": window_start,
            "window_end": window_start + self.tiling_config.window_size,
            "is_complete": True,  # Assume complete for now
            "created_at": datetime.now(),
        }

        return tile

    def _get_window_start(self, timestamp: datetime) -> datetime:
        """Get the start of the tiling window for a timestamp"""
        # Round down to the nearest window boundary
        window_seconds = self.tiling_config.get_window_size_seconds()
        epoch_seconds = int(timestamp.timestamp())
        window_start_seconds = (epoch_seconds // window_seconds) * window_seconds
        return datetime.fromtimestamp(window_start_seconds)

    def _create_entity_key(self, entity_values: tuple) -> str:
        """Create a string key for the entity"""
        return "|".join(str(val) for val in entity_values)

    def _write_tiles_to_online_store(self, tiles: List[Dict[str, Any]]):
        """Write tiles to the online store"""
        # Convert tiles to the format expected by the online store
        online_data = []

        for tile in tiles:
            # Encode the tile
            encoded_tile = self.tile_codec.encode_tile(
                tile["ir"],
                tile["window_start"],
                tile["is_complete"],
                tile["entity_key"],
            )

            # Create entity key proto
            entity_key_proto = self._create_entity_key_proto(tile["entity_key"])

            # Create feature data
            features = {
                "tile_data": encoded_tile,
                "window_start": tile["window_start"].isoformat(),
                "window_end": tile["window_end"].isoformat(),
            }

            online_data.append(
                (entity_key_proto, features, tile["window_start"], tile["created_at"])
            )

        # Write to online store
        if online_data:
            self.fs._get_provider().online_write_batch(
                config=self.fs.config, table=self.sfv, data=online_data, progress=None
            )

    def _write_tiles_to_offline_store(self, tiles: List[Dict[str, Any]]):
        """Write tiles to the offline store"""
        # Convert tiles to DataFrame
        tile_data = []
        for tile in tiles:
            tile_data.append(
                {
                    "entity_key": tile["entity_key"],
                    "window_start": tile["window_start"],
                    "window_end": tile["window_end"],
                    "ir": tile["ir"],
                    "is_complete": tile["is_complete"],
                    "created_at": tile["created_at"],
                }
            )

        if tile_data:
            import pandas as pd

            df = pd.DataFrame(tile_data)
            self.fs.write_to_offline_store(self.sfv.name, df)

    def _create_entity_key_proto(self, entity_key: Any):
        """Create an EntityKeyProto from various entity key formats"""
        from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto

        proto = EntityKeyProto()

        if isinstance(entity_key, str):
            # Simple string key - split by delimiter
            key_parts = entity_key.split("|")
            proto.join_keys.extend(key_parts)

        elif isinstance(entity_key, dict):
            # Complex entity key with multiple parts
            self._handle_complex_entity_key(proto, entity_key)

        elif isinstance(entity_key, (list, tuple)):
            # Multi-part entity key
            for part in entity_key:
                proto.join_keys.append(str(part))

        elif isinstance(entity_key, EntityKeyProto):
            # Already a proto, return as-is
            return entity_key

        else:
            # Convert to string and handle as simple key
            proto.join_keys.append(str(entity_key))

        return proto

    def _handle_complex_entity_key(self, proto, entity_key: Dict[str, Any]):
        """Handle complex entity key structures with proper serialization"""
        # Sort keys for consistent ordering
        sorted_items = sorted(entity_key.items())

        for key_name, key_value in sorted_items:
            # Create a structured key: "key_name:key_value"
            structured_key = f"{key_name}:{key_value}"
            proto.join_keys.append(structured_key)

            # Also store the raw key-value pairs for advanced processing
            if not hasattr(proto, "entity_values"):
                proto.entity_values = {}
            proto.entity_values[key_name] = self._create_value_proto_from_any(key_value)

    def _create_value_proto_from_any(self, value: Any):
        """Create a ValueProto from any Python value"""
        from feast.protos.feast.types.Value_pb2 import Value as ValueProto

        proto = ValueProto()

        if isinstance(value, bool):
            proto.bool_val = value
        elif isinstance(value, int):
            proto.int64_val = value
        elif isinstance(value, float):
            proto.double_val = value
        elif isinstance(value, str):
            proto.string_val = value
        elif isinstance(value, bytes):
            proto.bytes_val = value
        elif isinstance(value, list):
            # Handle list values - determine the appropriate list type
            if all(isinstance(item, str) for item in value):
                proto.string_list_val.val.extend(value)
            elif all(isinstance(item, int) for item in value):
                proto.int64_list_val.val.extend(value)
            elif all(isinstance(item, float) for item in value):
                proto.double_list_val.val.extend(value)
            elif all(isinstance(item, bool) for item in value):
                proto.bool_list_val.val.extend(value)
            else:
                # For mixed types, convert to string list
                proto.string_list_val.val.extend([str(item) for item in value])
        elif isinstance(value, dict):
            # Handle dict values - convert to string representation
            proto.string_val = str(value)
        else:
            # Default to string representation
            proto.string_val = str(value)

        return proto

    def _validate_entity_key(self, entity_key: Any) -> bool:
        """Validate entity key structure and format"""
        if entity_key is None:
            return False

        if isinstance(entity_key, str):
            # Simple string key - check for empty or invalid characters
            return len(entity_key.strip()) > 0 and not entity_key.startswith("|")

        elif isinstance(entity_key, dict):
            # Complex entity key - validate structure
            return self._validate_complex_entity_key(entity_key)

        elif isinstance(entity_key, (list, tuple)):
            # Multi-part entity key - validate each part
            return all(self._validate_entity_key_part(part) for part in entity_key)

        else:
            # Other types - convert to string and validate
            return self._validate_entity_key(str(entity_key))

    def _validate_complex_entity_key(self, entity_key: Dict[str, Any]) -> bool:
        """Validate complex entity key structure"""
        if not entity_key:
            return False

        # Check that all keys are strings and values are valid
        for key_name, key_value in entity_key.items():
            if not isinstance(key_name, str) or not key_name.strip():
                return False
            if not self._validate_entity_key_part(key_value):
                return False

        return True

    def _validate_entity_key_part(self, part: Any) -> bool:
        """Validate a single entity key part"""
        if part is None:
            return False

        if isinstance(part, (str, int, float, bool)):
            return True

        if isinstance(part, (list, tuple)):
            return all(self._validate_entity_key_part(item) for item in part)

        if isinstance(part, dict):
            return self._validate_complex_entity_key(part)

        return False

    def _normalize_entity_key(self, entity_key: Any) -> Any:
        """Normalize entity key to a consistent format"""
        if isinstance(entity_key, str):
            # Normalize string keys
            return entity_key.strip()

        elif isinstance(entity_key, dict):
            # Normalize complex entity keys
            normalized = {}
            for key_name, key_value in entity_key.items():
                normalized_key = key_name.strip()
                normalized_value = self._normalize_entity_key_part(key_value)
                normalized[normalized_key] = normalized_value
            return normalized

        elif isinstance(entity_key, (list, tuple)):
            # Normalize multi-part keys
            return [self._normalize_entity_key_part(part) for part in entity_key]

        else:
            return entity_key

    def _normalize_entity_key_part(self, part: Any) -> Any:
        """Normalize a single entity key part"""
        if isinstance(part, str):
            return part.strip()
        elif isinstance(part, (list, tuple)):
            return [self._normalize_entity_key_part(item) for item in part]
        elif isinstance(part, dict):
            return self._normalize_entity_key(part)
        else:
            return part

    def _get_processing_time(self) -> str:
        """Get the processing time trigger for the streaming job"""
        # Use a fraction of the window size for processing time
        window_seconds = self.tiling_config.get_window_size_seconds()
        processing_seconds = max(1, window_seconds // 10)  # Process every 10% of window
        return f"{processing_seconds} seconds"
