"""
Tile Codec for Feast

This module implements encoding and decoding of tiles for storage and retrieval.
"""

import gzip
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from feast.infra.tiling.tiling_config import TilingConfig


class TileCodec:
    """
    Encodes and decodes tiles for storage and retrieval.

    Tiles contain intermediate representations (IRs) along with metadata
    such as timestamps and completion status.
    """

    def __init__(self, tiling_config: Optional[TilingConfig] = None):
        self.tiling_config = tiling_config
        self.compression_enabled = (
            tiling_config.compression_enabled if tiling_config else True
        )

    def encode_tile(
        self,
        ir: Dict[str, Any],
        timestamp: datetime,
        is_complete: bool,
        entity_key: Optional[str] = None,
    ) -> bytes:
        """
        Encode a tile for storage.

        Args:
            ir: Intermediate representation dictionary
            timestamp: Timestamp of the tile
            is_complete: Whether the tile is complete (no more updates expected)
            entity_key: Optional entity key for the tile

        Returns:
            Encoded tile as bytes
        """
        tile_data = {
            "ir": ir,
            "timestamp": timestamp.isoformat(),
            "is_complete": is_complete,
            "entity_key": entity_key,
            "version": "1.0",  # For future compatibility
        }

        # Convert to JSON
        json_data = json.dumps(tile_data, default=str)
        encoded_data = json_data.encode("utf-8")

        # Compress if enabled
        if self.compression_enabled:
            encoded_data = gzip.compress(encoded_data)

        return encoded_data

    def decode_tile(
        self, data: bytes
    ) -> Tuple[Dict[str, Any], datetime, bool, Optional[str]]:
        """
        Decode a tile from storage.

        Args:
            data: Encoded tile data

        Returns:
            Tuple of (ir, timestamp, is_complete, entity_key)
        """
        # Decompress if needed
        if self._is_compressed(data):
            data = gzip.decompress(data)

        # Parse JSON
        tile_data = json.loads(data.decode("utf-8"))

        # Extract components
        ir = tile_data["ir"]
        timestamp = datetime.fromisoformat(tile_data["timestamp"])
        is_complete = tile_data["is_complete"]
        entity_key = tile_data.get("entity_key")

        return ir, timestamp, is_complete, entity_key

    def _is_compressed(self, data: bytes) -> bool:
        """Check if data is compressed using gzip"""
        return data.startswith(b"\x1f\x8b")

    def encode_tile_batch(self, tiles: List[Dict[str, Any]]) -> bytes:
        """
        Encode multiple tiles as a batch.

        Args:
            tiles: List of tile dictionaries

        Returns:
            Encoded batch as bytes
        """
        batch_data = {
            "tiles": tiles,
            "batch_timestamp": datetime.now().isoformat(),
            "count": len(tiles),
        }

        json_data = json.dumps(batch_data, default=str)
        encoded_data = json_data.encode("utf-8")

        if self.compression_enabled:
            encoded_data = gzip.compress(encoded_data)

        return encoded_data

    def decode_tile_batch(self, data: bytes) -> List[Dict[str, Any]]:
        """
        Decode a batch of tiles.

        Args:
            data: Encoded batch data

        Returns:
            List of tile dictionaries
        """
        if self._is_compressed(data):
            data = gzip.decompress(data)

        batch_data = json.loads(data.decode("utf-8"))
        return batch_data["tiles"]

    def get_tile_size(self, ir: Dict[str, Any]) -> int:
        """Get the size of a tile in bytes"""
        encoded = self.encode_tile(ir, datetime.now(), True)
        return len(encoded)

    def estimate_compression_ratio(self, sample_tiles: List[Dict[str, Any]]) -> float:
        """
        Estimate the compression ratio for a sample of tiles.

        Args:
            sample_tiles: List of sample tile dictionaries

        Returns:
            Compression ratio (compressed_size / uncompressed_size)
        """
        if not sample_tiles:
            return 1.0

        # Calculate uncompressed size
        uncompressed_size = 0
        for tile in sample_tiles:
            json_data = json.dumps(tile, default=str)
            uncompressed_size += len(json_data.encode("utf-8"))

        # Calculate compressed size
        compressed_size = 0
        for tile in sample_tiles:
            encoded = self.encode_tile(
                tile["ir"],
                datetime.fromisoformat(tile["timestamp"]),
                tile["is_complete"],
            )
            compressed_size += len(encoded)

        return compressed_size / uncompressed_size if uncompressed_size > 0 else 1.0
