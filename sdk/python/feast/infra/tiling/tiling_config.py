"""
This module defines the configuration options for tiling support.
"""

from datetime import timedelta
from typing import List, Optional

from pydantic import model_validator

from feast.repo_config import FeastConfigBaseModel


class TilingConfig(FeastConfigBaseModel):
    """
    Configuration for tiling support.

    Tiling enables pre-aggregated feature storage and retrieval for improved
    performance, especially for high-frequency entities and complex aggregations.
    """

    enabled: bool = True
    """Whether tiling is enabled for this feature view"""

    window_size: timedelta = timedelta(hours=1)
    """Size of the tiling window (e.g., 1 hour, 5 minutes)"""

    aggregation_functions: List[str] = ["sum", "count", "max", "min", "avg"]
    """List of aggregation functions to support in tiles"""

    max_tile_age_hours: int = 24
    """Maximum age for tiles before cleanup (in hours)"""

    compression_enabled: bool = True
    """Whether to compress tiles for storage efficiency"""

    hot_key_threshold: Optional[int] = None
    """Threshold for considering an entity as a hot key (events per window)"""

    @model_validator(mode="after")
    def validate_config(self):
        """Validate configuration after initialization"""
        if self.window_size.total_seconds() <= 0:
            raise ValueError("Window size must be positive")

        if self.max_tile_age_hours <= 0:
            raise ValueError("Max tile age must be positive")

        valid_aggregations = {"sum", "count", "max", "min", "avg", "std", "var"}
        invalid_aggregations = set(self.aggregation_functions) - valid_aggregations
        if invalid_aggregations:
            raise ValueError(f"Invalid aggregation functions: {invalid_aggregations}")

        return self

    def get_window_size_seconds(self) -> int:
        """Get the window size in seconds"""
        return int(self.window_size.total_seconds())

    def is_hot_key(self, event_count: int) -> bool:
        """Check if an entity is considered a hot key"""
        if self.hot_key_threshold is None:
            return False
        return event_count >= self.hot_key_threshold
