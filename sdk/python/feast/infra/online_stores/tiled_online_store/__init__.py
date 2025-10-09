"""
Tiled Online Store for Feast

This module provides tiling support for Feast's online stores, enabling
pre-aggregated feature storage and retrieval for improved performance.
"""

from .tiled_online_store import TiledOnlineStore, TiledOnlineStoreConfig

__all__ = ["TiledOnlineStore", "TiledOnlineStoreConfig"]
