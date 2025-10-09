"""
This module provides memory-efficient tile processing for large-scale deployments.
It includes streaming processing, memory pooling, and garbage collection optimization.
"""

import gc
import threading
import time
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

import psutil

from feast.infra.tiling.tiling_config import TilingConfig


class MemoryPressureLevel(Enum):
    """Memory pressure levels"""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class MemoryStats:
    """Memory statistics for monitoring"""

    total_memory: int
    available_memory: int
    used_memory: int
    memory_percentage: float
    pressure_level: MemoryPressureLevel
    timestamp: float


class MemoryManager:
    """
    Memory manager for efficient tile processing at scale.

    Features:
    - Memory pressure monitoring
    - Adaptive processing strategies
    - Memory pooling and reuse
    - Garbage collection optimization
    - Streaming processing for large datasets
    """

    def __init__(self, config: TilingConfig):
        self.config = config
        self.memory_pool: Dict[str, List[Any]] = {}
        self.memory_stats_history: deque = deque(maxlen=100)
        self.monitoring_thread = None
        self.stop_monitoring = threading.Event()
        self.memory_thresholds = {
            "low": 0.3,
            "medium": 0.6,
            "high": 0.8,
            "critical": 0.95,
        }

    def start_monitoring(self):
        """Start memory monitoring in background thread"""
        if self.monitoring_thread is None or not self.monitoring_thread.is_alive():
            self.monitoring_thread = threading.Thread(target=self._monitor_memory)
            self.monitoring_thread.daemon = True
            self.monitoring_thread.start()

    def stop_monitoring_thread(self):
        """Stop memory monitoring"""
        self.stop_monitoring.set()
        if self.monitoring_thread and self.monitoring_thread.is_alive():
            self.monitoring_thread.join(timeout=5)

    def _monitor_memory(self):
        """Background memory monitoring"""
        while not self.stop_monitoring.is_set():
            try:
                stats = self.get_memory_stats()
                self.memory_stats_history.append(stats)

                # Trigger garbage collection if memory pressure is high
                if stats.pressure_level in [
                    MemoryPressureLevel.HIGH,
                    MemoryPressureLevel.CRITICAL,
                ]:
                    self._optimize_memory()

                time.sleep(1)  # Monitor every second
            except Exception as e:
                print(f"Memory monitoring error: {e}")
                time.sleep(5)

    def get_memory_stats(self) -> MemoryStats:
        """Get current memory statistics"""
        memory = psutil.virtual_memory()
        percentage = memory.percent / 100.0

        # Determine pressure level
        if percentage < self.memory_thresholds["low"]:
            pressure_level = MemoryPressureLevel.LOW
        elif percentage < self.memory_thresholds["medium"]:
            pressure_level = MemoryPressureLevel.MEDIUM
        elif percentage < self.memory_thresholds["high"]:
            pressure_level = MemoryPressureLevel.HIGH
        else:
            pressure_level = MemoryPressureLevel.CRITICAL

        return MemoryStats(
            total_memory=memory.total,
            available_memory=memory.available,
            used_memory=memory.used,
            memory_percentage=percentage,
            pressure_level=pressure_level,
            timestamp=time.time(),
        )

    def _optimize_memory(self):
        """Optimize memory usage based on pressure level"""
        stats = self.get_memory_stats()

        if stats.pressure_level == MemoryPressureLevel.HIGH:
            # Force garbage collection
            gc.collect()

            # Clear memory pool for non-essential items
            self._clear_memory_pool(keep_essential=True)

        elif stats.pressure_level == MemoryPressureLevel.CRITICAL:
            # Aggressive memory optimization
            gc.collect()
            self._clear_memory_pool(keep_essential=False)

            # Force garbage collection multiple times
            for _ in range(3):
                gc.collect()

    def _clear_memory_pool(self, keep_essential: bool = True):
        """Clear memory pool to free up memory"""
        if keep_essential:
            # Keep only essential items in pool
            essential_keys = ["tile_codec", "aggregator"]
            keys_to_remove = [
                k for k in self.memory_pool.keys() if k not in essential_keys
            ]
            for key in keys_to_remove:
                del self.memory_pool[key]
        else:
            # Clear entire pool
            self.memory_pool.clear()

    def get_pooled_object(self, key: str, factory_func, *args, **kwargs):
        """Get object from memory pool or create new one"""
        if key in self.memory_pool:
            return self.memory_pool[key]

        # Create new object
        obj = factory_func(*args, **kwargs)

        # Add to pool if memory pressure is low
        stats = self.get_memory_stats()
        if stats.pressure_level == MemoryPressureLevel.LOW:
            self.memory_pool[key] = obj

        return obj

    def process_tiles_streaming(
        self, tiles: List[Dict], batch_size: Optional[int] = None
    ) -> List[Dict]:
        """Process tiles in streaming fashion to manage memory"""
        if not tiles:
            return []

        # Determine batch size based on memory pressure
        if batch_size is None:
            batch_size = self._calculate_optimal_batch_size(len(tiles))

        results = []

        # Process tiles in batches
        for i in range(0, len(tiles), batch_size):
            batch = tiles[i : i + batch_size]
            batch_results = self._process_tile_batch(batch)
            results.extend(batch_results)

            # Check memory pressure after each batch
            stats = self.get_memory_stats()
            if stats.pressure_level in [
                MemoryPressureLevel.HIGH,
                MemoryPressureLevel.CRITICAL,
            ]:
                # Force garbage collection between batches
                gc.collect()
                time.sleep(0.1)  # Brief pause to allow GC

        return results

    def _calculate_optimal_batch_size(self, total_tiles: int) -> int:
        """Calculate optimal batch size based on memory pressure"""
        stats = self.get_memory_stats()

        if stats.pressure_level == MemoryPressureLevel.LOW:
            return min(1000, total_tiles)
        elif stats.pressure_level == MemoryPressureLevel.MEDIUM:
            return min(500, total_tiles)
        elif stats.pressure_level == MemoryPressureLevel.HIGH:
            return min(100, total_tiles)
        else:  # CRITICAL
            return min(10, total_tiles)

    def _process_tile_batch(self, batch: List[Dict]) -> List[Dict]:
        """Process a batch of tiles"""
        # This is a placeholder - in production, implement actual tile processing
        return batch

    def optimize_tile_storage(self, tiles: List[Dict]) -> List[Dict]:
        """Optimize tile storage for memory efficiency"""
        optimized_tiles = []

        for tile in tiles:
            optimized_tile = self._optimize_single_tile(tile)
            optimized_tiles.append(optimized_tile)

        return optimized_tiles

    def _optimize_single_tile(self, tile: Dict) -> Dict:
        """Optimize a single tile for memory efficiency"""
        optimized_tile = {}

        # Copy essential fields
        for key in ["entity_key", "window_start", "window_end", "timestamp"]:
            if key in tile:
                optimized_tile[key] = tile[key]

        # Optimize features data
        if "features" in tile:
            optimized_tile["features"] = self._optimize_features_data(tile["features"])

        # Optimize metadata
        if "metadata" in tile:
            optimized_tile["metadata"] = self._optimize_metadata(tile["metadata"])

        return optimized_tile

    def _optimize_features_data(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """Optimize features data for memory efficiency"""
        optimized_features = {}

        for feature_name, feature_data in features.items():
            if isinstance(feature_data, dict):
                # Compress aggregation data
                optimized_features[feature_name] = self._compress_aggregation_data(
                    feature_data
                )
            else:
                optimized_features[feature_name] = feature_data

        return optimized_features

    def _compress_aggregation_data(
        self, aggregation_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Compress aggregation data to reduce memory usage"""
        compressed: Dict[str, Any] = {}

        for agg_name, agg_value in aggregation_data.items():
            if isinstance(agg_value, list) and len(agg_value) > 100:
                # For large lists, use sampling or compression
                if agg_name in ["median", "p25", "p75", "p90", "p95", "p99"]:
                    # For percentiles, keep only essential values
                    compressed[agg_name] = self._compress_percentile_data(agg_value)
                else:
                    compressed[agg_name] = agg_value
            else:
                compressed[agg_name] = agg_value

        return compressed

    def _compress_percentile_data(self, data: List[Any]) -> Dict[str, Any]:
        """Compress percentile data using sampling"""
        if len(data) <= 100:
            return {"sampled_data": data, "original_size": len(data)}

        # Sample data for memory efficiency
        sample_size = min(100, len(data))
        step = len(data) // sample_size
        sampled_data = [data[i] for i in range(0, len(data), step)]

        return {
            "sampled_data": sampled_data,
            "original_size": len(data),
            "compression_ratio": len(sampled_data) / len(data),
        }

    def _optimize_metadata(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Optimize metadata for memory efficiency"""
        optimized = {}

        # Keep only essential metadata
        essential_keys = ["tile_id", "created_at", "updated_at", "quality_score"]
        for key in essential_keys:
            if key in metadata:
                optimized[key] = metadata[key]

        return optimized

    def get_memory_usage_report(self) -> Dict[str, Any]:
        """Get comprehensive memory usage report"""
        stats = self.get_memory_stats()

        return {
            "current_stats": {
                "total_memory_gb": stats.total_memory / (1024**3),
                "available_memory_gb": stats.available_memory / (1024**3),
                "used_memory_gb": stats.used_memory / (1024**3),
                "memory_percentage": stats.memory_percentage,
                "pressure_level": stats.pressure_level.value,
            },
            "memory_pool_size": len(self.memory_pool),
            "memory_pool_keys": list(self.memory_pool.keys()),
            "history_size": len(self.memory_stats_history),
            "gc_counts": gc.get_count(),
            "recommendations": self._get_memory_recommendations(stats),
        }

    def _get_memory_recommendations(self, stats: MemoryStats) -> List[str]:
        """Get memory optimization recommendations"""
        recommendations = []

        if stats.pressure_level == MemoryPressureLevel.HIGH:
            recommendations.append("Consider reducing batch size for tile processing")
            recommendations.append("Enable aggressive garbage collection")
            recommendations.append("Clear non-essential memory pools")

        elif stats.pressure_level == MemoryPressureLevel.CRITICAL:
            recommendations.append("Reduce processing batch size to minimum")
            recommendations.append("Force garbage collection immediately")
            recommendations.append("Clear all memory pools")
            recommendations.append("Consider pausing non-essential processing")

        return recommendations
