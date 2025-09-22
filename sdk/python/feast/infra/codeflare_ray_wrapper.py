# Copyright 2025 The Feast Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
CodeFlare Ray wrapper for KubeRay integration using TLS certificates and direct Ray connection.
"""

import logging
import os
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import pyarrow as pa
import ray

from feast.infra.ray_config_manager import RayConfigManager, RayExecutionMode

# Set Ray Client environment variables at MODULE IMPORT TIME
# This ensures they're available before any Ray operations
print("🔧 Setting Ray Client environment variables at MODULE IMPORT TIME...")
os.environ["RAY_CLIENT_SERVER_TIMEOUT_MS"] = "300000"  # 5 minutes
os.environ["RAY_CLIENT_RECONNECT_GRACE_PERIOD"] = "120"  # 2 minutes
os.environ["RAY_CLIENT_HEARTBEAT_TIMEOUT_MS"] = "120000"  # 2 minutes
os.environ["RAY_CLIENT_QUEUE_TIMEOUT_MS"] = "300000"  # 5 minutes
os.environ["RAY_CLIENT_DATA_CHANNEL_TIMEOUT_MS"] = "300000"  # 5 minutes
# Try additional variations that might work
os.environ["RAY_CLIENT_TIMEOUT_MS"] = "300000"
os.environ["RAY_CLIENT_QUEUE_TIMEOUT"] = "300"  # seconds
os.environ["RAY_CLIENT_DATA_TIMEOUT"] = "300"  # seconds
os.environ["RAY_CLIENT_WORKER_TIMEOUT"] = "300"  # seconds
print("🔧 Ray Client environment variables set at MODULE IMPORT!")

logger = logging.getLogger(__name__)

# Global schema cache to avoid multiple ray.get() calls across different proxy instances
_global_schema_cache: Dict[str, Any] = {}


class RemoteDatasetProxy:
    """Proxy class that executes Ray Data operations remotely on cluster workers."""

    def __init__(self, dataset_ref: Any):
        """Initialize with a reference to the remote dataset."""
        print(f"🔧 RemoteDatasetProxy created with ref: {type(dataset_ref)}")

        # Add delay during proxy creation to prevent Ray Client overload
        import time

        time.sleep(1)  # 1sec delay during proxy creation

        self._dataset_ref = dataset_ref
        self._cached_schema = None  # Cache schema to avoid multiple ray.get() calls

    def __getattr__(self, name):
        """Catch any method calls that we haven't explicitly implemented."""
        print(
            f"🚨 CRITICAL: Unknown method/attribute '{name}' accessed on RemoteDatasetProxy!"
        )
        print("🚨 This might be causing the timeout - method not implemented in proxy")
        print("🚨 Stack trace will help identify what's calling this...")
        import traceback

        traceback.print_stack()
        raise AttributeError(f"RemoteDatasetProxy has no attribute '{name}'")

    def map_batches(self, func, **kwargs) -> "RemoteDatasetProxy":
        """Execute map_batches remotely on cluster workers."""

        @ray.remote
        def _remote_map_batches(dataset, function, batch_kwargs):
            print("🔧 REMOTE: Starting map_batches operation on cluster worker...")
            try:
                # Create the operation but don't execute it yet - keep it lazy
                result = dataset.map_batches(function, **batch_kwargs)
                print("🔧 REMOTE: map_batches operation completed successfully (lazy)")
                # Return the dataset reference without materializing
                return result
            except Exception as e:
                print(f"🚨 REMOTE: map_batches operation failed: {e}")
                raise

        print("🔧 CLIENT: Submitting map_batches operation to cluster...")
        # Keep the dataset reference on remote cluster, don't transfer data
        new_ref = _remote_map_batches.remote(self._dataset_ref, func, kwargs)
        print("🔧 CLIENT: map_batches operation submitted, returning proxy (lazy)")

        # Add a longer delay to avoid overwhelming Ray Client
        import time

        time.sleep(0.5)  # 500ms delay to give Ray Client more time to process

        return RemoteDatasetProxy(new_ref)

    def __str__(self):
        """String representation without triggering data transfer."""
        return f"RemoteDatasetProxy(ref={type(self._dataset_ref).__name__})"

    def __repr__(self):
        """Representation without triggering data transfer."""
        return f"RemoteDatasetProxy(ref={type(self._dataset_ref).__name__})"

    def count(self) -> int:
        """Get count without transferring data - this might be called implicitly."""
        print("🚨 CRITICAL: count() called - this will transfer data to client!")

        @ray.remote
        def _remote_count(dataset):
            print("🔧 REMOTE: Getting count on cluster worker...")
            try:
                count = dataset.count()
                print(f"🔧 REMOTE: count retrieved: {count}")
                return count
            except Exception as e:
                print(f"🚨 REMOTE: count failed: {e}")
                raise

        print("🔧 CLIENT: Requesting count from cluster...")
        result = ray.get(_remote_count.remote(self._dataset_ref))
        print(f"🔧 CLIENT: count retrieved: {result}")
        return result

    def take(self, limit: int = 20) -> Any:
        """Get first N rows - this might be called implicitly for debugging."""
        print(f"🚨 CRITICAL: take({limit}) called - this will transfer data to client!")

        @ray.remote
        def _remote_take(dataset, n):
            print(f"🔧 REMOTE: Getting take({n}) on cluster worker...")
            try:
                result = dataset.take(n)
                print(f"🔧 REMOTE: take({n}) retrieved {len(result)} rows")
                return result
            except Exception as e:
                print(f"🚨 REMOTE: take({n}) failed: {e}")
                raise

        print(f"🔧 CLIENT: Requesting take({limit}) from cluster...")
        result = ray.get(_remote_take.remote(self._dataset_ref, limit))
        print(f"🔧 CLIENT: take({limit}) retrieved {len(result)} rows")
        return result

    def show(self, limit: int = 20) -> None:
        """Show dataset - this might be called implicitly."""
        print(f"🚨 CRITICAL: show({limit}) called - this will transfer data to client!")

        # Just call take() and print it
        rows = self.take(limit)
        for row in rows:
            print(row)

    def sort(self, key, descending=False) -> "RemoteDatasetProxy":
        """Execute sort remotely on cluster workers."""

        @ray.remote
        def _remote_sort(dataset, sort_key, desc):
            return dataset.sort(sort_key, descending=desc)

        # Keep the dataset reference on remote cluster, don't transfer data
        new_ref = _remote_sort.remote(self._dataset_ref, key, descending)
        return RemoteDatasetProxy(new_ref)

    def union(self, other: "RemoteDatasetProxy") -> "RemoteDatasetProxy":
        """Execute union remotely on cluster workers."""

        @ray.remote
        def _remote_union(dataset1, dataset2):
            return dataset1.union(dataset2)

        # Keep the dataset reference on remote cluster, don't transfer data
        new_ref = _remote_union.remote(self._dataset_ref, other._dataset_ref)
        return RemoteDatasetProxy(new_ref)

    def write_parquet(self, path: str) -> None:
        """Execute write_parquet remotely on cluster workers."""

        @ray.remote
        def _remote_write_parquet(dataset, file_path):
            dataset.write_parquet(file_path)
            return None

        ray.get(_remote_write_parquet.remote(self._dataset_ref, path))

    def to_pandas(self) -> pd.DataFrame:
        """Convert dataset to pandas DataFrame with chunking to avoid Ray Client timeouts."""
        print("🚨 CRITICAL: to_pandas() called - this will transfer data to client!")

        @ray.remote
        def _remote_get_row_count(dataset):
            return dataset.count()

        @ray.remote
        def _remote_to_pandas_chunked(dataset, offset, limit):
            # For older Ray versions that don't have skip() method
            try:
                if offset > 0:
                    if hasattr(dataset, "skip"):
                        chunked_dataset = dataset.skip(offset).limit(limit)
                    else:
                        # Fallback: convert to pandas and slice
                        full_df = dataset.to_pandas()
                        return full_df.iloc[offset : offset + limit]
                else:
                    chunked_dataset = dataset.limit(limit)

                return chunked_dataset.to_pandas()

            except (AttributeError, Exception):
                # Ultimate fallback: convert full dataset and slice
                full_df = dataset.to_pandas()
                if offset > 0:
                    return full_df.iloc[offset : offset + limit]
                else:
                    return full_df.iloc[:limit]

        try:
            print("🔧 Getting row count for chunking...")
            # Get total row count first
            total_rows = ray.get(_remote_get_row_count.remote(self._dataset_ref))
            print(f"🔧 Converting dataset to pandas: {total_rows} total rows")

            # Process in chunks to avoid 10-second Ray Client timeout
            chunk_size = 1000  # Process 1000 rows at a time
            chunks = []

            for offset in range(0, total_rows, chunk_size):
                current_chunk_size = min(chunk_size, total_rows - offset)
                print(
                    f"🔧 Processing chunk: rows {offset} to {offset + current_chunk_size}"
                )

                try:
                    chunk_df = ray.get(
                        _remote_to_pandas_chunked.remote(
                            self._dataset_ref, offset, current_chunk_size
                        )
                    )
                    chunks.append(chunk_df)
                except Exception as e:
                    print(f"🚨 Chunk failed, trying smaller size: {e}")
                    # If chunk fails, try even smaller chunks
                    smaller_chunk_size = 100
                    for small_offset in range(
                        offset, offset + current_chunk_size, smaller_chunk_size
                    ):
                        small_size = min(
                            smaller_chunk_size,
                            offset + current_chunk_size - small_offset,
                        )
                        small_chunk_df = ray.get(
                            _remote_to_pandas_chunked.remote(
                                self._dataset_ref, small_offset, small_size
                            )
                        )
                        chunks.append(small_chunk_df)

            # Combine all chunks
            if chunks:
                result_df = pd.concat(chunks, ignore_index=True)
                print(
                    f"🔧 Successfully combined {len(chunks)} chunks into DataFrame with {len(result_df)} rows"
                )
                return result_df
            else:
                return pd.DataFrame()

        except Exception as e:
            print(
                f"🚨 Chunked conversion failed, falling back to direct conversion: {e}"
            )

            # Fallback to direct conversion (may timeout)
            @ray.remote
            def _remote_to_pandas_direct(dataset):
                return dataset.to_pandas()

            return ray.get(_remote_to_pandas_direct.remote(self._dataset_ref))

    def to_arrow(self) -> pa.Table:
        """Convert dataset to PyArrow table with chunking to avoid Ray Client timeouts."""
        print("🚨 CRITICAL: to_arrow() called - this will transfer data to client!")

        @ray.remote
        def _remote_get_row_count(dataset):
            return dataset.count()

        @ray.remote
        def _remote_to_arrow_chunked(dataset, offset, limit):
            # For older Ray versions, convert entire dataset to pandas and slice
            import pyarrow as pa

            try:
                # Try newer Ray API first
                if offset > 0:
                    if hasattr(dataset, "skip"):
                        chunked_dataset = dataset.skip(offset).limit(limit)
                    else:
                        # Fallback: convert to pandas and slice
                        full_df = dataset.to_pandas()
                        chunked_df = full_df.iloc[offset : offset + limit]
                        return pa.Table.from_pandas(chunked_df)
                else:
                    chunked_dataset = dataset.limit(limit)

                # Try to_arrow() method
                if hasattr(chunked_dataset, "to_arrow"):
                    return chunked_dataset.to_arrow()
                else:
                    # Fallback for older Ray versions
                    pandas_df = chunked_dataset.to_pandas()
                    return pa.Table.from_pandas(pandas_df)

            except (AttributeError, Exception):
                # Ultimate fallback: convert full dataset to pandas and slice
                full_df = dataset.to_pandas()
                if offset > 0:
                    chunked_df = full_df.iloc[offset : offset + limit]
                else:
                    chunked_df = full_df.iloc[:limit]
                return pa.Table.from_pandas(chunked_df)

        try:
            print("🔧 Getting row count for arrow conversion...")
            # Get total row count first
            total_rows = ray.get(_remote_get_row_count.remote(self._dataset_ref))
            print(f"🔧 Converting dataset to arrow: {total_rows} total rows")

            # Process in chunks to avoid 10-second Ray Client timeout
            chunk_size = 1000  # Process 1000 rows at a time
            chunks = []

            for offset in range(0, total_rows, chunk_size):
                current_chunk_size = min(chunk_size, total_rows - offset)
                print(
                    f"🔧 Processing arrow chunk: rows {offset} to {offset + current_chunk_size}"
                )

                try:
                    chunk_table = ray.get(
                        _remote_to_arrow_chunked.remote(
                            self._dataset_ref, offset, current_chunk_size
                        )
                    )
                    chunks.append(chunk_table)
                except Exception as e:
                    print(f"🚨 Arrow chunk failed, trying smaller size: {e}")
                    # If chunk fails, try even smaller chunks
                    smaller_chunk_size = 100
                    for small_offset in range(
                        offset, offset + current_chunk_size, smaller_chunk_size
                    ):
                        small_size = min(
                            smaller_chunk_size,
                            offset + current_chunk_size - small_offset,
                        )
                        small_chunk_table = ray.get(
                            _remote_to_arrow_chunked.remote(
                                self._dataset_ref, small_offset, small_size
                            )
                        )
                        chunks.append(small_chunk_table)

            # Combine all chunks using PyArrow
            if chunks:
                result_table = pa.concat_tables(chunks)
                print(
                    f"🔧 Successfully combined {len(chunks)} arrow chunks into table with {len(result_table)} rows"
                )
                return result_table
            else:
                return pa.Table.from_pandas(pd.DataFrame())

        except Exception as e:
            print(
                f"🚨 Chunked arrow conversion failed, falling back to direct conversion: {e}"
            )

            # Fallback to direct conversion (may timeout)
            @ray.remote
            def _remote_to_arrow_direct(dataset):
                # Older Ray versions don't have to_arrow(), use to_pandas() then convert
                try:
                    return dataset.to_arrow()
                except AttributeError:
                    # Fallback for older Ray versions
                    import pyarrow as pa

                    pandas_df = dataset.to_pandas()
                    return pa.Table.from_pandas(pandas_df)

            return ray.get(_remote_to_arrow_direct.remote(self._dataset_ref))

    def schema(self) -> Any:
        """Execute schema remotely on cluster workers with global caching to avoid multiple ray.get() calls."""

        # Create a cache key based on the dataset reference
        cache_key = str(self._dataset_ref)

        # Return cached schema if available globally
        if cache_key in _global_schema_cache:
            print("🔧 CLIENT: Returning globally cached schema (avoiding ray.get())")
            return _global_schema_cache[cache_key]

        @ray.remote
        def _remote_schema(dataset):
            print("🔧 REMOTE: Getting schema on cluster worker...")
            try:
                schema = dataset.schema()
                print("🔧 REMOTE: Schema retrieved successfully")
                return schema
            except Exception as e:
                print(f"🚨 REMOTE: Schema retrieval failed: {e}")
                raise

        print(
            "🔧 CLIENT: Requesting schema from cluster (first time for this dataset)..."
        )
        result = ray.get(_remote_schema.remote(self._dataset_ref))
        print("🔧 CLIENT: Schema retrieved and transferred to client, caching globally")

        # Cache the schema globally to avoid future ray.get() calls from ANY proxy
        _global_schema_cache[cache_key] = result
        return result

    def limit(self, count: int) -> "RemoteDatasetProxy":
        """Execute limit remotely on cluster workers."""

        @ray.remote
        def _remote_limit(dataset, limit_count):
            return dataset.limit(limit_count)

        # Keep the dataset reference on remote cluster, don't transfer data
        new_ref = _remote_limit.remote(self._dataset_ref, count)
        return RemoteDatasetProxy(new_ref)

    def materialize(self) -> "RemoteDatasetProxy":
        """Execute materialize remotely on cluster workers."""

        @ray.remote
        def _remote_materialize(dataset):
            return dataset.materialize()

        # Keep the dataset reference on remote cluster, don't transfer data
        new_ref = _remote_materialize.remote(self._dataset_ref)
        return RemoteDatasetProxy(new_ref)

    def filter(self, fn) -> "RemoteDatasetProxy":
        """Execute filter remotely on cluster workers."""

        @ray.remote
        def _remote_filter(dataset, filter_fn):
            print("🔧 REMOTE: Starting filter operation on cluster worker...")
            try:
                result = dataset.filter(filter_fn)
                print("🔧 REMOTE: Filter operation completed successfully")
                return result
            except Exception as e:
                print(f"🚨 REMOTE: Filter operation failed: {e}")
                raise

        print("🔧 CLIENT: Submitting filter operation to cluster...")
        # Keep the dataset reference on remote cluster, don't transfer data
        new_ref = _remote_filter.remote(self._dataset_ref, fn)
        print("🔧 CLIENT: Filter operation submitted, returning proxy")

        # Add delay to avoid overwhelming Ray Client
        import time

        time.sleep(0.5)  # 500ms delay

        return RemoteDatasetProxy(new_ref)

    def size_bytes(self) -> int:
        """Execute size_bytes remotely on cluster workers."""
        print("🚨 CRITICAL: size_bytes() called - this will transfer data to client!")

        @ray.remote
        def _remote_size_bytes(dataset):
            print("🔧 REMOTE: Getting size_bytes on cluster worker...")
            try:
                size = dataset.size_bytes()
                print(f"🔧 REMOTE: size_bytes retrieved: {size}")
                return size
            except Exception as e:
                print(f"🚨 REMOTE: size_bytes failed: {e}")
                raise

        print("🔧 CLIENT: Requesting size_bytes from cluster...")
        result = ray.get(_remote_size_bytes.remote(self._dataset_ref))
        print(f"🔧 CLIENT: size_bytes retrieved: {result}")
        return result

    def repartition(self, **kwargs) -> "RemoteDatasetProxy":
        """Execute repartition remotely on cluster workers."""

        @ray.remote
        def _remote_repartition(dataset, repartition_kwargs):
            return dataset.repartition(**repartition_kwargs)

        # Keep the dataset reference on remote cluster, don't transfer data
        new_ref = _remote_repartition.remote(self._dataset_ref, kwargs)
        return RemoteDatasetProxy(new_ref)

    def random_shuffle(self, **kwargs) -> "RemoteDatasetProxy":
        """Execute random_shuffle remotely on cluster workers."""

        @ray.remote
        def _remote_random_shuffle(dataset, shuffle_kwargs):
            return dataset.random_shuffle(**shuffle_kwargs)

        # Keep the dataset reference on remote cluster, don't transfer data
        new_ref = _remote_random_shuffle.remote(self._dataset_ref, kwargs)
        return RemoteDatasetProxy(new_ref)

    def min(self, column: str) -> Any:
        """Execute min remotely on cluster workers."""
        print(f"🚨 CRITICAL: min({column}) called - this will transfer data to client!")

        @ray.remote
        def _remote_min(dataset, col):
            print(f"🔧 REMOTE: Getting min({col}) on cluster worker...")
            try:
                result = dataset.min(col)
                print(f"🔧 REMOTE: min({col}) retrieved: {result}")
                return result
            except Exception as e:
                print(f"🚨 REMOTE: min({col}) failed: {e}")
                raise

        print(f"🔧 CLIENT: Requesting min({column}) from cluster...")
        result = ray.get(_remote_min.remote(self._dataset_ref, column))
        print(f"🔧 CLIENT: min({column}) retrieved: {result}")
        return result

    def max(self, column: str) -> Any:
        """Execute max remotely on cluster workers."""
        print(f"🚨 CRITICAL: max({column}) called - this will transfer data to client!")

        @ray.remote
        def _remote_max(dataset, col):
            print(f"🔧 REMOTE: Getting max({col}) on cluster worker...")
            try:
                result = dataset.max(col)
                print(f"🔧 REMOTE: max({col}) retrieved: {result}")
                return result
            except Exception as e:
                print(f"🚨 REMOTE: max({col}) failed: {e}")
                raise

        print(f"🔧 CLIENT: Requesting max({column}) from cluster...")
        result = ray.get(_remote_max.remote(self._dataset_ref, column))
        print(f"🔧 CLIENT: max({column}) retrieved: {result}")
        return result

    def copy(self) -> "RemoteDatasetProxy":
        """Execute copy remotely on cluster workers."""

        @ray.remote
        def _remote_copy(dataset):
            return dataset.copy()

        # Keep the dataset reference on remote cluster, don't transfer data
        new_ref = _remote_copy.remote(self._dataset_ref)
        return RemoteDatasetProxy(new_ref)


class CodeFlareRayWrapper:
    """
    Wrapper for CodeFlare SDK integration with KubeRay clusters using TLS certificates.

    This wrapper uses CodeFlare's TLS certificate generation to establish a direct
    Ray connection to the KubeRay cluster, allowing native Ray operations to run remotely.
    """

    def __init__(self, config: Optional[Union[Dict[str, Any], object]] = None):
        """Initialize the CodeFlare Ray wrapper with TLS-based Ray connection."""
        # Set Ray Client environment variables IMMEDIATELY at object creation
        import os

        print("🔧 Setting Ray Client environment variables at OBJECT INIT...")
        os.environ["RAY_CLIENT_SERVER_TIMEOUT_MS"] = "300000"  # 5 minutes
        os.environ["RAY_CLIENT_RECONNECT_GRACE_PERIOD"] = "120"  # 2 minutes
        os.environ["RAY_CLIENT_HEARTBEAT_TIMEOUT_MS"] = "120000"  # 2 minutes
        os.environ["RAY_CLIENT_QUEUE_TIMEOUT_MS"] = "300000"  # 5 minutes
        os.environ["RAY_CLIENT_DATA_CHANNEL_TIMEOUT_MS"] = "300000"  # 5 minutes
        # Try additional variations
        os.environ["RAY_CLIENT_TIMEOUT_MS"] = "300000"
        os.environ["RAY_CLIENT_QUEUE_TIMEOUT"] = "300"  # seconds
        os.environ["RAY_CLIENT_DATA_TIMEOUT"] = "300"  # seconds
        print("🔧 Ray Client environment variables set at OBJECT INIT!")

        self.config_manager = RayConfigManager(config or {})
        self.execution_mode = self.config_manager.determine_execution_mode()
        self.use_kuberay = self.execution_mode == RayExecutionMode.KUBERAY

        # Get configuration from manager for KubeRay connections
        kuberay_config = self.config_manager.get_kuberay_config()
        self.cluster_name = kuberay_config.get("cluster_name")
        self.namespace = kuberay_config.get("namespace", "default")
        self.auth_token = kuberay_config.get("auth_token")
        self.auth_server = kuberay_config.get("auth_server")
        self.skip_tls = kuberay_config.get("skip_tls", False)

        self.cluster = None
        self._ray_initialized = False

        logger.info(
            f"CodeFlare Ray wrapper initialized for cluster: {self.cluster_name}"
        )

        # Initialize connection if using KubeRay
        if self.use_kuberay:
            if not self._authenticate_codeflare():
                raise RuntimeError("CodeFlare authentication failed")
            if not self._setup_ray_connection():
                raise RuntimeError("Ray connection setup failed")

    def _authenticate_codeflare(self) -> bool:
        """Authenticate with CodeFlare SDK."""
        try:
            # Set Ray client environment variables EARLY, before any ray.init() calls
            import os

            from codeflare_sdk import TokenAuthentication

            print("🔧 Setting Ray client environment variables EARLY...")
            os.environ["RAY_CLIENT_SERVER_TIMEOUT_MS"] = "300000"  # 5 minutes
            os.environ["RAY_CLIENT_RECONNECT_GRACE_PERIOD"] = "120"  # 2 minutes
            os.environ["RAY_CLIENT_HEARTBEAT_TIMEOUT_MS"] = "120000"  # 2 minutes
            os.environ["RAY_CLIENT_QUEUE_TIMEOUT_MS"] = (
                "300000"  # 5 minutes for queue operations
            )
            os.environ["RAY_CLIENT_DATA_CHANNEL_TIMEOUT_MS"] = (
                "300000"  # 5 minutes for data channel
            )
            print("🔧 Ray client timeout environment variables set!")

            auth = TokenAuthentication(
                token=self.auth_token,
                server=self.auth_server,
                skip_tls=self.skip_tls,
            )
            auth.login()
            logger.info("✓ CodeFlare SDK authentication successful")
            return True
        except Exception as e:
            logger.error(f"✗ CodeFlare SDK authentication failed: {e}")
            return False

    def _setup_ray_connection(self) -> bool:
        """Set up direct Ray connection with TLS certificates."""
        print("🚨 CRITICAL: CodeFlare _setup_ray_connection() called!")
        try:
            print("🔧 Step 1: Importing CodeFlare SDK...")
            from codeflare_sdk import generate_cert, get_cluster

            print("🔧 Step 2: Getting cluster...")
            # Get existing cluster
            self.cluster = get_cluster(
                cluster_name=self.cluster_name,
                namespace=self.namespace,
            )
            print(f"🔧 Step 3: Cluster obtained: {self.cluster}")

            # Generate TLS certificates for secure Ray connection
            print("🔧 Step 4: Generating TLS certificates...")
            logger.info(
                f"🔐 Generating TLS certificates for Ray connection to {self.cluster_name}"
            )
            generate_cert.generate_tls_cert(self.cluster_name, self.namespace)
            generate_cert.export_env(self.cluster_name, self.namespace)
            print("🔧 Step 5: TLS certificates generated and exported")

            # Log TLS environment variables to verify they're set
            import os

            tls_vars = [
                "RAY_TLS_SERVER_KEY",
                "RAY_TLS_SERVER_CERT",
                "RAY_USE_TLS",
                "RAY_TLS_CA_CERT",
            ]
            logger.info("🔍 TLS Environment Variables:")
            for var in tls_vars:
                value = os.environ.get(var, "NOT SET")
                logger.info(f"  {var}: {'SET' if value != 'NOT SET' else 'NOT SET'}")

            # Also log the client timeout setting
            timeout_ms = os.environ.get("RAY_client_server_timeout_ms", "NOT SET")
            logger.info(f"  RAY_client_server_timeout_ms: {timeout_ms}")

            # Initialize Ray with direct connection to cluster
            if self.cluster is None:
                raise RuntimeError("Cluster not available")

            # The issue is Ray Client mode has hardcoded 10-second timeouts
            # Let's try to avoid Ray Client entirely and connect directly to GCS
            try:
                client_uri = self.cluster.cluster_uri()  # ray://host:10001
                print(f"🔗 Original cluster URI: {client_uri}")

                # Extract hostname from ray://hostname:port
                if client_uri.startswith("ray://"):
                    host_port = client_uri[6:]  # Remove "ray://"
                    host = host_port.split(":")[0]

                    # Test different connection approaches
                    # Option 1: Direct GCS connection (port 6379)
                    gcs_uri = f"{host}:6379"
                    print(f"🔗 Option 1 - Direct GCS: {gcs_uri}")

                    # Option 2: Keep Ray Client but with different host resolution
                    client_alt_uri = f"ray://{host}:10001"
                    print(f"🔗 Option 2 - Ray Client: {client_alt_uri}")

                    # GCS port (6379) is not accessible externally in KubeRay
                    # Let's go back to Ray Client but try to configure it better
                    cluster_uri = client_alt_uri
                else:
                    cluster_uri = client_uri
                    print(f"🔗 Using original URI: {cluster_uri}")

            except Exception as e:
                print(f"🔗 URI parsing failed: {e}")
                cluster_uri = self.cluster.cluster_uri()
                print(f"🔗 Using fallback: {cluster_uri}")

            # Shutdown any existing Ray connection first
            ray.shutdown()

            # Environment variables already set in _authenticate_codeflare()
            print("🔧 Step 7: Environment variables already set early")

            print("🔧 Step 8: Initializing Ray connection...")
            # Ray will now run operations directly on the remote KubeRay cluster
            # Try to pass timeout parameters directly to ray.init()
            # Create runtime environment with Feast dependencies
            runtime_env = {
                "pip": [
                    "feast",
                    "pyarrow",
                ],  # Include Feast and pyarrow for data conversion
                "env_vars": {
                    "RAY_DISABLE_IMPORT_WARNING": "1"  # Suppress import warnings
                },
            }

            ray_init_kwargs = {
                "address": cluster_uri,
                "ignore_reinit_error": True,
                "logging_level": "INFO",
                "runtime_env": runtime_env,
            }

            # If using Ray Client, rely on environment variables for timeouts
            if cluster_uri.startswith("ray://"):
                print(
                    "🔧 Using Ray Client mode - relying on environment variables for timeouts..."
                )
                # Don't pass invalid timeout parameters to ray.init()
                # Ray Client should read timeout values from environment variables
            else:
                print("🔧 Using direct cluster connection...")

            ray.init(**ray_init_kwargs)

            self._ray_initialized = True
            print(
                f"🔧 Step 9: Ray connected successfully to cluster: {self.cluster_name}"
            )
            logger.info(f"✓ Ray connected successfully to cluster: {self.cluster_name}")
            return True

        except Exception as e:
            print(f"🚨 ERROR: Ray connection setup failed at some step: {e}")
            logger.error(f"✗ Ray connection setup failed: {e}")
            return False

    # Ray Data API methods - wrapped in @ray.remote to execute on cluster workers
    def read_parquet(self, path: Union[str, List[str]]) -> Any:
        """Read parquet files - runs remotely on KubeRay cluster workers."""

        @ray.remote
        def _remote_read_parquet(file_path):
            import ray

            return ray.data.read_parquet(file_path)

        # Keep the dataset reference on remote cluster, don't transfer data
        return RemoteDatasetProxy(_remote_read_parquet.remote(path))

    def read_csv(self, path: Union[str, List[str]]) -> Any:
        """Read CSV files - runs remotely on KubeRay cluster workers."""

        @ray.remote
        def _remote_read_csv(file_path):
            import ray

            return ray.data.read_csv(file_path)

        # Keep the dataset reference on remote cluster, don't transfer data
        return RemoteDatasetProxy(_remote_read_csv.remote(path))

    def from_pandas(self, df: pd.DataFrame) -> Any:
        """Create dataset from pandas DataFrame - runs remotely on KubeRay cluster."""

        @ray.remote
        def _remote_from_pandas(dataframe):
            import ray

            return ray.data.from_pandas(dataframe)

        # Keep the dataset reference on remote cluster, don't transfer data
        return RemoteDatasetProxy(_remote_from_pandas.remote(df))

    def from_arrow(self, table: pa.Table) -> Any:
        """Create dataset from PyArrow table - runs remotely on KubeRay cluster."""

        @ray.remote
        def _remote_from_arrow(arrow_table):
            import ray

            return ray.data.from_arrow(arrow_table)

        # Keep the dataset reference on remote cluster, don't transfer data
        return RemoteDatasetProxy(_remote_from_arrow.remote(table))

    def to_pandas(self, dataset: Any) -> pd.DataFrame:
        """Convert dataset to pandas DataFrame."""
        if isinstance(dataset, RemoteDatasetProxy):
            return dataset.to_pandas()
        else:
            return dataset.to_pandas()

    def to_arrow(self, dataset: Any) -> pa.Table:
        """Convert dataset to PyArrow Table."""
        if isinstance(dataset, RemoteDatasetProxy):
            return dataset.to_arrow()
        elif hasattr(dataset, "to_arrow"):
            return dataset.to_arrow()
        else:
            return pa.Table.from_pandas(dataset.to_pandas())

    def is_initialized(self) -> bool:
        """Check if Ray wrapper is initialized."""
        return self._ray_initialized


def get_ray_wrapper() -> CodeFlareRayWrapper:
    """Get the global Ray wrapper instance."""
    if _global_ray_wrapper is None:
        raise RuntimeError(
            "Ray wrapper not initialized. Call initialize_ray_wrapper_from_config() first."
        )
    return _global_ray_wrapper


def initialize_ray_wrapper_from_config(config) -> CodeFlareRayWrapper:
    """Initialize Ray wrapper from Feast config."""
    global _global_ray_wrapper
    _global_ray_wrapper = CodeFlareRayWrapper(config)
    return _global_ray_wrapper


# Global wrapper instance
_global_ray_wrapper: Optional[CodeFlareRayWrapper] = None
