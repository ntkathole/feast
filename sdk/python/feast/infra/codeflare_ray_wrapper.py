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
from typing import Any, List, Optional, Union

import pandas as pd
import pyarrow as pa
import ray

from feast.infra.ray_config_manager import RayConfigManager

logger = logging.getLogger(__name__)


class RemoteDatasetProxy:
    """Proxy class that executes Ray Data operations remotely on cluster workers."""

    def __init__(self, dataset_ref: Any):
        """Initialize with a reference to the remote dataset."""
        self._dataset_ref = dataset_ref

    def map_batches(self, func, **kwargs) -> "RemoteDatasetProxy":
        """Execute map_batches remotely on cluster workers."""

        @ray.remote
        def _remote_map_batches(dataset, function, batch_kwargs):
            return dataset.map_batches(function, **batch_kwargs)

        new_ref = _remote_map_batches.remote(self._dataset_ref, func, kwargs)
        return RemoteDatasetProxy(new_ref)

    def filter(self, fn) -> "RemoteDatasetProxy":
        """Execute filter remotely on cluster workers."""

        @ray.remote
        def _remote_filter(dataset, filter_fn):
            return dataset.filter(filter_fn)

        new_ref = _remote_filter.remote(self._dataset_ref, fn)
        return RemoteDatasetProxy(new_ref)

    def to_pandas(self) -> pd.DataFrame:
        """Execute to_pandas remotely and transfer result to client."""

        @ray.remote
        def _remote_to_pandas(dataset):
            return dataset.to_pandas()

        result_ref = _remote_to_pandas.remote(self._dataset_ref)
        return ray.get(result_ref)

    def to_arrow(self) -> pa.Table:
        """Execute to_arrow remotely and transfer result to client."""

        @ray.remote
        def _remote_to_arrow(dataset):
            try:
                return dataset.to_arrow()
            except AttributeError:
                # Fallback for older Ray versions
                import pyarrow as pa

                pandas_df = dataset.to_pandas()
                return pa.Table.from_pandas(pandas_df)

        result_ref = _remote_to_arrow.remote(self._dataset_ref)
        return ray.get(result_ref)

    def schema(self) -> Any:
        """Get dataset schema."""

        @ray.remote
        def _remote_schema(dataset):
            return dataset.schema()

        schema_ref = _remote_schema.remote(self._dataset_ref)
        return ray.get(schema_ref)

    def sort(self, key, descending=False) -> "RemoteDatasetProxy":
        """Execute sort remotely on cluster workers."""

        @ray.remote
        def _remote_sort(dataset, sort_key, desc):
            return dataset.sort(sort_key, descending=desc)

        new_ref = _remote_sort.remote(self._dataset_ref, key, descending)
        return RemoteDatasetProxy(new_ref)

    def limit(self, count) -> "RemoteDatasetProxy":
        """Execute limit remotely on cluster workers."""

        @ray.remote
        def _remote_limit(dataset, limit_count):
            return dataset.limit(limit_count)

        new_ref = _remote_limit.remote(self._dataset_ref, count)
        return RemoteDatasetProxy(new_ref)

    def union(self, other) -> "RemoteDatasetProxy":
        """Execute union remotely on cluster workers."""

        @ray.remote
        def _remote_union(dataset1, dataset2):
            return dataset1.union(dataset2)

        new_ref = _remote_union.remote(self._dataset_ref, other._dataset_ref)
        return RemoteDatasetProxy(new_ref)

    def materialize(self) -> "RemoteDatasetProxy":
        """Execute materialize remotely on cluster workers."""

        @ray.remote
        def _remote_materialize(dataset):
            return dataset.materialize()

        new_ref = _remote_materialize.remote(self._dataset_ref)
        return RemoteDatasetProxy(new_ref)

    def count(self) -> int:
        """Execute count remotely and return result."""

        @ray.remote
        def _remote_count(dataset):
            return dataset.count()

        result_ref = _remote_count.remote(self._dataset_ref)
        return ray.get(result_ref)

    def take(self, n=20) -> list:
        """Execute take remotely and return result."""

        @ray.remote
        def _remote_take(dataset, num):
            return dataset.take(num)

        result_ref = _remote_take.remote(self._dataset_ref, n)
        return ray.get(result_ref)

    def __getattr__(self, name):
        """Catch any method calls that we haven't explicitly implemented."""
        raise AttributeError(f"RemoteDatasetProxy has no attribute '{name}'")


class CodeFlareRayWrapper:
    """Wrapper for Ray operations on KubeRay clusters using CodeFlare SDK."""

    def __init__(
        self,
        cluster_name: str,
        namespace: str,
        auth_token: str,
        auth_server: str,
        skip_tls: bool = False,
        connection_timeout: int = 60,
        max_retries: int = 3,
        retry_delay: int = 5,
    ):
        """Initialize CodeFlare Ray wrapper with cluster connection parameters."""
        self.cluster_name = cluster_name
        self.namespace = namespace
        self.auth_token = auth_token
        self.auth_server = auth_server
        self.skip_tls = skip_tls
        self.connection_timeout = connection_timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.cluster = None

        # Authenticate and setup Ray connection
        self._authenticate_codeflare()
        self._setup_ray_connection()

    def _authenticate_codeflare(self):
        """Authenticate with CodeFlare SDK."""
        try:
            from codeflare_sdk import TokenAuthentication

            auth = TokenAuthentication(
                token=self.auth_token,
                server=self.auth_server,
                skip_tls=self.skip_tls,
            )
            auth.login()
            logger.info("✓ CodeFlare authentication successful")
        except Exception as e:
            logger.error(f"✗ CodeFlare authentication failed: {e}")
            raise

    def _setup_ray_connection(self):
        """Setup Ray connection to KubeRay cluster using TLS certificates."""
        try:
            from codeflare_sdk import generate_cert, get_cluster

            # Get the cluster
            self.cluster = get_cluster(
                cluster_name=self.cluster_name, namespace=self.namespace
            )

            if self.cluster is None:
                raise RuntimeError(
                    f"Failed to find KubeRay cluster '{self.cluster_name}' in namespace '{self.namespace}'"
                )

            # Generate TLS certificates and export environment variables
            generate_cert.generate_tls_cert(self.cluster_name, self.namespace)
            generate_cert.export_env(self.cluster_name, self.namespace)

            cluster_uri = self.cluster.cluster_uri()

            # Create runtime environment with Feast dependencies
            runtime_env = {
                "pip": ["feast", "pyarrow"],
                "env_vars": {"RAY_DISABLE_IMPORT_WARNING": "1"},
            }

            # Initialize Ray with runtime environment
            ray.shutdown()
            ray.init(
                address=cluster_uri,
                ignore_reinit_error=True,
                logging_level="INFO",
                runtime_env=runtime_env,
            )

            logger.info(f"✓ Ray connected successfully to cluster: {self.cluster_name}")

        except Exception as e:
            logger.error(f"✗ Ray connection failed: {e}")
            raise

    # Ray Data API methods - wrapped in @ray.remote to execute on cluster workers
    def read_parquet(self, path: Union[str, List[str]]) -> Any:
        """Read parquet files - runs remotely on KubeRay cluster workers."""

        @ray.remote
        def _remote_read_parquet(file_path):
            import ray

            return ray.data.read_parquet(file_path)

        return RemoteDatasetProxy(_remote_read_parquet.remote(path))

    def read_csv(self, path: Union[str, List[str]]) -> Any:
        """Read CSV files - runs remotely on KubeRay cluster workers."""

        @ray.remote
        def _remote_read_csv(file_path):
            import ray

            return ray.data.read_csv(file_path)

        return RemoteDatasetProxy(_remote_read_csv.remote(path))

    def from_pandas(self, df: pd.DataFrame) -> Any:
        """Create dataset from pandas DataFrame - runs remotely on KubeRay cluster workers."""

        @ray.remote
        def _remote_from_pandas(dataframe):
            import ray

            return ray.data.from_pandas(dataframe)

        return RemoteDatasetProxy(_remote_from_pandas.remote(df))

    def from_arrow(self, table: pa.Table) -> Any:
        """Create dataset from Arrow table - runs remotely on KubeRay cluster workers."""

        @ray.remote
        def _remote_from_arrow(arrow_table):
            import ray

            return ray.data.from_arrow(arrow_table)

        return RemoteDatasetProxy(_remote_from_arrow.remote(table))

    def to_pandas(self, dataset: Any) -> pd.DataFrame:
        """Convert dataset to pandas DataFrame."""
        if isinstance(dataset, RemoteDatasetProxy):
            return dataset.to_pandas()
        else:
            return dataset.to_pandas()

    def to_arrow(self, dataset: Any) -> pa.Table:
        """Convert dataset to Arrow table."""
        if isinstance(dataset, RemoteDatasetProxy):
            return dataset.to_arrow()
        else:
            return dataset.to_arrow()


# Global wrapper instance
_global_ray_wrapper: Optional[CodeFlareRayWrapper] = None


def initialize_ray_wrapper_from_config(config) -> CodeFlareRayWrapper:
    """Initialize Ray wrapper from Feast configuration."""
    global _global_ray_wrapper

    if _global_ray_wrapper is not None:
        return _global_ray_wrapper

    # Create RayConfigManager instance and get kuberay config
    config_manager = RayConfigManager(config)
    kuberay_config = config_manager.get_kuberay_config()

    _global_ray_wrapper = CodeFlareRayWrapper(
        cluster_name=kuberay_config["cluster_name"],
        namespace=kuberay_config["namespace"],
        auth_token=kuberay_config["auth_token"],
        auth_server=kuberay_config["auth_server"],
        skip_tls=kuberay_config.get("skip_tls", False),
        connection_timeout=kuberay_config.get("connection_timeout", 60),
        max_retries=kuberay_config.get("max_retries", 3),
        retry_delay=kuberay_config.get("retry_delay", 5),
    )

    return _global_ray_wrapper


def get_ray_wrapper() -> CodeFlareRayWrapper:
    """Get the global Ray wrapper instance."""
    if _global_ray_wrapper is None:
        raise RuntimeError(
            "Ray wrapper not initialized. Call initialize_ray_wrapper_from_config() first."
        )
    return _global_ray_wrapper
