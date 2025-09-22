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
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import pyarrow as pa
import ray

from feast.infra.ray_config_manager import RayConfigManager, RayExecutionMode

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

        new_ref = ray.get(_remote_map_batches.remote(self._dataset_ref, func, kwargs))
        return RemoteDatasetProxy(new_ref)

    def sort(self, key, descending=False) -> "RemoteDatasetProxy":
        """Execute sort remotely on cluster workers."""

        @ray.remote
        def _remote_sort(dataset, sort_key, desc):
            return dataset.sort(sort_key, descending=desc)

        new_ref = ray.get(_remote_sort.remote(self._dataset_ref, key, descending))
        return RemoteDatasetProxy(new_ref)

    def union(self, other: "RemoteDatasetProxy") -> "RemoteDatasetProxy":
        """Execute union remotely on cluster workers."""

        @ray.remote
        def _remote_union(dataset1, dataset2):
            return dataset1.union(dataset2)

        new_ref = ray.get(_remote_union.remote(self._dataset_ref, other._dataset_ref))
        return RemoteDatasetProxy(new_ref)

    def write_parquet(self, path: str) -> None:
        """Execute write_parquet remotely on cluster workers."""

        @ray.remote
        def _remote_write_parquet(dataset, file_path):
            dataset.write_parquet(file_path)
            return None

        ray.get(_remote_write_parquet.remote(self._dataset_ref, path))

    def to_pandas(self) -> pd.DataFrame:
        """Execute to_pandas remotely on cluster workers."""

        @ray.remote
        def _remote_to_pandas(dataset):
            return dataset.to_pandas()

        return ray.get(_remote_to_pandas.remote(self._dataset_ref))

    def to_arrow(self) -> pa.Table:
        """Execute to_arrow remotely on cluster workers."""

        @ray.remote
        def _remote_to_arrow(dataset):
            return dataset.to_arrow()

        return ray.get(_remote_to_arrow.remote(self._dataset_ref))

    def schema(self) -> Any:
        """Execute schema remotely on cluster workers."""

        @ray.remote
        def _remote_schema(dataset):
            return dataset.schema()

        return ray.get(_remote_schema.remote(self._dataset_ref))

    def limit(self, count: int) -> "RemoteDatasetProxy":
        """Execute limit remotely on cluster workers."""

        @ray.remote
        def _remote_limit(dataset, limit_count):
            return dataset.limit(limit_count)

        new_ref = ray.get(_remote_limit.remote(self._dataset_ref, count))
        return RemoteDatasetProxy(new_ref)

    def materialize(self) -> "RemoteDatasetProxy":
        """Execute materialize remotely on cluster workers."""

        @ray.remote
        def _remote_materialize(dataset):
            return dataset.materialize()

        new_ref = ray.get(_remote_materialize.remote(self._dataset_ref))
        return RemoteDatasetProxy(new_ref)

    def filter(self, fn) -> "RemoteDatasetProxy":
        """Execute filter remotely on cluster workers."""

        @ray.remote
        def _remote_filter(dataset, filter_fn):
            return dataset.filter(filter_fn)

        new_ref = ray.get(_remote_filter.remote(self._dataset_ref, fn))
        return RemoteDatasetProxy(new_ref)

    def size_bytes(self) -> int:
        """Execute size_bytes remotely on cluster workers."""

        @ray.remote
        def _remote_size_bytes(dataset):
            return dataset.size_bytes()

        return ray.get(_remote_size_bytes.remote(self._dataset_ref))

    def repartition(self, **kwargs) -> "RemoteDatasetProxy":
        """Execute repartition remotely on cluster workers."""

        @ray.remote
        def _remote_repartition(dataset, repartition_kwargs):
            return dataset.repartition(**repartition_kwargs)

        new_ref = ray.get(_remote_repartition.remote(self._dataset_ref, kwargs))
        return RemoteDatasetProxy(new_ref)

    def random_shuffle(self, **kwargs) -> "RemoteDatasetProxy":
        """Execute random_shuffle remotely on cluster workers."""

        @ray.remote
        def _remote_random_shuffle(dataset, shuffle_kwargs):
            return dataset.random_shuffle(**shuffle_kwargs)

        new_ref = ray.get(_remote_random_shuffle.remote(self._dataset_ref, kwargs))
        return RemoteDatasetProxy(new_ref)

    def min(self, column: str) -> Any:
        """Execute min remotely on cluster workers."""

        @ray.remote
        def _remote_min(dataset, col):
            return dataset.min(col)

        return ray.get(_remote_min.remote(self._dataset_ref, column))

    def max(self, column: str) -> Any:
        """Execute max remotely on cluster workers."""

        @ray.remote
        def _remote_max(dataset, col):
            return dataset.max(col)

        return ray.get(_remote_max.remote(self._dataset_ref, column))

    def copy(self) -> "RemoteDatasetProxy":
        """Execute copy remotely on cluster workers."""

        @ray.remote
        def _remote_copy(dataset):
            return dataset.copy()

        new_ref = ray.get(_remote_copy.remote(self._dataset_ref))
        return RemoteDatasetProxy(new_ref)


class CodeFlareRayWrapper:
    """
    Wrapper for CodeFlare SDK integration with KubeRay clusters using TLS certificates.

    This wrapper uses CodeFlare's TLS certificate generation to establish a direct
    Ray connection to the KubeRay cluster, allowing native Ray operations to run remotely.
    """

    def __init__(self, config: Optional[Union[Dict[str, Any], object]] = None):
        """Initialize the CodeFlare Ray wrapper with TLS-based Ray connection."""
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
            from codeflare_sdk import TokenAuthentication

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
        try:
            from codeflare_sdk import generate_cert, get_cluster

            # Get existing cluster
            self.cluster = get_cluster(
                cluster_name=self.cluster_name,
                namespace=self.namespace,
            )

            # Generate TLS certificates for secure Ray connection
            logger.info("Generating TLS certificates for Ray connection")
            generate_cert.generate_tls_cert(self.cluster_name, self.namespace)
            generate_cert.export_env(self.cluster_name, self.namespace)

            # Initialize Ray with direct connection to cluster
            if self.cluster is None:
                raise RuntimeError("Cluster not available")

            cluster_uri = self.cluster.cluster_uri()
            logger.info(f"Connecting to Ray cluster: {cluster_uri}")

            # Ray will now run operations directly on the remote KubeRay cluster
            ray.init(
                address=cluster_uri, ignore_reinit_error=True, logging_level="INFO"
            )

            self._ray_initialized = True
            logger.info(f"✓ Ray connected successfully to cluster: {self.cluster_name}")
            return True

        except Exception as e:
            logger.error(f"✗ Ray connection setup failed: {e}")
            return False

    # Ray Data API methods - wrapped in @ray.remote to execute on cluster workers
    def read_parquet(self, path: Union[str, List[str]]) -> Any:
        """Read parquet files - runs remotely on KubeRay cluster workers."""

        @ray.remote
        def _remote_read_parquet(file_path):
            import ray

            return ray.data.read_parquet(file_path)

        return RemoteDatasetProxy(ray.get(_remote_read_parquet.remote(path)))

    def read_csv(self, path: Union[str, List[str]]) -> Any:
        """Read CSV files - runs remotely on KubeRay cluster workers."""

        @ray.remote
        def _remote_read_csv(file_path):
            import ray

            return ray.data.read_csv(file_path)

        return RemoteDatasetProxy(ray.get(_remote_read_csv.remote(path)))

    def from_pandas(self, df: pd.DataFrame) -> Any:
        """Create dataset from pandas DataFrame - runs remotely on KubeRay cluster."""

        @ray.remote
        def _remote_from_pandas(dataframe):
            import ray

            return ray.data.from_pandas(dataframe)

        return RemoteDatasetProxy(ray.get(_remote_from_pandas.remote(df)))

    def from_arrow(self, table: pa.Table) -> Any:
        """Create dataset from PyArrow table - runs remotely on KubeRay cluster."""

        @ray.remote
        def _remote_from_arrow(arrow_table):
            import ray

            return ray.data.from_arrow(arrow_table)

        return RemoteDatasetProxy(ray.get(_remote_from_arrow.remote(table)))

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
