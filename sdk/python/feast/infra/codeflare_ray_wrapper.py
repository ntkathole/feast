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
from feast.infra.ray_shared_utils import RemoteDatasetProxy

logger = logging.getLogger(__name__)


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
