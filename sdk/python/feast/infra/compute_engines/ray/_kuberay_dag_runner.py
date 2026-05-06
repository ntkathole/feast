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
KubeRay DAG runner — executed inside the ephemeral RayCluster head node.

Design
------
Feast **orchestration** is the driver's job.  The cluster does only
**distributed compute**.  This maps exactly to how Ray Data works naturally:
the driver defines the pipeline, workers execute the tasks.

What the driver does:
  1. Builds ``ExecutionPlan`` (DAG of Ray Data nodes) via ``RayFeatureBuilder``.
  2. cloudpickle-serialises the plan to ``_feast_plan.pkl``.
     cloudpickle is a Ray core dependency — always available on the cluster.
  3. Ships the plan + this runner + the feature repo (feature_store.yaml,
     feature definitions) to the cluster via ``runtime_env.working_dir``.
  4. Passes task parameters as plain env vars.

What this script does on the cluster:
  1. Loads the plan from ``_feast_plan.pkl`` (cloudpickle).
  2. Reconstructs ``ExecutionContext`` from ``feature_store.yaml`` (already
     in the current directory via ``working_dir``) and task env vars.
     No JSON parsing, no protobuf, no custom serialisation.
  3. Calls ``plan.execute(context)`` — Ray Data distributes tasks to workers.
  4. For ``get_historical_features``: writes results to ``FEAST_OUTPUT_PATH``.

Why cloudpickle instead of dill?
  cloudpickle is always present (Ray dependency).  dill would require an extra
  ``pip install`` on every worker pod.  For the objects we serialise (plan nodes
  with Python callables / UDFs), cloudpickle is equivalent.

Environment variables
---------------------
FEAST_RUNNER_OP           ``"materialize"`` (default) | ``"historical_features"``
FEAST_PLAN_FILE           path to ``_feast_plan.pkl``  (default: ``_feast_plan.pkl``)
FEAST_RAY_EXECUTION_MODE  set to ``"local"`` by the driver — prevents recursive
                          KubeRay job submission when feast runs plan.execute().

Materialisation:
  FEAST_FEATURE_VIEW      feature view name
  FEAST_START_TIME        ISO-8601 start datetime
  FEAST_END_TIME          ISO-8601 end datetime

Historical features:
  FEAST_FEATURE_REFS      comma-separated feature refs
  FEAST_FULL_FEATURE_NAMES  ``"true"`` | ``"false"``
  FEAST_ENTITY_DF_PATH    staging path for entity DataFrame parquet
  FEAST_OUTPUT_PATH       staging path where result parquet is written
"""

import logging
import os
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


def _load_plan(plan_file: str):
    import cloudpickle

    with open(plan_file, "rb") as fh:
        plan = cloudpickle.load(fh)
    logger.info(
        "ExecutionPlan loaded from '%s' (%d nodes).", plan_file, len(plan.nodes)
    )
    return plan


def _build_context(op: str, entity_df=None):
    """
    Reconstruct ``ExecutionContext`` from ``feature_store.yaml`` + env vars.

    ``feature_store.yaml`` is present in the working directory because
    the CodeFlare SDK ships ``working_dir`` (= ``feature_repo_dir``) to every
    pod.  No custom serialisation is needed.
    """
    from feast import FeatureStore
    from feast.infra.compute_engines.dag.context import ExecutionContext
    from feast.infra.offline_stores.offline_utils import get_offline_store_from_config
    from feast.infra.online_stores.helpers import get_online_store_from_config

    # FeatureStore("."): reads feature_store.yaml from the current directory
    # (working_dir), which the SDK already shipped to every pod.
    fs = FeatureStore(".")
    repo_config = fs.config

    offline_store = get_offline_store_from_config(repo_config.offline_store)
    online_store = get_online_store_from_config(repo_config.online_store)
    entity_defs = list(fs.list_entities())

    return ExecutionContext(
        project=repo_config.project,
        repo_config=repo_config,
        offline_store=offline_store,
        online_store=online_store,
        entity_defs=entity_defs,
        entity_df=entity_df,
    )


def main() -> None:
    import cloudpickle  # noqa: F401 — verify it is importable early

    op = os.environ.get("FEAST_RUNNER_OP", "materialize").lower()
    plan_file = os.environ.get("FEAST_PLAN_FILE", "_feast_plan.pkl")

    logger.info("KubeRay DAG runner starting (op=%s).", op)

    # ── Load plan ─────────────────────────────────────────────────────────
    plan = _load_plan(plan_file)

    # ── Prepare entity DataFrame (historical features) ────────────────────
    entity_df = None
    output_path: str = ""

    if op == "historical_features":
        output_path = os.environ.get("FEAST_OUTPUT_PATH", "")
        if not output_path:
            raise EnvironmentError(
                "FEAST_OUTPUT_PATH must be set for op=historical_features."
            )

        entity_df_path = os.environ.get("FEAST_ENTITY_DF_PATH", "")
        if not entity_df_path:
            raise EnvironmentError(
                "FEAST_ENTITY_DF_PATH must be set for op=historical_features."
            )

        import pandas as pd

        logger.info("Loading entity DataFrame from '%s'…", entity_df_path)
        entity_df = pd.read_parquet(entity_df_path)
        logger.info("Entity DataFrame loaded (%d rows).", len(entity_df))

    # ── Reconstruct context from feature_store.yaml + env vars ───────────
    context = _build_context(op, entity_df=entity_df)
    logger.info("ExecutionContext built (project='%s').", context.project)

    # ── Connect to the running cluster ────────────────────────────────────
    # Initialise Ray explicitly (no params) so subsequent Feast calls that
    # also call ray.init() get a harmless no-op instead of raising an error
    # about "num_cpus must not be provided when connecting to an existing cluster".
    import ray

    if not ray.is_initialized():
        ray.init()
        logger.info("Ray initialised (connected to existing cluster).")

    # ── Execute the plan ─────────────────────────────────────────────────
    # Ray Data distributes map_batches / reads / writes to cluster workers.
    result = plan.execute(context)
    logger.info("Plan execution completed.")

    # ── Write result for historical features ──────────────────────────────
    if op == "historical_features":
        _write_result(result.data, output_path)

    logger.info("KubeRay DAG runner finished (op=%s).", op)


def _write_result(data, output_path: str) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq
    from ray.data import Dataset

    if isinstance(data, Dataset):
        data.write_parquet(output_path)
    elif isinstance(data, pa.Table):
        pq.write_table(data, output_path)
    else:
        import pandas as pd

        if isinstance(data, pd.DataFrame):
            data.to_parquet(output_path, index=False)
        else:
            raise TypeError(f"Unsupported result type: {type(data)}")

    logger.info("Results written to '%s'.", output_path)


if __name__ == "__main__":
    main()
