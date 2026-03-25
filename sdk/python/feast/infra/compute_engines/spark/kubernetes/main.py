"""Materialization entrypoint for Spark Operator driver pods.

This script runs inside the Spark driver container when execution_mode is 'kubernetes'.
It deserializes the Feast config, loads the feature view, and runs materialization.
"""

import argparse
import base64
import json
import logging
import sys
from datetime import datetime

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(
        description="Feast Spark Kubernetes Materialization Job"
    )
    parser.add_argument("--feature-view", required=True, help="Feature view name")
    parser.add_argument("--start-date", required=True, help="Start date (ISO format)")
    parser.add_argument("--end-date", required=True, help="End date (ISO format)")
    parser.add_argument(
        "--config-base64", required=True, help="Base64-encoded RepoConfig JSON"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    logger.info(
        "Starting Feast Spark Kubernetes materialization",
        extra={
            "feature_view": args.feature_view,
            "start_date": args.start_date,
            "end_date": args.end_date,
        },
    )

    try:
        config_json = base64.b64decode(args.config_base64).decode()
        config_dict = json.loads(config_json)

        from feast.repo_config import RepoConfig

        repo_config = RepoConfig(**config_dict)

        start_date = datetime.fromisoformat(args.start_date)
        end_date = datetime.fromisoformat(args.end_date)

        from feast import FeatureStore

        store = FeatureStore(config=repo_config)

        store.materialize(
            start_date=start_date,
            end_date=end_date,
            feature_views=[args.feature_view],
        )

        logger.info(
            "Feast Spark Kubernetes materialization completed successfully",
            extra={"feature_view": args.feature_view},
        )

    except Exception as e:
        logger.error(
            "Feast Spark Kubernetes materialization failed",
            extra={
                "feature_view": args.feature_view,
                "error": str(e),
            },
            exc_info=True,
        )
        sys.exit(1)


if __name__ == "__main__":
    main()
