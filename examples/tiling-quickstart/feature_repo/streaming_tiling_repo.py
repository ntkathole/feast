"""
Real Tiling Example with Streaming Data

This example demonstrates REAL tiling with:
- Kafka as streaming source
- StreamFeatureView with aggregations (enables tiling)
- Redis as online store
- Real-time tile updates
"""

from datetime import timedelta

from feast import Entity, FeatureService, Field, FileSource, KafkaSource, StreamFeatureView, ValueType
from feast.aggregation import Aggregation
from feast.data_format import JsonFormat
from feast.types import Float32, Int64, String

# ===========================
# Entities
# ===========================

customer = Entity(
    name="customer",
    join_keys=["customer_id"],
    value_type=ValueType.STRING,
    description="Customer entity for tracking behavior",
)

# ===========================
# Data Sources
# ===========================

# Batch source (for backfilling)
customer_behavior_batch = FileSource(
    name="customer_behavior_batch",
    path="data/customer_behavior.parquet",
    timestamp_field="event_timestamp",
)

# Kafka streaming source
customer_behavior_stream = KafkaSource(
    name="customer_behavior_stream",
    kafka_bootstrap_servers="localhost:9092",
    topic="customer_events",
    timestamp_field="event_timestamp",
    batch_source=customer_behavior_batch,  # For backfilling
    message_format=JsonFormat(
        schema_json='''{
            "type": "object",
            "properties": {
                "customer_id": {"type": "string"},
                "purchase_amount": {"type": "number"},
                "page_views": {"type": "integer"},
                "session_duration": {"type": "number"},
                "event_timestamp": {"type": "string", "format": "date-time"}
            }
        }'''
    ),
)

# ===========================
# StreamFeatureView with Aggregations (Tiling Enabled!)
# ===========================

# Define aggregations - THIS ENABLES TILING!
# Tiles will be created with these pre-aggregated values
customer_behavior_aggregations = [
    # Purchase aggregations - Hourly
    Aggregation(
        column="purchase_amount",
        function="sum",
        time_window=timedelta(hours=1),  # Hourly tiles
    ),
    Aggregation(
        column="purchase_amount",
        function="count",
        time_window=timedelta(hours=1),
    ),
    Aggregation(
        column="purchase_amount",
        function="max",
        time_window=timedelta(hours=1),
    ),
    # NOTE: Daily aggregations disabled - mixing multiple window sizes causes issues
    # because Redis stores only one timestamp per entity. For production, use separate
    # FeatureViews for different window sizes, or implement multi-timestamp support.
    # Aggregation(
    #     column="purchase_amount",
    #     function="sum",
    #     time_window=timedelta(days=1),
    # ),
    # Aggregation(
    #     column="purchase_amount",
    #     function="count",
    #     time_window=timedelta(days=1),
    # ),
    # Page view aggregations
    Aggregation(
        column="page_views",
        function="sum",
        time_window=timedelta(hours=1),
    ),
    Aggregation(
        column="page_views",
        function="count",
        time_window=timedelta(hours=1),
    ),
    # Session duration aggregations
    Aggregation(
        column="session_duration",
        function="sum",
        time_window=timedelta(hours=1),
    ),
    Aggregation(
        column="session_duration",
        function="count",
        time_window=timedelta(hours=1),
    ),
]

# StreamFeatureView with tiling support
# Schema defines the AGGREGATED features (output columns), not input columns
customer_behavior_tiled = StreamFeatureView(
    name="customer_behavior_tiled",
    entities=[customer],
    source=customer_behavior_stream,
    aggregations=customer_behavior_aggregations,  # THIS ENABLES TILING!
    schema=[
        # Hourly purchase aggregations
        Field(name="sum_purchase_amount_3600s", dtype=Float32),
        Field(name="count_purchase_amount_3600s", dtype=Int64),
        Field(name="max_purchase_amount_3600s", dtype=Float32),
        # Hourly page views
        Field(name="sum_page_views_3600s", dtype=Int64),
        Field(name="count_page_views_3600s", dtype=Int64),
        # Hourly session duration
        Field(name="sum_session_duration_3600s", dtype=Float32),
        Field(name="count_session_duration_3600s", dtype=Int64),
    ],
    timestamp_field="event_timestamp",
    ttl=timedelta(days=7),  # Keep 7 days of tiles
    mode="python",  # Stream processing mode (not used for materialization)
    description="Customer behavior features with tiling support. "
                "This StreamFeatureView automatically creates pre-aggregated tiles "
                "(hourly and daily) and stores them in Redis for fast queries. "
                "Materialization uses the local compute engine (defined in feature_store.yaml).",
    tags={
        "use_case": "real_time_analytics",
        "tiling": "enabled",
        "description": "Demonstrates real tiling with Kafka and Redis"
    },
)

# ===========================
# Feature Service
# ===========================

customer_analytics_service = FeatureService(
    name="customer_analytics_tiled",
    features=[customer_behavior_tiled],
    tags={
        "use_case": "real_time_analytics",
        "tiling": "enabled",
        "description": "Real-time customer analytics with tiling support"
    },
)
