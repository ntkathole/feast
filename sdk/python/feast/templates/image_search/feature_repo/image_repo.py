"""
Image feature definitions for Feast image search template.

This module defines feature views for storing and searching images with embeddings.
Demonstrates image storage and vector embeddings for similarity search.
"""

from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource
from feast.data_format import ParquetFormat
from feast.types import Array, Float32, Int64, String
from feast.value_type import ValueType

# Entity definitions
image_item = Entity(
    name="image_id",
    description="Unique identifier for images",
    value_type=ValueType.INT64,
)

# Data sources
image_source = FileSource(
    file_format=ParquetFormat(),
    path="../data/images_with_embeddings.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

# Feature Views

# Image storage and embeddings feature view
image_embeddings_fv = FeatureView(
    name="image_embeddings",
    entities=[image_item],
    ttl=timedelta(days=365),  # Images are relatively static
    schema=[
        # Store image file path for display
        Field(
            name="image_path",
            dtype=String,
            description="Path to image file for display",
        ),
        # Store image embeddings for vector search
        Field(
            name="embedding",
            dtype=Array(Float32),
            vector_index=True,  # Enable vector indexing
            vector_search_metric="COSINE",  # Use cosine similarity
            description="Image embedding vector for similarity search",
        ),
        # Metadata fields
        Field(name="filename", dtype=String, description="Original filename"),
        Field(name="width", dtype=Int64, description="Image width in pixels"),
        Field(name="height", dtype=Int64, description="Image height in pixels"),
        Field(
            name="format", dtype=String, description="Image format (JPEG, PNG, etc.)"
        ),
        Field(name="category", dtype=String, description="Image category/tag"),
    ],
    source=image_source,
    online=True,
)
