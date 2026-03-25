"""Feature definitions for the Spark RAG + Feast example."""

from datetime import timedelta

from feast import Entity, FeatureView, Field
from feast.types import Array, Float32, Int64, String

from data_sources import document_source

# Entity: a document identified by its unique ID
document = Entity(
    name="document",
    join_keys=["document_id"],
    description="A document in the RAG corpus",
)

# Feature view: document embeddings computed via Spark
document_embeddings = FeatureView(
    name="document_embeddings",
    entities=[document],
    ttl=timedelta(days=30),
    schema=[
        Field(name="document_id", dtype=String),
        Field(name="title", dtype=String),
        Field(name="chunk_index", dtype=Int64),
        Field(name="chunk_text", dtype=String),
        Field(name="embedding", dtype=Array(Float32)),
        Field(name="token_count", dtype=Int64),
    ],
    source=document_source,
    online=True,
    description="Document embeddings for RAG retrieval, computed via Spark",
)
