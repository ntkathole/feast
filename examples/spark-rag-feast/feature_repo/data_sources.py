"""Data source definitions for the Spark RAG + Feast example."""

from feast import FileSource

# Source: Parquet files containing document chunks with embeddings
document_source = FileSource(
    name="document_chunks",
    path="data/document_chunks.parquet",
    timestamp_field="event_timestamp",
    description="Document chunks with precomputed embeddings from Spark",
)
