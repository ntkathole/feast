"""Embedding computation script for the Spark RAG + Feast pipeline.

Distributes sentence-transformer inference across Spark executors
to compute embeddings for document chunks, then writes results
for Feast materialization.
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, FloatType


def compute_embeddings_partition(iterator):
    """Compute embeddings for a partition of document chunks.

    Uses sentence-transformers loaded once per executor.
    """
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer("all-MiniLM-L6-v2")

    for batch in iterator:
        texts = batch["chunk_text"].tolist()
        embeddings = model.encode(texts, show_progress_bar=False)
        batch["embedding"] = [emb.tolist() for emb in embeddings]
        yield batch


def main():
    parser = argparse.ArgumentParser(description="Compute embeddings via Spark")
    parser.add_argument("--input-path", required=True, help="Input Parquet path (document chunks)")
    parser.add_argument("--output-path", required=True, help="Output Parquet path (with embeddings)")
    parser.add_argument("--model-name", default="all-MiniLM-L6-v2", help="Sentence transformer model")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("feast-rag-embed").getOrCreate()

    chunks_df = spark.read.parquet(args.input_path)

    # Use mapInPandas to distribute embedding computation
    from pyspark.sql.types import (
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    output_schema = StructType(
        [
            StructField("document_id", StringType()),
            StructField("title", StringType()),
            StructField("chunk_index", LongType()),
            StructField("chunk_text", StringType()),
            StructField("token_count", IntegerType()),
            StructField("event_timestamp", TimestampType()),
            StructField("embedding", ArrayType(FloatType())),
        ]
    )

    embedded_df = chunks_df.mapInPandas(
        compute_embeddings_partition, schema=output_schema
    )

    embedded_df.write.mode("overwrite").parquet(args.output_path)

    count = embedded_df.count()
    print(f"Computed embeddings for {count} chunks, output at {args.output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
