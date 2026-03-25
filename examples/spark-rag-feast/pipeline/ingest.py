"""Document ingestion script for the Spark RAG + Feast pipeline.

Reads documents from a source directory, chunks them, and writes
to Parquet for downstream Spark embedding computation.
"""

import argparse
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, IntegerType, StringType, StructField, StructType


def chunk_text(text: str, chunk_size: int = 512, overlap: int = 64) -> list:
    """Split text into overlapping chunks."""
    words = text.split()
    chunks = []
    start = 0
    while start < len(words):
        end = start + chunk_size
        chunk = " ".join(words[start:end])
        chunks.append(chunk)
        start += chunk_size - overlap
    return chunks


def main():
    parser = argparse.ArgumentParser(description="Ingest documents for RAG pipeline")
    parser.add_argument("--input-dir", required=True, help="Directory containing text documents")
    parser.add_argument("--output-path", required=True, help="Output Parquet path")
    parser.add_argument("--chunk-size", type=int, default=512, help="Words per chunk")
    parser.add_argument("--overlap", type=int, default=64, help="Overlap between chunks")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("feast-rag-ingest").getOrCreate()

    # Read text files
    raw_df = spark.read.text(args.input_dir, wholetext=True)

    # Add document_id from input file name
    raw_df = raw_df.withColumn(
        "document_id",
        F.monotonically_increasing_id().cast(StringType()),
    ).withColumn("title", F.lit("document"))

    # Chunk documents using a UDF
    from pyspark.sql.functions import udf

    chunk_udf = udf(
        lambda text: chunk_text(text, args.chunk_size, args.overlap),
        ArrayType(StringType()),
    )

    chunked_df = (
        raw_df.withColumn("chunks", chunk_udf(F.col("value")))
        .select("document_id", "title", F.posexplode("chunks").alias("chunk_index", "chunk_text"))
        .withColumn("token_count", F.size(F.split(F.col("chunk_text"), " ")))
        .withColumn("event_timestamp", F.lit(datetime.utcnow()))
    )

    chunked_df.write.mode("overwrite").parquet(args.output_path)
    print(f"Ingested {chunked_df.count()} chunks to {args.output_path}")

    spark.stop()


if __name__ == "__main__":
    main()
