# # # # # # # # # # # # # # # # # # # # # # # #
# Ray RAG Template - Feature Definitions   #
# Demonstrates Ray-powered batch embedding  #
# generation using ODFV during             #
# materialization for RAG applications     #
# # # # # # # # # # # # # # # # # # # # # # # #

from pathlib import Path
from typing import Any, Dict

import pandas as pd

from feast import Entity, FeatureService, Field, RequestSource, ValueType
from feast.batch_feature_view import BatchFeatureView
from feast.infra.offline_stores.file_source import FileSource
from feast.on_demand_feature_view import on_demand_feature_view
from feast.types import Array, Float32, String

# Constants
CURRENT_DIR = Path(__file__).parent
EMBED_MODEL_ID = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIM = 384


def embed_text(text: str) -> list[float]:
    """Generate embedding for text using sentence transformers."""
    try:
        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer(EMBED_MODEL_ID)
        return model.encode([text], normalize_embeddings=True).tolist()[0]
    except ImportError:
        import numpy as np

        embedding = np.random.normal(0, 1, EMBEDDING_DIM)
        return (embedding / np.linalg.norm(embedding)).tolist()


def generate_embeddings_udf(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ray-powered UDF for distributed embedding generation.

    This function is executed by Ray compute engine during materialization,
    enabling distributed processing of embeddings across the cluster.

    The UDF:
    1. Receives raw movie data with Description field
    2. Generates embeddings using sentence transformers
    3. Returns only the fields defined in the BatchFeatureView schema
    """
    try:
        print(f"UDF received columns: {df.columns.tolist()}")
        print(f"UDF received {len(df)} rows")

        import pandas as pd
        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer(EMBED_MODEL_ID)

        # Create document_id from the id column
        if "id" in df.columns:
            df["document_id"] = "movie_" + df["id"].astype(str)

        # Generate embeddings for movie descriptions
        if "Description" in df.columns:
            embeddings = model.encode(
                df["Description"].fillna("").tolist(),
                normalize_embeddings=True,
                show_progress_bar=False,
            )
            df["embedding"] = embeddings.tolist()
            df["embedding_model"] = EMBED_MODEL_ID

        # Handle NaT (Not-a-Time) values in timestamp columns
        # Replace NaT with a default timestamp to avoid serialization errors
        if "event_timestamp" in df.columns:
            df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
            # Replace NaT with epoch start (1970-01-01) for rows with missing timestamps
            df["event_timestamp"] = df["event_timestamp"].fillna(
                pd.Timestamp("1970-01-01", tz="UTC")
            )

        if "DatePublished" in df.columns:
            df["DatePublished"] = pd.to_datetime(df["DatePublished"])
            df["DatePublished"] = df["DatePublished"].fillna(
                pd.Timestamp("1970-01-01", tz="UTC")
            )

        # Return ALL fields - no filtering, everything gets persisted
        print(f"UDF returning columns: {df.columns.tolist()}")
        print(f"UDF returning {len(df)} rows")
        if len(df) > 0 and "embedding" in df.columns:
            print(
                f"First embedding length: {len(df['embedding'].iloc[0]) if df['embedding'].iloc[0] else 'None'}"
            )
        return df

    except Exception as e:
        import traceback

        print(f"Error generating embeddings: {e}")
        print(traceback.format_exc())
        # Fallback to random embeddings for testing
        import numpy as np
        import pandas as pd

        # Create document_id if not exists
        if "id" in df.columns and "document_id" not in df.columns:
            df["document_id"] = "movie_" + df["id"].astype(str)

        df["embedding"] = [
            (
                np.random.normal(0, 1, EMBEDDING_DIM)
                / np.linalg.norm(np.random.normal(0, 1, EMBEDDING_DIM))
            ).tolist()
            for _ in range(len(df))
        ]
        df["embedding_model"] = f"{EMBED_MODEL_ID} (fallback)"

        # Handle NaT values in timestamp columns
        if "event_timestamp" in df.columns:
            df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
            df["event_timestamp"] = df["event_timestamp"].fillna(
                pd.Timestamp("1970-01-01", tz="UTC")
            )

        if "DatePublished" in df.columns:
            df["DatePublished"] = pd.to_datetime(df["DatePublished"])
            df["DatePublished"] = df["DatePublished"].fillna(
                pd.Timestamp("1970-01-01", tz="UTC")
            )

        # Return ALL fields
        return df


# Entities
document = Entity(
    name="document",
    description="Document entity for RAG retrieval",
    value_type=ValueType.STRING,
    join_keys=["document_id"],
)

# Data sources
movies_source = FileSource(
    name="movies_data",
    path=f"{CURRENT_DIR}/data/raw_movies.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="DatePublished",
)

# Request source for real-time query embedding
text_embedding_request = RequestSource(
    name="text_embedding_request",
    schema=[
        Field(name="text_input", dtype=String),
        Field(name="document_id", dtype=String),
    ],
)

# Document embeddings feature view with Ray UDF for distributed embedding generation
# This demonstrates Feast orchestrating Ray for distributed embedding generation
# Note: All fields returned by UDF are persisted (no transient fields)
document_embeddings_view = BatchFeatureView(
    name="document_embeddings",
    entities=[document],
    schema=[
        # Input fields (also persisted for reference)
        Field(name="id", dtype=String),  # Original movie ID
        Field(name="Description", dtype=String),  # Original movie description
        # Generated/output fields
        Field(name="document_id", dtype=String),  # Generated entity key from id
        Field(name="Name", dtype=String),  # Movie title
        Field(name="Genres", dtype=String),  # Movie genres
        Field(name="Director", dtype=String),  # Movie director
        Field(
            name="embedding",
            dtype=Array(Float32),
            vector_index=True,  # Enable Milvus vector indexing
            vector_length=EMBEDDING_DIM,  # 384-dimensional vectors
            vector_search_metric="COSINE",  # Cosine similarity for semantic search
        ),
        Field(name="embedding_model", dtype=String),  # Generated by UDF
    ],
    source=movies_source,  # Ray offline store handles data I/O
    udf=generate_embeddings_udf,  # Ray UDF for distributed embedding generation
    udf_string="generate_embeddings_udf",  # UDF name for tracking
    online=True,
    offline=False,  # BatchFeatureView is for transformation, not offline retrieval
    tags={"team": "ml_platform", "processing": "ray_udf", "data_type": "embeddings"},
)


# Real-time query embedding ODFV with proper inference
@on_demand_feature_view(
    sources=[text_embedding_request],
    schema=[
        Field(name="document_id", dtype=String),
        Field(name="text_input", dtype=String),
        Field(name="query_embedding", dtype=Array(Float32)),
        Field(name="embedding_model", dtype=String),
    ],
    mode="python",
)
def real_time_text_embedding(inputs: Dict[str, Any]) -> Dict[str, Any]:
    """Real-time embedding generation for query text."""
    # Provide sample data for inference
    sample_text = "Sample query for embedding generation"
    sample_doc_id = "sample_query_1"

    # Handle actual inputs
    text_inputs = inputs.get("text_input", [sample_text])
    if not isinstance(text_inputs, list):
        text_inputs = [text_inputs]

    document_ids = inputs.get("document_id", [sample_doc_id])
    if not isinstance(document_ids, list):
        document_ids = [document_ids]

    # Generate embeddings for actual inputs
    embeddings = []
    for text in text_inputs:
        embedding = embed_text(text)
        embeddings.append(embedding)

    return {
        "document_id": document_ids,
        "text_input": text_inputs,
        "query_embedding": embeddings,
        "embedding_model": [EMBED_MODEL_ID] * len(text_inputs),
    }


# Feature Services
unified_embedding_service = FeatureService(
    name="unified_embedding",
    features=[document_embeddings_view],
    tags={
        "use_case": "unified_embedding",
        "compute_engine": "ray",
        "processing": "udf",
    },
)

real_time_embedding_service = FeatureService(
    name="real_time_embedding",
    features=[real_time_text_embedding],
    tags={"use_case": "query_embedding", "compute_engine": "ray", "mode": "real_time"},
)
