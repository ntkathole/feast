"""RAG query script for the Spark RAG + Feast pipeline.

Retrieves embeddings from Feast online store, performs similarity search,
and returns relevant document chunks.
"""

import argparse

import numpy as np


def cosine_similarity(a, b):
    """Compute cosine similarity between two vectors."""
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))


def embed_query(text: str, model_name: str = "all-MiniLM-L6-v2"):
    """Embed a query string using the same model as document embeddings."""
    from sentence_transformers import SentenceTransformer

    model = SentenceTransformer(model_name)
    return model.encode(text)


def main():
    parser = argparse.ArgumentParser(description="RAG query against Feast")
    parser.add_argument("--query", required=True, help="Query text")
    parser.add_argument("--top-k", type=int, default=5, help="Number of results")
    parser.add_argument("--repo-path", default=".", help="Feast repo path")
    args = parser.parse_args()

    from feast import FeatureStore

    store = FeatureStore(repo_path=args.repo_path)

    # Get all document IDs (in practice, use an index or filter)
    # For this example, we retrieve features for a sample set
    import pandas as pd

    query_embedding = embed_query(args.query)

    # Retrieve document embeddings from Feast online store
    entity_rows = [{"document_id": str(i)} for i in range(100)]
    features = store.get_online_features(
        features=["document_embeddings:embedding", "document_embeddings:chunk_text"],
        entity_rows=entity_rows,
    ).to_dict()

    # Compute similarity scores
    results = []
    for i, emb in enumerate(features.get("embedding", [])):
        if emb is not None:
            score = cosine_similarity(query_embedding, np.array(emb))
            results.append(
                {
                    "document_id": features["document_id"][i],
                    "chunk_text": features.get("chunk_text", [None])[i],
                    "score": float(score),
                }
            )

    # Sort by similarity and return top-k
    results.sort(key=lambda x: x["score"], reverse=True)
    top_results = results[: args.top_k]

    print(f"\nQuery: {args.query}")
    print(f"Top {args.top_k} results:\n")
    for i, r in enumerate(top_results, 1):
        print(f"{i}. [Score: {r['score']:.4f}] {r['chunk_text'][:200]}...")
        print()


if __name__ == "__main__":
    main()
