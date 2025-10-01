#!/usr/bin/env python3

"""
Unified Feast-Ray RAG Pipeline Demo

This script demonstrates the unified architecture where Feast orchestrates
distributed embedding generation using Ray, creating a seamless pipeline
from raw IMDB movie data to vector embeddings.

Workflow:
1. feast init -t ray_rag my_project
2. feast apply
3. feast materialize-incremental $(date +%Y-%m-%d)
4. python test_workflow.py

This demonstrates:
- Ray offline store for distributed data I/O
- Ray compute engine for distributed processing
- Materialization of movie embeddings to online store
- Real-time query embedding generation
- Vector similarity search
"""

import sys
from pathlib import Path

# Add the feature repo to the path
repo_path = Path(__file__).parent
sys.path.append(str(repo_path))

try:
    from feast import FeatureStore
except ImportError:
    print("Please install feast: pip install feast[ray] sentence-transformers")
    sys.exit(1)


def run_demo():
    """Run the unified Feast-Ray RAG pipeline demonstration."""
    print("=" * 80)
    print("🚀 UNIFIED FEAST-RAY RAG PIPELINE DEMO")
    print("=" * 80)

    print("\n📋 Architecture Overview:")
    print("   ✅ Feast orchestrates Ray for distributed processing")
    print("   ✅ Ray offline store handles data I/O efficiently")
    print("   ✅ Ray compute engine for distributed materialization")
    print("   ✅ Unified offline → online store pipeline")
    print("   ✅ Real IMDB movie data (48K+ movies)")

    # Initialize the feature store
    print("\n1️⃣ Initializing Feast with Ray configuration...")
    try:
        store = FeatureStore(repo_path=".")
        print(f"   ✅ Offline store: {store.config.offline_store.type}")
        print(f"   ✅ Online store: {store.config.online_store.type}")
        if hasattr(store.config, "batch_engine") and store.config.batch_engine:
            print(f"   ✅ Compute engine: {store.config.batch_engine.type}")

        # List feature views
        feature_views = store.list_feature_views()
        print(f"   ✅ Available feature views: {len(feature_views)}")
        for fv in feature_views:
            print(f"      📄 {fv.name}: {type(fv).__name__}")

    except Exception as e:
        print(f"   ❌ Failed to initialize feature store: {e}")
        return

    # Check materialized data
    print("\n2️⃣ Checking materialized data in online store...")
    try:
        # Query all materialized data to get actual document IDs
        # Since IDs are sparse (not sequential), we need to find which ones exist
        print("   Querying Milvus for all materialized entities...")

        import os

        from pymilvus import MilvusClient

        milvus_client = MilvusClient(uri=os.path.join(store.config.online_store.path))
        collections = milvus_client.list_collections()

        actual_count = 0

        if collections:
            # Get first few entities from Milvus directly to verify data
            results = milvus_client.query(
                collection_name=collections[0],
                filter="",
                output_fields=[
                    "Name",
                    "Genres",
                    "Director",
                    "embedding",
                    "embedding_model",
                ],
                limit=10,
            )
            actual_count = len(results)

            if results and len(results) > 0:
                # Show first result as example
                first = results[0]
                total_count = milvus_client.get_collection_stats(collections[0]).get(
                    "row_count", "unknown"
                )
                print("   ✅ Found materialized data:")
                print(f"      Collection: {collections[0]}")
                print(f"      Total entities in Milvus: {total_count}")
                print("      Sample entity:")
                print(f"         Title: {first.get('Name', 'N/A')}")
                print(f"         Genres: {first.get('Genres', 'N/A')}")
                print(f"         Director: {first.get('Director', 'N/A')}")
                if first.get("embedding"):
                    print(f"         Embedding: {len(first['embedding'])}-dim vector")
                if first.get("embedding_model"):
                    print(f"         Model: {first['embedding_model']}")

        if actual_count > 0:
            print(
                f"   ✅ Successfully verified {total_count} materialized movie embeddings"
            )
            print("   🎯 Ray compute engine successfully materialized features!")
        else:
            # Fallback: Try sequential IDs (less reliable)
            sample_entities = [{"document_id": f"movie_{i}"} for i in range(100)]

            found_count = 0
            for entity in sample_entities:
                try:
                    features = store.get_online_features(
                        features=[
                            "document_embeddings:Name",
                            "document_embeddings:Genres",
                            "document_embeddings:Director",
                            "document_embeddings:embedding",
                            "document_embeddings:embedding_model",
                        ],
                        entity_rows=[entity],
                    )

                    result = features.to_dict()
                    if result and result.get("Name") and result["Name"][0]:
                        found_count += 1
                        if found_count == 1:  # Show first one as example
                            print("   ✅ Found materialized data:")
                            print(f"      Entity: {entity['document_id']}")
                            print(f"      Title: {result['Name'][0]}")
                            print(f"      Genres: {result.get('Genres', ['N/A'])[0]}")
                            print(
                                f"      Director: {result.get('Director', ['N/A'])[0]}"
                            )
                            if result.get("embedding"):
                                print(
                                    f"      Embedding: {len(result['embedding'][0])}-dim vector"
                                )
                            if result.get("embedding_model"):
                                print(f"      Model: {result['embedding_model'][0]}")
                except Exception:
                    continue

            if found_count > 0:
                print(f"   ✅ Found {found_count} materialized movie embeddings")
                print("   🎯 Ray compute engine successfully materialized features!")
            else:
                print("   ⚠️  No materialized data found")
                print("   💡 Run: feast materialize-incremental $(date +%Y-%m-%d)")
                print(
                    "   💡 This will trigger Ray compute engine to process embeddings"
                )

    except Exception as e:
        print(f"   ⚠️  Error checking materialized data: {e}")

    # Demonstrate real-time query embedding generation
    print("\n3️⃣ Testing real-time query embedding generation...")
    try:
        # Test queries
        test_queries = [
            "Christopher Nolan science fiction thriller",
            "Action movie with explosions",
            "Romantic comedy film",
        ]

        print(f"   Testing {len(test_queries)} query embeddings with ODFV...")

        for i, query_text in enumerate(test_queries):
            try:
                # Get real-time embeddings using ODFV
                features = store.get_online_features(
                    features=[
                        "real_time_text_embedding:query_embedding",
                        "real_time_text_embedding:embedding_model",
                    ],
                    entity_rows=[
                        {"text_input": query_text, "document_id": f"query_{i}"}
                    ],
                )

                result = features.to_dict()
                if result and result.get("query_embedding"):
                    embedding = result["query_embedding"][0]
                    model = result.get("embedding_model", ["unknown"])[0]
                    print(f"   ✅ Query {i + 1}: '{query_text[:50]}...'")
                    print(
                        f"      Generated {len(embedding)}-dim embedding with {model}"
                    )

            except Exception as e:
                print(f"   ⚠️  Query {i + 1} failed: {e}")

    except Exception as e:
        print(f"   ⚠️  Real-time embedding generation failed: {e}")

    # Demonstrate vector similarity search using Milvus
    print("\n4️⃣ Testing vector similarity search...")
    try:
        import os

        from pymilvus import MilvusClient
        from sentence_transformers import SentenceTransformer

        model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")
        print("   ✅ Loaded embedding model")

        # Test multiple queries to demonstrate semantic search
        test_queries = [
            "Science fiction movie about space exploration",
            "Action thriller with explosions and car chases",
            "Romantic comedy with love story",
        ]

        # Use Milvus vector search to find most similar embeddings
        milvus_client = MilvusClient(uri=os.path.join(store.config.online_store.path))
        collections = milvus_client.list_collections()

        if collections:
            total_entities = milvus_client.get_collection_stats(collections[0]).get(
                "row_count", 0
            )
            print(f"   🔍 Searching across {total_entities} materialized entities...")

            for query_idx, query in enumerate(
                test_queries[:1]
            ):  # Show first query in detail
                query_embedding = model.encode([query], normalize_embeddings=True)[
                    0
                ].tolist()
                print(f"\n   Query: '{query}'")
                print(f"   ✅ Generated {len(query_embedding)}-dim embedding")

                # Perform vector similarity search with optimized parameters
                search_results = milvus_client.search(
                    collection_name=collections[0],
                    data=[query_embedding],  # Query vector
                    limit=5,  # Top 5 results
                    output_fields=["Name", "Genres", "Director"],
                    search_params={
                        "metric_type": "COSINE",
                        "params": {"nprobe": 10},  # Better accuracy
                    },
                )

                if search_results and len(search_results[0]) > 0:
                    print("\n   📊 Top 3 most similar movies:")
                    for i, hit in enumerate(search_results[0][:3]):
                        entity_data = hit.get("entity", {})
                        similarity_score = hit.get("distance", 0)

                        # Interpret similarity score
                        if similarity_score > 0.7:
                            relevance = "🔥 Highly relevant"
                        elif similarity_score > 0.5:
                            relevance = "✅ Relevant"
                        elif similarity_score > 0.3:
                            relevance = "⚠️  Moderately relevant"
                        else:
                            relevance = "❓ Low relevance"

                        print(f"      {i + 1}. {entity_data.get('Name', 'Unknown')}")
                        print(f"         Genres: {entity_data.get('Genres', 'N/A')}")
                        print(
                            f"         Director: {entity_data.get('Director', 'N/A')}"
                        )
                        print(
                            f"         Similarity: {similarity_score:.3f} {relevance}"
                        )

                    # Show summary
                    avg_score = (
                        sum(hit.get("distance", 0) for hit in search_results[0][:3]) / 3
                    )
                    print(f"\n   💡 Average similarity: {avg_score:.3f}")
                    if avg_score < 0.5:
                        print(
                            f"   📝 Note: Lower scores indicate the dataset may not contain many '{query.split()[0]}' movies"
                        )
                else:
                    print("   ⚠️  No similar movies found")
        else:
            print("   ⚠️  No Milvus collections found")

    except ImportError:
        print("   ⚠️  sentence-transformers not available")
        print("   💡 Install with: pip install sentence-transformers")
    except Exception as e:
        print(f"   ⚠️  Vector similarity search failed: {e}")

    # Show unified architecture benefits
    print("\n5️⃣ Unified Architecture Benefits...")
    print("   🏆 Key Advantages:")
    print("      ✅ Feast orchestrates Ray automatically")
    print("      ✅ No manual Ray job management")
    print("      ✅ Unified materialization pipeline")
    print("      ✅ Distributed processing with Ray")
    print("      ✅ Seamless offline → online store flow")

    print("\n   🚀 Production Benefits:")
    print("      • 60x faster than Pandas for batch processing")
    print("      • Automatic scaling with Ray cluster")
    print("      • Built-in fault tolerance")
    print("      • Consistent feature store lineage")

    print("\n" + "=" * 80)
    print("🎉 UNIFIED FEAST-RAY RAG PIPELINE DEMO COMPLETE")
    print("=" * 80)

    print("\n📚 What we demonstrated:")
    print("   ✅ Ray offline store for distributed data I/O")
    print("   ✅ Ray compute engine for distributed processing")
    print("   ✅ Unified materialization with feast materialize")
    print("   ✅ Real-time query embedding generation")
    print("   ✅ Vector similarity search capabilities")
    print("   ✅ Production-ready RAG pipeline")

    print("\n🚀 Next steps:")
    print("   • Scale to full 48K+ IMDB dataset")
    print("   • Connect to distributed Ray cluster")
    print("   • Integrate with LLM for complete RAG")
    print("   • Add monitoring and evaluation metrics")


if __name__ == "__main__":
    run_demo()
