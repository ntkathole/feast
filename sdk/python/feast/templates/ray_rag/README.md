# Feast Ray RAG Template - Unified Architecture

This template demonstrates **Feast's unified architecture** for **Retrieval-Augmented Generation (RAG)** applications using **real IMDB movie data**, featuring **Ray UDF-powered distributed embedding generation** orchestrated entirely by Feast.

## 🚀 Unified Architecture

**Feast orchestrates Ray UDF for distributed embedding generation** - no separate Ray jobs or manual parquet handling required. This creates a seamless pipeline from raw data to vector embeddings with automatic distributed processing.

## 🚀 What This Template Provides

- **🎬 Real Movie Data**: IMDB movie dataset for realistic RAG demonstrations
- **🏗️ Unified Architecture**: Feast orchestrates Ray UDF for distributed processing
- **📝 RequestSource**: Dynamic text input for real-time embedding generation
- **🔥 Ray UDF Processing**: 60x faster distributed embedding generation
- **🧠 Automatic Transformations**: Ray UDF applied during materialization
- **🔍 Vector Search**: Milvus integration for movie similarity search
- **🤖 RAG Ready**: FeastRAGRetriever integration for movie recommendation RAG
- **⚡ Distributed**: Scale across Ray clusters automatically
- **📊 Unified Pipeline**: Complete offline→online materialization with Ray

## 📁 Template Structure

```
ray_rag-template/
├── feature_repo/
│   ├── feature_store.yaml      # Ray + Milvus configuration
│   ├── example_repo.py         # On-demand feature views + RAG entities
│   ├── test_workflow.py        # Complete RAG workflow demo
│   └── data/                   # Real IMDB data + embeddings
│       ├── final_data.csv              # Real IMDB movies from Kaggle (direct CSV usage)
│       └── document_embeddings.parquet # Pre-computed embeddings for demo
├── bootstrap.py                # Downloads real IMDB data from Kaggle
└── README.md                   # This file
```

## 🎯 Key Features

### Unified Feast-Ray Architecture
- **Raw Data Source**: Bootstrap downloads IMDB data and converts to Parquet
- **FeatureView with Ray UDF**: Defines transformation from raw movies to embeddings
- **Ray UDF**: `RayEmbeddingUDF()` processes movies during materialization
- **Automatic Processing**: `feast materialize-incremental` triggers Ray UDF automatically
- **No Manual Steps**: Feast orchestrates Ray UDF for distributed embedding generation

### Ray UDF Distributed Processing
- **Automatic Distribution**: Ray UDF automatically distributes across workers
- **Batch Processing**: `map_batches` with Ray UDF for efficient processing
- **60x Performance**: Massive speedup over Pandas processing
- **Fault Tolerance**: Built-in retry logic and error handling

### Vector Database Integration  
- **Milvus Online Store**: Optimized for vector similarity search
- **Vector Index**: Automatic indexing with COSINE similarity
- **Bulk Operations**: Efficient batch ingestion

### RAG Applications
- **Document Embeddings**: Ready-to-use feature views
- **Similarity Search**: `retrieve_online_documents_v2` support
- **FeastRAGRetriever**: Integration with HuggingFace transformers
- **Multi-Modal**: BGE-M3 support for dense/sparse/multi-vector embeddings

## 🚦 Quick Start

### 1. Initialize Template
```bash
feast init -t ray_rag my_rag_project
cd my_rag_project/feature_repo
```

### 2. Install Dependencies
```bash
# Core dependencies
pip install feast[ray]

# For embeddings
pip install sentence-transformers

# For vector database
pip install pymilvus

# Optional: for RAG retriever
pip install transformers torch

# Optional: for Kaggle dataset download
pip install kaggle
```

### 3. (Optional) Setup Kaggle API for Real Dataset
The template automatically downloads the IMDB movies dataset from Kaggle (public dataset, no authentication required):

```bash
# Kaggle API will be used automatically if available
pip install kaggle
```

For private datasets or better rate limits, you can optionally set up API credentials:
```bash
# 1. Get API credentials from https://www.kaggle.com/account
# 2. Download kaggle.json and place it in ~/.kaggle/
mkdir -p ~/.kaggle
cp kaggle.json ~/.kaggle/
chmod 600 ~/.kaggle/kaggle.json
```

**Note**: The template works with a comprehensive fallback dataset if Kaggle download fails.

### 4. Start Milvus (Optional)
```bash
# Using Docker (recommended for production)
docker run -p 19530:19530 milvusdb/milvus:latest

# Or use Milvus Lite (embedded, no setup required)
```

### 5. Apply Feature Definitions
```bash
feast apply
```

### 6. Materialize Features (Generate Embeddings)
```bash
# Option 1: Materialize full dataset (48K+ movies, RECOMMENDED for best search results)
feast materialize 1900-01-01T00:00:00 2026-12-31T23:59:59

# Option 2: Materialize recent movies only (~900 movies, faster but limited results)
feast materialize 2020-01-01T00:00:00 2025-12-31T23:59:59

# Option 3: Incremental materialization (materializes new data since last run)
feast materialize-incremental $(date +%Y-%m-%d)
```

**💡 Recommendation**: Use Option 1 for the full 48,513 IMDB movies to get the best semantic search results!

### 7. Run the Demo
```bash
# Run the unified Feast-Ray RAG demo
python test_workflow.py

# This demonstrates:
# - Ray offline store for distributed I/O
# - Ray compute engine for distributed processing  
# - Unified materialization pipeline
# - Real-time embedding generation
# - Vector similarity search
```

## 🎬 Working Demo Results

The `working_demo.py` script shows actual working results:

```bash
python working_demo.py
```

**What you'll see:**
- ✅ **Real Movie Data**: Actual IMDB movies with titles, categories, directors
- ✅ **Vector Similarity**: Working similarity search with meaningful results
- ✅ **Query Results**: Actual matches for queries like "early documentary about motion"
- ✅ **Ray Processing**: Movies processed with 384-dim embeddings via BatchFeatureView
- ✅ **Entity Keys**: Correct format (movie_0, movie_1, etc.)
- ✅ **Schema Fixed**: BatchFeatureView schema matches UDF output perfectly

**Sample Output:**
```
Query: 'early documentary about motion and movement'
📊 Top 3 most similar movies:
   1. Man Walking Around the Corner (Documentary) - Score: 0.441
   2. Newark Athlete (Documentary) - Score: 0.409  
   3. Buffalo Running (Documentary) - Score: 0.375
```

## 🏆 Unified Architecture Benefits

### Key Advantages
- **✅ Feast Orchestration**: Feast automatically manages Ray UDF execution
- **✅ No Manual Steps**: No separate Ray jobs or parquet file handling
- **✅ Unified Pipeline**: Seamless offline → online store flow
- **✅ Automatic Scaling**: Ray UDF distributes across cluster automatically
- **✅ Built-in Fault Tolerance**: Retry logic and error handling included
- **✅ Consistent Lineage**: Full feature store lineage tracking

### Performance Benefits

Based on the [Zilliz article](https://zilliz.com/blog/embedding-inference-at-scale-for-RAG-app-with-ray-data-and-milvus):

| Method | Dataset Size | Processing Time | Speedup |
|--------|-------------|-----------------|---------|
| Pandas | 45K rows    | >4 hours       | 1x      |
| **Ray UDF** | **45K rows** | **4 minutes** | **60x** |

## 🔧 Configuration

### Ray Compute Engine
```yaml
batch_engine:
  type: ray.engine
  max_workers: 8
  embedding_config:
    default_model: "BAAI/bge-m3"    # BGE-M3 for multi-modal
    chunk_size: 512                 # Text chunk size
    batch_size: 32                  # Embedding batch size
    device: "cuda"                  # GPU acceleration
```

### Milvus Vector Database
```yaml
online_store:
  type: milvus
  host: localhost
  port: 19530
  index_type: "IVF_FLAT"
  metric_type: "COSINE"
```

## 🤖 RAG Integration

### Vector Similarity Search
```python
# Query similar documents
results = store.retrieve_online_documents_v2(
    feature_view="document_embeddings",
    embedding=query_embedding,
    top_k=10
)
```

### FeastRAGRetriever
```python
from feast.rag_retriever import FeastRAGRetriever

retriever = FeastRAGRetriever(
    feast_repo_path=".",
    feature_view=document_embeddings_view,
    features=["title", "content", "embedding"],
    search_type="vector"
)
```

### Complete RAG Pipeline
```python
# 1. Generate embeddings with Ray Data (60x faster)
dataset = ray.data.from_pandas(documents_df)
chunked = dataset.flat_map(chunk_documents)
embedded = chunked.map_batches(ComputeEmbeddings(), batch_size=32)

# 2. Materialize to Milvus via Feast
store.materialize_incremental(end_date)

# 3. Query for RAG
results = store.retrieve_online_documents_v2(
    feature_view="document_embeddings", 
    embedding=query_embedding,
    top_k=5
)

# 4. Use with LLM for generation
context = extract_context_from_results(results)
response = llm.generate(query + context)
```

## 🚀 Scaling Up

### Distributed Ray Cluster
```yaml
# Connect to Ray cluster
offline_store:
  ray_address: "ray://head-node:10001"
batch_engine:
  ray_address: "ray://head-node:10001"
  max_workers: 100
```

### Production Setup
```yaml
# Production Milvus
online_store:
  type: milvus
  host: milvus-cluster.example.com
  port: 19530
  database: production_embeddings

# BGE-M3 for multi-modal embeddings  
batch_engine:
  embedding_config:
    default_model: "BAAI/bge-m3"
    embedding_types: ["dense", "sparse", "multi_vector"]
    device: "cuda"
```

## 📈 Use Cases

- **📚 Knowledge Base Search**: Semantic search over documentation
- **💬 Chatbot RAG**: Context retrieval for conversational AI
- **🔍 Document Discovery**: Find similar documents in large collections
- **📊 Content Recommendation**: Recommend related articles/papers
- **🏢 Enterprise Search**: Search across internal documents and wikis

## 🛠️ Advanced Features

### Multi-Modal Embeddings (BGE-M3)
```python
# Generate dense, sparse, and multi-vector embeddings
embedding_config:
  default_model: "BAAI/bge-m3"
  embedding_types: ["dense", "sparse", "multi_vector"]
```

### Custom Embedding Models
```python
# Use your own embedding model
class CustomEmbeddingModel:
    def __call__(self, batch):
        # Your custom embedding logic
        return batch_with_embeddings

embedded = dataset.map_batches(CustomEmbeddingModel())
```

### Hybrid Search
```python
# Combine vector and text search
results = store.retrieve_online_documents_v2(
    feature_view="document_embeddings",
    embedding=query_embedding,
    query_string="machine learning",  # Hybrid search
    top_k=10
)
```

## 🔍 Monitoring & Debugging

- **Ray Dashboard**: Monitor distributed processing at `http://localhost:8265`
- **Milvus Attu**: Vector database UI for debugging
- **Feast UI**: Feature store monitoring (experimental)

## 📚 Resources

- **Ray Data**: [Documentation](https://docs.ray.io/en/latest/data/)
- **Milvus**: [Vector Database Docs](https://milvus.io/docs)
- **BGE-M3**: [HuggingFace Model](https://huggingface.co/BAAI/bge-m3)
- **Feast**: [Feature Store Docs](https://docs.feast.dev/)
- **Zilliz Article**: [Embedding Inference at Scale](https://zilliz.com/blog/embedding-inference-at-scale-for-RAG-app-with-ray-data-and-milvus)

## 🤝 Contributing

This template demonstrates the integration of Ray Data's 60x performance improvements with Feast's feature store capabilities for production RAG applications. Contributions and improvements are welcome!
