# Feast Image Search Template

A complete template for building image similarity search applications using Feast and Milvus. This template demonstrates how to store images, generate embeddings, and perform vector similarity search using Feast's image features.

## 🚀 Quick Start

### 1. Initialize the Template
```bash
feast init -t image_search my_image_search
cd my_image_search
```

### 2. Start Milvus Vector Database
```bash
docker-compose up -d
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Add Your Images
Place images in the `feature_repo/data/sample_images/` directory:
```bash
# Example directory structure
feature_repo/data/sample_images/
├── products/
│   ├── electronics/
│   │   ├── smartphone.jpg
│   │   └── laptop.jpg
│   └── clothing/
│       ├── shirt.jpg
│       └── shoes.jpg
└── nature/
    ├── mountains.jpg
    └── ocean.jpg
```

### 5. Run Image Search
```bash
python image_search_workflow.py
```

## 🎯 What's Included

### Core Components
- **`feature_repo/image_repo.py`** - Feast feature definitions for image embeddings
- **`feature_repo/feature_store.yaml`** - Feast configuration with Milvus online store
- **`image_search_workflow.py`** - Main workflow script with interactive search and image display
- **`docker-compose.yml`** - Milvus vector database setup

### Key Features
- 🖼️ **Image Loading** - Automatically processes images from directories and subdirectories
- 🧠 **Embeddings** - Uses ResNet34 to generate image embeddings
- ⚡ **Vector Search** - Milvus-powered similarity search with cosine distance
- 🖥️ **Terminal Display** - View images and search results directly in terminal using matplotlib
- 🎮 **Interactive Search** - Simple prompt-based image search interface

## 📖 Usage

### Interactive Search
```bash
python image_search_workflow.py
```

The workflow automatically:
1. **Loads Images** - Recursively scans `feature_repo/data/sample_images/` directory
2. **Generates Embeddings** - Uses ResNet34 to create 512-dimensional vectors
3. **Sets up Feast** - Runs `feast apply` and materializes features to Milvus
4. **Interactive Search** - Prompts for image names and displays visual results

**Example interaction:**
```
🎯 Feast Image Similarity Search Template
============================================================
📥 Loading images from feature_repo/data/sample_images directory...
  ✓ Loaded sunset.jpg
  ✓ Loaded camera.jpg
  ✓ Loaded city.jpg
🔄 Creating image dataset with embeddings...
  ✓ Processed sunset.jpg
  ✓ Processed camera.jpg
🏪 Setting up Feast feature store...
  🔧 Running feast apply...
  ✓ Applied feature definitions
  📊 Materializing features to online store...
  ✓ Features materialized

============================================================
🔍 INTERACTIVE IMAGE SEARCH
============================================================
💡 Search for similar images by entering an image name!

🔍 Enter the name of an image to search for (or 'exit' to quit):
Query image: sunset.jpg

🔍 Finding images similar to 'sunset.jpg'...
📊 Top 3 similar images:
  1. sunset.jpg (category: general, distance: 1.0)
  2. sunset_hill.jpg (category: general, distance: 0.84)
  3. beach_sunset.jpg (category: general, distance: 0.76)

[Matplotlib window opens showing query image and similar results]

🔄 Would you like to search with another image? (y/n)
```

## 🏗️ Architecture

### Data Flow
1. **Image Loading** - Recursively scan `feature_repo/data/sample_images/` directory
2. **Embedding Generation** - Extract features using ResNet34 neural network
3. **Feature Store** - Store embeddings and metadata in Feast
4. **Vector Search** - Query Milvus for similar images using cosine similarity
5. **Results Display** - Show matching images with similarity scores

### Components
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Image Files   │───▶│  Feast Features │───▶│ Milvus Vector DB│
│ (feature_repo/  │    │   (embeddings)  │    │  (similarity)   │
│ data/sample_    │    │                 │    │                 │
│ images/)        │    │                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   ResNet34      │    │ Parquet Storage │    │ Matplotlib      │
│  (embeddings)   │    │   (offline)     │    │ Image Display   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```
