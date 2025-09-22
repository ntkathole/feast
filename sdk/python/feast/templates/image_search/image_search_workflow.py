"""
Feast Image Search Template

This script demonstrates image similarity search using Feast with Milvus.
Features:
- Recursive image loading from directories
- Image embedding generation using ResNet
- Vector similarity search with Milvus
- Terminal-based image display
- Interactive query mode

Usage:
  python image_search_workflow.py
"""

import io
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
from PIL import Image

from feast import FeatureStore
from feast.image_utils import ImageFeatureExtractor

try:
    import matplotlib.image as mpimg
    import matplotlib.pyplot as plt
    from rich.console import Console

    IMAGE_DISPLAY_AVAILABLE = True
    console: Optional[Console] = Console()
except ImportError as e:
    print(f"⚠️  Image display utilities not available: {e}")
    IMAGE_DISPLAY_AVAILABLE = False
    console = None


def display_search_results_with_images(
    results_df,
    query_image_path=None,
    image_column="image_data",
    filename_column="filename",
    max_results=3,
):
    """Display search results with actual images in a grid."""
    try:
        num_results = min(len(results_df), max_results)
        if query_image_path:
            total_images = num_results + 1
            cols = min(4, total_images)
            rows = (total_images + cols - 1) // cols
        else:
            total_images = num_results
            cols = min(3, total_images)
            rows = (total_images + cols - 1) // cols

        # Create figure
        fig, axes = plt.subplots(rows, cols, figsize=(4 * cols, 4 * rows))
        if total_images == 1:
            axes = [axes]
        elif rows == 1:
            axes = axes if hasattr(axes, "__len__") else [axes]
        else:
            axes = axes.flatten()

        current_idx = 0

        if query_image_path and os.path.exists(query_image_path):
            query_image = mpimg.imread(query_image_path)
            axes[current_idx].imshow(query_image)
            axes[current_idx].set_title(
                "🔍 Query Image", fontsize=12, fontweight="bold", color="blue"
            )
            axes[current_idx].axis("off")
            current_idx += 1

        # Display search results
        for i, (_, row) in enumerate(results_df.head(num_results).iterrows()):
            if current_idx >= len(axes):
                break

            try:
                if image_column in row and row[image_column] is not None:
                    image_path = row[image_column]
                    try:
                        if os.path.exists(image_path):
                            image = Image.open(image_path)
                            axes[current_idx].imshow(image)
                        else:
                            axes[current_idx].text(
                                0.5,
                                0.5,
                                f"File Not Found\n{os.path.basename(image_path)}",
                                ha="center",
                                va="center",
                                fontsize=10,
                                color="red",
                            )
                            axes[current_idx].set_title(
                                f"#{i + 1}: File Not Found", fontsize=10, color="red"
                            )
                            axes[current_idx].axis("off")
                            current_idx += 1
                            continue
                    except Exception as e:
                        print(f"Debug: Error loading image from {image_path}: {e}")
                        axes[current_idx].text(
                            0.5,
                            0.5,
                            f"Load Error\n{str(e)[:30]}...",
                            ha="center",
                            va="center",
                            fontsize=10,
                            color="red",
                        )
                        axes[current_idx].set_title(
                            f"#{i + 1}: Load Error", fontsize=10, color="red"
                        )
                        axes[current_idx].axis("off")
                        current_idx += 1
                        continue

                    filename = row.get(filename_column, f"Result {i + 1}")
                    distance = row.get("distance", "N/A")
                    if distance != "N/A":
                        title = f"#{i + 1}: {filename}\nDistance: {distance:.4f}"
                    else:
                        title = f"#{i + 1}: {filename}"

                    axes[current_idx].set_title(title, fontsize=10, fontweight="bold")
                    axes[current_idx].axis("off")

                else:
                    axes[current_idx].text(
                        0.5,
                        0.5,
                        f"No Image\n{row.get(filename_column, 'Unknown')}",
                        ha="center",
                        va="center",
                        fontsize=12,
                    )
                    axes[current_idx].set_title(f"#{i + 1}: No Image", fontsize=10)
                    axes[current_idx].axis("off")

                current_idx += 1

            except Exception as e:
                print(f"⚠️  Failed to display result {i + 1}: {e}")
                axes[current_idx].text(
                    0.5,
                    0.5,
                    f"Error\n{str(e)[:50]}...",
                    ha="center",
                    va="center",
                    fontsize=10,
                    color="red",
                )
                axes[current_idx].set_title(
                    f"#{i + 1}: Error", fontsize=10, color="red"
                )
                axes[current_idx].axis("off")
                current_idx += 1

        for idx in range(current_idx, len(axes)):
            axes[idx].axis("off")

        plt.tight_layout()
        plt.suptitle("Image Search Results", fontsize=16, fontweight="bold", y=0.98)
        plt.show()
    except Exception as e:
        print(f"Failed to display search results: {e}")


def load_all_images_from_directory(
    directory: str = "feature_repo/data/sample_images",
) -> List[Tuple[str, bytes]]:
    """Load all images from the specified directory, including subdirectories."""
    print(f"Loading images from {directory} directory...")

    sample_dir = Path(directory)
    if not sample_dir.exists():
        print(
            f"Directory {directory} not found. Please add sample images to this directory."
        )
        return []

    image_extensions = {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".tiff", ".webp"}

    images = []

    for filepath in sorted(sample_dir.rglob("*")):
        if filepath.is_file() and filepath.suffix.lower() in image_extensions:
            try:
                relative_path = filepath.relative_to(sample_dir)

                with open(filepath, "rb") as f:
                    image_data = f.read()

                Image.open(filepath).verify()

                images.append((str(relative_path), image_data))
                print(f"Loaded {relative_path}")

            except Exception as e:
                print(f"Failed to load {filepath}: {e}")

    print(f"Successfully loaded {len(images)} images from directory")
    return images


def create_image_dataset(images: List[Tuple[str, bytes]]) -> pd.DataFrame:
    """Create a dataset with images and their embeddings."""
    print("🔄 Creating image dataset with embeddings...")

    # Initialize image feature extractor
    extractor = ImageFeatureExtractor(model_name="resnet34")

    data = []
    for i, (filename, image_data) in enumerate(images):
        try:
            # Generate embedding
            embedding = extractor.extract_embedding(image_data)

            # Get image metadata
            img = Image.open(io.BytesIO(image_data))
            width, height = img.size
            img_format = img.format or "JPEG"

            category = (
                str(Path(filename).parent)
                if "/" in filename or "\\" in filename
                else "general"
            )

            if hasattr(embedding, "tolist"):
                embedding_list = embedding.tolist()
            else:
                embedding_list = embedding

            data.append(
                {
                    "image_id": i + 1,
                    "image_path": f"feature_repo/data/sample_images/{filename}",
                    "embedding": embedding_list,
                    "filename": filename,
                    "width": width,
                    "height": height,
                    "format": img_format,
                    "category": category,
                    "event_timestamp": datetime.now() - timedelta(hours=1),
                    "created_timestamp": datetime.now() - timedelta(hours=1),
                }
            )
            print(f"Processed {filename}")

        except Exception as e:
            print(f"Failed to process {filename}: {e}")

    df = pd.DataFrame(data)
    parquet_path = "data/images_with_embeddings.parquet"
    df.to_parquet(parquet_path, index=False)
    print(f"💾 Saved dataset to {parquet_path}")

    return df


def setup_feature_store():
    """Setup and configure the Feast feature store."""
    print("🏪 Setting up Feast feature store...")

    os.chdir("feature_repo")

    print("  🔧 Running feast apply...")
    import subprocess

    result = subprocess.run(["feast", "apply"], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"feast apply failed: {result.stderr}")
        raise RuntimeError(f"feast apply failed: {result.stderr}")
    print("Applied feature definitions")

    store = FeatureStore(".")

    print("  📊 Materializing features to online store...")
    store.materialize_incremental(end_date=datetime.now())
    print("  ✓ Features materialized")

    os.chdir("..")
    return FeatureStore("feature_repo")


def demonstrate_image_search(store: FeatureStore, query_image_path: str):
    """Demonstrate image similarity search."""
    print(f"\n🔍 Performing image similarity search with {query_image_path}")

    try:
        # Load query image
        with open(query_image_path, "rb") as f:
            query_image_bytes = f.read()

        # Perform image search
        results = store.retrieve_online_documents_v2(
            features=[
                "image_embeddings:embedding",
                "image_embeddings:image_path",
                "image_embeddings:filename",
                "image_embeddings:category",
                "image_embeddings:width",
                "image_embeddings:height",
            ],
            query_image_bytes=query_image_bytes,
            top_k=3,
            distance_metric="COSINE",
        )

        # Display results
        results_df = results.to_df()
        print("\n📊 Top 3 similar images:")

        for i, row in results_df.iterrows():
            filename = row.get("filename", "Unknown")
            category = row.get("category", "Unknown")
            distance = row.get("distance", "N/A")
            print(f"  {i + 1}. {filename} (category: {category}, distance: {distance})")

        # Display actual images if available
        if IMAGE_DISPLAY_AVAILABLE and len(results_df) > 0:
            print("\n🖼️  Displaying search results with images...")
            display_search_results_with_images(
                results_df, query_image_path, image_column="image_path"
            )

        return results_df

    except Exception as e:
        print(f"  ✗ Image search failed: {e}")
        return None


def get_user_query_image(available_images: pd.DataFrame) -> Optional[str]:
    """Get query image from user input with validation."""
    while True:
        print("\n🔍 Enter the name of an image to search for (or 'exit' to quit):")
        query_input = input("Query image: ").strip()

        if query_input.lower() in ["exit", "quit", "q"]:
            return None

        if not query_input:
            print("Please enter an image name")
            continue

        sample_dir = Path("feature_repo/data/sample_images")

        query_path = sample_dir / query_input
        if query_path.exists():
            return str(query_path)

        for filepath in sample_dir.rglob("*"):
            if filepath.name.lower() == query_input.lower():
                return str(filepath)

        print(f"Image '{query_input}' not found")
        filenames = [
            row.get("filename", "Unknown") for _, row in available_images.iterrows()
        ]
        print(
            f"💡 Available images: {', '.join(filenames[:10])}{'...' if len(filenames) > 10 else ''}"
        )


def main():
    """Main function to run the interactive image search workflow."""
    try:
        print("🎯 Feast Image Similarity Search Template")
        print("=" * 60)

        # Step 1: Load images from directory (supports recursive loading)
        images = load_all_images_from_directory()

        # Step 2: Create image dataset with embeddings
        create_image_dataset(images)

        # Step 3: Setup Feast feature store
        store = setup_feature_store()

        # Step 4: Interactive search mode
        print("\n" + "=" * 60)
        print("🔍 INTERACTIVE IMAGE SEARCH")
        print("=" * 60)
        print("💡 Search for similar images by entering an image name!")

        # Create a simple available images list for user reference
        available_images = pd.DataFrame(
            [{"filename": filename} for filename, _ in images]
        )

        while True:
            query_filename = get_user_query_image(available_images)
            if query_filename is None:
                print("👋 Goodbye!")
                break

            # Try to construct the query path (support recursive search)
            sample_dir = Path("feature_repo/data/sample_images")
            query_path = None

            # If it's already a full path, use it
            if Path(query_filename).exists():
                query_path = query_filename
            else:
                # Try recursive search
                for filepath in sample_dir.rglob("*"):
                    if filepath.name.lower() == Path(query_filename).name.lower():
                        query_path = str(filepath)
                        break

            if query_path:
                print(f"\n🔍 Finding images similar to '{Path(query_path).name}'...")
                demonstrate_image_search(store, query_path)

                # Ask if user wants to search again
                print("\n🔄 Would you like to search with another image? (y/n)")
                continue_search = input("Continue? ").strip().lower()
                if continue_search not in ["y", "yes"]:
                    print("👋 Goodbye!")
                    break
            else:
                print(f"Could not find image: {query_filename}")

        print("\n✅ Image similarity search completed successfully!")
        print("\n📖 Next steps:")
        print("  1. Add your own images to the sample_images directory")
        print("  2. Try different query images from your collection")
        print("  3. Experiment with different vision models (resnet34, resnet50, etc.)")
        print("  4. Use the image embeddings for your own ML applications")
        print("  5. Integrate with your existing Feast deployment")

    except KeyboardInterrupt:
        print("\n⏹️  Search interrupted by user")
    except Exception as e:
        print(f"\n❌ Search failed: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
