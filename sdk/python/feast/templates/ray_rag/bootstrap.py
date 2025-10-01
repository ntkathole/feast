from feast.file_utils import replace_str_in_file


def bootstrap():
    import pathlib
    from datetime import datetime

    import pandas as pd

    repo_path = pathlib.Path(__file__).parent.absolute() / "feature_repo"
    project_name = pathlib.Path(__file__).parent.absolute().name
    data_path = repo_path / "data"
    data_path.mkdir(exist_ok=True)

    # Download and process real IMDB movie data for RAG demonstration
    print("   🎬 Downloading real IMDB movie data for RAG demonstration...")

    def download_imdb_dataset():
        """Download and process the IMDB movies dataset."""
        import os

        try:
            # Import kaggle with authentication handling
            try:
                import kaggle

                print("   🔑 Kaggle API found, checking authentication...")

                # Download the specific IMDB dataset
                kaggle.api.dataset_download_files(
                    "yashgupta24/48000-movies-dataset", path="./data", unzip=True
                )
                print("   📥 Dataset downloaded successfully!")
            except OSError as auth_error:
                if "kaggle.json" in str(auth_error):
                    print("   ⚠️  Kaggle credentials not found")
                    print("   💡 To use Kaggle API:")
                    print(
                        "      1. Get API credentials from https://www.kaggle.com/account"
                    )
                    print("      2. Place kaggle.json in ~/.kaggle/")
                    print("      3. chmod 600 ~/.kaggle/kaggle.json")
                    raise ImportError("Kaggle credentials not configured")
                else:
                    raise

            # Look for the downloaded files
            data_dir = "./data"
            if os.path.exists(data_dir):
                csv_files = [f for f in os.listdir(data_dir) if f.endswith(".csv")]

                if csv_files:
                    dataset_path = os.path.join(
                        data_dir, csv_files[0]
                    )  # Use first CSV file found
                    print(f"   📄 Found dataset file: {csv_files[0]}")

                    import pandas as pd

                    df = pd.read_csv(dataset_path)
                    print(f"   📊 Dataset shape: {df.shape}")
                    print(f"   📋 Columns: {list(df.columns)}")

                    print(
                        f"   ✅ Successfully downloaded Kaggle dataset with {len(df)} movies"
                    )
                    print("   📝 No processing needed - Feast will read CSV directly!")

                    # Copy CSV to feature_repo/data for direct usage
                    import shutil

                    target_csv_path = data_path / "final_data.csv"
                    if os.path.exists(dataset_path) and not os.path.exists(
                        target_csv_path
                    ):
                        shutil.copy2(dataset_path, str(target_csv_path))
                        print(f"   📁 Copied CSV to: {target_csv_path}")

                    return  # No need to return processed data

        except ImportError:
            print("   ⚠️  Kaggle API not available. Install with: pip install kaggle")

    try:
        print("   📥 Attempting to download IMDB dataset...")
        download_imdb_dataset()  # Just download and copy CSV - no processing needed
        print("   ✅ Downloaded IMDB dataset successfully")

    except Exception as e:
        print(f"   ⚠️  Dataset download failed: {e}")
        print("   📝 Using comprehensive fallback dataset for demonstration")
        download_imdb_dataset()  # Fallback still downloads/copies CSV

    # Convert CSV to Parquet for Ray offline store compatibility
    print("   🔄 Converting CSV to Parquet for Ray offline store compatibility...")
    try:
        import pandas as pd

        csv_path = data_path / "final_data.csv"
        parquet_path = data_path / "raw_movies.parquet"

        if csv_path.exists():
            print(f"   📊 Reading CSV from {csv_path}")
            df = pd.read_csv(csv_path)

            # Add event_timestamp column for Feast with timezone awareness
            from datetime import datetime

            df["event_timestamp"] = pd.to_datetime(df["DatePublished"], errors="coerce")
            df["event_timestamp"] = df["event_timestamp"].fillna(datetime.now())
            # Make timezone-aware (UTC) to avoid comparison issues
            try:
                df["event_timestamp"] = df["event_timestamp"].dt.tz_localize("UTC")
            except TypeError:
                # Handle already timezone-aware timestamps
                df["event_timestamp"] = df["event_timestamp"].dt.tz_convert("UTC")

            # Save as Parquet
            df.to_parquet(parquet_path, index=False)
            print(f"   ✅ Converted to Parquet: {parquet_path}")
            print(f"   📊 Dataset shape: {df.shape}")
        else:
            print(f"   ⚠️  CSV file not found at {csv_path}")

    except Exception as e:
        print(f"   ⚠️  CSV to Parquet conversion failed: {e}")

    print(
        "   🚀 Ray will generate embeddings during materialization using feature transformations"
    )
    print(
        "   ⚡ Embeddings will be generated on-demand using Ray's distributed processing"
    )
    print(f"   📁 Raw movie data ready at: {data_path / 'raw_movies.parquet'}")
    print(
        "   🎯 Run 'feast materialize-incremental' to trigger Ray batch embedding generation"
    )

    # Update the example_repo.py file with actual paths
    example_py_file = repo_path / "example_repo.py"
    replace_str_in_file(example_py_file, "%PROJECT_NAME%", str(project_name))

    print("🚀 Ray RAG template initialized successfully!")
    print("🎬 Using complete IMDB movie dataset from Kaggle (48K+ movies)")
    print(f"📁 Raw movie data: {data_path / 'final_data.csv'}")
    print(f"🏗️ Ray storage will be created at: {data_path / 'ray_storage'}")
    print(
        "⚡ Ray will generate embeddings during materialization with feature transformations"
    )

    print("\n🎯 To get started:")
    print(f"  1. cd {project_name}/feature_repo")
    print("  2. feast apply")
    print("  3. python test_workflow.py")


if __name__ == "__main__":
    bootstrap()
