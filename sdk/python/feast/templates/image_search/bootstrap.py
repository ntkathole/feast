"""
Bootstrap script for Feast Image Search Template.
"""

from pathlib import Path


def bootstrap():
    """Bootstrap the image search template with sample data and directories."""
    print("🚀 Bootstrapping Feast Image Search Template...")

    current_dir = Path(__file__).parent

    # Create a simple requirements.txt for the template
    requirements_content = """# Feast Image Search Template Requirements
# Core Feast with Milvus and image support
feast[milvus]
feast[image]
matplotlib
rich
"""
    requirements_path = current_dir / "requirements.txt"
    with open(requirements_path, "w") as f:
        f.write(requirements_content)
    print("Created requirements.txt")

    print("✅ Bootstrap completed successfully!")
    print("\n📖 Next steps:")
    print("  1. Start Milvus: docker-compose up -d")
    print("  2. Install dependencies: pip install -r requirements.txt")
    print("  3. Run interactive search: python image_search_workflow.py")
    print("  4. Add your own images to feature_repo/data/sample_images/ directory")
