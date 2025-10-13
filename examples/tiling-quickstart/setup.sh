#!/bin/bash

# Real Tiling Demo Setup Script

set -e

echo "🚀 Setting up Real Tiling Demo..."
echo "=================================="
echo ""

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

# Step 1: Install Python dependencies
echo "📦 Installing Python dependencies..."
pip install -q kafka-python redis feast pyarrow

# Step 2: Start Docker services
echo ""
echo "🐳 Starting Docker services (Kafka, Redis, Zookeeper)..."
docker-compose up -d

# Step 3: Wait for services
echo ""
echo "⏳ Waiting for services to be ready (30 seconds)..."
sleep 30

# Step 4: Create data directory
echo ""
echo "📁 Creating data directories..."
mkdir -p feature_repo/data

# Step 5: Apply Feast configuration
echo ""
echo "🍽️  Applying Feast configuration..."
cd feature_repo
feast apply
cd ..

echo ""
echo "✅ Setup Complete!"
echo "=================================="
echo ""
echo "📋 Services Status:"
docker-compose ps

echo ""
echo "🎯 Next Steps:"
echo "   1. Run Kafka producer (in terminal 1):"
echo "      python kafka_producer.py batch 1000"
echo ""
echo "   2. Run tiling demo (in terminal 2):"
echo "      python real_tiling_demo.py"
echo ""
echo "   3. View Kafka UI (optional):"
echo "      http://localhost:8080"
echo ""
echo "To stop services: docker-compose down"
echo ""

