#!/bin/bash

# Bento Cloudflare Container Deployment Script
# This script helps deploy Bento to Cloudflare Containers

set -e

echo "🚀 Bento Cloudflare Container Deployment"
echo "========================================"
echo ""

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check prerequisites
check_prerequisites() {
    echo "📋 Checking prerequisites..."

    # Check Docker
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}❌ Docker is not installed${NC}"
        echo "Please install Docker: https://docs.docker.com/get-docker/"
        exit 1
    fi

    # Check if Docker is running
    if ! docker ps &> /dev/null; then
        echo -e "${RED}❌ Docker is not running${NC}"
        echo "Please start Docker and try again"
        exit 1
    fi
    echo -e "${GREEN}✅ Docker is installed and running${NC}"

    # Check Wrangler
    if ! command -v wrangler &> /dev/null; then
        echo -e "${YELLOW}⚠️  Wrangler is not installed${NC}"
        echo "Installing Wrangler..."
        npm install -g wrangler
    else
        echo -e "${GREEN}✅ Wrangler is installed${NC}"
    fi

    # Check if logged in to Cloudflare
    if ! wrangler whoami &> /dev/null; then
        echo -e "${YELLOW}⚠️  Not logged in to Cloudflare${NC}"
        echo "Please log in to Cloudflare:"
        wrangler login
    else
        echo -e "${GREEN}✅ Logged in to Cloudflare${NC}"
    fi

    echo ""
}

# Check configuration
check_configuration() {
    echo "⚙️  Checking configuration..."

    if [ ! -f "wrangler.toml" ]; then
        echo -e "${RED}❌ wrangler.toml not found${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ wrangler.toml found${NC}"

    if [ ! -f "src/worker.js" ]; then
        echo -e "${RED}❌ src/worker.js not found${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ worker.js found${NC}"

    if [ ! -f "resources/docker/Dockerfile" ]; then
        echo -e "${RED}❌ Dockerfile not found${NC}"
        exit 1
    fi
    echo -e "${GREEN}✅ Dockerfile found${NC}"

    echo ""
}

# Build Docker image locally (optional test)
build_docker_image() {
    echo "🐳 Testing Docker build locally..."

    if docker build -t bento-cloudflare -f resources/docker/Dockerfile . > /dev/null 2>&1; then
        echo -e "${GREEN}✅ Docker image builds successfully${NC}"
        docker rmi bento-cloudflare > /dev/null 2>&1 || true
    else
        echo -e "${RED}❌ Docker build failed${NC}"
        echo "Please check your Dockerfile and try again"
        exit 1
    fi

    echo ""
}

# Deploy to Cloudflare
deploy() {
    echo "☁️  Deploying to Cloudflare Containers..."
    echo ""

    wrangler deploy

    if [ $? -eq 0 ]; then
        echo ""
        echo -e "${GREEN}✅ Deployment successful!${NC}"
        echo ""
        echo "⏳ Note: First deployment takes a few minutes to provision containers"
        echo "   Wait 2-5 minutes before sending requests"
        echo ""
        echo "📊 Next steps:"
        echo "   1. Check deployment status: wrangler deployments list"
        echo "   2. View logs: wrangler tail"
        echo "   3. Test health endpoint: curl https://your-worker.workers.dev/ping"
        echo ""
    else
        echo -e "${RED}❌ Deployment failed${NC}"
        exit 1
    fi
}

# Main execution
main() {
    check_prerequisites
    check_configuration

    # Optional: test Docker build locally
    read -p "Test Docker build locally first? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        build_docker_image
    fi

    # Confirm deployment
    read -p "Deploy to Cloudflare Containers? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        deploy
    else
        echo "Deployment cancelled"
        exit 0
    fi
}

# Run main function
main
