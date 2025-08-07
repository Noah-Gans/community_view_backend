#!/bin/bash

# Community View Backend - Service Startup Script
# This script starts the search API and Tegola server

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo -e "${GREEN}🚀 Starting Community View Backend Services${NC}"
echo "Project root: $PROJECT_ROOT"

# Change to project root
cd "$PROJECT_ROOT"

# Activate virtual environment
if [ -f "venv/bin/activate" ]; then
    echo -e "${YELLOW}📦 Activating virtual environment...${NC}"
    source venv/bin/activate
else
    echo -e "${RED}❌ Virtual environment not found. Please run setup first.${NC}"
    exit 1
fi

# Start Search API
echo -e "${YELLOW}🔍 Starting Search API...${NC}"
nohup python search_api/app.py > logs/search_api.log 2>&1 &
SEARCH_API_PID=$!
echo "Search API PID: $SEARCH_API_PID"

# Wait for Search API to start
sleep 5

# Check if Search API is running
if curl -s http://localhost:8000/health > /dev/null; then
    echo -e "${GREEN}✅ Search API is running${NC}"
else
    echo -e "${RED}❌ Search API failed to start${NC}"
    kill $SEARCH_API_PID 2>/dev/null || true
    exit 1
fi

# Start Tegola Server
echo -e "${YELLOW}🗺️ Starting Tegola Server...${NC}"

# Check for Tegola binary (both local and system-wide)
TEGOLA_BIN=""
if [ -f "./tegola" ]; then
    TEGOLA_BIN="./tegola"
elif command -v tegola >/dev/null 2>&1; then
    TEGOLA_BIN="tegola"
else
    echo -e "${RED}❌ Tegola binary not found. Please install Tegola first.${NC}"
    echo "Download from: https://github.com/go-spatial/tegola/releases"
    echo "Or install system-wide and ensure it's in your PATH"
    exit 1
fi

# Start Tegola with the found binary
nohup $TEGOLA_BIN serve --config tegola_config.toml > logs/tegola.log 2>&1 &
TEGOLA_PID=$!
echo "Tegola PID: $TEGOLA_PID"

# Wait for Tegola to start
sleep 5

# Check if Tegola is running
if curl -s http://localhost:8081/capabilities > /dev/null; then
    echo -e "${GREEN}✅ Tegola Server is running${NC}"
else
    echo -e "${RED}❌ Tegola Server failed to start${NC}"
    kill $TEGOLA_PID 2>/dev/null || true
    exit 1
fi

# Save PIDs for later management
mkdir -p logs
echo "$SEARCH_API_PID" > logs/search_api.pid
echo "$TEGOLA_PID" > logs/tegola.pid

echo -e "${GREEN}🎉 All services started successfully!${NC}"
echo "Search API: http://localhost:8000"
echo "Tegola: http://localhost:8081"
echo ""
echo "To stop services, run: scripts/stop_services.sh"

# Exit immediately after starting services
exit 0 