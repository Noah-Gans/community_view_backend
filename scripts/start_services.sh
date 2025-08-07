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
python search_api/start_api.py &
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

# Skip Tegola startup - managed manually on VM
echo -e "${YELLOW}🗺️  Skipping Tegola startup (managed manually)${NC}"
TEGOLA_PID=""

# Save PIDs for later management
mkdir -p logs
echo "$SEARCH_API_PID" > logs/search_api.pid
# Remove any old Tegola PID file since we're not starting it
rm -f logs/tegola.pid

echo -e "${GREEN}🎉 Search API started successfully!${NC}"
echo "Search API: http://localhost:8000"
echo "Tegola: Start manually with 'tegola serve --config=tegola_config.toml'"
echo ""
echo "To stop services, run: scripts/stop_services.sh"

# Exit immediately after starting services
exit 0 