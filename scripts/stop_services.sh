#!/bin/bash

# Community View Backend - Service Stop Script
# This script stops the search API and Tegola server

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get the directory of this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo -e "${YELLOW}🛑 Stopping Community View Backend Services${NC}"

# Change to project root
cd "$PROJECT_ROOT"

# Stop Search API
if [ -f "logs/search_api.pid" ]; then
    SEARCH_API_PID=$(cat logs/search_api.pid)
    if kill -0 "$SEARCH_API_PID" 2>/dev/null; then
        echo -e "${YELLOW}🔍 Stopping Search API (PID: $SEARCH_API_PID)...${NC}"
        kill "$SEARCH_API_PID"
        sleep 2
        if kill -0 "$SEARCH_API_PID" 2>/dev/null; then
            echo -e "${YELLOW}⚠️  Force killing Search API...${NC}"
            kill -9 "$SEARCH_API_PID"
        fi
        echo -e "${GREEN}✅ Search API stopped${NC}"
    else
        echo -e "${YELLOW}⚠️  Search API was not running${NC}"
    fi
    rm -f logs/search_api.pid
else
    echo -e "${YELLOW}⚠️  Search API PID file not found${NC}"
fi

# Stop Tegola
if [ -f "logs/tegola.pid" ]; then
    TEGOLA_PID=$(cat logs/tegola.pid)
    if kill -0 "$TEGOLA_PID" 2>/dev/null; then
        echo -e "${YELLOW}🗺️  Stopping Tegola (PID: $TEGOLA_PID)...${NC}"
        kill "$TEGOLA_PID"
        sleep 2
        if kill -0 "$TEGOLA_PID" 2>/dev/null; then
            echo -e "${YELLOW}⚠️  Force killing Tegola...${NC}"
            kill -9 "$TEGOLA_PID"
        fi
        echo -e "${GREEN}✅ Tegola stopped${NC}"
    else
        echo -e "${YELLOW}⚠️  Tegola was not running${NC}"
    fi
    rm -f logs/tegola.pid
else
    echo -e "${YELLOW}⚠️  Tegola PID file not found${NC}"
fi

# Kill any remaining processes by name
pkill -f "python.*start_api" 2>/dev/null || true
pkill -f "tegola serve" 2>/dev/null || true

echo -e "${GREEN}🎉 All services stopped${NC}" 