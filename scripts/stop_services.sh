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

# Function to stop a service
stop_service() {
    local name=$1
    local pid_file=$2
    local process_pattern=$3
    
    # First try using PID file
    if [ -f "$pid_file" ]; then
        local PID=$(cat "$pid_file")
        if kill -0 "$PID" 2>/dev/null; then
            echo -e "${YELLOW}🔄 Stopping $name (PID: $PID)...${NC}"
            kill "$PID"
            sleep 2
            if kill -0 "$PID" 2>/dev/null; then
                echo -e "${YELLOW}⚠️  Force killing $name...${NC}"
                kill -9 "$PID"
            fi
        fi
        rm -f "$pid_file"
    fi
    
    # Then try killing by pattern
    if pgrep -f "$process_pattern" > /dev/null; then
        echo -e "${YELLOW}🔄 Stopping remaining $name processes...${NC}"
        pkill -f "$process_pattern"
        sleep 2
        if pgrep -f "$process_pattern" > /dev/null; then
            echo -e "${YELLOW}⚠️  Force killing remaining $name processes...${NC}"
            pkill -9 -f "$process_pattern"
        fi
    fi
    
    # Verify service is stopped
    if ! pgrep -f "$process_pattern" > /dev/null; then
        echo -e "${GREEN}✅ $name stopped${NC}"
        return 0
    else
        echo -e "${RED}❌ Failed to stop $name${NC}"
        return 1
    fi
}

# Stop Search API
stop_service "Search API" "logs/search_api.pid" "python.*app.py"

# Stop Tegola
stop_service "Tegola" "logs/tegola.pid" "tegola serve"

# Final verification
if pgrep -f "python.*app.py|tegola serve" > /dev/null; then
    echo -e "${RED}❌ Some services are still running${NC}"
    exit 1
else
    echo -e "${GREEN}🎉 All services stopped successfully${NC}"
    exit 0
fi 