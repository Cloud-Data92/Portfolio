#!/bin/bash
# PolyBot — Stop (runner + advisor)

# Stop advisor first (graceful)
ADV_PID=$(pgrep -f "python.*src\.advisor_monitor" 2>/dev/null)
if [ -n "$ADV_PID" ]; then
    echo "Stopping Advisor (PID $ADV_PID)..."
    kill "$ADV_PID" 2>/dev/null
    sleep 1
    if pgrep -f "python.*src\.advisor_monitor" > /dev/null 2>&1; then
        kill -9 "$ADV_PID" 2>/dev/null
    fi
    echo "Advisor stopped."
else
    echo "Advisor is not running."
fi

# Stop runner
PID=$(pgrep -f "python.*src\.runner" 2>/dev/null)
if [ -n "$PID" ]; then
    echo "Stopping PolyBot (PID $PID)..."
    kill "$PID" 2>/dev/null
    sleep 1
    if pgrep -f "python.*src\.runner" > /dev/null 2>&1; then
        echo "Force killing..."
        kill -9 "$PID" 2>/dev/null
    fi
    echo "Stopped."
else
    echo "PolyBot is not running."
fi
