#!/bin/bash
# PolyBot — Start
# Usage: ./start.sh [--live] [--max-bet-dollars 5]

cd "$(dirname "$0")"
source venv/bin/activate

# Kill any existing instance (handle multiple PIDs)
PIDS=$(pgrep -f "python.*-m src\.runner" 2>/dev/null)
if [ -n "$PIDS" ]; then
    echo "Stopping existing bot (PIDs: $PIDS)..."
    echo "$PIDS" | xargs kill 2>/dev/null
    sleep 2
    # Force kill if still alive
    PIDS=$(pgrep -f "python.*-m src\.runner" 2>/dev/null)
    if [ -n "$PIDS" ]; then
        echo "$PIDS" | xargs kill -9 2>/dev/null
        sleep 1
    fi
fi

# Default args — dry run with $5 cap
ARGS="--max-bet-dollars 5.00"

# Override with user args if provided
if [ $# -gt 0 ]; then
    ARGS="$@"
fi

echo "Starting PolyBot..."
echo "  Args: $ARGS"
echo "  Log:  /tmp/polybot.log"
echo "  Dashboard: http://localhost:8420"
echo ""

nohup python -u -m src.runner $ARGS > /tmp/polybot.log 2>&1 &
NEW_PID=$!
echo "Started! PID: $NEW_PID"
echo ""

# ── Advisor Sidecar DISABLED (deterministic engine only) ──
# Kill any stale advisor process
ADVISOR_PIDS=$(pgrep -f "python.*-m src\.advisor_monitor" 2>/dev/null)
if [ -n "$ADVISOR_PIDS" ]; then
    echo "Killing stale advisor (PIDs: $ADVISOR_PIDS)..."
    echo "$ADVISOR_PIDS" | xargs kill 2>/dev/null
fi
echo "Advisor: DISABLED (deterministic only)"
echo ""

# Wait a moment and show initial output
sleep 3
head -20 /tmp/polybot.log
echo ""
echo "Tail logs: tail -f /tmp/polybot.log"
