#!/bin/bash
# PolyBot — Status check

PID=$(pgrep -f "python.*-m src\.runner" 2>/dev/null | head -1)
if [ -n "$PID" ]; then
    echo "PolyBot is RUNNING (PID $PID)"
    echo ""
    # Show quick status from API
    STATUS=$(curl -s http://localhost:8420/api/status 2>/dev/null)
    if [ -n "$STATUS" ]; then
        echo "$STATUS" | python3 -c "
import sys, json
d = json.load(sys.stdin)
mode = 'LIVE' if not d.get('dryRun', True) else 'DRY RUN'
print(f'  Mode: {mode} | Trade: {d.get(\"tradeMode\",\"?\")}')
print(f'  Cycle: {d.get(\"cycle\",0)} | BTC: \${d.get(\"btcPrice\",0):,.2f}')
print(f'  Bankroll: \${d.get(\"bankroll\",0):.2f} | CLOB: \${d.get(\"clobBalance\",0):.2f}')
print(f'  P&L: \${d.get(\"pnl\",0):.4f} | W/L: {d.get(\"wins\",0)}/{d.get(\"losses\",0)}')
print(f'  Bets: {d.get(\"totalBets\",0)} | Markets: {len(d.get(\"markets\",[]))}')
print(f'  Max Bet: \${d.get(\"maxBetDollars\",0):.2f}')
" 2>/dev/null
    fi
    echo ""
    echo "Dashboard: http://localhost:8420"
    echo "Logs: tail -f /tmp/polybot.log"
else
    echo "PolyBot is NOT running."
    echo "Start with: ./start.sh"
fi

# Advisor status
ADV_PID=$(pgrep -f "python.*-m src\.advisor_monitor" 2>/dev/null | head -1)
echo ""
if [ -n "$ADV_PID" ]; then
    echo "Advisor is RUNNING (PID $ADV_PID)"
    echo "  Logs: tail -f /tmp/advisor.log"
else
    echo "Advisor is NOT running."
fi
