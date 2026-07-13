#!/bin/bash
cd "$(dirname "$0")"
source venv/bin/activate
echo ""
echo "  ⚡ PolyBot — BTC Up/Down Dashboard"
echo "  Dashboard will open at http://localhost:8420"
echo "  Press Ctrl+C to stop"
echo ""
python -m src.runner "$@"
