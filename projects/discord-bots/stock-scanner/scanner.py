"""
Stock/Options Scanner — minimal watchlist scanner.

Pulls recent price/volume history for a watchlist via yfinance, computes
1-month change and a volume-anomaly ratio, flags tickers trading at unusual
volume, and includes an Ollama hook for local-LLM analysis of results.

Usage:
    python scanner.py              # Run once
    python scanner.py --watch      # Continuous monitoring
"""

import os
import sys
import json
import asyncio
from datetime import datetime

import yfinance as yf
import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv

load_dotenv()

OLLAMA_URL = os.getenv('OLLAMA_URL', 'http://localhost:11434')
OLLAMA_MODEL = os.getenv('OLLAMA_MODEL', 'mistral')


def query_ollama(prompt, model=OLLAMA_MODEL):
    """Query local Ollama for AI analysis."""
    resp = requests.post(
        f'{OLLAMA_URL}/api/generate',
        json={'model': model, 'prompt': prompt, 'stream': False},
    )
    resp.raise_for_status()
    return resp.json()['response']


def scan_ticker(symbol):
    """Fetch and analyze a single ticker. Customize this."""
    ticker = yf.Ticker(symbol)
    hist = ticker.history(period='1mo')

    if hist.empty:
        return None

    current = hist['Close'].iloc[-1]
    change_pct = ((current - hist['Close'].iloc[0]) / hist['Close'].iloc[0]) * 100
    avg_volume = hist['Volume'].mean()
    latest_volume = hist['Volume'].iloc[-1]
    volume_ratio = latest_volume / avg_volume if avg_volume > 0 else 0

    return {
        'symbol': symbol,
        'price': round(current, 2),
        'change_1mo': round(change_pct, 2),
        'volume_ratio': round(volume_ratio, 2),
        'timestamp': datetime.now().isoformat(),
    }


def scan_watchlist(symbols):
    """Scan a list of symbols."""
    results = []
    for sym in symbols:
        try:
            data = scan_ticker(sym)
            if data:
                results.append(data)
        except Exception as e:
            print(f'Error scanning {sym}: {e}')
    return results


if __name__ == '__main__':
    # Default watchlist - replace with yours
    watchlist = ['AAPL', 'MSFT', 'NVDA', 'TSLA', 'SPY', 'QQQ']

    print(f'Scanning {len(watchlist)} symbols...')
    results = scan_watchlist(watchlist)

    df = pd.DataFrame(results)
    print('\n' + df.to_string(index=False))

    # Flag unusual volume
    alerts = df[df['volume_ratio'] > 1.5]
    if not alerts.empty:
        print(f'\n⚠ Unusual volume detected:')
        print(alerts[['symbol', 'price', 'volume_ratio']].to_string(index=False))
