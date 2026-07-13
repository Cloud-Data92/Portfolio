# 📈 Automated Trading Bot

A Python trading bot that runs on a schedule on my Mac mini home server, pulling market data, evaluating strategy rules, and placing/managing orders through a broker API.

> 🚧 **Status:** migrating code from my private server into this public repo, following the
> [security checklist](../../SECURITY.md) (broker API keys moved to `.env`, history scanned) before anything is pushed.
>
> ⚠️ **Disclaimer:** personal project for my own account. Not financial advice; not intended for others to trade with.

## Overview

<!-- TODO(Matt): fill these in from the real bot —
     - Which broker/exchange API it uses (Alpaca? Robinhood? Coinbase? IBKR?)
     - The strategy in one or two sentences (e.g., MA crossover on a watchlist, momentum, DCA)
     - Risk rules (max position size, stop-loss logic, trading hours)
     - How long it has been running and any results you're comfortable sharing -->

## Architecture

```
scheduler (launchd/cron on Mac mini)
   └── bot run:
        1. fetch market data  ──►  market data API
        2. evaluate strategy  ──►  signal engine (pure Python, unit-testable)
        3. size & place order ──►  broker API (paper or live, set by env var)
        4. log everything     ──►  local SQLite trade log
```

The strategy logic mirrors the SQL analysis in my [SQL Stock Analytics project](../sql-stock-analytics/) — e.g., the golden-cross detection there is the query form of the same moving-average signals used here.

## Secure configuration

Broker keys are the most dangerous secrets in this portfolio — they can move real money. They are handled exactly like all secrets in this repo:

- Loaded from a git-ignored `.env` via `python-dotenv` — see [`.env.example`](.env.example)
- `PAPER_TRADING=true` is the default; live trading requires explicitly flipping it
- The real `.env` exists only on the Mac mini

## Running

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env      # fill in broker API keys (start with paper trading!)
python3 bot.py --dry-run  # evaluate signals without placing orders
```
