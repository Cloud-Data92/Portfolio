# 🤖 Discord Bots

Python Discord bots I built and run 24/7 on a Mac mini home server.

> 🚧 **Status:** migrating code from my private server into this public repo, following the
> [security checklist](../../SECURITY.md) (secrets moved to `.env`, history scanned) before anything is pushed.

## What runs here today

<!-- TODO(Matt): one short paragraph per bot — what it does, what APIs it talks to,
     roughly how many servers/users it serves. Example:
     - **StockAlert Bot** — posts price alerts to my server when watchlist tickers move ±X%.
       Uses discord.py + <market data API>. Runs via launchd on the Mac mini. -->

## Architecture & secure configuration

Every bot in this folder follows the same pattern — the bot token and any API keys are **never in the code**:

```python
import os
from dotenv import load_dotenv

load_dotenv()                                # reads git-ignored .env
TOKEN = os.environ["DISCORD_TOKEN"]          # crashes loudly if missing — never a hardcoded fallback
```

- [`.env.example`](.env.example) documents the required variables with fake values
- Real `.env` lives only on the Mac mini and is git-ignored
- [`requirements.txt`](requirements.txt) pins dependencies

## Running a bot locally

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env      # then fill in your real token (from the Discord Developer Portal)
python3 bot.py
```

## Deployment

The bots run continuously on a Mac mini using `launchd`, which restarts them automatically on crash or reboot.
