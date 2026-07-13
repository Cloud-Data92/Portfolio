# stock-scanner

A minimal Python scanner that watches a stock watchlist for **unusual volume** — the
classic early tell that something is happening in a name — and includes a hook to run
findings through a **local LLM (Ollama)** for a plain-English read.

## What it does

For each symbol in the watchlist it pulls a month of daily history via `yfinance` and
computes:

- current price and 1-month percent change
- **volume ratio** — latest volume vs. the 1-month average
- flags anything trading at **>1.5× normal volume**

Results print as a pandas table; the `query_ollama()` hook sends scan output to a
local model (default `mistral`) for narrative analysis — same local-first AI approach
as my other bots: no API cost, nothing leaves the machine.

## Run it

```bash
cp .env.example .env        # optional: Discord webhook / Ollama settings
pip install -r requirements.txt
python scanner.py
```

## Honest scope

This is the compact utility version — a single-file scanner built around one signal
(volume anomaly) that I use as the base for experiments. The natural extensions
(options-flow data, scheduled scans posting to Discord, more signals) are wired for in
`.env.example` and `requirements.txt` (`discord.py`, `SCAN_INTERVAL_MINUTES`), and the
deeper analytics work lives in the [trading-bot](../../trading-bot/) and
`sql-stock-analytics` projects.
