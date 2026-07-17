# Discord Bots

Two small, self-hosted bots that run on my Mac mini home server. Both follow the same
principles: local-first AI (Ollama — no API costs, no data leaving the machine),
credentials only via `.env` (see each bot's `.env.example`), and honest scope — these
are utilities I actually use, not frameworks.

| Bot | Language | What it does |
|---|---|---|
| [discord-ai-bot](discord-ai-bot/) | Node.js | Chat with a local LLM from any Discord channel (`!ai <prompt>`), with per-message model switching |
| [stock-scanner](stock-scanner/) | Python | Scans a stock watchlist for unusual volume and pipes findings through a local LLM for analysis |
