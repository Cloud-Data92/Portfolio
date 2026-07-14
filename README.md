# Matthew Cicco — Project Portfolio

Data-focused technologist with a background in Information Systems (BBA, Kennesaw State University, CS minor) and hands-on experience in SQL, data warehousing, analytics, and Python automation. This repository showcases my personal projects and technical skills.

📫 **Contact:** ciccomatthew@gmail.com

---

## 📂 Projects

| Project | What it shows | Status |
|---|---|---|
| [📊 SQL Stock Market Analytics](projects/sql-stock-analytics/) | Advanced SQL: schema design, joins, CTEs, window functions, portfolio P&L analysis, plus a Tableau-ready data pipeline | ✅ Complete & runnable |
| [📈 PolyBot — Trading Engine](projects/trading-bot/) | 24/7 async Python engine for Polymarket BTC binary markets: multi-timeframe momentum signals, Kelly-criterion sizing, SQLite trade history, real-time dashboard, and an LLM advisor that can suggest but never execute | ✅ Migrated & documented |
| [🤖 discord-ai-bot](projects/discord-bots/discord-ai-bot/) | Discord chatbot backed by a fully local LLM (Ollama) — private, zero API cost, per-message model switching | ✅ Migrated & documented |
| [📉 stock-scanner](projects/discord-bots/stock-scanner/) | Watchlist scanner flagging unusual-volume moves, with a local-LLM analysis hook | ✅ Migrated & documented |

---

## 🧠 How I use AI — strategically, not blindly

- **Bounded advisor, deterministic core.** The trading bot's LLM sidecar reads engine
  state and writes schema-validated "nudges" to a file the engine may consult — but the
  LLM *cannot* place or cancel orders, and if it's down the bot is unchanged. AI adds
  judgment at the margins; auditable code keeps control.
- **Local models where privacy and cost matter.** The Discord bot and scanner run on
  Ollama models on my own hardware — no per-token bills, no data leaving the machine.
- **Agents for repeatable work.** I run scheduled AI agents (launchd on a Mac mini
  home server) for things like daily job-posting digests delivered over iMessage —
  automation where the output is checked, not worshipped.

---

## 🛠️ Core Skills

- **SQL & Databases** — complex queries, aggregate functions, window functions, CTEs, cross-tab reporting, MySQL / SQLite / MS Access, data warehousing & ETL
- **Data Analysis & Visualization** — Tableau, Excel (Power Pivot, VBA), heat-maps, executive reporting
- **Python** — automation, bots, API integrations, data pipelines
- **Other** — R, SAS, C#, ASP.NET, HTML/CSS, WordPress, project management

---

## 🔒 Security Practices in This Repo

No API keys, tokens, or credentials are ever committed here:

- All secrets load from environment variables / `.env` files, which are **git-ignored** — only `.env.example` templates (with fake placeholder values) are committed
- A [secret-scanning CI workflow](.github/workflows/secret-scan.yml) (Gitleaks) runs on every push and pull request
- GitHub push protection is enabled on the repository
- See [SECURITY.md](SECURITY.md) for the full checklist I follow

---

*This portfolio is a living repo — projects are added and improved over time.*
