# Matthew Cicco — Project Portfolio

I build data products that connect operational systems to business decisions. My range: **advanced SQL and BI engineering** (BigQuery, SQL Server, Tableau), **data warehousing and ETL**, and **real-time Python systems**. Professionally that has meant enterprise delivery analytics at Home Depot (reporting used by 200+ field and leadership users) and earlier analytics roles at UPS and UL Environment; personally it means a home lab of 24/7 systems. Information Systems BBA (Kennesaw State University, CS minor).

📫 **Contact:** ciccomatthew@gmail.com · 🌐 **Website:** [cloud-data92.github.io/Portfolio](https://cloud-data92.github.io/Portfolio/)

---

## 🧭 Quick Tour — find what you came to see

| Looking for evidence of… | Start here |
|---|---|
| **Advanced SQL on real business problems** | [Enterprise Delivery Analytics](projects/enterprise-delivery-analytics/) — 10 case studies, each opening with the business problem |
| **SQL you can run yourself** | [SQL Stock Market Analytics](projects/sql-stock-analytics/) — clone it, two commands, every result reproduces |
| **Data engineering & ETL discipline** | [ETL → Star-Schema Warehouse](projects/etl-data-warehouse/) and [case study 10](projects/enterprise-delivery-analytics/sql/10_incremental_merge_architecture.sql) (incremental `MERGE`) |
| **Real-time Python / systems engineering** | [PolyBot](projects/trading-bot/) — async engine, live order books, risk gates, dashboard |
| **Practical, bounded AI integration** | PolyBot's [advisory-only LLM sidecar](projects/trading-bot/#the-ai-advisor-sidecar) and the [local-AI Discord bots](projects/discord-bots/) |
| **Visualization & Tableau thinking** | The [KPI heatmap](projects/enterprise-delivery-analytics/#the-kpi-heatmap), the [SQL result charts](projects/sql-stock-analytics/#visualized-results), PolyBot's live dashboard |
| **Data quality & validation habits** | [Validation query suite](projects/enterprise-delivery-analytics/validation/data_quality_checks.sql) and the ETL project's reject quarantine |

---

## 📂 Projects

| Project | What it shows | Status |
|---|---|---|
| [🚚 Enterprise Delivery Analytics](projects/enterprise-delivery-analytics/) | 10 sanitized enterprise logistics case studies — route/slot utilization, BigQuery geospatial serviceability, delivery-attempt modeling, KPI scoring, cross-system reconciliation, incremental BI architecture | ✅ Case studies published |
| [📊 SQL Stock Market Analytics](projects/sql-stock-analytics/) | Advanced SQL: schema design, joins, CTEs, window functions, portfolio P&L analysis, plus a Tableau-ready data pipeline | ✅ Complete & runnable |
| [🏗️ ETL Pipeline → Star-Schema Warehouse](projects/etl-data-warehouse/) | Production-style ETL: messy-data cleansing, reject quarantine, data-quality gates, Kimball dimensional modeling, idempotent loads, unit tests | ✅ Complete & runnable |
| [📈 PolyBot — Trading Engine](projects/trading-bot/) | 24/7 async Python engine for Polymarket BTC binary markets: multi-timeframe momentum signals, Kelly-criterion sizing, SQLite trade history, real-time dashboard, and an LLM advisor that can suggest but never execute | ✅ Migrated & documented |
| [🤖 discord-ai-bot](projects/discord-bots/discord-ai-bot/) | Discord chatbot backed by a fully local LLM (Ollama) — private, zero API cost, per-message model switching | ✅ Migrated & documented |
| [📉 stock-scanner](projects/discord-bots/stock-scanner/) | Watchlist scanner flagging unusual-volume moves, with a local-LLM analysis hook | ✅ Migrated & documented |

<a href="projects/trading-bot/"><img alt="PolyBot live dashboard running in dry-run mode" src="projects/trading-bot/assets/dashboard.png" width="880"></a>

*PolyBot's real-time dashboard, captured live in dry-run mode — [see the project →](projects/trading-bot/)*

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
