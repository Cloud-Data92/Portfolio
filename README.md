# Matthew Cicco — Project Portfolio

Data-focused technologist with a background in Information Systems (BBA, Kennesaw State University, CS minor) and hands-on experience in SQL, data warehousing, analytics, and Python automation. This repository showcases my personal projects and technical skills.

📫 **Contact:** ciccomatthew@gmail.com

---

## 📂 Projects

| Project | What it shows | Status |
|---|---|---|
| [📊 SQL Stock Market Analytics](projects/sql-stock-analytics/) | Advanced SQL: schema design, joins, CTEs, window functions, portfolio P&L analysis, plus a Tableau-ready data pipeline | ✅ Complete & runnable |
| [🤖 Discord Bots](projects/discord-bots/) | Python, event-driven programming, API integration, deployment on always-on hardware (Mac mini home server) | 🚧 Code migration in progress |
| [📈 Automated Trading Bot](projects/trading-bot/) | Python, broker/market-data APIs, strategy logic, risk management, scheduled automation | 🚧 Code migration in progress |

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
