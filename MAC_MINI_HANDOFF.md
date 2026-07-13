# Handoff: Migrate My Bot Projects Into My GitHub Portfolio

> **How to use this doc (for Matt):** open Claude Code in a terminal on the Mac mini and
> paste this entire document as your first message. It contains everything the assistant
> needs — it has no memory of the session that created the portfolio.

---

## Who I am and what this is

I'm Matthew Cicco (GitHub: **Cloud-Data92**, email ciccomatthew@gmail.com). I have a public
portfolio repository at **https://github.com/Cloud-Data92/Portfolio** used to showcase my
projects to employers. It was scaffolded in a cloud session that could NOT reach this Mac
mini, so the actual code for my bots was never migrated. That's your job now.

**Your mission:** find my Discord bot(s) and my stock/trading bot on this machine, make them
safe to publish (zero secrets), move them into the portfolio repo structure, write honest
READMEs describing what they really do, and push to GitHub.

## State of the portfolio repo

- The scaffolding lives on branch **`claude/github-portfolio-setup-6h2wok`**. If I haven't
  merged it into `main` yet, base your work on that branch.
- Relevant structure already in place:
  - `projects/discord-bots/` — README with `TODO(Matt)` markers, `.env.example`, `requirements.txt`
  - `projects/trading-bot/` — same pattern
  - `projects/sql-stock-analytics/` — complete, don't touch
  - `SECURITY.md` — the security checklist; **read it before touching anything**
  - `.gitignore` — already blocks `.env`, `*.db`, logs, keys
  - `.github/workflows/secret-scan.yml` — Gitleaks CI runs on every push

## Step-by-step plan

### 1. Clone and branch

```bash
git clone https://github.com/Cloud-Data92/Portfolio.git
cd Portfolio
# base on main if the setup branch was merged, otherwise on the setup branch:
git checkout claude/github-portfolio-setup-6h2wok 2>/dev/null || git checkout main
git checkout -b migrate-bots
```

### 2. Find the projects on this machine

Search likely locations for my bot code (adjust as needed):

```bash
find ~ -maxdepth 4 \( -iname "*discord*" -o -iname "*bot*" -o -iname "*trad*" -o -iname "*stock*" \) \
  -type d 2>/dev/null | grep -viE "library|caches|node_modules|\.git/"
```

Also check: `~/Documents`, `~/Projects`, `~/dev`, launchd jobs (`ls ~/Library/LaunchAgents/`)
and crontab (`crontab -l`) — whatever keeps the bots running points at their install paths.
Confirm with me which projects you found before migrating them.

### 3. Secret audit — BEFORE any git add

For each project, find every secret:

```bash
grep -rniE "(api[_-]?key|apikey|token|secret|passw|bearer|webhook)" \
  --include="*.py" --include="*.json" --include="*.js" --include="*.sh" \
  --include="*.plist" --include="*.txt" --include="*.cfg" --include="*.ini" .
```

Secrets hide in: source files, config files, Jupyter notebook outputs, `.plist` launchd
files, log files, SQLite databases, shell history helpers. Treat ALL of these as findings.

### 4. Refactor to the .env pattern

Every hardcoded secret becomes an environment variable:

```python
import os
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.environ["DISCORD_TOKEN"]   # no hardcoded fallback, ever
```

- Real values go into a `.env` file **that stays on this machine** (it's git-ignored)
- Update each project's `.env.example` with the variable names + fake placeholder values
- Update launchd plists/cron to still work after the refactor (they can keep running from
  their current location — the repo copy is for showcase; discuss with me if you think the
  repo should become the live deployment)
- **Verify the bots still run after refactoring** before moving on

### 5. Copy into the repo

- Discord bot code → `projects/discord-bots/` (one subfolder per bot if there are several)
- Trading bot code → `projects/trading-bot/`
- Do NOT copy: `.env`, databases, logs, `__pycache__`, virtualenvs, any file containing
  real account numbers, balances, or trade history I wouldn't want public
- Update `requirements.txt` files to match real dependencies

### 6. Write the real READMEs

Replace the `TODO(Matt)` blocks in both project READMEs with the truth: what each bot does,
what APIs it uses, how it's deployed (launchd on Mac mini), how long it's been running.
Interview-honest, no embellishment. Ask me for anything you can't infer from the code.

### 7. Final scan, then push

```bash
# install the scanner if needed: brew install gitleaks
gitleaks detect --source . -v          # must be clean
git status                             # confirm no .env / .db / logs staged
git add . && git commit -m "Migrate Discord bots and trading bot from Mac mini"
git push -u origin migrate-bots
```

Then tell me to open and merge the pull request on github.com.

## Hard rules (non-negotiable)

1. **Never commit a real secret.** If in doubt, it's a secret.
2. **If any key is ALREADY in any git history anywhere, tell me immediately — I must rotate
   it at the provider (Discord Developer Portal / broker dashboard) before we proceed.**
   Deleting the file in a new commit does not un-leak it.
3. Trading bot: paper-trading defaults only in committed code; never commit anything that
   could place live orders with embedded credentials.
4. Don't delete or move my original project folders — copy, don't relocate. The live bots
   must keep running.
5. Don't force-push, and don't push to any branch other than `migrate-bots` without asking.
6. If trade logs / P&L data are present, ask me before publishing any of it.
