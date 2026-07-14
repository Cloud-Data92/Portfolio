# Security Checklist — Keeping API Keys Out of GitHub

This repo follows a strict "no secrets in git, ever" policy. This document is both
the policy and the how-to, especially for migrating existing projects (e.g., from
my Mac mini) into this public portfolio.

---

## The Golden Rules

1. **Secrets live in `.env` files or environment variables — never in code.**
2. **`.env` is git-ignored.** Only `.env.example` (with FAKE placeholder values) gets committed.
3. **Scan before you push.** Gitleaks runs in CI on every push, but catching it locally first is better.
4. **If a secret ever touches a commit — even for one second — rotate it.** Deleting the file in a later commit does NOT remove it from git history.

---

## The Pattern Every Project Here Uses

**Bad (never do this):**
```python
DISCORD_TOKEN = "MTA4NzY1NDMyMTA5ODc2NTQzMjE.GaBcDe.FgHiJkLmNoPqRsTuVwXyZ"  # ← leaked forever
```

**Good:**
```python
import os
from dotenv import load_dotenv

load_dotenv()  # reads .env file (git-ignored)
DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]
```

With a committed `.env.example` so others know what's needed:
```
# .env.example — copy to .env and fill in real values. NEVER commit .env
DISCORD_TOKEN=your-discord-bot-token-here
```

---

## Checklist: Before Migrating a Project From the Mac mini

Run through this for the Discord bots and trading bot **before** `git add`:

- [ ] Search the project for hardcoded secrets:
  ```bash
  grep -rniE "(api[_-]?key|token|secret|passw|bearer)" --include="*.py" --include="*.json" --include="*.js" .
  ```
- [ ] Move every real value into a `.env` file; replace code references with `os.environ[...]`
- [ ] Create a `.env.example` with placeholder values
- [ ] Confirm `.gitignore` covers `.env`, `*.db`, logs, and any config with credentials
- [ ] Check for secrets hiding in: Jupyter notebook outputs, log files, database files, shell scripts, launchd/cron plist files
- [ ] Run a local scan: `gitleaks detect --source . -v` (install: `brew install gitleaks`)
- [ ] Only then: `git add`, `git commit`, `git push`

---

## GitHub-Side Protections (one-time setup)

In the repo on github.com → **Settings → Advanced Security** (or "Code security and analysis"):

- ✅ Enable **Secret scanning** — GitHub scans for known token formats and alerts you
- ✅ Enable **Push protection** — GitHub *blocks the push* if it detects a secret, before it ever becomes public

These are free for public repositories.

---

## If a Secret Leaks Anyway (incident response)

1. **Rotate the key immediately** — go to the provider (Discord Developer Portal, broker/exchange dashboard, etc.), revoke the old key, and generate a new one. This is the step that actually protects you; assume a leaked key was copied within minutes.
2. Remove it from git history (optional cleanup, AFTER rotating):
   ```bash
   # install: brew install git-filter-repo
   git filter-repo --replace-text <(echo "THE_LEAKED_VALUE==>REMOVED")
   git push --force
   ```
3. Check provider dashboards for any unauthorized usage.

Remember: rotation is mandatory, history-scrubbing is cosmetic.
