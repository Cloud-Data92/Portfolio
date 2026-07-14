# GitHub From Zero — A Personal Field Guide

My working notes on how git and GitHub actually work, written assuming no prior knowledge.

---

## 1. Git vs GitHub (they are different things)

- **Git** is a program on your computer that tracks the history of a folder of files. Every "save point" you make is called a **commit**. Git works entirely offline — it's just history-tracking.
- **GitHub** is a website that hosts copies of git-tracked folders (**repositories**, or "repos") online, so you can back them up, share them, and show them to other people (like employers).

Think of it as: *git = track changes for code; GitHub = Google Drive for git projects, with a social/professional layer on top.*

## 2. The core objects

| Term | What it is |
|---|---|
| **Repository (repo)** | One project folder plus its entire history. My portfolio is one repo. |
| **Commit** | A snapshot of the repo at a moment in time, with a message ("Add SQL portfolio project"). The history is a chain of commits. |
| **Branch** | A parallel line of commits. `main` is the default. You create a branch to work on something without touching `main`, then merge it back when it's ready. |
| **Remote** | The copy of the repo on GitHub (named `origin` by convention). Your laptop has a **local** copy; you sync them explicitly. |
| **Clone** | Download a repo (and its whole history) from GitHub to your machine. |
| **Push / Pull** | Push = upload your new local commits to GitHub. Pull = download commits others (or you, from another machine) added. |
| **Pull Request (PR)** | A GitHub page that says "here are the commits on branch X — review them, then merge into main." Solo, it's optional but shows professionalism; in teams, it's how all code review happens. |
| **Fork** | Your own GitHub copy of someone else's repo, so you can modify it and propose changes back. |

## 3. The everyday workflow (90% of what you'll ever type)

```bash
git clone https://github.com/Cloud-Data92/Portfolio.git   # once: get the repo
cd Portfolio

# ... edit files ...

git status                        # what changed?
git add .                         # stage everything changed (choose files individually with git add <file>)
git commit -m "Describe the change"   # snapshot it locally
git push                          # upload to GitHub
```

And when coming back to a machine later: `git pull` first, so you start from the latest version.

## 4. Branches and merging, concretely

```bash
git checkout -b add-new-bot     # create + switch to a new branch
# ... commits ...
git push -u origin add-new-bot  # publish the branch to GitHub
```
Then on github.com a banner appears: "add-new-bot had recent pushes — **Compare & pull request**." Open the PR, read your own diff (great habit), click **Merge**. `main` now contains the work. Delete the branch.

## 5. What employers actually look at

1. **Your profile page** (github.com/Cloud-Data92) — the pinned repos and your profile README are the landing page. Pin your best 2–4 repos.
2. **README files** — most reviewers read READMEs and skim code. A clear README explaining *what it does, how to run it, and what skills it shows* matters more than perfect code.
3. **Commit history** — regular, well-described commits ("Add portfolio P&L query with CTE pipeline") read far better than one giant "upload files" commit. The green contribution graph shows consistency.
4. **The special profile repo:** a repo named exactly the same as your username (`Cloud-Data92/Cloud-Data92`) — its README.md displays on your profile page. Content for mine is prepared in [`profile-readme/`](profile-readme/).

## 6. Public vs private repos

- **Public** — anyone can *see* it (only you can *change* it). This is what you share with companies.
- **Private** — only you and people you invite. Good for work-in-progress or anything personal.
- You can flip a repo between the two in Settings at any time.

## 7. Keeping secrets out (the part that bites beginners)

The #1 beginner disaster: committing an API key to a public repo. Bots scrape GitHub for keys **within seconds** of a push — a leaked broker key or Discord token gets abused nearly instantly.

The defense (all implemented in this repo — see [SECURITY.md](SECURITY.md)):

1. Secrets go in a `.env` file; code reads them from the environment.
2. `.gitignore` lists `.env` so git *can't* commit it.
3. A committed `.env.example` documents what variables are needed, with fake values.
4. GitHub **push protection** (Settings → Advanced Security) blocks pushes containing recognizable secrets.
5. A **Gitleaks** CI workflow re-scans every push.
6. If a key ever leaks: **rotate it at the provider immediately.** Deleting the file later doesn't help — git history remembers everything.

## 8. GitHub Actions (the robots)

The `.github/workflows/` folder holds automation ("Actions") that GitHub runs for you on every push — free for public repos. This repo uses one to scan for leaked secrets. Teams use the same mechanism to run tests, deploy apps, etc. Green checkmark ✅ next to a commit = the automation passed.

## 9. Handy vocabulary you'll hear

- **`.gitignore`** — file listing patterns git should never track (secrets, caches, huge files)
- **Merge conflict** — you and another change touched the same lines; git asks a human to pick. Rare when working solo.
- **`git log`** — show the commit history. **`git diff`** — show unstaged changes.
- **Issues** — a repo's built-in to-do/bug tracker
- **Stars / Watch** — bookmarks/notifications on repos
- **Markdown (`.md`)** — the simple formatting language README files are written in (`# heading`, `**bold**`, `- bullet`)

## 10. My repo's layout as a worked example

```
Portfolio/
├── README.md                  ← landing page (GitHub renders this automatically)
├── GITHUB_GUIDE.md            ← this file
├── SECURITY.md                ← secrets policy + migration checklist
├── .gitignore                 ← blocks .env, *.db, caches, etc.
├── .github/workflows/         ← automation (secret scanning)
├── profile-readme/            ← content for the Cloud-Data92/Cloud-Data92 profile repo
└── projects/
    ├── sql-stock-analytics/   ← complete runnable project
    ├── discord-bots/          ← scaffolded, code migrating from Mac mini
    └── trading-bot/           ← scaffolded, code migrating from Mac mini
```
