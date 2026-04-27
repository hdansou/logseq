---
description: Generate a Logseq changelog from git history for a date range
argument-hint: <start-date> <end-date>
allowed-tools: Bash(git fetch:*), Bash(git log:*), Bash(git branch:*), Bash(git rev-parse:*), Write
---

Generate a professional Logseq changelog covering commits from **$1** to **$2**.

## Step 1 — Gather data

Run these in parallel:

1. `git fetch --all --quiet` to refresh remotes.
2. `git log origin/master --since=$1 --until=$2 --no-merges --pretty=format:'%H%x09%an%x09%cd%x09%s' --date=short` — these are **Released** commits (merged to master).
3. `git for-each-ref --format='%(refname:short)' refs/remotes/ | grep -v 'origin/HEAD\|origin/master'` — list non-master remote branches.
4. For each non-master branch from (3), run `git log <branch> --not origin/master --since=$1 --until=$2 --no-merges --pretty=format:'%H%x09%an%x09%cd%x09%s%x09<branch>' --date=short` — these are **Work in Progress** commits (on a branch, not yet on master).

Dedupe WIP commits by SHA across branches; if a commit appears on multiple branches, list it once and note the first branch you saw it on.

If either section is empty, omit the section entirely (don't print an empty heading).

## Step 2 — Categorize

For each commit, assign:

- A **primary category** (in this order): New Feature, Enhancement, Bug Fix, Performance, Refactoring, Chore.
- A **functional area** (secondary heading): Block editing, Graph visualization, Search and queries, Plugin system, Database operations, UI/UX, File handling, Sync and storage, Mobile, Developer tools, RTC, Whiteboards, PDF annotations, Settings.

Use conventional-commit prefixes when present (`feat:`, `fix:`, `perf:`, `refactor:`, `chore:`, `docs:`) but don't trust them blindly — read the subject line.

Skip: pure dependency bumps, version bumps, lockfile-only changes, merge commits, and trivial typo fixes — unless they're notable.

Combine related commits into one entry when they describe the same user-visible change.

## Step 3 — Write output

Write to `changelog_$1_to_$2.md` in the repo root with this structure:

```markdown
# Changelog ($1 to $2)

## Spotlight

[2-3 bullets highlighting the most user-impactful initiatives across BOTH released and WIP work. Lead with bold name, then one sentence on user benefit. Skip if the range is too small to warrant a spotlight.]

## Released

[Merged-to-master changes, grouped by primary category, then functional area. See formatting rules below.]

## Work in Progress

[Unmerged feature-branch changes, same structure. Add a one-line note at the top: "These changes are on feature branches and not yet shipped."]

## Contributors

[All unique authors from both sections, alphabetically sorted, comma-separated, prefixed with @.]
```

### Formatting rules

- Outliner-style with `-` bullets.
- Each entry: `- <user-facing description> ([<short-sha>](https://github.com/logseq/logseq/commit/<full-sha>))`
- Use the **full** SHA in the URL, the **7-char** SHA in the link text.
- Multiple commits in one entry: list each link comma-separated inside the parentheses.
- Present tense, active voice, user-facing language. "Fix caret position after undo" not "Fixed bug where caret position was incorrect after undo operation in editor.cljs".
- Don't surface internal refactors in user-facing categories — put them under Refactoring.
- For WIP entries on a non-obvious branch, optionally suffix the branch name in italics: `_(branch: feat/foo)_`.

## Step 4 — Report

After writing the file, print a one-line summary: how many commits in each section and the output path. Don't echo the full changelog back.
