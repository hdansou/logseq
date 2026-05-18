# Changelog & What's New — Style Guide

Practical rules learned across the 2026-04→05 release cycles. Read this before drafting the next changelog.

## Audiences and artifacts

Three artifacts ship from each cycle. They share content but not voice or detail.

| File                              | Audience           | Tone                  | Char budget |
|-----------------------------------|--------------------|-----------------------|-------------|
| `changelog_START_to_END.md`       | Engineers, power users | Categorized, dense    | **≤ 32,000** (forum hard cap) |
| `whats-new-END.md`                | Community at large | Narrative, "in it for you" | ~16,000 (forum is comfortable) |
| `discord-END.md`                  | Discord            | Punchy, scannable     | **≤ 2,000** (single message) |

## Voice — active, no fluff

Everything below has actually appeared in our drafts; cut it on sight.

- **Drop "so users can…" / "so that…" / "to keep…" clauses** when the verb already implies it.
  - ❌ "Add `qmd query` so scripts can run Logseq queries from the terminal."
  - ✅ "`qmd query` runs Logseq queries from the terminal."
- **Drop "now" and "no longer"** hedges.
  - ❌ "Snapshot payloads are now capped so they no longer stall sync."
  - ✅ "Snapshot payloads cap; oversized payloads drop."
- **Lead with the verb, not "Improve/Fix X to do Y".**
  - ❌ "Fix shortcut handling to open missing journal pages."
  - ✅ "Keyboard shortcut creates missing journal pages."
- **Combine related fixes with semicolons.** Three loosely-related fixes on one subsystem read better as one semicolon-joined bullet than three terse ones.

## Hard rule: char budget is a precondition, not a post-edit

Don't draft long and trim later. Check `wc -c` after the first pass. Coming in at 38k and rewriting to 32k cost ~2 hours one cycle.

## Section taxonomy

The categorized changelog uses this fixed structure. New subsections only when a conventional-commit *scope* appears 3+ times in the window and doesn't fit an existing one.

```
## Spotlight                              # 4–6 bullets, highest-impact only
## Currently Available
  ### New Feature
    #### Sync and storage
    #### Developer tools
    #### Plugin system
    #### File handling
    #### Settings and configuration
    #### Media
    #### Window management
    #### Properties
  ### Enhancement                         # same subsections as needed
  ### Bug Fix
    #### Sync and storage
    #### Search and queries
    #### Database operations
    #### UI/UX
    #### Properties
    #### Recurring tasks
    #### Graph visualization
    #### Developer tools
    #### Mobile
    #### File handling
    #### Import and export
    #### Publishing
    #### Plugin system
  ### Performance
  ### Refactoring
  ### Chore
## Upcoming                               # feature branches, not shipped
## Contributors
```

## Conventional-commit prefix → section mapping

The `changelog-extend.sh` script applies this automatically. Override during human review when needed.

| Prefix                  | Section                |
|-------------------------|------------------------|
| `feat:`                 | New Feature            |
| `enhance:`              | Enhancement            |
| `fix:`                  | Bug Fix                |
| `perf:`                 | Performance            |
| `refactor:`             | Refactoring            |
| `chore:` `dev:` `ci:` `test:` `docs:` | Chore   |

## Scope → subsection mapping

| Scope                              | Subsection             |
|------------------------------------|------------------------|
| `cli` `dev` `ci` `e2e`             | Developer tools        |
| `ui` `ux` `editor` `header` `fsrs` `pdf` `breadcrumb` `i18n` | UI/UX |
| `graph-view`                       | Graph visualization    |
| `sync` `assets` `asset`            | Sync and storage       |
| `plugin` `plugins`                 | Plugin system          |
| `publish`                          | Publishing             |
| `mobile` `ios` `android`           | Mobile                 |
| `search` `query`                   | Search and queries     |
| `db` `database`                    | Database operations    |
| `exporter` `export` `import`       | Import and export      |
| `property` `properties`            | Properties             |

## Always skip

Don't fold these into the community changelog. The `EXCLUDE.txt` patterns catch them automatically; this is the reasoning.

- `^enhance\(skill\):` — internal `.agents/skills/` updates; community doesn't run those.
- `^chore: remove debug log$` / `^add download debug log$` — workflow noise.
- `^fix lint$` / `^Fix lint$` — auto-fix CI.
- `^Potential fix for pull request finding` — Copilot Autofix CI.
- `^\[codex\] [Pp]otential` — codex auto-rebase chatter.
- `^Update logseq task-on-lambda` — agent skill workflow.

If 5+ similar internal commits land in one window, collapse them into one summary bullet under Enhancement → Developer tools (e.g. "CLI agent skills refined; new X, Y, Z added").

## Spotlight rules

- **4–6 bullets, never more.** If you can't pick six, the release isn't done.
- **PRs over commits.** A PR is usually a coherent unit; a direct commit usually isn't worth Spotlight space.
- **One bullet per theme**, even if the theme spans 10 commits. Cite 3–4 anchor links.
- **Lead with what it is, then why it matters.** "Markdown Mirror — read-only on-disk projection of DB graphs that updates as you edit."

## What's New (narrative) rules

- **One H2 per coherent story.** Cap at ~13 sections — more becomes a list, not a narrative.
- **"What's in it for you" callouts** only when the user-side value isn't obvious from the prose.
- **TL;DR at the end** with concrete "try this" instructions.
- **End with three links**: forum changelog thread, full categorized changelog, bug-report tracker (`https://github.com/logseq/db-test/issues`).

## Discord rules

- **Wrap every URL in `<>`** to suppress Discord's auto-preview cards. Three previews shoves the message off-screen.
- **No `#` headers** — Discord renders them huge. Use `**bold**` for the title line.
- **Lead with announcements, then features**, then links. Discord readers don't scroll.

## Link format

- **PR**: `[PR #12506](https://github.com/logseq/logseq/pull/12506)`
- **Commit**: `[abc1234567](https://github.com/logseq/logseq/commit/abc1234567)` — full 10-char hash
- For Spotlight and multi-link bullets, prefer PR links over commit hashes (PRs are durable; commits are anonymous).
- If a bullet has 5+ commit hashes, collapse to `[first](url), [last](url) and related`.

## Contributors

- Lowercased duplicates (`charlie`, `megayu`) collapse to the canonical name (`Charlie`, `Mega Yu`) — see `dz/scripts/authors.txt`.
- Order alphabetically.
- Footer line: `Total analyzed: N non-merge commits on master, M on feature branches, K merged pull requests`. Re-run the count when extending the window.

## Forum cross-links

End every published changelog/What's New with:

- Full categorized changelog: `https://discuss.logseq.com/t/logseq-db-changelog/30013`
- Community What's New: `https://discuss.logseq.com/t/whats-new-with-logseq-db-<date>/<id>` (new ID per post)
- Bug tracker: `https://github.com/logseq/db-test/issues`
