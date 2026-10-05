---
title: 'Performance Optimization Guide'
status: 🟢 Stable
version: 'DB graph schema 65.33'
quadrant: 'How-To'
platform: 'Desktop + Mobile'
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
verifiedScope: 'Full audit 2026-09-10 (7490ff2964). The 2026-09-12 date covers only the Settings > Features platform gates (the Plugins row is web-only; HTTP server and Markdown Mirror are Electron-only).'
reviewStatus: audited
related-docs:
  - '[Database Integrity](../../explanation/architecture/database-integrity.md)'
  - '[File Organization](../../explanation/concepts/file-organization.md)'
  - '[Backup Strategies](../organization/backup-strategies.md)'
tags: ['performance', 'optimization', 'speed', 'troubleshooting']
---

# Performance Optimization Guide

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

This guide covers the tools Logseq actually provides for keeping a DB graph responsive: rebuilding the search index, keeping blocks under the size limit, garbage-collecting the SQLite file, validating the graph, and turning off features you do not use.

A DB graph is a single `db.sqlite` file plus an `assets/` folder under `~/logseq/graphs` ([`deps/db/src/logseq/db/common/sqlite.cljs:29-33`](../../../../../deps/db/src/logseq/db/common/sqlite.cljs); [`deps/common/src/logseq/common/config.cljs:58-59`](../../../../../deps/common/src/logseq/common/config.cljs)), not a folder of Markdown pages. Advice written for file graphs (re-indexing a folder, moving journal files, tuning file watchers) does not apply.

> Removed claim: earlier revisions of this page described Settings entries for **auto-save delay**, **number of loaded pages**, **batch size**, **clear cache**, **re-index**, **vacuum database**, **check database integrity**, **migrate schema**, **build search index** and **enable full-text search**, plus feature toggles for **graph view**, **whiteboards** and **PDF annotations**. None of these rows exists in the settings screens ([`src/main/frontend/components/settings.cljs:883-949`](../../../../../src/main/frontend/components/settings.cljs), [`:1141-1193`](../../../../../src/main/frontend/components/settings.cljs)). The real equivalents, where there are any, are below.

**What You'll Learn:**

- Where Logseq's real maintenance commands are
- The block size limit and what happens above it
- Garbage-collecting and validating a DB graph
- Which features can be switched off

---

## Table of Contents

1. [Performance Benchmarks](#performance-benchmarks)
2. [Quick Wins](#quick-wins)
3. [Graph Optimization](#graph-optimization)
4. [Database Tuning](#database-tuning)
5. [Asset Management](#asset-management)
6. [Memory Optimization](#memory-optimization)
7. [Platform-Specific Tips](#platform-specific-tips)
8. [Troubleshooting Slow Performance](#troubleshooting-slow-performance)

---

## Performance Benchmarks

### Measure Your Performance

**Graph Load Time:**

```markdown
1. Close Logseq completely
2. Note start time
3. Open Logseq
4. Graph fully loaded when:
   - Page list visible
   - Search returns results
   - Recent pages render
5. Calculate: time to full load
```

Measure before and after each change so you know whether it helped.

> Removed claim: a "Target Times" table (for example "< 1,000 blocks: < 3 seconds", "50,000+ blocks: 30-60 seconds") had no measurement or source behind it.

> Removed claim: "Settings → General" does not show total blocks, pages, assets or database size. The General section renders version, language, theme, font, title bar, accent colour and config-file rows ([`settings.cljs:883-899`](../../../../../src/main/frontend/components/settings.cljs)).

> Removed claim: the "Performance Test Commands" (`(count (d/datoms @conn :avet :block/uuid))` and similar) are ClojureScript, which the DevTools console does not evaluate. For inspecting data, turn on **Developer mode** and use the "(Dev) Show block data" / "(Dev) Show page data" commands ([`modules/shortcut/config.cljs:495-508`](../../../../../src/main/frontend/modules/shortcut/config.cljs)).

---

## Quick Wins

### Immediate Improvements

**1. Rebuild the Search Index**

```markdown
Command: "Rebuild search index"
Shortcut: mod+c mod+s

When: search results look stale or incomplete
Effect: rebuilds the search indices for the current graph
```

The command is `:search/re-index` ([`modules/shortcut/config.cljs:337-338`](../../../../../src/main/frontend/modules/shortcut/config.cljs); label at [`src/resources/dicts/en.edn:414`](../../../../../src/resources/dicts/en.edn)) and calls `search-handler/rebuild-indices!` ([`handler/search.cljs:109-118`](../../../../../src/main/frontend/handler/search.cljs)). There is no "Re-index" row in Settings.

**2. Clear Cache**

> Removed claim: "Settings → Advanced → Clear cache". The Clear cache row is commented out: its definition ([`settings.cljs:569-573`](../../../../../src/main/frontend/components/settings.cljs)) and its only use in `settings-advanced` ([`:943`](../../../../../src/main/frontend/components/settings.cljs)) are both `;;` comments.

**3. Update Logseq**

```markdown
Settings → General → current version (with the updater)
Settings → Advanced → auto update (desktop app, macOS and Windows)
```

([`settings.cljs:575-578`](../../../../../src/main/frontend/components/settings.cljs), [`:936`](../../../../../src/main/frontend/components/settings.cljs).)

**4. Disable Unused Plugins**

```markdown
Settings → Features → Plugins
```

The Plugins row switches the plugin system on or off ([`settings.cljs:790-793`](../../../../../src/main/frontend/components/settings.cljs); label "Plugins" at [`en.edn:1738`](../../../../../src/resources/dicts/en.edn)). **The row appears only in the web app**: it is rendered `(when (and web-platform? config/feature-plugin-system-on?) ...)` ([`settings.cljs:1166-1167`](../../../../../src/main/frontend/components/settings.cljs)), and `web-platform?` is false on desktop and on native mobile ([`util.cljc:128-130`](../../../../../src/main/frontend/util.cljc)). On desktop, disable plugins one at a time from the plugins page instead.

> Removed claim: the plugin-count impact figures ("3-5 plugins: Minimal … 20+ plugins: Significant slowdown") had no source.

**5. Reduce Auto-Save Frequency**

> Removed claim: "Settings → Editor → Auto-save delay (default 2000ms)". No such setting exists; the Editor section's rows are date format, brackets, wide mode, spell check, logical outdenting, auto-expand block refs, the tooltip toggles and all-pages-public ([`settings.cljs:900-929`](../../../../../src/main/frontend/components/settings.cljs)).

---

## Graph Optimization

### Structure Your Content

**1. Keep Blocks Under the Size Limit**

This is the one size threshold Logseq enforces. The config key `:block/title-max-length` defaults to `10000` characters; the template's comment says "Block content larger than `block/title-max-length` will not be searchable or editable for performance." ([`deps/common/resources/templates/config.edn:117-120`](../../../../../deps/common/resources/templates/config.edn)). It is read by `state/block-content-max-length`, which also accepts the older `:block/content-max-length` ([`src/main/frontend/state.cljs:703-708`](../../../../../src/main/frontend/state.cljs)).

Above the limit:

- the block shows "Large block will not be editable or searchable to not slow down the app, please use another editor to edit this block." unless it is a code block ([`components/block.cljs:3537-3540`](../../../../../src/main/frontend/components/block.cljs); text at [`en.edn:129`](../../../../../src/resources/dicts/en.edn));
- clicking the block does not enter editing ([`block.cljs:2735-2738`](../../../../../src/main/frontend/components/block.cljs)).

Split very long blocks into child blocks rather than raising the limit.

**2. Split Large Pages**

Pages with very many blocks take longer to render; split them into separate pages linked from an index page.

```markdown
Before:

- [[Projects]] (one very long page)

After:

- [[Projects]]
  - [[Work Projects]]
  - [[Personal Projects]]
  - [[Learning Projects]]
```

> Removed claim: "Pages with 500+ blocks slow rendering" and "Impact: 3-5x faster page rendering" had no measurement behind them.

**3. Use Namespaces Wisely**

In a DB graph, a title such as `Projects/Work/ClientA` is split and each segment becomes its own page under **Library** ([`deps/outliner/src/logseq/outliner/page.cljs:464-467`](../../../../../deps/outliner/src/logseq/outliner/page.cljs)). Deep paths therefore create many pages.

> Removed claim: namespaces do not provide "lazy loading (only load visible pages)"; nothing in the page-splitting code loads pages lazily.

**4. Optimize Queries**

`#+BEGIN_QUERY` is deprecated in DB graphs and renders a warning ([`components/block.cljs:5107-5108`](../../../../../src/main/frontend/components/block.cljs)). Create advanced queries with `/Advanced Query`, which tags the block `Query` and stores the query in a Clojure code property ([`src/main/frontend/commands.cljs:125-132`](../../../../../src/main/frontend/commands.cljs)).

**Slow Query** — matches every block that has a title:

```clojure
{:query [:find (pull ?b [*])
         :where [?b :block/title]]}
```

**Narrower Query** — only blocks whose Status is Todo. Status is a closed-value ref, so match its `:db/ident` ([`deps/db/src/logseq/db/frontend/property.cljs:313-318`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)):

```clojure
{:query [:find (pull ?b [*])
         :where
         [?b :logseq.property/status ?s]
         [?s :db/ident :logseq.property/status.todo]]}
```

**Query Performance Tips:**

```markdown
✅ Do:

- Add a selective clause first (a status, a tag, a specific page)
- Pull only the attributes you render instead of [*]

❌ Avoid:

- Queries that match every block
- Joins without a selective clause
- Many heavy queries on one page
```

Two corrections to earlier revisions:

- `:block/content` and `:block/marker` are not DB-graph attributes; use `:block/title` and `:logseq.property/status`.
- The `:7d-before` / `:30d-before` input style is deprecated. It is still accepted and converted by `resolve-keyword-input :DEPRECATED-relative-date` ([`deps/db/src/logseq/db/frontend/inputs.cljs:135-138`](../../../../../deps/db/src/logseq/db/frontend/inputs.cljs)).

> Removed claim: `simple-query? true` does not appear anywhere in `src/main/`, so it has no effect.

> **Unverified:** earlier revisions recommended `:limit 50` in the query map. Whether a top-level `:limit` key is honoured was not confirmed.

**5. Archive Old Content**

> Removed claim: the "archive-old-journals.sh" script concatenated and deleted `journals/*.md` files. In a DB graph, journals live in `db.sqlite`, not in a `journals/` folder. The optional Markdown Mirror only writes "a derived Markdown copy of edited pages to the graph's mirror/markdown folder" ([`en.edn:1733`](../../../../../src/resources/dicts/en.edn)). Moving or deleting Markdown files does not archive anything in the graph.

---

## Database Tuning

### Database Graph Performance

The database maintenance commands are **developer commands**. Turn on **Settings → Advanced → Developer mode** first (not shown in the native mobile app; [`settings.cljs:580-587`](../../../../../src/main/frontend/components/settings.cljs), [`:938`](../../../../../src/main/frontend/components/settings.cljs); label at [`en.edn:1703`](../../../../../src/resources/dicts/en.edn)). Each command below is inactive unless developer mode is on.

**1. Garbage-Collect the Graph (includes VACUUM)**

```markdown
Command: "(Dev) Garbage collect graph (remove unused data in SQLite)"

What it does:

- Full garbage collection of the SQLite key-value table
- Runs SQLite VACUUM
- Records the time in :logseq.kv/graph-last-gc-at
```

The command is `:dev/gc-graph` ([`modules/shortcut/config.cljs:485-488`](../../../../../src/main/frontend/modules/shortcut/config.cljs)) → `repo-handler/gc-graph!` ([`handler/repo.cljs:198-204`](../../../../../src/main/frontend/handler/repo.cljs)) → `:thread-api/gc-graph`, which calls `gc-kvs-table!` with `:full-gc? true`, executes `VACUUM` and stores `:logseq.kv/graph-last-gc-at` ([`worker/handler/maintenance.cljs:28-40`](../../../../../src/main/frontend/worker/handler/maintenance.cljs)).

> Removed claim: "⋯ → Settings → Advanced → Vacuum database", "Rebuilds indexes" and "Effect: 20-40% size reduction". There is no such settings row, and no figure for the size reduction exists in source.

**2. Validate the Graph**

```markdown
Command: "(Dev) Validate current graph"

When: after a crash, before an EDN export
```

The command is `:dev/validate-db` ([`modules/shortcut/config.cljs:522-525`](../../../../../src/main/frontend/modules/shortcut/config.cljs)). The EDN export runs the same kind of validation and refuses to export when it finds errors ([`handler/db_based/export.cljs:82-86`](../../../../../src/main/frontend/handler/db_based/export.cljs)).

> Removed claim: "Check database integrity — Runs: PRAGMA integrity_check — Fixes: Corrupted indexes". The string `integrity_check` does not appear in `src/main/`, `src/electron/` or `deps/*/src/`, and the command is described as validating the graph, not repairing it.

**3. Schema Version**

The current DB schema version is **65.34** ([`deps/db/src/logseq/db/frontend/schema.cljs:33`](../../../../../deps/db/src/logseq/db/frontend/schema.cljs)).

> Removed claim: `@(get-in @state/state [:db/schema-version])` ("should be 65.16 or higher") and "⋯ → Settings → Advanced → Migrate schema". `:db/schema-version` does not appear in `src/main/`, and there is no Migrate schema row in Settings.

**4. Reduce Transaction Size**

> Removed claim: "Settings → Editor → Batch size (Default: 100 blocks)". No such setting exists (see the Editor rows under [Quick Wins](#quick-wins)).

---

## Asset Management

### Optimize Media Files

Logseq saves pasted and uploaded files into the graph's assets directory itself (`db-based-save-assets!`, [`handler/editor.cljs:1837-1870`](../../../../../src/main/frontend/handler/editor.cljs); directory from `ensure-assets-dir!`, [`handler/assets.cljs:321-325`](../../../../../src/main/frontend/handler/assets.cljs)). Smaller files mean a smaller graph folder and smaller backups.

**1. Compress Images**

**Before Upload:**

```bash
# Compress PNG
for img in *.png; do
  pngquant --quality=60-80 --ext .png --force "$img"
done

# Compress JPEG
for img in *.jpg; do
  jpegoptim --max=85 "$img"
done
```

**2. Use Web Formats**

```markdown
Replace:

- PNG → WebP
- JPEG → WebP or AVIF
- GIF → MP4

Tools:

- cwebp (PNG/JPEG → WebP)
- ffmpeg (GIF → MP4)
```

> Removed claim: the size-reduction figures ("50-80%", "50% smaller", "10x smaller") had no source.

**3. External Asset Hosting**

An image link whose target starts with `http` is used directly as the image source ([`components/block.cljs:661-663`](../../../../../src/main/frontend/components/block.cljs)):

```markdown
![](https://example.com/abc123.png)
```

Trade-off: the image needs a network connection and depends on the external host.

**4. Clean Unused Assets**

> Removed claim: the "find-unused-assets.sh" script grepped `pages/` and `journals/` Markdown for `../assets/` links. A DB graph keeps its pages in `db.sqlite`, not in `pages/` and `journals/` folders, so the script would report every asset as unused.

**5. Asset Location**

Keep assets in the graph's own `assets/` directory, which is where Logseq writes them ([`deps/common/src/logseq/common/config.cljs:59`](../../../../../deps/common/src/logseq/common/config.cljs)).

> Removed claim: "Single location = faster indexing" and the `images/`, `pdfs/`, `videos/`, `audio/` subfolder layout. Neither claim had a source; Logseq writes new assets into `assets/` itself.

---

## Memory Optimization

### Reduce Memory Usage

**1. Check Memory Usage**

```markdown
macOS: Activity Monitor → Logseq
Windows: Task Manager → Logseq.exe
Linux: htop → logseq
```

> Removed claim: the "Typical" memory table ("Small graph: 200-400 MB … Very large: 1.5-3 GB") had no source.

**2. Reduce Loaded Pages**

> Removed claim: "Settings → Editor → Number of loaded pages". No such setting exists.

**3. Disable Features**

**Settings → Features** renders exactly these rows ([`settings.cljs:1141-1193`](../../../../../src/main/frontend/components/settings.cljs)):

```markdown
- Plugins (plugin system)   — web only
- HTTP server               — Electron only
- Markdown Mirror           — Electron only ("Write a derived Markdown copy of edited pages to the graph's mirror/markdown folder. Desktop only.", en.edn:1733)
- Flashcards
- Semantic search
```

Three of those rows are gated: Plugins on `web-platform?`, HTTP server and Markdown Mirror on
`(util/electron?)` ([`settings.cljs:1166-1171`](../../../../../src/main/frontend/components/settings.cljs)). So no build shows all five.

(Row functions `plugin-system-switcher-row`, `http-server-switcher-row`, `markdown-mirror-row`, `flashcards-switcher-row` and `semantic-search-row`.) Turn off the ones you do not use.

> Removed claim: toggles for graph view ("saves 100-200 MB"), whiteboards, PDF annotations and full-text search. None of these is a row in Settings → Features, and the memory savings had no source.

**4. Restart When Slow**

If Logseq has become slow after a long session, restart it and measure again.

> Removed claim: the ">2 GB" memory-leak threshold had no source.

**5. Close Unused Windows**

> Removed claim: "Cmd+W (close active tab)" for right-sidebar tabs. `mod+w` is `:window/close`, which closes the desktop app window, not a sidebar tab ([`modules/shortcut/config.cljs:454-456`](../../../../../src/main/frontend/modules/shortcut/config.cljs)). The "Each tab: 20-50 MB" figure had no source.

---

## Platform-Specific Tips

A DB graph is one SQLite file per graph ([`sqlite.cljs:29-33`](../../../../../deps/db/src/logseq/db/common/sqlite.cljs)), and the desktop app has no file watcher (no `chokidar` or `watcher` code under `src/electron/`). Tips aimed at many small Markdown files do not apply.

> Removed claim: raising Linux `inotify` watch limits and macOS `ulimit -n` "for Logseq". Both only matter to an app that watches many files; a DB graph does not.

> Removed claim: "RAM Disk for Cache — `ln -s /mnt/ramdisk ~/.logseq/cache`". `~/.logseq` is the desktop app's `dot-root`, which holds `graphs.edn` and installed plugins ([`src/electron/electron/configs.cljs:10`, `:16`](../../../../../src/electron/electron/configs.cljs); [`plugin.cljs:227`](../../../../../src/electron/electron/plugin.cljs)); nothing under `src/electron/` refers to a `cache` directory.

> **Unverified:** excluding `~/logseq/graphs` from antivirus real-time scanning (for example Windows Defender `Add-MpPreference -ExclusionPath`) or from Spotlight/Windows Search indexing may reduce disk contention while Logseq writes `db.sqlite` and hourly backups. This was not measured. Weigh it against your security policy.

> Removed claim: "Use APFS snapshots" and "Increase page file". Neither has anything to do with Logseq's code.

---

## Troubleshooting Slow Performance

### Common Issues

**1. Slow Graph Loading**

**Causes:**

- Very large graph
- Many plugins
- Slow disk

**Solutions:**

```markdown
1. Measure load time (see benchmarks)
2. Turn off plugins (Settings → Features → Plugins) and compare
3. Move ~/logseq/graphs to faster storage
4. Update Logseq
5. Split very large pages
```

**2. Laggy Typing**

**Causes:**

- A block near or above the 10,000-character limit
- Very long page
- Heavy queries on the page

**Solutions:**

```markdown
1. Split long blocks into child blocks
2. Split the page into smaller pages
3. Narrow or remove queries on the page
4. Restart Logseq
```

**3. Slow or Stale Search**

**Causes:**

- Out-of-date search index
- Blocks over the size limit (not searchable)

**Solutions:**

```markdown
1. Run "Rebuild search index" (mod+c mod+s)
2. Split oversized blocks
```

> Removed claim: "Fuzzy search enabled", "Use exact search (disable fuzzy)" and "Settings → Editor → Enable full-text search". There are no such settings.

**4. Slow Page Rendering**

**Causes:**

- Very many blocks on the page
- Complex queries
- Large images

**Solutions:**

```markdown
1. Split the page
2. Narrow queries (selective clause first)
3. Compress or remove large images
4. Replace deprecated {{embed}} macros with /Node embed
```

`{{embed}}` renders only a deprecation warning in a DB graph ([`components/block.cljs:1934-1935`](../../../../../src/main/frontend/components/block.cljs)).

**5. High CPU Usage**

**Causes:**

- A query that matches most of the graph
- A plugin bug
- Search index rebuild in progress

**Solutions:**

```markdown
1. Check queries on open pages
2. Turn off plugins
3. Wait for the index rebuild to finish
```

---

## Performance Checklist

**Before Optimization:**

- [ ] Measure current performance (load time, memory)
- [ ] Identify the slowest operations
- [ ] Back up the graph (Export SQLite DB)

**Quick Wins:**

- [ ] Rebuild search index
- [ ] Update Logseq
- [ ] Turn off unused plugins

**Graph Structure:**

- [ ] Split blocks near the 10,000-character limit
- [ ] Split very long pages
- [ ] Narrow queries
- [ ] Replace `#+BEGIN_QUERY` and `{{embed}}`

**Database (Developer mode):**

- [ ] (Dev) Validate current graph
- [ ] (Dev) Garbage collect graph

**Assets:**

- [ ] Compress images before adding them
- [ ] Host very large media externally if acceptable

**Memory:**

- [ ] Turn off unused features (Settings → Features)
- [ ] Restart Logseq when it becomes slow

**After Optimization:**

- [ ] Measure performance again
- [ ] Compare before/after

---

## Best Practices

### Daily Habits

```markdown
1. Keep blocks short; use child blocks
2. Keep pages focused
```

### Weekly Maintenance

```markdown
1. Review slow queries
2. Compress newly added large images
```

### Monthly Tasks

```markdown
1. Rebuild search index if search looks stale
2. (Dev) Validate current graph
3. (Dev) Garbage collect graph
4. Update Logseq
5. Review plugin usage
```

---

## Next Steps

**Optimize Your Graph:**

1. Run performance benchmarks
2. Apply quick wins
3. Fix oversized blocks and slow queries
4. Run database maintenance in developer mode
5. Measure again

**Related:**

- [Database Integrity](../../explanation/architecture/database-integrity.md)
- [File Organization](../../explanation/concepts/file-organization.md)
- [Backup Strategies](../organization/backup-strategies.md)
