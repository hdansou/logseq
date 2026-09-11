---
title: 'Import and Export Guide'
status: 🟢 Stable
version: 'DB graph schema 65.33'
quadrant: 'How-To'
platform: 'Desktop + Mobile'
lastVerified: '2026-09-10'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
related-docs:
  - '[Backup Strategies](./backup-strategies.md)'
  - '[File vs Database Graphs](../../explanation/concepts/file-vs-db.md)'
tags: ['import', 'export', 'migration', 'data-portability', 'backup']
---

# Import and Export Guide

This guide shows you how to get data into a Logseq DB graph and export it back out. A DB graph is a single `db.sqlite` file plus an `assets/` folder, not a folder of Markdown files ([`deps/db/src/logseq/db/common/sqlite.cljs:29-33`](../../../../../deps/db/src/logseq/db/common/sqlite.cljs)), so export and import work on that database.

**What You'll Learn:**

- Exporting a whole graph (SQLite, SQLite + assets, EDN, Markdown, HTML, debug transit)
- Exporting a page or a selection of blocks (text, OPML, HTML, PNG)
- Importing into a new DB graph (SQLite, SQLite + assets, EDN, a Logseq file graph, debug transit)
- Automatic backups and the command-line tool

---

## Table of Contents

1. [Export Overview](#export-overview)
2. [Import Overview](#import-overview)
3. [Importing from Apps](#importing-from-apps)
4. [Export Formats](#export-formats)
5. [Database Graph Operations](#database-graph-operations)
6. [Automation](#automation)
7. [Troubleshooting](#troubleshooting)

---

## Export Overview

### Export Options

**Access:** Click **⋯ (More)** → **Export graph**. The menu item opens the `export` dialog ([`components/header.cljs:227-230`](../../../../../src/main/frontend/components/header.cljs); dialog at [`components/export.cljs:119`](../../../../../src/main/frontend/components/export.cljs)).

The dialog lists exactly these entries ([`components/export.cljs:125-153`](../../../../../src/main/frontend/components/export.cljs); labels at [`src/resources/dicts/en.edn:632-680`](../../../../../src/resources/dicts/en.edn)):

| Entry                                              | Output                                  | Shown                  | Handler |
| -------------------------------------------------- | --------------------------------------- | ---------------------- | ------- |
| **Export SQLite DB**                               | one `.sqlite` file                      | always                 | [`handler/export.cljs:146-176`](../../../../../src/main/frontend/handler/export.cljs) |
| **Export both SQLite DB and assets**               | `.zip` with `db.sqlite` and the assets  | always                 | [`handler/export.cljs:88-129`](../../../../../src/main/frontend/handler/export.cljs) |
| **Export EDN file**                                | one `.edn` file                         | not on mobile          | [`handler/db_based/export.cljs:79-100`](../../../../../src/main/frontend/handler/db_based/export.cljs) |
| **Export as standard Markdown (no block properties)** | `.zip` of `.md` files                | not in the native mobile app | [`handler/export/text.cljs:38-50`](../../../../../src/main/frontend/handler/export/text.cljs) |
| **Export public pages**                            | `index.html` plus assets                | desktop app only       | [`handler/export.cljs:30-48`](../../../../../src/main/frontend/handler/export.cljs) |
| **Export debug transit file**                      | one `.transit` file                     | always                 | [`handler/export.cljs:135-144`](../../../../../src/main/frontend/handler/export.cljs) |

There is **no JSON export and no graph-level OPML export**. A Roam JSON exporter (`<roam-data`, `export-repo-as-roam-json!`) still exists in source but sits entirely inside a `(comment …)` form under an ";; Export to roam json ;;" banner, so it is never compiled into a callable function ([`handler/export.cljs:174-202`](../../../../../src/main/frontend/handler/export.cljs)).

### Exporting a page or selected blocks

A page's menu has **Export page** ([`components/page_menu.cljs:137-141`](../../../../../src/main/frontend/components/page_menu.cljs)); the block context menu and the multi-block selection menu have a copy/export item ([`components/content.cljs:316-321`, `:118-125`](../../../../../src/main/frontend/components/content.cljs)). All three open `export-blocks` ([`components/export.cljs:275`](../../../../../src/main/frontend/components/export.cljs)), which offers **Text**, **OPML**, **HTML** and **PNG** output ([`components/export.cljs:317-333`](../../../../../src/main/frontend/components/export.cljs)). For text, HTML and OPML it shows these options ([`components/export.cljs:382-433`](../../../../../src/main/frontend/components/export.cljs); labels at [`en.edn:648-659`](../../../../../src/resources/dicts/en.edn)):

- `[[text]] -> text` (remove page-ref brackets)
- remove emphasis
- remove #tags
- newline after block
- remove properties

### EDN data commands

Four commands have no default key binding and are registered in the shortcut config ([`modules/shortcut/config.cljs:510-520`](../../../../../src/main/frontend/modules/shortcut/config.cljs); labels at [`en.edn:400-403`](../../../../../src/resources/dicts/en.edn)):

| Command | What it does | Source |
| ------- | ------------ | ------ |
| Export block EDN data | Exports the current block as EDN | [`handler/db_based/export.cljs:25`](../../../../../src/main/frontend/handler/db_based/export.cljs) |
| Export page EDN data | Copies the current page's EDN export to the clipboard | [`handler/db_based/export.cljs:49-58`](../../../../../src/main/frontend/handler/db_based/export.cljs) |
| Export graph's tags and properties EDN data | Copies the graph's classes and properties (`:graph-ontology`) to the clipboard | [`handler/db_based/export.cljs:60-70`](../../../../../src/main/frontend/handler/db_based/export.cljs) |
| Import EDN data | Opens a dialog to paste and import `sqlite.build` EDN data | [`handler/db_based/import.cljs:269-290`](../../../../../src/main/frontend/handler/db_based/import.cljs) |

### When to Export

**Regular Backups:**

- Before major changes
- Before migrations
- Alongside the automatic backups (see [Backups](#backups))

**Sharing:**

- Publish selected pages as HTML
- Hand a page or block to someone as text or OPML

---

## Import Overview

### Supported Import Sources

**Access:** Click **⋯ (More)** → **Import** (shown only when editing is enabled, [`components/header.cljs:232-235`](../../../../../src/main/frontend/components/header.cljs)), or **Import existing notes** on the graphs screen ([`components/repo.cljs:433-438`](../../../../../src/main/frontend/components/repo.cljs)). Both open the `importer` page, which offers exactly five sources ([`components/imports.cljs:558-635`](../../../../../src/main/frontend/components/imports.cljs); labels at [`en.edn:911-956`](../../../../../src/resources/dicts/en.edn)):

| Entry                       | Input                                              | Source |
| --------------------------- | -------------------------------------------------- | ------ |
| **SQLite**                  | a SQLite DB export of a Logseq graph               | [`imports.cljs:572-582`](../../../../../src/main/frontend/components/imports.cljs) → [`handler/db_based/import.cljs:104`](../../../../../src/main/frontend/handler/db_based/import.cljs) |
| **SQLite + assets (.zip)**  | a zip containing `db.sqlite` and an assets folder  | [`imports.cljs:584-595`](../../../../../src/main/frontend/components/imports.cljs) → [`import.cljs:123`](../../../../../src/main/frontend/handler/db_based/import.cljs) |
| **File to DB graph**        | a file-based Logseq graph folder (not on mobile)   | [`imports.cljs:597-611`](../../../../../src/main/frontend/components/imports.cljs) → [`:477-510`](../../../../../src/main/frontend/components/imports.cljs) |
| **Debug Transit**           | a debug transit file                               | [`imports.cljs:613-623`](../../../../../src/main/frontend/components/imports.cljs) → [`import.cljs:180`](../../../../../src/main/frontend/handler/db_based/import.cljs) |
| **EDN to DB graph**         | a DB graph's EDN export                            | [`imports.cljs:625-635`](../../../../../src/main/frontend/components/imports.cljs) → [`import.cljs:192-231`](../../../../../src/main/frontend/handler/db_based/import.cljs) |

**Every import creates a new DB graph.** Each entry first asks for a graph name and rejects an empty name or one that already exists ([`imports.cljs:74-145`](../../../../../src/main/frontend/components/imports.cljs), [`:500-510`](../../../../../src/main/frontend/components/imports.cljs)); there is no "import into the current graph" option on this page.

> Removed claim: earlier revisions listed Roam Research, Notion, Obsidian, Evernote, Bear, Apple Notes, OneNote, Google Keep and "Plain Files" as import sources, with "File → Import → Choose format" and "Roam JSON" / "Notion Markdown" / "Obsidian Vault" choices. None of these exist: the `importer` page has only the five entries above, and there is no Roam, Notion, Obsidian or OPML importer under `src/main/` or `deps/*/src/`.

### Import Methods

**Method 1: Import page** — the five entries above. Each produces a new DB graph.

**Method 2: Import EDN data command** — pastes `sqlite.build` EDN (see [EDN data commands](#edn-data-commands)).

**Method 3: Command line** — `graph import --type edn|sqlite` (see [Automation](#automation)).

> Removed claim: "Copy files to graph folder". A DB graph is a `db.sqlite` file, not a folder of Markdown pages, and file graphs cannot be opened: the graph list filters them out with the comment "use `config/db-based-graph?` to avoid loading old file graphs" ([`components/repo.cljs:320-323`](../../../../../src/main/frontend/components/repo.cljs)).

---

## Importing from Apps

**Logseq has no importer for other note-taking apps.** The only folder-based import is **File to DB graph**, and it accepts a *Logseq* file graph: it refuses any folder without `logseq/config.edn`, showing "Import failed as the file 'logseq/config.edn' was not found for a Logseq graph." ([`imports.cljs:495-498`](../../../../../src/main/frontend/components/imports.cljs); text at [`en.edn:938`](../../../../../src/resources/dicts/en.edn)). An Obsidian vault, a Notion export or an Evernote conversion is rejected as-is.

> **Unverified:** whether a folder of Markdown from another app converts usefully once it is laid out as a Logseq file graph with a `logseq/config.edn` was not tested.

> Removed claim: earlier revisions gave step-by-step Roam, Notion, Obsidian and Evernote procedures, including `sed` scripts that rewrite Obsidian `![[Page]]` embeds to `{{embed [[Page]]}}`. Besides depending on importers that do not exist, `{{embed}}` is deprecated in DB graphs and renders a warning ([`components/block.cljs:1917-1918`](../../../../../src/main/frontend/components/block.cljs)).

### From a Logseq file graph (File to DB)

File graphs cannot be opened directly ([`components/repo.cljs:320-323`](../../../../../src/main/frontend/components/repo.cljs)), so this import is how an existing file graph gets into Logseq. The result is a new DB graph; the original folder is only read.

**Step 1: Start the import**

1. **⋯ (More)** → **Import**
2. Choose **File to DB graph** ("Import a file-based Logseq graph folder into a new DB graph", [`en.edn:926-927`](../../../../../src/resources/dicts/en.edn))
3. Select the graph's root folder (the picker is a directory input, [`imports.cljs:605-611`](../../../../../src/main/frontend/components/imports.cljs))

**Step 2: Set options**

The `import-file-graph-dialog` ([`imports.cljs:171-267`](../../../../../src/main/frontend/components/imports.cljs)) has these fields; comma-separated lists are split and trimmed by `build-file-graph-worker-options` ([`imports.cljs:359-368`](../../../../../src/main/frontend/components/imports.cljs)) and read by the file-graph converter in `deps/graph-parser`:

| Field (label) | Form key | Effect | Reader |
| ------------- | -------- | ------ | ------ |
| New graph name | `graph-name` | Name of the new DB graph; defaults to the folder name | [`imports.cljs:481-483`](../../../../../src/main/frontend/components/imports.cljs) |
| Extract inline code snippets as child blocks | `extract-code-snippets?` | A block with code fences becomes a Code node; with this on, multiple fences are extracted into child blocks in order | [`graph_parser/exporter.cljs:2106-2113`](../../../../../deps/graph-parser/src/logseq/graph_parser/exporter.cljs) |
| Import all tags | `convert-all-tags?` | Converts every tag to a tag class | [`exporter.cljs:257-261`](../../../../../deps/graph-parser/src/logseq/graph_parser/exporter.cljs) |
| Import specific tags | `tag-classes` | Converts only the listed tags ("Tags are case insensitive"); `card` always converts | [`exporter.cljs:257-261`](../../../../../deps/graph-parser/src/logseq/graph_parser/exporter.cljs) |
| Remove inline tags ("Default behavior for DB graphs") | `remove-inline-tags?` | Strips converted `#tags` from block titles instead of rewriting them as refs | [`exporter.cljs:408-425`](../../../../../deps/graph-parser/src/logseq/graph_parser/exporter.cljs) |
| Import additional tags from property values | `property-classes` | Treats values of the listed properties as tags | [`exporter.cljs:1083-1093`](../../../../../deps/graph-parser/src/logseq/graph_parser/exporter.cljs) |
| Import tag parents from property values | `property-parent-classes` | Uses values of the listed properties as tag parents | [`exporter.cljs:1129`](../../../../../deps/graph-parser/src/logseq/graph_parser/exporter.cljs) |

(Form fields at [`imports.cljs:197-265`](../../../../../src/main/frontend/components/imports.cljs); labels at [`en.edn:911`, `:922`, `:924`, `:942`, `:945-947`, `:949`, `:955`](../../../../../src/resources/dicts/en.edn).)

**Step 3: Wait for the import**

`import-file-graph` creates the new graph with `repo-handler/new-db!` and hands the files to the worker ([`imports.cljs:444-476`](../../../../../src/main/frontend/components/imports.cljs)), where `:thread-api/import-file-graph` runs `gp-exporter/export-file-graph` ([`worker/db_core.cljs:1005-1007`, `:420`](../../../../../src/main/frontend/worker/db_core.cljs)).

**What gets read:**

- Only files ending in `.md`, `.markdown`, `.org`, `.js`, `.edn` or `.css`, plus everything under `assets/`; hidden files and folders, `logseq/bak`, `logseq/version-files` and `node_modules` are skipped ([`imports.cljs:34-59`](../../../../../src/main/frontend/components/imports.cljs), applied at [`:492-494`](../../../../../src/main/frontend/components/imports.cljs)).
- Org files are imported as Markdown: "Imported {1} org file(s) as markdown. Support for org files will be added later." ([`en.edn:941`](../../../../../src/resources/dicts/en.edn)).
- Assets over 100 MB are skipped ([`imports.cljs:336`](../../../../../src/main/frontend/components/imports.cljs); [`en.edn:912`](../../../../../src/resources/dicts/en.edn)).

**When it finishes**, notifications report ignored files, ignored assets, ignored properties and invalid blocks ([`imports.cljs:269-290`](../../../../../src/main/frontend/components/imports.cljs); texts at [`en.edn:931-935`](../../../../../src/resources/dicts/en.edn)). For ignored properties the app's advice is "To fix a property type, change the property value to the correct type and reimport the graph."

---

## Export Formats

### SQLite DB

**Best For:** Complete backups. The in-app description is "Primary way to backup graph's content to a single .sqlite file." ([`en.edn:678`](../../../../../src/resources/dicts/en.edn)).

**Export:** ⋯ → Export graph → **Export SQLite DB**. The desktop app writes the file through IPC and reports the saved path; in the browser it uses the file save picker where available ([`handler/export.cljs:146-176`](../../../../../src/main/frontend/handler/export.cljs)).

**Import:** Import page → **SQLite** (new graph).

### SQLite DB and assets (.zip)

**Best For:** Backups that include images and other files ("Primary way to backup graph's content and assets to a .zip file.", [`en.edn:680`](../../../../../src/resources/dicts/en.edn)).

The zip holds `db.sqlite` plus every asset returned by `assets-handler/<get-all-assets` ([`handler/export.cljs:88-100`](../../../../../src/main/frontend/handler/export.cljs)).

**Import:** Import page → **SQLite + assets (.zip)** (new graph).

### EDN Export

**Best For:** A readable, editable dump. The in-app description warns: "Exports to a readable and editable .edn file. Don't rely on this as a primary backup." ([`en.edn:635`](../../../../../src/resources/dicts/en.edn)).

**Export:** ⋯ → Export graph → **Export EDN file** (not on mobile). The graph is validated first and the export aborts with "Graph validation failed" if validation reports errors ([`handler/db_based/export.cljs:82-86`](../../../../../src/main/frontend/handler/db_based/export.cljs)). It then calls `build-export` with `:export-type :graph` ([`:87-90`](../../../../../src/main/frontend/handler/db_based/export.cljs)), which `build-export` maps to `build-graph-datoms-export` ([`deps/db/src/logseq/db/sqlite/export.cljs:1116-1117`](../../../../../deps/db/src/logseq/db/sqlite/export.cljs)).

**Import:** Import page → **EDN to DB graph**. The file must parse as EDN ("The given EDN file is not valid EDN. Please fix and try again.", [`en.edn:936`](../../../../../src/resources/dicts/en.edn)) and pass `sqlite-export/validate-export` before a new graph is created ([`handler/db_based/import.cljs:200-214`](../../../../../src/main/frontend/handler/db_based/import.cljs)).

> Removed claim: earlier revisions showed the file's contents as `{:schema-version 65.16 :pages [...] :blocks [...] :transactions [...]}` with `:block/properties`, and offered "Create new graph OR Import to existing". That shape was invented, and the Import page only creates a new graph.

### Markdown Export

**Best For:** Reading pages outside Logseq. The label itself says "(no block properties)".

**Export:** ⋯ → Export graph → **Export as standard Markdown (no block properties)** (not in the native mobile app).

**Output structure:** one flat zip named `<graph>_markdown_<unix seconds>`, containing one `<page title>.md` file per page ([`handler/export/text.cljs:41-46`](../../../../../src/main/frontend/handler/export/text.cljs); file paths built at [`handler/export/common.cljs:44-48`](../../../../../src/main/frontend/handler/export/common.cljs)). There are no `pages/`, `journals/` or `assets/` folders and no export options — the function's docstring is "TODO: indent-style and remove-options" ([`text.cljs:39`](../../../../../src/main/frontend/handler/export/text.cljs)).

> Removed claim: earlier revisions listed "Include assets / Include journals / Include pages" options and a `pages/`, `journals/`, `assets/images/` output tree with `property:: value` lines. None of that is in the exporter.

### HTML Export

**Best For:** Publishing a read-only site from selected pages.

**Export:** ⋯ → Export graph → **Export public pages** (desktop app only). The worker builds the HTML with `:thread-api/build-publishing-html`, and the desktop app writes `index.html` and the used assets with `exportPublishAssets` ([`handler/export.cljs:30-48`](../../../../../src/main/frontend/handler/export.cljs)).

**Which pages are included:** if the graph config sets `:publishing/all-pages-public?` (or the older `:all-pages-public?`), the whole graph is exported; otherwise only pages whose `:logseq.property/publishing-public?` is `true`, plus what they directly depend on ([`deps/publishing/src/logseq/publishing/html.cljs:141-147`](../../../../../deps/publishing/src/logseq/publishing/html.cljs); [`deps/publishing/src/logseq/publishing/db.cljs:9-17`](../../../../../deps/publishing/src/logseq/publishing/db.cljs)).

This is the `deps/publishing` library, which "handles exporting the `frontend.publishing` single page application" (its README). It is separate from `deps/publish`, the "Shared library for page publish features" used by server-side page publishing.

> Removed claim: earlier revisions listed "Theme / Include search / Include graph view" options, a `pages/*.html` + `graph.json` output tree, and a `logseq/custom.css` customization step. None of these is read by the HTML export path above.

### OPML and PNG

OPML and PNG exist only in the page/block `export-blocks` dialog ([`components/export.cljs:323`, `:333`](../../../../../src/main/frontend/components/export.cljs)), not as whole-graph exports.

### Debug Transit

"Exports to a .transit file to send to us for debugging. Any sensitive data will be removed in the exported file." ([`en.edn:633`](../../../../../src/resources/dicts/en.edn)). The Import page's **Debug Transit** entry loads one into a new graph.

---

## Database Graph Operations

### Backups

**Desktop app:** automatic backups run hourly and keep 12 versions (`backup-interval-ms` = one hour, `automatic-backup-keep-versions` = 12, [`src/electron/electron/db.cljs:15-18`](../../../../../src/electron/electron/db.cljs); `run-auto-backup!` at [`:117`](../../../../../src/electron/electron/db.cljs)). They are written under the graph's `backup/` directory ([`src/main/logseq/db_worker/graph_backup.cljs:10`, `:39-47`](../../../../../src/main/logseq/db_worker/graph_backup.cljs)), i.e. `~/logseq/graphs/<graph>/backup/` ([`frontend/config.cljs:326-335`](../../../../../src/main/frontend/config.cljs)). The Export dialog shows "Hourly backups are enabled for this graph" with a link that opens that folder ([`components/export.cljs:155-164`](../../../../../src/main/frontend/components/export.cljs); [`en.edn:672`, `:675`](../../../../../src/resources/dicts/en.edn)).

**Browser (desktop web, not mobile):** the Export dialog shows an auto-backup section ([`components/export.cljs:165-169`](../../../../../src/main/frontend/components/export.cljs)). After you choose a backup folder, `auto-db-backup!` backs up every hour ([`handler/export.cljs:287-299`](../../../../../src/main/frontend/handler/export.cljs)): it writes `db.sqlite` into that folder, moves the previous copy into a `backups/` subfolder, and keeps the latest 12 versioned files ([`handler/export.cljs:229-275`](../../../../../src/main/frontend/handler/export.cljs), [`:204-212`](../../../../../src/main/frontend/handler/export.cljs)).

### Export Database Graph

**Full export:** ⋯ → Export graph → **Export SQLite DB** or **Export both SQLite DB and assets**. The EDN export is readable but the app itself says not to rely on it as a primary backup.

**Partial export (Markdown):** ⋯ → Export graph → **Export as standard Markdown (no block properties)**, for reading outside Logseq. Properties are not included.

### Import to Database Graph

**From SQLite / zip / EDN:** Import page → the matching entry → enter a new graph name. The import always creates that new graph.

**From Markdown:** only as a Logseq file graph, through **File to DB graph** (see [above](#from-a-logseq-file-graph-file-to-db)).

> Removed claim: "Create file-based graph first → ⋯ → Convert to database graph". File graphs cannot be opened ([`components/repo.cljs:320-323`](../../../../../src/main/frontend/components/repo.cljs)), so there is no open file graph to convert; the conversion path is the File to DB import.

### Migrate File-Based → Database

**Process:**

```markdown
Step 1: Back up the file graph folder (copy it)

Step 2: ⋯ → Import → File to DB graph

- Select the file graph's root folder (it must contain logseq/config.edn)
- Enter a name for the new DB graph
- Choose tag and code-snippet options

Step 3: Wait for the import and read the notifications

- Ignored files, assets and properties
- Invalid blocks

Step 4: Verify in the new graph

- Page count
- Assets load
- Tags and properties
```

**What Changes:**

| Feature        | File-Based    | Database                                   |
| -------------- | ------------- | ------------------------------------------ |
| **Storage**    | `.md` files   | one `db.sqlite` plus `assets/`             |
| **Properties** | `key:: value` | typed properties, added with `/Add property` |
| **Tasks**      | `TODO`/`DOING` markers | the Status property (`/Todo`, `/Doing`, …) |

([`deps/db/src/logseq/db/common/sqlite.cljs:29-33`](../../../../../deps/db/src/logseq/db/common/sqlite.cljs); [`src/main/frontend/commands.cljs:354-358`](../../../../../src/main/frontend/commands.cljs); status values at [`deps/db/src/logseq/db/frontend/property.cljs:313-318`](../../../../../deps/db/src/logseq/db/frontend/property.cljs).)

> Removed claim: earlier rows compared performance ("Slower/Faster"), sync ("Git/cloud" vs "Logseq Sync") and queries ("Simple/Advanced") with no source behind them.

---

## Automation

### Command-line export and import

The `cli/` package (installed as `logseq-cli.js`, [`cli/bin/dune`](../../../../../cli/bin/dune)) has graph export, import and backup commands:

| Command | Options | Source |
| ------- | ------- | ------ |
| `graph export` | `--type edn\|sqlite` (required), `--file`, `--edn-options`, `--pretty-print` | [`cli/lib/cli_parse.ml:188-189`, `:872-891`](../../../../../cli/lib/cli_parse.ml) |
| `graph import` | `--type edn\|sqlite` (required), `--input` (required) | [`cli_parse.ml:190`, `:893-902`](../../../../../cli/lib/cli_parse.ml) |
| `graph backup create` / `restore` / `remove` | `--name` / `--src --dst` / `--src` | [`cli_parse.ml:184-187`](../../../../../cli/lib/cli_parse.ml) |

The accepted `--type` values are exactly `edn` and `sqlite` ([`cli/lib/graph.ml:85-93`](../../../../../cli/lib/graph.ml)); anything else fails with "graph export --type must be edn or sqlite". `--graph` is a global option ([`cli_parse.ml:137-140`](../../../../../cli/lib/cli_parse.ml)).

> **Unverified:** the script below was assembled from the option parser and was not run. Check the CLI's help output for how the binary is invoked on your system and where `--graph` goes.

```bash
#!/bin/bash
# auto-export.sh - export a DB graph as SQLite and EDN

GRAPH_NAME="my-graph"
DATE=$(date +%Y-%m-%d)
BACKUP_DIR="$HOME/Backups/Logseq/$DATE"
mkdir -p "$BACKUP_DIR"

logseq-cli --graph "$GRAPH_NAME" graph export --type sqlite --file "$BACKUP_DIR/$GRAPH_NAME.sqlite"
logseq-cli --graph "$GRAPH_NAME" graph export --type edn --file "$BACKUP_DIR/$GRAPH_NAME.edn"

echo "Export completed: $DATE"
```

**Schedule with Cron:**

```bash
# Run daily at 2 AM
0 2 * * * /path/to/auto-export.sh >> ~/logseq-export.log 2>&1
```

> Removed claim: earlier revisions used `logseq-cli export --format json|markdown` and `--namespace`, `--from/--to` and `--exclude-journals` flags. The CLI's export takes only `--type edn|sqlite` plus the options in the table above.

### Programmatic Export (Plugin)

> Removed claim: earlier revisions showed `logseq.api.export_graph({format, path})`. No `export_graph` (or `exportGraph`) exists in `libs/` or `src/main/`; the plugin API has no graph export call.

---

## Troubleshooting

### Import Issues

**Problem: "Import failed as the file 'logseq/config.edn' was not found for a Logseq graph."**

File to DB graph only accepts a Logseq file graph's root folder ([`imports.cljs:495-498`](../../../../../src/main/frontend/components/imports.cljs)). Select the folder that contains `logseq/`, not `pages/` or a folder from another app.

**Problem: "Please specify another name as another graph with this name already exists!"**

Every import creates a new graph; pick an unused name ([`en.edn:929`](../../../../../src/resources/dicts/en.edn)).

**Problem: "Import ignored N files"**

Only `.md`, `.markdown`, `.org`, `.js`, `.edn` and `.css` files (plus `assets/`) are read ([`imports.cljs:34-59`](../../../../../src/main/frontend/components/imports.cljs)). The JavaScript console lists what was skipped.

**Problem: "Import ignored N properties"**

"To fix a property type, change the property value to the correct type and reimport the graph" ([`en.edn:933-934`](../../../../../src/resources/dicts/en.edn)).

**Problem: "Import detected N invalid block(s)"**

"These blocks may be buggy when you interact with them. See the javascript console for more." ([`en.edn:935`](../../../../../src/resources/dicts/en.edn)).

**Problem: an asset is missing after import**

Assets over 100 MB are skipped with "Skipped copying asset … because it exceeds the 100 MB limit." ([`en.edn:912`](../../../../../src/resources/dicts/en.edn)).

**Problem: "The given EDN file is not valid EDN."**

The EDN import reads the whole file with `edn/read-string` and stops on a parse error, or on a `validate-export` error, before creating the graph ([`handler/db_based/import.cljs:200-214`](../../../../../src/main/frontend/handler/db_based/import.cljs)).

### Export Issues

**Problem: EDN export fails**

The EDN export validates the graph first and throws "Graph validation failed" when validation reports errors ([`handler/db_based/export.cljs:82-86`](../../../../../src/main/frontend/handler/db_based/export.cljs)). Use **Export SQLite DB** for a backup in the meantime.

**Problem: HTML export is missing pages**

Only pages with `publishing-public?` set to true are exported unless `:publishing/all-pages-public?` is set ([`deps/publishing/src/logseq/publishing/html.cljs:141-147`](../../../../../deps/publishing/src/logseq/publishing/html.cljs)).

**Problem: the Markdown export ignores my export options**

The graph-level Markdown export has no options wired in (docstring "TODO: indent-style and remove-options", [`handler/export/text.cljs:39`](../../../../../src/main/frontend/handler/export/text.cljs)). The text options apply only to the page/block export dialog.

**Problem: there is no "Export as JSON" / "Export to Roam"**

Correct — neither exists. The Roam JSON exporter is inside a `(comment …)` form ([`handler/export.cljs:180`](../../../../../src/main/frontend/handler/export.cljs)).

---

## Best Practices

### Before Export/Import

**Checklist:**

- [ ] Back up the current graph (Export SQLite DB, or SQLite + assets)
- [ ] Test on a small sample first
- [ ] Check available disk space
- [ ] Update Logseq to the latest release

### During Migration

**Steps:**

1. Copy the file graph folder
2. Run File to DB graph on a copy first
3. Read the import notifications
4. Verify all content is present
5. Fix ignored properties and reimport if needed
6. Keep the old folder until you are satisfied

### After Import

**Validation:**

```markdown
1. Check page count

   - Compare source vs. destination

2. Verify assets

   - Images load
   - PDF attachments open
   - Nothing was skipped for size (100 MB limit)

3. Test features

   - Block references work
   - Queries return results
   - Templates insert
```

---

## Next Steps

**Complete Your Migration:**

1. Choose the import or export entry
2. Test on sample data
3. Run the full import or export
4. Verify and fix issues
5. Document your process
