---
title: 'File vs Database Graphs'
description: 'Understanding the differences between file-based and database-backed graphs in Logseq'
status: stable
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
reviewStatus: audited
category: concepts
relatedDocs:
  reference:
    - '[Database Schema](../../reference/database/schema-overview.md)'
  how-to:
    - '[Import and Export](../../how-to/organization/import-export.md)'
  explanation:
    - '[RTC vs File Sync](../comparisons/rtc-vs-file-sync.md)'
    - '[Local-First Architecture](local-first-architecture.md)'
    - '[Data Architecture](../architecture/data-architecture.md)'
---

# File vs Database Graphs

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

> **Last revised:** 2026-09-11 (verified against `be800f1711`)

> Understanding the two storage architectures in Logseq and why the current codebase has settled on one of them.

## Overview

Logseq has had two ways of storing a knowledge graph:

| Aspect                  | File-Based                | Database-Based                       |
| ----------------------- | ------------------------- | ------------------------------------ |
| **Storage**             | Markdown/Org files        | SQLite database (`db.sqlite`)        |
| **Repo-name prefix**    | `logseq_local_`           | `logseq_db_`                         |
| **External editing**    | ✅ The files *are* the graph | ⚠️ One-way Markdown Mirror only (desktop) |
| **Typed properties**    | ❌ Text only              | ✅ Full type system                  |
| **Listed in the app**   | ❌ Filtered out           | ✅                                   |

Both prefixes are defined in
[`deps/common/src/logseq/common/config.cljs:33-34`](../../../../../deps/common/src/logseq/common/config.cljs).

> **Important:** at this revision the graph switcher only lists DB graphs. The repo list is
> filtered with `config/db-based-graph?`. The comment in the source reads
> "use `config/db-based-graph?` to avoid loading old file graphs"
> ([`src/main/frontend/components/repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs)).
> The CLI server also ignores `logseq_local_`-prefixed graph directories
> (`ignored-graph-dir?`, [`src/main/logseq/cli/server.cljs:345-349`](../../../../../src/main/logseq/cli/server.cljs),
> applied at `:562` and `:570`).
> File graphs therefore survive in this codebase mainly as an **import source** (`deps/graph-parser/`),
> not as a graph type you can open next to a DB graph. Read everything below as
> "what the two models are", not as a live product choice.

---

## File-Based Graphs

### How they worked

A file graph was a folder of plain-text files, and the files were the source of truth. Logseq
parsed them into DataScript. That parser, `deps/graph-parser`, now drives the importer:

```
my-graph/
├── pages/
│   ├── project-ideas.md
│   └── reading-list.md
├── journals/
│   ├── 2026-01-29.md
│   └── 2026-01-28.md
├── assets/
│   └── image.png
└── logseq/
    ├── config.edn            # required: the importer refuses a folder without it
    ├── custom.css
    ├── bak/                  # file-graph housekeeping
    └── version-files/        # file-graph housekeeping
```

Each page was a separate `.md` (or `.org`) file, and structure and metadata lived in the text itself:

```markdown
- TODO Draft the proposal
  priority:: high
- A child block referencing ((6650a8e2-…)) by block ref
- Another block with a [[link]]
```

- **Properties** were `key:: value` lines, stored as text.
- **Tasks** were leading markers (`TODO`, `LATER`, `NOW`, `DOING`, `DONE`…).
- **Block references** used `((uuid))`.

`logseq/bak` and `logseq/version-files` were file-graph working directories. The file-graph
importer skips them
([`src/main/frontend/components/imports.cljs:34-38`](../../../../../src/main/frontend/components/imports.cljs)),
and so does the shared ignore list
([`deps/common/src/logseq/common/graph.cljs:73,84`](../../../../../deps/common/src/logseq/common/graph.cljs)).

> **Removed:** "Git works perfectly with file graphs", Git/Dropbox/iCloud/Syncthing setup
> recipes, and "Choose file-based if…" advice. They describe a graph type the current app will
> not open (see [Overview](#overview)). File Sync was deleted in `bcc478b5f7`; see
> [RTC vs File Sync](../comparisons/rtc-vs-file-sync.md).

---

## Database Graphs

### How they work

A DB graph is a single SQLite file. On desktop (and for the CLI's `db-worker-node`) graphs live
under `~/logseq/graphs`
(`default-graphs-dir`, [`deps/common/src/logseq/common/config.cljs:58`](../../../../../deps/common/src/logseq/common/config.cljs)):

```
~/logseq/graphs/my-graph/     # note: the on-disk dir name has NO logseq_db_ prefix
├── db.sqlite                 # main database (PRAGMA journal_mode=WAL at open)
├── backup/                   # automatic desktop backups
├── mirror/markdown/          # only if Markdown Mirror is enabled (desktop)
└── assets/
    └── image.png
```

The `logseq_db_` prefix belongs to the **repo name** the app uses internally, not to the
directory. `repo->graph-dir-key` strips it and `repo->encoded-graph-dir-name` encodes the rest
([`deps/common/src/logseq/common/graph_dir.cljs:41-46, 71-75`](../../../../../deps/common/src/logseq/common/graph_dir.cljs)).
The full DB path is `<graphs-dir>/<encoded-dir-name>/db.sqlite`
([`deps/db/src/logseq/db/common/sqlite.cljs:29-33`](../../../../../deps/db/src/logseq/db/common/sqlite.cljs)).
In the browser build there is no graphs directory. The worker installs an OPFS SAH-pool VFS
instead
([`src/main/frontend/worker/platform/browser.cljs:74-77`](../../../../../src/main/frontend/worker/platform/browser.cljs);
node vs. browser branch at
[`src/main/frontend/worker/db_core.cljs:84-99`](../../../../../src/main/frontend/worker/db_core.cljs)).

Some details of the desktop layout:

- **Backups.** Automatic desktop backups go to `<graph>/backup/`
  ([`src/main/logseq/db_worker/graph_backup.cljs:10`](../../../../../src/main/logseq/db_worker/graph_backup.cljs),
  [`src/main/frontend/config.cljs:333-335`](../../../../../src/main/frontend/config.cljs)). They run hourly and
  keep 12 versions
  ([`src/electron/electron/db.cljs:15-18`](../../../../../src/electron/electron/db.cljs)). The
  `get-db-backups-path` helper at `sqlite.cljs:35-38` builds a `backups/` path, but only tests call it.
- **WAL.** WAL mode is set at
  [`src/main/frontend/worker/db_core.cljs:593`](../../../../../src/main/frontend/worker/db_core.cljs).
- **Config files.** `config.edn`, `custom.css` and `custom.js` are **not files in the graph folder**. They are
  `:file/path` entities inside the database
  ([`deps/db/src/logseq/db/sqlite/create_graph.cljs:205-217`](../../../../../deps/db/src/logseq/db/sqlite/create_graph.cljs)).

### SQLite is storage, DataScript is the database

`db.sqlite` holds a single table, used as a key-value store for DataScript's storage layer:

```sql
create table if not exists kvs (addr INTEGER primary key, content TEXT, addresses JSON)
```

([`deps/db/src/logseq/db/common/sqlite.cljs:10-13`](../../../../../deps/db/src/logseq/db/common/sqlite.cljs)).
The connection is restored from that storage with `d/restore-conn`
(`get-storage-conn`, `sqlite.cljs:15-19`), and queries run as DataScript datalog, not SQL. So,
as in a file graph, the query engine is DataScript. What changed is where the data persists and
how it is typed.

> **Corrected:** earlier drafts described a `logseq.db` file, a `version-files/*.transit`
> snapshot directory with automatic 30-minute exports, and a relational `blocks` table with
> `marker`/`priority`/`deadline` columns. None of these exist. The file is `db.sqlite`, the only
> table is `kvs`, and `version-files` is a file-graph directory (see above). Transit appears only
> as a manual **debug transit** export/import
> ([`export.cljs:150-152`](../../../../../src/main/frontend/components/export.cljs),
> [`en.edn:920-921`](../../../../../src/resources/dicts/en.edn)).

Data is stored as entities with typed attributes:

```clojure
{:db/id 123
 :block/uuid #uuid "..."
 :block/title "This is a block"
 :block/parent {:db/id 122}
 :block/tags [:logseq.class/Task]
 ;; status is a ref to a closed-value entity, shown here by its :db/ident
 :logseq.property/status :logseq.property/status.todo
 ;; deadline is :datetime, stored as epoch milliseconds (a number, not an #inst)
 :logseq.property/deadline 1771113600000}
```

`:logseq.property/deadline` and `:logseq.property/scheduled` are both declared `{:type :datetime}`
([`deps/db/src/logseq/db/frontend/property.cljs:340-353`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)),
and the `:datetime` validator is plain `number?`
([`deps/db/src/logseq/db/frontend/property/type.cljs:210-212`](../../../../../deps/db/src/logseq/db/frontend/property/type.cljs)).

### Markdown Mirror: the text view of a DB graph

A DB graph is not editable with external tools, but the desktop app can **write** a Markdown copy
of it. The "Markdown Mirror" setting is described as "Write a derived Markdown copy of edited
pages to the graph's mirror/markdown folder. Desktop only."
([`src/resources/dicts/en.edn:1733`](../../../../../src/resources/dicts/en.edn)). Where the code enforces that:

- **Desktop only.** The toggle renders only under Electron
  ([`src/main/frontend/components/settings.cljs:1170-1171`](../../../../../src/main/frontend/components/settings.cljs)),
  and the worker writes only on a node or Electron runtime
  ([`src/main/frontend/worker/markdown_mirror.cljs:200-203`](../../../../../src/main/frontend/worker/markdown_mirror.cljs)).
  Browser workers throw on mirror writes
  ([`platform/browser.cljs:164-168`](../../../../../src/main/frontend/worker/platform/browser.cljs)).
- **Location.** Output goes to `<graph-dir>/mirror/markdown`
  ([`markdown_mirror.cljs:15-17`](../../../../../src/main/frontend/worker/markdown_mirror.cljs)).
- **Off by default.** It is controlled by the `:feature/markdown-mirror?` config key and is on only when that key is `true`
  ([`settings.cljs:810`](../../../../../src/main/frontend/components/settings.cljs)).
- **One-way.** The copy is derived output. `mirror/markdown` is on the shared ignore list
  ([`graph.cljs:74,84`](../../../../../deps/common/src/logseq/common/graph.cljs)), so editing those
  files does not change the graph.

---

## Syntax That Changed

Content that looks the same in both models often means something different:

| Written as                 | File graph                     | DB graph |
| -------------------------- | ------------------------------ | -------- |
| **Block reference**        | `((uuid))`                     | `[[uuid]]`. "Copy block ref" ([`content.cljs:291`](../../../../../src/main/frontend/components/content.cljs)) builds a page-ref around the UUID ([`src/main/frontend/util/ref.cljs:5-7`](../../../../../src/main/frontend/util/ref.cljs)); `((uuid))` renders as literal text ([`block.cljs:1573-1574`](../../../../../src/main/frontend/components/block.cljs)) |
| **Task**                   | `TODO` / `LATER` / `NOW` marker | `#Task` with a closed-value status: backlog, todo, doing, in-review, done, canceled ([`property.cljs:313-318`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)) |
| **Priority**               | Letter `A` / `B` / `C` (`:block/priority`, [`exporter.cljs:512-514`](../../../../../deps/graph-parser/src/logseq/graph_parser/exporter.cljs)) | Closed values low, medium, high, urgent ([`property.cljs:334-337`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)) |
| **Simple query**           | `{{query …}}`                  | Deprecated: renders a warning ([`block.cljs:1887-1888`](../../../../../src/main/frontend/components/block.cljs)); use `/Query` |
| **Embed**                  | `{{embed …}}`                  | Deprecated: renders a warning ([`block.cljs:1934-1935`](../../../../../src/main/frontend/components/block.cljs)) |
| **Advanced query**         | `#+BEGIN_QUERY … #+END_QUERY`  | Deprecated: see [Query Syntax Differences](#query-syntax-differences) |
| **Format**                 | Markdown or Org                | Markdown only: `get-preferred-format` always returns `:markdown` ([`src/main/frontend/state.cljs:555-557`](../../../../../src/main/frontend/state.cljs)) |

---

## Feature Comparison

### Properties

| Feature             | File                 | Database            |
| ------------------- | -------------------- | ------------------- |
| **Text**            | ✅                   | ✅                  |
| **Numbers**         | ❌ (text)            | ✅ (`:number`)      |
| **Dates**           | ❌ (text)            | ✅ (`:date`, `:datetime`) |
| **Checkboxes**      | ❌                   | ✅ (`:checkbox`)    |
| **Select options**  | ❌                   | ✅ (closed values)  |
| **Page references** | ✅ (text)            | ✅ (`:node`, typed) |
| **Multi-value**     | ⚠️ (comma-separated) | ✅ (cardinality many) |

The eight user-facing types are `:default :number :date :datetime :checkbox :url :node :asset`.
There are also 11 internal types, 19 in all
([`deps/db/src/logseq/db/frontend/property/type.cljs:15-21`](../../../../../deps/db/src/logseq/db/frontend/property/type.cljs)).

> **Corrected:** an earlier example said `priority:: urgent` is rejected as "not allowed".
> `urgent` is one of the four built-in priority closed values
> ([`property.cljs:337`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)).

### Classes & Tags

| Feature                 | File      | Database                |
| ----------------------- | --------- | ----------------------- |
| **Tags**                | ✅ `#tag` | ✅ `#[[Class]]`         |
| **Inheritance**         | ❌        | ✅ `:logseq.property.class/extends` |
| **Required properties** | ❌        | ✅ `:required-properties` in a class `:schema` |
| **Built-in classes**    | ❌        | ✅ 18 of them: Task, Card, Asset, Query… |

Built-in classes are defined in
[`deps/db/src/logseq/db/frontend/class.cljs:16-104`](../../../../../deps/db/src/logseq/db/frontend/class.cljs)
(`:required-properties` appears on `:logseq.class/Asset` and `:logseq.class/Pdf-annotation`,
`:74` and `:97`). `:logseq.property.class/extends` is at
[`deps/db/src/logseq/db/frontend/property.cljs:164`](../../../../../deps/db/src/logseq/db/frontend/property.cljs).

### Queries

| Feature                 | File             | Database                                    |
| ----------------------- | ---------------- | ------------------------------------------- |
| **Basic queries**       | ✅               | ✅                                          |
| **Property filtering**  | ⚠️ Text matching | ✅ Type-aware (`ref-property` vs `scalar-property` rules) |
| **Date ranges**         | ⚠️ `(between …)` over journal days | ✅ `(between …)` over any `:datetime` property |
| **Numeric comparisons** | ❌               | ⚠️ No DSL operator. Use a datalog predicate |
| **Class queries**       | ❌               | ✅ `(tags …)`                                |

The simple-query DSL recognises exactly these operators: `and`, `or`, `not`, `between`,
`property`, `private-property`, `todo`/`task`, `priority`, `page`, `sample`, `tags`, plus raw
datalog clauses ([`src/main/frontend/worker/query_dsl.cljs:502-577`](../../../../../src/main/frontend/worker/query_dsl.cljs)).
There is **no** `>` / `<` / `>=` DSL operator. On a DB graph, `(between …)` compiles to `>=` / `<`
bounds on a timestamp property. On journal days it compiles to the `:between` rule instead
(`:300-327`). Numeric filtering needs an advanced datalog query. Because number properties are
ref entities, you must deref the value:

```clojure
[?b :user.property/Weight-a1b2c3d4 ?v]
[?v :logseq.property/value ?n]
[(> ?n 100)]
```

> **Removed:** startup-time, query-latency and memory benchmark tables ("50x faster", "20s vs
> 1.5s"). No benchmark in the source backs them. The only benchmark namespace,
> `src/main/frontend/worker/search_benchmark.cljs`, is "Small benchmark helpers for comparing
> worker search result quality". It does not time file graphs against DB graphs.

---

## Query Syntax Differences

This is the sharpest break between the two models. The **container** for an advanced query
changed, not just the clauses inside it.

### File-Based Query

A file graph stores the query as org-style markup inside the block's text:

```clojure
#+BEGIN_QUERY
{:title "High Priority Tasks"
 :query [:find (pull ?b [*])
         :where
         [?b :block/content ?c]
         [(clojure.string/includes? ?c "priority:: high")]]}
#+END_QUERY
```

### Database Query

**A DB graph cannot run `#+BEGIN_QUERY`.** The renderer matches `["Custom" "query" …]` and shows a
deprecation warning instead of a result set
([`src/main/frontend/components/block.cljs:5107-5108`](../../../../../src/main/frontend/components/block.cljs)).
The string at [`src/resources/dicts/en.edn:120`](../../../../../src/resources/dicts/en.edn) reads
"#+BEGIN_QUERY is deprecated. Use '/Advanced Query' command instead."

In a DB graph an advanced query is not markup at all. It is a **block tagged
`:logseq.class/Query`** whose `:logseq.property/query` property holds a Clojure code block. The
`/Advanced Query` slash command builds exactly that
([`src/main/frontend/commands.cljs:125-132`](../../../../../src/main/frontend/commands.cljs)):

```clojure
[[:editor/set-property :block/tags :logseq.class/Query]
 [:editor/set-property :logseq.property/query ""]
 [:editor/set-property-on-block-property :logseq.property/query
  :logseq.property.node/display-type :code]
 [:editor/set-property-on-block-property :logseq.property/query
  :logseq.property.code/lang "clojure"]]
```

The query map you type into that code block is the datalog itself. Note there is **no `:title`
key**. The parsed map is passed straight to `custom-query`
([`block.cljs:4139-4152`](../../../../../src/main/frontend/components/block.cljs)), but only
built-in queries render `query-title`. User queries go to the untitled `[:div.bd …]` branch
([`src/main/frontend/components/query.cljs:166-177`](../../../../../src/main/frontend/components/query.cljs)).
Put the heading in a `:view` or in the block above:

```clojure
{:query [:find (pull ?b [*])
         :where
         [?b :block/tags ?t]
         [?t :db/ident :logseq.class/Task]
         [?b :logseq.property/priority ?p]
         [?p :db/ident :logseq.property/priority.high]]}
```

`:logseq.property/priority.high` is a closed value, not a string
([`deps/db/src/logseq/db/frontend/property.cljs:323-338`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)).

---

## What Each Model Buys You

This is not really a menu any more, because the app lists DB graphs only (see
[Overview](#overview)). The trade-off is still worth understanding, because it explains what
the importer has to give up.

### The file model optimised for:

- **Editing with external tools.** Any text editor, grep, or scripts work on the files.
- **Version control over the graph itself.** Text diffs are meaningful.
- **Portability.** The folder is its own backup and its own export.
- **Plain-text simplicity.**

It paid for that with a parser in the loop: every file had to be parsed into DataScript, and
metadata could only be text.

### The database model optimises for:

- **Typed properties.** Numbers, dates, checkboxes, closed values and node references (see
  [Properties](#properties)).
- **Classes.** Inheritance, required properties, and 18 built-in classes.
- **Built-in task management.** `:logseq.class/Task` with closed-value status and priority.
- **Multi-user collaboration over db-sync.** The graph list separates your own remote graphs from
  graphs shared with you
  ([`components/repo.cljs:324-326`](../../../../../src/main/frontend/components/repo.cljs)).
- **End-to-end encryption**
  ([`src/main/frontend/worker/sync/crypt.cljs`](../../../../../src/main/frontend/worker/sync/crypt.cljs)).

It pays for that with opacity:

- `db.sqlite` is a `kvs` table of serialized storage blobs. It cannot be edited by hand, and a
  diff of it tells you nothing.
- Getting text out takes an export or the one-way Markdown Mirror.

---

## Sync and Collaboration

File graphs synced either through third-party folder sync or through Logseq's File Sync, which
was deleted in `bcc478b5f7`. DB graphs sync only through **db-sync**. For how it works (server-ordered
transactions, rebase of pending local edits, presence as "which block each user is editing"),
see [RTC vs File Sync](../comparisons/rtc-vs-file-sync.md).

> **Corrected:** db-sync is not described anywhere in the source as a CRDT, and presence does not
> share cursor positions. `git grep -i crdt` finds nothing under `src/main` or
> `deps/db-sync/src`, and presence carries the editing block's UUID
> ([RTC vs File Sync](../comparisons/rtc-vs-file-sync.md#db-sync--rtc)). Subscription prices
> ("$5/month") have no source and are dropped.

A few db-sync runtime behaviours have no file-graph counterpart at all:

- **DB sync config strips auth fields.** `:thread-api/set-db-sync-config` runs its payload through
  `worker-state/non-auth-db-sync-config` before storing it
  ([`src/main/frontend/worker/handler/sync.cljs:11-18`](../../../../../src/main/frontend/worker/handler/sync.cljs);
  `non-auth-db-sync-config` at [`worker/state.cljs:39`](../../../../../src/main/frontend/worker/state.cljs)).
  Callers pass only `{:enabled? :ws-url :http-base}`
  ([`components/repo.cljs:631-634`](../../../../../src/main/frontend/components/repo.cljs),
  [`components/settings.cljs:655-658`](../../../../../src/main/frontend/components/settings.cljs)).
  Auth state travels separately via `:thread-api/sync-app-state`
  ([`handler/db_based/sync.cljs:207-226`](../../../../../src/main/frontend/handler/db_based/sync.cljs)).
- **Graph readiness gates.** Sync HTTP handlers return `409 "graph not ready"` during
  bootstrap and import
  ([`deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs:669,694,729,782`](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs)).
  The WebSocket upgrade is rejected the same way at
  [`worker/handler/ws.cljs:65`](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/ws.cljs)
  and [`node/server.cljs:159`](../../../../../deps/db-sync/src/logseq/db_sync/node/server.cljs).
- **E2EE password APIs.** `:thread-api/verify-and-save-e2ee-password` fetches the user's
  server-held encrypted private key and verifies the password against it before saving
  ([`src/main/frontend/worker/sync/crypt.cljs:360-376, 893-896`](../../../../../src/main/frontend/worker/sync/crypt.cljs)).
  Missing-password failures go through `fail-missing-e2ee-password!` (`crypt.cljs:68`).

---

## Migration

### File → Database

The import dialog's **"File to DB graph"** option ("Import a file-based Logseq graph folder into a
new DB graph", [`src/resources/dicts/en.edn:926-927`](../../../../../src/resources/dicts/en.edn))
is the one supported path. It does not convert the folder in place:

- **Reads the folder.** It collects files from the chosen directory and requires
  `logseq/config.edn`, failing with "the file 'logseq/config.edn' was not found" otherwise
  ([`src/main/frontend/components/imports.cljs:486-498`](../../../../../src/main/frontend/components/imports.cljs),
  [`en.edn:938`](../../../../../src/resources/dicts/en.edn)).
- **Creates a new graph.** It creates a **new** DB graph with `repo-handler/new-db!`, then runs
  `:thread-api/import-file-graph` against it
  ([`imports.cljs:444-465`](../../../../../src/main/frontend/components/imports.cljs)).
- **Copies assets.** Asset bytes are written into the new graph's assets
  ([`src/main/frontend/worker/db_core.cljs:373-395`](../../../../../src/main/frontend/worker/db_core.cljs)).

The dialog's tag options are:

- `convert-all-tags?`
- `tag-classes`: a comma-separated list, disabled while "all tags" is on.
- `remove-inline-tags?`
- property-value tag and property-value tag-parent options.

They are defined at [`imports.cljs:177-261`](../../../../../src/main/frontend/components/imports.cljs).

Conversion is done by `logseq.graph-parser.exporter`
([`deps/graph-parser/src/logseq/graph_parser/exporter.cljs`](../../../../../deps/graph-parser/src/logseq/graph_parser/exporter.cljs)).
It is what turns file syntax into the DB forms in [Syntax That Changed](#syntax-that-changed):

- **Task markers.** Each marker becomes a `#Task` tag plus a status.
  - `TODO`/`LATER` → todo, `NOW`/`DOING` → doing, `DONE` → done, `CANCELED`/`CANCELLED` → canceled
    (`:438-445`).
  - `WAIT`, `WAITING` and `IN-PROGRESS` get custom status values (`:447-448`).
  - Anything else is logged and set to todo. The marker is stripped from the title (`:486-507`).
- **Priorities.** `A` → high, `B` → medium, `C` → low. Unknown values are logged and set to low
  (`:509-514`).
- **Block references.** `((uuid))` is rewritten to `[[uuid]]` (`convert-block-refs-to-page-refs`,
  `:1444-1449`).
- **Properties.** Property values that don't fit the inferred type are dropped and reported:
  "Import ignored N properties". The fix is "change the property value to the correct type and
  reimport the graph" ([`en.edn:933-934`](../../../../../src/resources/dicts/en.edn)).
- **Org files.** They are imported **as Markdown**
  ([`en.edn:941`](../../../../../src/resources/dicts/en.edn)).

For the steps, see [Import and Export](../../how-to/organization/import-export.md).

> **Corrected:**
> - **No in-place converter.** There is no "File → Convert to database" command, and imported
>   files are not "moved to `backup/`". The import writes a new graph (above), and a DB graph's
>   `backup/` holds SQLite backups.
> - **No CLI path.** The CLI does not import file graphs. `graph import --type` must be `edn`
>   or `sqlite` ([`cli/lib/cli_parse.ml:896`](../../../../../cli/lib/cli_parse.ml)).

> **Unverified:** whether block UUIDs from `id::` properties are preserved on import. This was
> not traced at this revision, so no guarantee is made either way.

### Database → File

**There is no code path.** Nothing in the app creates or opens a file graph: the graph list
drops non-`logseq_db_` repos ([`repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs)).
You can get your content out as text in two ways, but neither produces a graph Logseq will open:

- **Markdown export.** Export screen → "Markdown", hidden on native mobile
  ([`src/main/frontend/components/export.cljs:140-143`](../../../../../src/main/frontend/components/export.cljs)).
- **Markdown Mirror.** A continuously updated one-way copy on desktop (see
  [above](#markdown-mirror-the-text-view-of-a-db-graph)).

The export screen's other options are SQLite DB, ZIP, DB EDN (hidden on mobile), public-pages HTML
(Electron only) and a debug transit file
([`export.cljs:125-152`](../../../../../src/main/frontend/components/export.cljs)). The EDN,
SQLite+assets ZIP and debug-transit exports can be imported back into a **new DB graph**
([`en.edn:918-921, 950-951`](../../../../../src/resources/dicts/en.edn)).

> **Removed:**
> - **Switching back.** "Start database, switch to file-based when you need Git…" and the
>   "open exported folder as a file graph" steps. The app will not list the result.
> - **JSON export.** The only JSON exporter, `export-repo-as-roam-json!`, sits inside a
>   `(comment …)` form
>   ([`src/main/frontend/handler/export.cljs:180-191`](../../../../../src/main/frontend/handler/export.cljs)).
> - **Maintenance menus.** "Settings → Database → Check integrity / Vacuum" don't exist: `en.edn`
>   has no integrity or vacuum string. The nearest real commands are "Rebuild search index"
>   ([`en.edn:414`](../../../../../src/resources/dicts/en.edn)) and a developer `:dev/validate-db`
>   command with no default binding
>   ([`src/main/frontend/modules/shortcut/config.cljs:522-525`](../../../../../src/main/frontend/modules/shortcut/config.cljs)).

---

## Schema Differences

### File-Based Schema

```clojure
;; Properties stored as keyword-value maps (values are strings or sets)
{:block/properties {:priority "high"
                    :due "2026-02-15"}
 :block/properties-text-values {:priority "high"
                                :due "2026-02-15"}}

;; Tags as page entity references
{:block/tags [{:db/id 45}]}  ; references to page entities
```

These file-graph attributes are **not part of the DB schema**:

- **Parser only.** `:block/marker`, `:block/priority`, `:block/namespace`,
  `:block/scheduled` and `:block/deadline` appear only in `deps/graph-parser/`, the importer.
- **Stripped or ignored.** `:block/properties-text-values` also appears outside it, but only
  where it is stripped or tolerated: excluded-attribute sets in
  [`src/main/frontend/worker/handler/block.cljs:50-53`](../../../../../src/main/frontend/worker/handler/block.cljs)
  and [`src/main/frontend/worker/plain_value.cljs:127-129`](../../../../../src/main/frontend/worker/plain_value.cljs),
  plus template code that reads it only when present (`map?` / `contains?` guards in
  [`deps/outliner/src/logseq/outliner/template.cljs:91,160`](../../../../../deps/outliner/src/logseq/outliner/template.cljs)).
- **Nowhere.** `:block/journal?` appears **nowhere in the source**. A doc or query that uses it
  is wrong.
- **Legacy alias.** `:block/content` survives only as an alias in the plugin SDK
  ([`src/main/logseq/sdk/utils.cljs:56`](../../../../../src/main/logseq/sdk/utils.cljs)) and in a
  `TODO: remove` guard ([`src/main/frontend/db/async/util.cljs:12-13`](../../../../../src/main/frontend/db/async/util.cljs)).
  The DB attribute is `:block/title`.
- **Computed.** `:block/properties` is not a stored DB attribute. It is computed on an Entity only
  ([`deps/db/src/logseq/db/common/entity_plus.cljc:138-146`](../../../../../deps/db/src/logseq/db/common/entity_plus.cljc)),
  so it never matches in a `:where` clause.

Note that `deps/graph-parser` is not *only* the importer. `logseq.graph-parser.mldoc` is still the
inline-markup parser DB graphs use to render block text (required at
[`block.cljs:87`](../../../../../src/main/frontend/components/block.cljs)). What does not survive
into a DB graph is the file-graph *entity model*, meaning the attributes above.

### Database Schema

```clojure
;; Properties as entity references
{:logseq.property/priority {:db/ident :logseq.property/priority.high}
 ;; :datetime is epoch milliseconds, not an #inst
 :logseq.property/deadline 1771113600000}

;; Tags as class references
{:block/tags [{:db/ident :logseq.class/Task}
              {:db/ident :user.class/Project}]}
```

The DB schema version is **65.33**
([`deps/db/src/logseq/db/frontend/schema.cljs:33`](../../../../../deps/db/src/logseq/db/frontend/schema.cljs)).

> **Note on user idents:** `:user.class/Project` above is illustrative only. User classes and
> user properties both get a random 8-character suffix, so a property titled "Status" becomes
> `:user.property/Status-<8 chars>` and a tag titled "Project" becomes `:user.class/Project-<8 chars>`
> (`create-db-ident-from-name`,
> [`deps/db/src/logseq/db/frontend/db_ident.cljc:66-90`](../../../../../deps/db/src/logseq/db/frontend/db_ident.cljc);
> called from [`property.cljs:854-859`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)
> and [`class.cljs:195-203`](../../../../../deps/db/src/logseq/db/frontend/class.cljs)).
> Only `plugin.class.*` namespaces are exempt from the suffix (`db_ident.cljc:82`).
> The `LOGSEQ_STABLE_IDENTS` environment variable also disables it, for tests.
> Never guess a user ident. Resolve it by title.

---

## Identifying Graph Type

In code (from `logseq.db.sqlite.util`,
[`deps/db/src/logseq/db/sqlite/util.cljs:52-55`](../../../../../deps/db/src/logseq/db/sqlite/util.cljs)):

```clojure
(defn db-based-graph?
  [graph-name]
  (when graph-name
    (string/starts-with? graph-name db-version-prefix)))
;; where db-version-prefix = "logseq_db_"
```

File graphs use the prefix `logseq_local_` (`file-version-prefix`,
[`deps/common/src/logseq/common/config.cljs:34`](../../../../../deps/common/src/logseq/common/config.cljs)).

The graph switcher filters on `config/db-based-graph?`
([`src/main/frontend/components/repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs)),
so in practice a `logseq_local_` repo name is a graph the current UI will not show you.

> **Deleted (was unverified):** claims that "database graphs are visually distinguished from file
> graphs" in the UI and that "the settings page shows graph type". Neither has a code path at this
> revision. Under `src/main/frontend/components/`, `db-based-graph?` is called only at
> `repo.cljs:36` (display-name formatting) and `repo.cljs:323` (the filter above). Neither renders
> a graph-type indicator, and `settings.cljs` contains no graph-type display.

---

## See Also

- **Database Schema**: [schema-overview.md](../../reference/database/schema-overview.md)
- **Schema system**: [schema-system.md](../architecture/schema-system.md)
- **Data architecture**: [data-architecture.md](../architecture/data-architecture.md)
- **System overview**: [system-overview.md](../architecture/system-overview.md)
- **Local-first design**: [local-first-architecture.md](local-first-architecture.md)
- **Sync Comparison**: [rtc-vs-file-sync.md](../comparisons/rtc-vs-file-sync.md)
- **Import and export steps**: [import-export.md](../../how-to/organization/import-export.md)
- **Query system**: [query-system.md](../../reference/advanced/query-system.md)

---

_Last updated: 2026-09-11. Verified against `be800f1711`._
