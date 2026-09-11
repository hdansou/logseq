---
title: 'File vs Database Graphs'
description: 'Understanding the differences between file-based and database-backed graphs in Logseq'
status: stable
lastVerified: '2026-09-10'
verifiedAgainst: 'be800f1711'
verifiedScope: 'Full audit 2026-09-09 (58c47afdd6). The 2026-09-10 date covers only the backup directory (287fe61eed).'
reviewStatus: audited
category: concepts
relatedDocs:
  reference:
    - '[Database Schema](../../reference/database/schema-overview.md)'
  explanation:
    - '[RTC vs File Sync](../comparisons/rtc-vs-file-sync.md)'
---

# File vs Database Graphs

> **Last revised:** 2026-09-09 (verified against `be800f1711`)

> Understanding the two storage architectures in Logseq and why the current codebase has settled on one of them.

## Overview

Logseq has two ways of storing a knowledge graph:

| Aspect                  | File-Based                | Database-Based                       |
| ----------------------- | ------------------------- | ------------------------------------ |
| **Storage**             | Markdown/Org files        | SQLite database (`db.sqlite`)        |
| **Repo-name prefix**    | `logseq_local_`           | `logseq_db_`                         |
| **External editing**    | ✅ Yes                    | ❌ No                                |
| **Typed properties**    | ❌ Text only              | ✅ Full type system                  |
| **Listed in the app**   | ❌ Filtered out           | ✅                                   |

Both prefixes are defined in
[`deps/common/src/logseq/common/config.cljs:33-34`](../../../../../deps/common/src/logseq/common/config.cljs).

> **Important:** at this revision the graph switcher only lists DB graphs. The repo list is
> filtered with `config/db-based-graph?` — the comment in source reads
> "use `config/db-based-graph?` to avoid loading old file graphs"
> ([`src/main/frontend/components/repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs)).
> File graphs therefore survive in this codebase mainly as an **import source** (`deps/graph-parser/`),
> not as a graph type you can open side by side with a DB graph. Read the comparison below as
> "what the two models are", not as a live product choice.

---

## File-Based Graphs

### How It Works

Your knowledge is stored as plain text files on disk:

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
    └── config.edn
```

Each page is a separate `.md` file with human-readable content:

```markdown
- This is a block
  - This is a child block
- Another block with a [[link]]
- Block with properties
  property:: value
```

### Advantages

✅ **Portable**: Files work anywhere Markdown works  
✅ **External editors**: Use VS Code, Vim, or any text editor  
✅ **Git-friendly**: Version control with standard tools  
✅ **Human-readable**: Open files with any application  
✅ **Simple backup**: Just copy the folder

### Limitations

❌ **Text-only properties**: No dates, numbers, or references  
❌ **Limited scalability**: Thousands of files can be slow  
❌ **Conflict-prone sync**: File-level conflicts in multi-device use  
❌ **No real-time collaboration**: Only periodic file sync  
❌ **Parser dependency**: Content must be re-parsed on load

---

## Database Graphs

### How It Works

Your knowledge is stored in a SQLite database. Graphs live under `~/logseq/graphs`
(`default-graphs-dir`, [`deps/common/src/logseq/common/config.cljs:55`](../../../../../deps/common/src/logseq/common/config.cljs)):

```
~/logseq/graphs/my-graph/     # note: the on-disk dir name has NO logseq_db_ prefix
├── db.sqlite                 # main database (PRAGMA journal_mode=WAL at open)
├── backup/                   # automatic desktop backups
└── assets/
    └── image.png
```

The `logseq_db_` prefix belongs to the **repo name** the app uses internally, not to the
directory. `repo->encoded-graph-dir-name` strips it before building the path
([`deps/common/src/logseq/common/graph_dir.cljs:40-74`](../../../../../deps/common/src/logseq/common/graph_dir.cljs)),
and the full DB path is `<graphs-dir>/<encoded-dir-name>/db.sqlite`
([`deps/db/src/logseq/db/common/sqlite.cljs:29-33`](../../../../../deps/db/src/logseq/db/common/sqlite.cljs)).
Automatic desktop backups go to `<graph>/backup/`
([`src/main/logseq/db_worker/graph_backup.cljs:10`](../../../../../src/main/logseq/db_worker/graph_backup.cljs),
[`src/main/frontend/config.cljs:334-336`](../../../../../src/main/frontend/config.cljs)). The
`get-db-backups-path` helper at `sqlite.cljs:35-38` builds a `backups/` path, but only tests call it. WAL is set at
[`src/main/frontend/worker/db_core.cljs:588`](../../../../../src/main/frontend/worker/db_core.cljs).

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

### Advantages

✅ **Typed properties**: Dates, numbers, checkboxes, references  
✅ **Built-in classes**: Task, Card, Asset with behaviors  
✅ **Higher scalability**: SQLite-backed storage and indexing for larger graphs  
✅ **Real-time sync**: RTC with automatic conflict resolution  
✅ **E2EE**: End-to-end encryption available  
✅ **Better queries**: Type-aware filtering and sorting

### Limitations

❌ **Logseq-only**: Cannot edit with external tools  
❌ **Less portable**: Requires export to move data  
❌ **Newer**: Less battle-tested than file-based  
❌ **Learning curve**: More concepts (classes, property types)

---

## Feature Comparison

### Properties

| Feature             | File                 | Database            |
| ------------------- | -------------------- | ------------------- |
| **Text**            | ✅                   | ✅                  |
| **Numbers**         | ❌ (text)            | ✅ (actual numbers) |
| **Dates**           | ❌ (text)            | ✅ (date picker)    |
| **Checkboxes**      | ❌                   | ✅                  |
| **Select options**  | ❌                   | ✅ (closed values)  |
| **Page references** | ✅ (text)            | ✅ (typed)          |
| **Multi-value**     | ⚠️ (comma-separated) | ✅ (native)         |

### Classes & Tags

| Feature                 | File      | Database                |
| ----------------------- | --------- | ----------------------- |
| **Tags**                | ✅ `#tag` | ✅ `#[[Class]]`         |
| **Inheritance**         | ❌        | ✅ `:logseq.property.class/extends` |
| **Required properties** | ❌        | ✅ `:required-properties` in a class `:schema` |
| **Built-in classes**    | ❌        | ✅ 18 of them: Task, Card, Asset, Query… |

Built-in classes are defined in
[`deps/db/src/logseq/db/frontend/class.cljs:18-105`](../../../../../deps/db/src/logseq/db/frontend/class.cljs)
(`:required-properties` appears on `:logseq.class/Asset` and `:logseq.class/Pdf-annotation`);
`:logseq.property.class/extends` is at
[`deps/db/src/logseq/db/frontend/property.cljs:164`](../../../../../deps/db/src/logseq/db/frontend/property.cljs).

### Queries

| Feature                 | File             | Database                                    |
| ----------------------- | ---------------- | ------------------------------------------- |
| **Basic queries**       | ✅               | ✅                                          |
| **Property filtering**  | ⚠️ Text matching | ✅ Type-aware (`ref-property` vs `scalar-property` rules) |
| **Date ranges**         | ⚠️ `(between …)` over journal days | ✅ `(between …)` over any `:datetime` property |
| **Numeric comparisons** | ❌               | ⚠️ No DSL operator — use a datalog predicate |
| **Class queries**       | ❌               | ✅ `(tags …)`                                |

The simple-query DSL recognises exactly these operators: `and`, `or`, `not`, `between`,
`property`, `private-property`, `todo`/`task`, `priority`, `page`, `sample`, `tags`, plus raw
datalog clauses ([`src/main/frontend/worker/query_dsl.cljs:502-577`](../../../../../src/main/frontend/worker/query_dsl.cljs)).
There is **no** `>` / `<` / `>=` DSL operator. `(between …)` on a DB graph compiles to `>=` / `<`
bounds on a timestamp property (`:300-325`); on journal days it compiles to the `:between` rule
instead. Numeric filtering needs an advanced datalog query — and because number properties are
ref entities, you must deref the value:

```clojure
[?b :user.property/Weight-a1b2c3d4 ?v]
[?v :logseq.property/value ?n]
[(> ?n 100)]
```

---

## Query Syntax Differences

This is the sharpest break between the two models: the **container** for an advanced query
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

**A DB graph cannot run `#+BEGIN_QUERY`.** The renderer matches `["Custom" "query" …]` and emits a
deprecation warning instead of a result set
([`src/main/frontend/components/block.cljs:5057-5058`](../../../../../src/main/frontend/components/block.cljs),
string at [`src/resources/dicts/en.edn:120`](../../../../../src/resources/dicts/en.edn):
"#+BEGIN_QUERY is deprecated. Use '/Advanced Query' command instead.").

In a DB graph an advanced query is not markup at all — it is a **block tagged
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
key** — `:title` is silently ignored in a DB graph; put the heading in a `:view` or in the block
above:

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

This is not really a menu any more — the app lists DB graphs only (see the note in
[Overview](#overview)) — but the trade-off is worth understanding, because it explains what the
importer has to give up.

### The file model optimises for:

- Editing with external tools
- Git version control over the graph itself
- Portability: the graph is its own backup
- Plain-text simplicity

### The database model optimises for:

- Typed properties: 19 property types in all — 8 offered in the UI
  (`:default :number :date :datetime :checkbox :url :node :asset`) plus 11 internal ones
  ([`deps/db/src/logseq/db/frontend/property/type.cljs:15-21`](../../../../../deps/db/src/logseq/db/frontend/property/type.cljs))
- Multi-user collaboration over DB sync
- Built-in task management via `:logseq.class/Task` and its closed-value status/priority
- End-to-end encryption
  ([`src/main/frontend/worker/sync/crypt.cljs`](../../../../../src/main/frontend/worker/sync/crypt.cljs))

---

## Migration

### File → Database

Logseq provides a built-in import flow from the app's import dialog that handles conversion from file graphs to DB graphs.

1. Use the import dialog in the app (or CLI)
2. Select the file-based graph folder
3. Configure tag conversion options
4. Review converted properties

The import dialog's tag options are `convert-all-tags?`, `tag-classes` (a comma-separated list,
disabled while "all tags" is on), `remove-inline-tags?`, plus property-value tag and
property-value tag-parent options
([`src/main/frontend/components/imports.cljs:177-261`](../../../../../src/main/frontend/components/imports.cljs)).

**Notes**:

- Property conversion is handled by `logseq.graph-parser.exporter`, the pipeline the import flow drives
  ([`deps/graph-parser/src/logseq/graph_parser/exporter.cljs`](../../../../../deps/graph-parser/src/logseq/graph_parser/exporter.cljs))
- Some formatting may change

> **Removed:** `frontend.components.imports/import-file-graph` as a stable public migration API reference. The UI flow exists, but relying on private symbol names here is brittle.

> **Removed:** "Block UUIDs are regenerated." Current importer/exporter paths do not document this as a guaranteed behavior.

> **Removed:** Specific priority mapping example (`"A" -> :logseq.property/priority.high`) as a fixed migration guarantee.

### Database → File

1. Export as Markdown (Export dialog → "Markdown"; hidden on native mobile)
2. Create new file-based graph
3. Copy exported files

The export dialog also offers SQLite DB, ZIP, DB EDN (desktop only), public-pages HTML (Electron
only) and a debug transit file
([`src/main/frontend/components/export.cljs:118-152`](../../../../../src/main/frontend/components/export.cljs)).

**Notes**:

- Typed properties become text
- Class metadata may be lost
- Property types not preserved

> **Unverified:** the three notes above are stated as a round trip, but Markdown export at this
> revision was not traced end to end to confirm exactly which class metadata is dropped. Treat as
> a direction of travel, not a guarantee.

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

These file-graph attributes are **not part of the DB schema**. At this revision
`:block/marker`, `:block/priority`, `:block/namespace`, `:block/scheduled`, `:block/deadline` and
`:block/properties-text-values` appear only in `deps/graph-parser/` (the importer), and
`:block/journal?` appears **nowhere in the source at all** — if a doc or a query references it, it
is wrong. `:block/content` survives only as a legacy alias in the plugin SDK
([`src/main/logseq/sdk/utils.cljs:56`](../../../../../src/main/logseq/sdk/utils.cljs)) and in a
`TODO: remove` guard ([`src/main/frontend/db/async/util.cljs:12-13`](../../../../../src/main/frontend/db/async/util.cljs));
the DB attribute is `:block/title`.

Note that `deps/graph-parser` is not *only* the importer: `logseq.graph-parser.mldoc` is still the
inline-markup parser DB graphs use to render block text. It is the file-graph *entity model* —
the attributes above — that does not survive into a DB graph.

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

> **Note on user idents:** `:user.class/Project` above is illustrative only. Both user classes and
> user properties get a random 8-character suffix, so a property titled "Status" becomes
> `:user.property/Status-<8 chars>` and a tag titled "Project" becomes `:user.class/Project-<8 chars>`
> (`create-db-ident-from-name`,
> [`deps/db/src/logseq/db/frontend/db_ident.cljc:66-89`](../../../../../deps/db/src/logseq/db/frontend/db_ident.cljc);
> called from [`property.cljs:852-859`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)
> and [`class.cljs:178-186`](../../../../../deps/db/src/logseq/db/frontend/class.cljs)).
> Only `plugin.class.*` namespaces are exempt from the suffix.
> Never guess a user ident — resolve it by title.

---

## Findings: New Capabilities

Re-validated against `be800f1711` (2026-09-09):

- **DB sync config strips auth fields.** `:thread-api/set-db-sync-config` runs its payload through
  `worker-state/non-auth-db-sync-config` before storing it
  ([`src/main/frontend/worker/handler/sync.cljs:11-18`](../../../../../src/main/frontend/worker/handler/sync.cljs);
  `non-auth-db-sync-config` at [`worker/state.cljs:39`](../../../../../src/main/frontend/worker/state.cljs)).
  Callers pass only `{:enabled? :ws-url :http-base}`
  ([`components/repo.cljs:630-634`](../../../../../src/main/frontend/components/repo.cljs),
  [`components/settings.cljs:643-647`](../../../../../src/main/frontend/components/settings.cljs)).
  Auth state travels separately via `:thread-api/sync-app-state`
  ([`handler/db_based/sync.cljs:207-226`](../../../../../src/main/frontend/handler/db_based/sync.cljs)).
- **Graph readiness gates.** Sync HTTP handlers return `409 "graph not ready"` during
  bootstrap/import windows
  ([`deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs:669,694,729,782`](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/sync.cljs);
  WS upgrade rejected the same way at
  [`worker/handler/ws.cljs:65`](../../../../../deps/db-sync/src/logseq/db_sync/worker/handler/ws.cljs)
  and [`node/server.cljs:159`](../../../../../deps/db-sync/src/logseq/db_sync/node/server.cljs)).
- **E2EE password APIs.** `:thread-api/verify-and-save-e2ee-password` verifies against the
  server-held encrypted key before writing, and missing-password failures are explicit
  (`fail-missing-e2ee-password!`)
  ([`src/main/frontend/worker/sync/crypt.cljs:360-395, 893-896`](../../../../../src/main/frontend/worker/sync/crypt.cljs)).

These are DB-graph runtime capabilities and have no file-graph equivalent.

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

Because the graph switcher filters on `config/db-based-graph?`
([`src/main/frontend/components/repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs)),
a `logseq_local_` repo name is in practice a graph the current UI will not show you.

> **Deleted (was unverified):** claims that "database graphs are visually distinguished from file
> graphs" in the UI and that "the settings page shows graph type". Neither has a code path at this
> revision — `db-based-graph?` is referenced in only three places under
> `src/main/frontend/components/`, none of which renders a graph-type indicator, and
> `settings.cljs` contains no graph-type display.

---

## See Also

- **Database Schema**: [schema-overview.md](../../reference/database/schema-overview.md)
- **Sync Comparison**: [rtc-vs-file-sync.md](../comparisons/rtc-vs-file-sync.md)
- **Query system**: [query-system.md](../../reference/advanced/query-system.md)

---

_Last updated: 2026-09-09 — verified against `be800f1711`._
