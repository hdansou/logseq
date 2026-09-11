---
title: 'File Organization and Namespaces'
status: 🟢 Stable
version: 'DB graph schema 65.33'
lastVerified: '2026-09-10'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
quadrant: 'Explanation'
platform: 'Desktop + Mobile'
related-docs:
  - '[File-Based vs Database Graphs](./file-vs-database.md)'
  - '[Graph Structure](./graph-visualization.md)'
  - '[Backup Strategies](../../how-to/organization/backup-strategies.md)'
tags: ['organization', 'namespaces', 'structure', 'file-system']
---

# File Organization and Namespaces

This document explains how a Logseq DB graph organizes your content — the logical structure (page hierarchies, tags) and the physical structure (one database file per graph, plus assets). Understanding both helps you build a knowledge base that stays manageable as it grows.

**What You'll Learn:**

- Logical vs physical organization
- How `A/B/C` page titles become a page hierarchy
- How a DB graph is stored on disk
- Export and backup options
- Organizing strategies

**Prerequisites:**

- Basic understanding of pages and blocks
- Familiarity with graph types (file-based vs database)

> **File graphs cannot be opened in this build.** The graph list keeps only DB
> graphs (`src/main/frontend/components/repo.cljs:318-323`). A file-based graph
> folder can still be brought in with **File to DB graph**, which imports it into
> a new DB graph (`src/main/frontend/components/imports.cljs:601`;
> `src/resources/dicts/en.edn:926-927`). Everything below describes DB graphs.

---

## Table of Contents

1. [Logical vs Physical Organization](#logical-vs-physical-organization)
2. [Namespace System](#namespace-system)
3. [File-Based Graphs](#file-based-graphs)
4. [Database Graph Structure](#database-graph-structure)
5. [Organization Strategies](#organization-strategies)
6. [Best Practices](#best-practices)

---

## Logical vs Physical Organization

### Logical Organization (What You See)

**In the graph:**

```
📄 Projects
  📄 Work
    📄 Alpha
    📄 Beta
  📄 Personal
    📄 Website
```

**This is logical.** Each entry is its own page with its own short title: `Alpha`'s parent is `Work`, and `Work`'s parent is `Projects`. None of it exists as folders.

### Physical Organization (Disk)

On desktop, every DB graph is a folder under `~/logseq/graphs`
(`deps/common/src/logseq/common/config.cljs:55`;
`deps/common/src/logseq/common/graph.cljs:123-126`;
`src/electron/electron/db.cljs:24-39`):

```
~/logseq/graphs/
└── my-graph/
    ├── db.sqlite    # all pages, blocks, properties and tags
    ├── assets/      # uploaded files
    └── backup/      # automatic desktop backups
```

- `db.sqlite` holds the graph (`deps/db/src/logseq/db/common/sqlite.cljs:29-33`).
- `assets/` sits next to it (`deps/common/src/logseq/common/config.cljs:56`;
  `src/main/frontend/handler/assets.cljs:283-294`).
- `backup/` is described under [Backups](#backups).

**Key Insight:** Logical hierarchy ≠ folder hierarchy. The hierarchy lives inside `db.sqlite`.

---

## Namespace System

### What are Namespaces?

A page title containing `/` is a **namespace** title. When such a page is created, Logseq splits the title into one page per segment and chains them with `:block/parent` (`deps/outliner/src/logseq/outliner/page.cljs:237-300, 385-388`):

```markdown
[[Projects]]                  # one page
[[Projects/Work]]             # pages Projects → Work
[[Projects/Work/Alpha]]       # pages Projects → Work → Alpha
[[Projects/Personal/Website]] # pages Projects → Personal → Website
```

The page you get back is the **leaf**, and its title is only the last segment — `Alpha`, not `Projects/Work/Alpha` (`deps/outliner/test/logseq/outliner/page_test.cljs:33-49`). Segments are trimmed and empty segments are dropped (`page.cljs:245-247`).

**Visual Hierarchy:**

```
Projects (root)
├── Work
│   ├── Alpha
│   ├── Beta
│   └── Gamma
└── Personal
    ├── Website
    └── Blog
```

A title is **not** split when it is a URL or starts with `./` or `../`
(`deps/common/src/logseq/common/util/namespace.cljs:12-20`), or when it is a
journal page (`page.cljs:385`).

The old `{{namespace}}` macro is deprecated and renders a warning that points to
the **Library** instead (`src/main/frontend/components/block.cljs:1876-1877`;
`src/resources/dicts/en.edn:158, 1008`).

### Creating Namespaces

**Method 1: Direct Creation**

```markdown
Create page: [[Projects/Work/NewProject]]
```

Automatically creates:

- `Projects` (if it doesn't exist)
- `Work`, as a child of `Projects` (if it doesn't exist there)
- `NewProject`, as a child of `Work` (your page)

Existing parent pages are reused (`page.cljs:251-254`;
`page_test.cljs:36, 45-46`). Pages created from the editor are split this way by
default (`src/main/frontend/handler/common/page.cljs:147-149`;
`src/main/frontend/handler/page.cljs:279-281`).

The top segment is filed under the built-in **Library** page
(`page.cljs:227-235`; `page_test.cljs:43-44`). If any segment already exists as
something other than an ordinary page, creation stops with "Cannot create this
page unless all parents are pages." (`page.cljs:262-267`).

**Method 2: The Library page**

**Library** has an **Add existing pages to Library** picker
(`src/main/frontend/components/library.cljs:15-73`;
`src/resources/dicts/en.edn:1006`). Choosing a page moves it under Library;
unchoosing it removes its parent (`library.cljs:39-50`).

Listing `[[Work]]` as a block on the `Projects` page only creates a reference. It
does not make `Work` a child of `Projects`.

**Tags use the same syntax, with a different relation.** When the page being
created is a tag, `/` builds a tag hierarchy through
`:logseq.property.class/extends` instead of `:block/parent`, and every segment
must be a tag (`page.cljs:269-274, 282-294`). A page that already has a parent
cannot be converted into a tag ("Namespaced pages can't be tags",
`src/main/frontend/handler/db_based/page.cljs:44-45`;
`src/resources/dicts/en.edn:1157`).

### Namespace Properties

**What the hierarchy is:** a `:block/parent` chain between pages. You can walk it
in an advanced query (`/Advanced Query`) with the built-in `parent` rule, which
takes the ancestor first and follows any number of levels
(`deps/db/src/logseq/db/frontend/rules.cljc:7-12`):

```clojure
{:query [:find (pull ?c [*])
         :where
         [?p :block/name "projects"]
         (parent ?p ?c)
         [?c :block/name]]}
```

`[?c :block/name]` keeps only pages, since blocks on those pages are descendants
too and `:block/name` is the page-name attribute
(`deps/db/src/logseq/db/frontend/schema.cljs:89-90`).

**What it is not:** Linked References are built from a block's references and
its page (`deps/db/src/logseq/db/common/reference.cljs:71-79`), not from
`:block/parent`, so child pages do not appear in a parent's Linked References
just because they are children.

> **Unverified:** how the page view displays a page's parents and children at
> this revision. No page component that reads a page's `:block/parent` was found
> in `src/main/frontend/components/page.cljs`.

---

## File-Based Graphs

This build does not open file-based graphs (see the note at the top). Their
folder layout (`pages/`, `journals/`, `logseq/`), file-name encoding and
`config.edn` keys matter only as input to **File to DB graph**, which refuses a
folder without `logseq/config.edn` (`src/resources/dicts/en.edn:938`). They are
not documented here.

---

## Database Graph Structure

### Single-File Storage

**Everything in One Database:**

```
~/logseq/graphs/<graph folder>/db.sqlite
```

- **Folder name.** The graph name without its internal `logseq_db_` prefix
  (`deps/common/src/logseq/common/graph_dir.cljs:41-46`), URI-encoded with
  spaces kept and `%` written as `~` (`graph_dir.cljs:6-12`).
- **Inside the file.** DataScript keeps its data in a `kvs` table
  (`deps/db/src/logseq/db/common/sqlite.cljs:10-13`). There are no per-page
  rows or files.
- **Config.** A DB graph's `logseq/config.edn` and `logseq/custom.css` are
  created as file entities inside the database
  (`deps/db/src/logseq/db/sqlite/create_graph.cljs:205-212`), not as files in the
  graph folder.

**Assets Still Separate:**

```
my-graph/
├── db.sqlite
└── assets/
    └── 0195b1cb-6b1f-7d70-94b8-6f1b2e4dbb31.png
```

Uploading a file (`/Upload an asset`, `src/main/frontend/commands.cljs:335-338`)
creates an asset node tagged `Asset` and writes the file to `assets/` as
`<asset uuid>.<extension>` (`src/main/frontend/handler/editor.cljs:1780-1802`).
The node's title comes from the original file name; a pasted file named `image`
gets a date-time title instead (`editor.cljs:1763-1769`). A file whose checksum
matches an existing asset is refused with a warning (`editor.cljs:1770-1778`),
and files over 100 MB are rejected (`editor.cljs:1785-1789`).

Because files are named by UUID, sorting them into your own subfolders of
`assets/` is not a supported way to organize them. Organize the asset **nodes**
instead: tag them, reference them, give them properties.

### Logical Organization Only

**Database graphs don't have:**

- ❌ Individual page files
- ❌ Folder hierarchies for pages
- ❌ File system organization

**Database graphs do have:**

- ✅ Page hierarchies (`:block/parent`) and tag hierarchies
  (`:logseq.property.class/extends`)
- ✅ Properties and tags
- ✅ Query-based organization
- ✅ Remote graphs listed alongside local ones when you are logged in
  (`src/main/frontend/components/repo.cljs:312-317`)

### Export Structure

**Export graph** offers (`src/main/frontend/components/export.cljs:119-153`):

| Option                                             | Output                                       | Notes                                                                                                   |
| -------------------------------------------------- | -------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Export SQLite DB                                   | One `.sqlite` file                           | "Primary way to backup graph's content to a single .sqlite file." (`en.edn:661, 678`)                   |
| Export both SQLite DB and assets                   | `.zip`                                       | "Primary way to backup graph's content and assets to a .zip file." (`en.edn:666, 680`)                  |
| Export EDN file                                    | `.edn`                                       | Not on mobile (`export.cljs:135`). "Don't rely on this as a primary backup." (`en.edn:632, 635`)       |
| Export as standard Markdown (no block properties)  | `.zip` of one `<page title>.md` per page     | Not in the native mobile app (`export.cljs:140`)                                                        |
| Export public pages                                | —                                            | Desktop only (`export.cljs:145-148`; `en.edn:656`)                                                      |
| Export debug transit file                          | `.transit`                                   | For bug reports; sensitive data removed (`en.edn:633-634`)                                              |

**EDN Export:**

The EDN file is a datom-level export of the whole graph
(`src/main/frontend/handler/db_based/export.cljs:87-90`;
`deps/db/src/logseq/db/sqlite/export.cljs:933-942, 1116-1117`):

```clojure
{:logseq.db.sqlite.export/schema-version ...   ; 65.33 at this revision
 :logseq.db.sqlite.export/graph-format :datoms
 :datoms [...]}
```

The schema version is `65.33` (`deps/db/src/logseq/db/frontend/schema.cljs:33`).

**Markdown Export:**

```
<graph>_markdown_<unix seconds>.zip
├── Projects.md
├── Work.md
└── Alpha.md
```

One file per page, named by page title, with no folders
(`src/main/frontend/handler/export/text.cljs:38-46`;
`src/main/frontend/handler/export/common.cljs:40-49`). Namespace pages are
exported by their short titles, so the hierarchy is not reflected in the file
names, and block properties are dropped (`en.edn:647`).

### Backups

- **Desktop.** An automatic backup is taken at most once an hour and the latest
  12 are kept (`src/electron/electron/db.cljs:15-18, 49-61`). Each backup is a
  folder under `<graph folder>/backup/` holding a `db.sqlite` copy and a
  `metadata.edn` (`src/main/logseq/db_worker/graph_backup.cljs:10-12, 39-69`).
  The Export page says "Hourly backups are enabled for this graph" and links to
  that folder (`export.cljs:155-164`; `en.edn:672, 675`).
- **Web (not mobile).** **Schedule backup** asks you to choose a backup folder
  and then backs up every hour, with a **Backup now** button
  (`export.cljs:51-117, 165-169`; `en.edn:674, 676`). It needs the browser's
  File System Access API, so it works only in Chromium-based browsers
  (`export.cljs:66, 112-117`; `en.edn:679`).

---

## Organization Strategies

### Strategy 1: Namespace Hierarchies

**Use Case:** Clear domain separation

**Example:**

```
Work/
├── Projects/
│   ├── Alpha
│   ├── Beta
│   └── Gamma
├── Meetings/
│   └── 2026/
│       └── Q1/
└── Team/
    ├── Engineering
    └── Design

Personal/
├── Finance/
├── Health/
└── Learning/
```

**Pros:**

- ✅ Clear boundaries
- ✅ Easy to find content
- ✅ The same short title can exist under different parents: the split looks
  a segment up by its parent (`page.cljs:251-253`), and when several pages share
  a name, a chosen page is linked by UUID
  (`src/main/frontend/handler/page.cljs:271-277`)

**Cons:**

- ❌ Rigid structure
- ❌ A page has exactly one parent, so it can only be in one namespace
- ❌ Can become too nested

### Strategy 2: Flat with Tags

**Use Case:** Flexible, multi-dimensional organization

**Example:**

```
Pages (flat), each carrying several tags:
- [[Project Alpha]]   #work #active #client-x #engineering
- [[Project Beta]]    #work #planning #client-y #design
- [[Weekend Hike]]    #personal #health #outdoors
```

Tags are nodes stored in `:block/tags`; a tag's properties are offered on every
node that carries it, together with the properties of the tags it extends
(`deps/outliner/src/logseq/outliner/property.cljs:1020-1044`).

**Pros:**

- ✅ Flexible (multiple tags)
- ✅ Cross-cutting concerns
- ✅ Easy to reorganize

**Cons:**

- ❌ Tags have no hierarchy unless you make one: a tag can extend another
  (`:logseq.property.class/extends`), and tag queries follow that
  (`deps/db/src/logseq/db/frontend/rules.cljc:14-19, 266-295`)
- ❌ Can be chaotic
- ❌ Requires discipline

### Strategy 3: Hybrid Approach

**Use Case:** Balance structure and flexibility

**Example:**

```
Namespaces for domains, tags across them:
- Work/Projects/Alpha      #active #client-x #q1
- Personal/Goals/2026      #health #finance #learning

Flat for cross-cutting pages:
- [[Weekly Review]]        #process #routine
```

**Pros:**

- ✅ Structure where needed
- ✅ Flexibility where helpful
- ✅ Best of both worlds

### Strategy 4: Date-Based (Zettelkasten)

**Use Case:** Chronological, atomic notes

**Example:**

```
Notes/
├── 202601210930 First Note
├── 202601210945 Second Note
└── 202601211015 Third Note

Content of "202601210930 First Note":
- Follows from [[202601210945 Second Note]]; see also [[Topic X]]
```

**Pros:**

- ✅ Unique identifiers
- ✅ Timestamped
- ✅ True graph structure

**Cons:**

- ❌ Opaque naming
- ❌ Hard to browse
- ❌ Requires discipline

---

## Best Practices

### Naming Conventions

**1. Consistent Capitalization**

```markdown
✅ Good:

- [[Project Alpha]]
- [[Project Beta]]
- [[Project Gamma]]

❌ Inconsistent:

- [[project alpha]]
- [[Project Beta]]
- [[PROJECT GAMMA]]
```

Page names are matched on the lowercased `:block/name`
(`deps/db/src/logseq/db/frontend/schema.cljs:89-90`), so `[[project alpha]]`
and `[[Project Alpha]]` name the same page. Consistency is about readability.

**2. Descriptive Names**

```markdown
✅ Good:

- [[Client Onboarding Process]]
- [[Q1 2026 Planning]]

❌ Vague:

- [[Process]]
- [[Planning]]
```

**3. Mind the `/`**

A DB graph has no page files, so file-name limits on characters such as `?` or
`:` do not apply to page titles. The character that changes meaning is `/`:

```markdown
✅ Intended:

- [[Work/Projects/Alpha]]   → pages Work → Projects → Alpha

❌ Accidental:

- [[TCP/IP Basics]]         → pages TCP → IP Basics
```

(`deps/common/src/logseq/common/util/namespace.cljs:12-20`)

### Namespace Guidelines

**Depth Limit:** Max 3-4 levels

```markdown
✅ Good:

- Work/Projects/Alpha
- Work/Team/Engineering

❌ Too Deep:

- Work/Projects/2026/Q1/Alpha/Tasks/Current
```

**Consistent Roots:**

```markdown
✅ Consistent:

- Projects/Work/Alpha
- Projects/Work/Beta
- Projects/Personal/Website

❌ Inconsistent:

- Work/Projects/Alpha
- Projects/Personal/Website
- MyProjects/Beta
```

### Graph Hygiene

**Regular Cleanup:**

1. **Weekly:**

   - Review pages nothing links to
   - Merge duplicates
   - Archive old content

2. **Monthly:**

   - Remove asset nodes you no longer use
   - Archive old journals

3. **Quarterly:**
   - Reorganize namespaces if needed
   - Export both SQLite DB and assets as an archive
   - Confirm backups are being written (see [Backups](#backups))

**Archiving Strategy:**

```
Active/
└── Current content

Archive/
├── 2025/
│   └── Old content
└── 2024/
    └── Even older content
```

### Migration Between Structures

**From Flat to Namespaced:**

Create the namespaced page (for example `[[Projects/Work/Alpha]]`) and move the
content into it.

> **Unverified:** whether renaming an existing page to a title containing `/`
> splits it into a hierarchy, and how to flatten an existing hierarchy. The split
> happens when a page is created (`page.cljs:385-388`); no rename code path that
> calls it was found at this revision.

---

## Platform Differences

### File-Based Graphs

Not openable in this build. Import them with **File to DB graph** (see the note
at the top).

### Database Graphs

**Advantages:**

- ✅ No page files, so no file-name encoding or path-length problems
- ✅ Automatic backups on desktop (see [Backups](#backups))
- ✅ Remote graphs when logged in

**Limitations:**

- ❌ Storage is a SQLite file, not text: no per-page diffs in Git, and no editing
  pages in an external text editor
- ❌ The Markdown export drops block properties and flattens the hierarchy

---

## Troubleshooting

**Page not found after rename:**

- Search for the old and new name
- Check whether the new title contained `/` and so names a page with a parent

**Namespace not showing hierarchy:**

- Ensure you used the `/` separator
- A title that is a URL or starts with `./` or `../` is not split
- Journal pages are not split
- If an existing segment is not an ordinary page, creation is refused
  (`page.cljs:262-267`)

**Asset files too large:**

- Files over 100 MB are rejected (`editor.cljs:1785-1789`)
- Store large files externally and link to them by URL; an asset node can carry
  an external URL (`editor.cljs:1763-1764, 1797`)
- Compress images before adding

---

## Next Steps

**Organize Your Graph:**

1. Choose organization strategy
2. Create namespace structure
3. Migrate existing content
4. Set up regular maintenance
5. Document your conventions

**Related:**

- [Graph Visualization](./graph-visualization.md)
- [File vs Database Comparison](./file-vs-database.md)
- [Backup Strategies](../../how-to/organization/backup-strategies.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
