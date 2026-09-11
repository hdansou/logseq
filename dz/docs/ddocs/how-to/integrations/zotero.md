---
title: 'Integrating Zotero with Logseq'
status: 'Stable (file resolution only)'
version: 'Built-in Zotero Web API importer removed in bcc478b5f7 (#12276) — it was already file-graph-only; file/path resolution remains'
quadrant: 'How-To Guide'
graphType: [db]
platform: 'Desktop (Electron). Zotero paths resolve to file:// URLs; the PDF viewer is not mounted in the native mobile app'
last-verified: '2026-09-11'
verified-against: 'be800f1711'
reviewStatus: audited
related-docs:
  - '../multimedia/asset-management.md'
  - '../../reference/features/pdf-annotations.md'
  - '../workflows/research-workflow.md'
  - '../../tutorials/getting-started/03-basic-editing.md'
---

# Integrating Zotero with Logseq

> **Last revised:** 2026-09-11

## Overview

Zotero is a popular reference management tool for researchers and academics. Logseq does not ship an in-app Zotero **importer**. What remains in the current source is Zotero **file resolution**: Logseq can open PDFs and other attachments that live in your Zotero data directory. The file-graph importer also converts Zotero attachment links into assets that keep resolving to Zotero storage.

> **Removed:** The built-in Zotero Web API integration consisted of:
> - the **Zotero** row in Settings and its `/settings/zotero` page
> - the API key and user/group fields
> - the `/Zotero` search-and-import slash command
>
> It was already **file-graph only** before it was deleted. At `bcc478b5f7^`, the slash command was wrapped in `(when-not db? …)` (`src/main/frontend/commands.cljs:428-429`) and the settings row in `(when-not db-based? …)` (`src/main/frontend/components/settings.cljs:1107`). Commit `bcc478b5f7` ("refactor: separate og and db version", #12276) then deleted `src/main/frontend/extensions/zotero/{api,extractor,handler,schema,setting}.cljs`, `zotero.css` and the extractor test. It reduced `src/main/frontend/extensions/zotero.cljs` to the 101-line file-opening helpers (`git show bcc478b5f7 --stat`). You cannot search or import a Zotero library from inside a DB graph.

**What This Guide Covers**:

- What the current source still does with Zotero data (file-resolution macros, `zotero://` asset URLs, `zotero://` links)
- Configuring the Zotero paths in `logseq/config.edn`
- Plugin and manual routes for getting item metadata into a graph
- Syncing PDFs and annotations
- Citation workflows
- Linking references to pages

**Prerequisites**:

- Zotero installed ([https://www.zotero.org](https://www.zotero.org))
- Logseq Desktop app. Zotero file resolution builds `file://` URLs (`src/main/frontend/extensions/zotero.cljs:70-77`, `:96-100`), and the PDF viewer is not mounted in the native mobile app (see [PDF Annotations — Platform Compatibility](../../reference/features/pdf-annotations.md#platform-compatibility))
- A DB graph (file graphs cannot be opened, `src/main/frontend/components/repo.cljs:321-323`)
- Basic familiarity with both tools

## Findings: What Is Left

- The settings map is keyed by profile name: `:zotero/settings-v2` in `config.edn` maps a profile name to settings (`src/main/frontend/extensions/zotero.cljs:29-33`, `:64-68`).
- The macros pick the profile stored in local storage under `:zotero/setting-profile` (`zotero.cljs:58-62`). Nothing at `be800f1711` writes that key any more: it appears only in the storage spec (`src/main/frontend/spec/storage.cljc:19`, `:62`). The fallback is `(first (all-profiles config))` over a **set** of profile names (`zotero.cljs:53-56`, `:62`), so with several profiles the choice is not tied to map order.
- PDF path resolution and the file-graph importer read the `"default"` profile unconditionally (`src/main/frontend/extensions/pdf/assets.cljs:220`; `deps/graph-parser/src/logseq/graph_parser/exporter.cljs:1268`, `:1813`, `:3356-3357`). **Use a single profile named `"default"`.**
- PDF handling distinguishes imported Zotero files from linked Zotero files, with separate resolution paths for `zotero-path://` and `zotero-link://` (`src/main/frontend/extensions/pdf/assets.cljs:218-232`).
- The default settings map still carries citekey preference, attachment and note toggles, page prefix, extra tags, overwrite mode and both directory paths (`src/main/frontend/extensions/zotero.cljs:35-44`). Only the two directory paths are read by anything.
- Only four Zotero i18n keys remain: `:zotero/attachments`, `:zotero/imported-file-warning`, `:zotero/linked-file-warning`, `:zotero/notes` (`src/resources/dicts/en.edn:2005-2008`).

---

## Method 1: Built-in Zotero File Resolution

This is what the current source provides. It lets Logseq **open** Zotero-managed files; it does not import bibliographic metadata.

### Setup

There is no Zotero settings pane. Open **Settings → Edit config.edn** (`src/main/frontend/components/settings.cljs:201-207`, shown when a graph is open at `:884`; label `src/resources/dicts/en.edn:1753`) and add:

```clojure
:zotero/settings-v2
{"default"
 {:zotero-data-directory                   "/Users/you/Zotero"
  :zotero-linked-attachment-base-directory "/Users/you/Zotero/linked"}}
```

In a DB graph, saving `logseq/config.edn` validates it and stores it in the database (`src/main/frontend/handler/db_based/editor.cljs:142-165`). The config schema is an open malli map (`src/main/frontend/schema/handler/common_config.cljc:5-7`) and has no Zotero entry, so the key is accepted as-is.

When you import a file graph, its `config.edn` is copied with only the file-only keys removed (`exporter.cljs:3359-3370`); `:zotero/settings-v2` is not one of them (`deps/common/src/logseq/common/config.cljs:105`). During import, relative Zotero directories are resolved against the old graph's root (`exporter.cljs:3344-3357`, `:3371`).

> **Removed:** Step-by-step instructions for **Settings > Zotero settings**, the API key and user ID fields, the `/Zotero` slash command and its **Add all** bulk import. None of those exist at `be800f1711`, and none were available in DB graphs before they were removed.

### Usage

Two macros render an **Open** button for a Zotero attachment (`src/main/frontend/components/block.cljs:1887-1894`):

```text
{{zotero-imported-file <item-key> "<filename>"}}
{{zotero-linked-file "<relative-path>"}}
```

- The filename/path argument is read with `read-string` (`zotero.cljs:85`, `:95`), so keep the double quotes.
- `{{zotero-imported-file}}` resolves to `file://<zotero-data-directory>/storage/<item-key>/<filename>` (`zotero.cljs:70-77`).
- `{{zotero-linked-file}}` resolves under `<zotero-linked-attachment-base-directory>`, stripping a leading `attachments:` prefix (`zotero.cljs:96-100`).
- If the matching directory setting is blank, Logseq renders a warning instead of the button: "This is a Zotero imported file, set Zotero data directory to open the file in Logseq." (or the linked-file equivalent) (`zotero.cljs:83-84`, `:93-94`; `en.edn:2006-2007`).
- A path ending in `pdf` opens in Logseq's PDF viewer. Anything else becomes a button link with `target="_blank"` (`zotero.cljs:13-27`).

A PDF opened from a macro has no asset block, because the viewer is given only the path (`zotero.cljs:20-22`). Choosing a highlight colour therefore opens the **Create asset** dialog rather than saving an annotation (`src/main/frontend/extensions/pdf/core.cljs:184-186`). See [PDF Annotations — Open a PDF](../../reference/features/pdf-annotations.md#open-a-pdf).

> **Unverified:** Opening a **non-PDF** attachment was not reproduced. Electron sends `target="_blank"` links through its window-open handler (`src/electron/electron/window.cljs:186-201`). That handler strips `file://` on macOS and Linux before parsing the URL (`window.cljs:155-165`), and `open-default-app!` does nothing when the URL does not parse (`window.cljs:134-138`). So on macOS/Linux the button may do nothing.

### Imported Zotero Assets

Asset blocks whose **External URL** starts with `zotero://` are resolved to local files when opened, both from the asset (`src/main/frontend/components/block.cljs:513-519`) and from an annotation's `P<page>` link (`src/main/frontend/extensions/pdf/assets.cljs:238-242`). The file comes from the asset's `:logseq.property.asset/external-file-name` (`deps/db/src/logseq/db/frontend/property.cljs:565-569`):

| `external-file-name` prefix | Resolves to                                                      |
| --------------------------- | ---------------------------------------------------------------- |
| `zotero-link://<path>`      | `file://<zotero-linked-attachment-base-directory>/<path>`        |
| `zotero-path://<path>`      | `file://<zotero-data-directory>/storage/<path>`                  |
| anything else               | `file://<zotero-data-directory>/storage/<item-id>/<name>`        |

(`assets.cljs:218-232`). Clicking a `zotero://` External URL value does not open the edit dialog that other external URLs get (`src/main/frontend/components/property/value.cljs:44-48`).

These assets are created by the **file-graph importer**. It detects:
- Markdown links with the `zotero` protocol (`deps/graph-parser/src/logseq/graph_parser/exporter.cljs:1261-1271`, `:1336-1337`)
- `{{zotero-imported-file}}` / `{{zotero-linked-file}}` macros (`exporter.cljs:1345-1357`)

It then builds `zotero-path://` / `zotero-link://` names (`exporter.cljs:1727-1730`) and stores them with the `zotero://` link as the external URL (`exporter.cljs:1743-1753`). A file graph that used the old integration keeps its PDFs openable after import. There is no UI for creating such assets by hand.

### Links Back to Zotero

A Markdown link with a custom protocol renders as a normal link (`block.cljs:1519-1523`):

```markdown
[Open in Zotero](zotero://select/library/items/ABCD1234)
```

On desktop, Electron asks "Are you sure you want to open this link?" for any protocol other than http, https and mailto, then hands the URL to the operating system (`window.cljs:134-149`, `window.cljs:186-201`; `en.edn:578`). URL-type properties accept `zotero://` values (`deps/db/src/logseq/db/frontend/property/type.cljs:85-93`).

> **Unverified:** Whether the OS then opens Zotero depends on Zotero having registered the `zotero:` protocol; this was not tested.

### Getting metadata in

Because there is no importer, create reference pages yourself or with a plugin (below), then attach the PDF with the macros above, a `file://` link, or an uploaded asset. In a DB graph:

- Add metadata with `/Add property` (`src/main/frontend/commands.cljs:354-358`). Typing `author:: Smith` into a block is plain text, not a property.
- User property idents get a random suffix (`:user.property/author-XXXXXXXX`), so query them by title. See [Research Workflow — Before You Start](../workflows/research-workflow.md#before-you-start-how-this-maps-onto-a-db-graph).

### Optional: Better BibTeX

For consistent citation keys (e.g., `smith2024` instead of the full item title):

1. Install [Better BibTeX for Zotero](https://retorque.re/zotero-better-bibtex/)
2. Configure your preferred citation key format (e.g., `[auth:lower][year]`)
3. Use those keys as your Logseq page names when you create reference pages

The `:prefer-citekey?` default still exists in `zotero.cljs:37` but nothing reads it.

---

## Method 2: Third-Party Plugins

With no built-in importer, a community plugin is the only way to search a Zotero library from inside Logseq. Plugins are not part of Logseq's source, so nothing about a specific plugin is verified here.

- Search the plugin marketplace for Zotero; check that a plugin supports **DB graphs** and the current Logseq version before relying on it.
- Plugins run without a sandbox, so review what you install.
- Plugins are not available in the native mobile app (`src/main/frontend/util.cljc:131`).

> **Unverified:** Plugin names, features and commands. The marketplace catalogue is not in the Logseq repository.

---

## Method 3: Manual Entry from a CSV Export

### Export from Zotero

1. In Zotero, select items to export
2. Right-click > **Export Items**
3. Format: **CSV**
4. Save to file (e.g., `zotero-export.csv`)

### Create Pages in Logseq

For each reference, create a page named after the citation key, then add properties with `/Add property`:

```txt
Page: @smith2024
  properties (added with /Add property):
    title       Machine Learning for Everyone
    author      John Smith, Jane Doe
    year        2024
    source-type Book
    publisher   MIT Press
    url         https://example.com/book
  tags: research, ML, book
```

The layout above is shorthand for the properties on the page, not text to type.

> **Unverified:** Logseq was not checked for a built-in CSV importer for DB graphs, and no specific CSV-import plugin is documented here.

---

## Method 4: Zotero Better Notes + Markdown Export

### Setup (Zotero side)

1. Install the **Zotero Better Notes** plugin:
   - Download: [https://github.com/windingwind/zotero-better-notes](https://github.com/windingwind/zotero-better-notes)
   - In Zotero: Tools > Add-ons > Install from file
2. Configure an export template. The `{{…}}` placeholders below are Better Notes syntax, not Logseq syntax:

   ```markdown
   # {{title}}

   author:: {{author}}
   year:: {{year}}
   url:: {{url}}

   ## Notes

   {{notes}}
   ```

> **Unverified:** Menu paths and template syntax belong to the Better Notes plugin and were not checked.

### Bring the Export into Logseq

A DB graph has no `pages/` folder to drop Markdown files into; it is a single `db.sqlite` plus `assets/` (`deps/db/src/logseq/db/common/sqlite.cljs:29-33`, `deps/common/src/logseq/common/config.cljs:56`). Copy the exported note's text and paste it into a page. Then re-create the metadata with `/Add property`.

> **Unverified:** Whether pasted `key:: value` lines are converted into properties was not checked. Assume they arrive as text.

---

## Syncing PDFs

### Zotero Data Directory

Logseq can open PDFs stored in Zotero's local data directory. Set `:zotero-data-directory` under `:zotero/settings-v2` → `"default"` in `logseq/config.edn` so Logseq can locate imported attachments. For linked attachments, set `:zotero-linked-attachment-base-directory`.

**Zotero's default data directory** (Zotero documentation, not Logseq source):

- **macOS**: `~/Zotero/`
- **Windows**: `C:\Users\YourName\Zotero\`
- **Linux**: `~/Zotero/`

### Copy PDFs into the Graph

If you prefer to keep PDFs in your Logseq graph:

1. In Zotero, right-click the item and choose **Show File**
2. In Logseq, open the reference page and start editing a block
3. Type `/Upload an asset` (`src/main/frontend/commands.cljs:335-338`) and pick the PDF, or drop the file onto the editor

Logseq copies the file to `assets/<asset-uuid>.pdf` and creates an `#Asset` block (`src/main/frontend/handler/editor.cljs:1760-1802`). The asset is inserted at the block you are editing (`editor.cljs:1840-1851`). Do not copy files into `assets/` on disk yourself; that creates no asset block.

### Zotero Link (Alternative)

Instead of copying, point at Zotero storage. Either use the macro, which follows your configured data directory:

```text
{{zotero-imported-file ABC123 "paper.pdf"}}
```

Or hard-code an absolute path in the block text:

```markdown
![paper](file:///Users/yourname/Zotero/storage/ABC123/paper.pdf)
```

With the leading `!`, a `file://` link to a PDF renders as a PDF link that opens the viewer (`src/main/frontend/components/block.cljs:1580-1582`, `:1417-1422`). Without it, Electron opens the file with the system app (`block.cljs:1583-1600`).

Note: `file://` paths break if the Zotero storage path changes. The macro form survives a move as long as you update `:zotero-data-directory`. PDFs opened either way have no asset block, so annotations require creating an asset first.

---

## Annotation Workflows

### Workflow 1: Annotate in Zotero, Copy to Logseq

1. **Annotate in Zotero**:

   - Open PDF in Zotero reader
   - Highlight text
   - Add notes

2. **Export Annotations** (Zotero feature):

   - Right-click item > Create Note from Annotations
   - Copy note content

3. **Paste in Logseq**:
   ```markdown
   - [[@smith2024]]
     - Annotations from Zotero:
       - "Key insight here" (p. 42) #highlight
         - **My note**: This relates to [[Other Paper]]
   ```

### Workflow 2: Annotate in Logseq

1. **Upload the PDF** as an asset (see above)
2. **Open it** in the Logseq PDF viewer
3. **Highlight** text or areas (see [PDF Annotations Reference](../../reference/features/pdf-annotations.md))
4. Each highlight becomes a block tagged `PDF Annotation` under the PDF asset (`src/main/frontend/extensions/pdf/assets.cljs:92-110`)

**Advantage**: Annotations are native Logseq blocks and can be linked and queried.

**Disadvantage**: Nothing writes annotations back to Zotero; no Zotero API client remains in the source.

---

## Citation Management

### BibTeX Keys

**Generate Citation Keys** (with Better BibTeX):

- Format: `smith2024` (author + year)
- Unique keys prevent collisions: `smith2024a`, `smith2024b`

**Use in Logseq**. The old file-graph importer prefixed page names with `@` by default (`:page-insert-prefix`, `zotero.cljs:44`); that convention is still a good one to keep by hand:

```markdown
- According to [[@smith2024]], the results show...
```

A `/` in a page title does **not** create a namespaced title in a DB graph. `[[zotero/smith2024]]` creates a `zotero` page with a child page `smith2024` (`deps/outliner/src/logseq/outliner/page.cljs:385-388`). Prefer a tag, e.g. tag reference pages `#Reference`.

### Citation Format

**APA Style**:

```markdown
- Smith, J., & Doe, J. (2024). _Machine Learning for Everyone_. MIT Press.
```

**Chicago Style**:

```markdown
- Smith, John, and Jane Doe. 2024. _Machine Learning for Everyone_. Cambridge, MA: MIT Press.
```

**In-Text Citation**:

```markdown
- Recent work (Smith & Doe, 2024) demonstrates...
```

**Generate Formatted Citations** (Zotero feature):

1. In Zotero: Right-click item > Create Bibliography from Item
2. Select style (APA, Chicago, MLA, etc.)
3. Copy formatted citation
4. Paste in Logseq

---

## Organizing Research

### Literature Note Template

Build the template as a block tree, then give its top block the built-in **Template** tag with the block's tag control. Add properties with `/Add property`. The outline below is shorthand; `prop: value` lines stand for properties, not text to type.

```txt
- Literature note                      ← tagged Template
  properties: source-type, author, year, publisher, url
  read-date: <% today %>

  - Bibliographic Info
    - (upload or link the PDF here)

  - Summary
    - Main argument:
    - Key findings:
      - Finding 1
      - Finding 2

  - Key Quotes
    - "Quote 1" (p. 42) #quote
    - "Quote 2" (p. 58) #quote

  - My Thoughts
    - This connects to [[Related Paper]]
    - Potential application: [[Project X]]
```

Only five `<% %>` variables are substituted when a template is inserted: `today`, `yesterday`, `tomorrow`, `time`, `current page` (`deps/outliner/src/logseq/outliner/template.cljs:104-112`). Any other name loses its wrapper and stays as bare text (`template.cljs:114-128`). There is no prompt for a title or author; fill those in after inserting.

### Use the Template

1. Create or open the reference page, e.g. `[[@smith2024]]`
2. Type `/Template` (`src/main/frontend/commands.cljs:339-343`) and pick "Literature note"

### Reading Workflow

**1. Create the Reference Page**

- Create the page by hand, via a plugin, or from a CSV / Better Notes export
- Name it with the citation key and a prefix, e.g. `[[@smith2024]]`

**2. Add PDF**

- If `:zotero-data-directory` is configured, use `{{zotero-imported-file <item-key> "<filename>"}}`
- Or upload the PDF as an asset on the reference page (`/Upload an asset`) — required if you want saved highlights

**3. First Pass Reading**

- Annotate the PDF with Logseq's highlighter
- Tag annotation blocks if useful: `#important`, `#definition`, `#quote`

**4. Extract Notes**

- Review annotations (toolbar **Annotations page**)
- Write summary in own words
- Connect to existing notes: `[[Related Concept]]`

**5. Synthesize**

- Create synthesis page: `[[Research Topic X]]`
- Link relevant papers: `See [[@smith2024]] for background`
- Query all notes on the topic — type `/Query`, then enter:
  ```clojure
  (and [[Research Topic X]] (tags research))
  ```

---

## Querying References (Database Graphs)

`{{query …}}` does not run in a DB graph; it renders a deprecation warning (`src/main/frontend/components/block.cljs:1870-1871`). Type `/Query` and enter the bare form. A form must start with `(`, otherwise it becomes a full-text search (`src/main/frontend/components/query/builder.cljs:543-552`).

`(property name value)` looks the property up by its **title** (`src/main/frontend/worker/query_dsl.cljs:343-356`), so these assume properties you created called `source-type`, `author`, `year` and `pdf`. A `[[Page]]` value is reduced to the page name (`query_dsl.cljs:329-336`). Avoid naming a property `Status`; it collides with the built-in task status (see [Research Workflow](../workflows/research-workflow.md#reading-status-tracking)).

### All References of a Type

```clojure
(property source-type "Book")
```

### By Author

```clojure
(property author [[John Smith]])
```

### By Year

```clojure
(property year 2024)
```

> **Unverified:** Numbers are passed through unchanged (`query_dsl.cljs:340-341`); matching against a Number-type property was not tested.

### Unread Papers

```clojure
(and (property source-type "Paper") (not (tags reading-completed)))
```

### References with a PDF Property

The one-argument form matches blocks that have the property at all (`query_dsl.cljs:394-409`):

```clojure
(property pdf)
```

---

## Troubleshooting

### `/Zotero` Does Nothing

**Problem**: There is no `/Zotero` slash command and no Zotero pane in Settings.

**Cause**: Expected. The Web API integration was file-graph only (`commands.cljs:428-429` and `settings.cljs:1107` at `bcc478b5f7^`) and was removed in `bcc478b5f7` (#12276). Use a plugin or one of the manual routes.

### "This is a Zotero imported file" Warning Instead of an Open Button

**Problem**: A `{{zotero-imported-file}}` or `{{zotero-linked-file}}` macro renders a warning.

**Solutions**:

1. Set `:zotero-data-directory` (imported files) or `:zotero-linked-attachment-base-directory` (linked files) under `:zotero/settings-v2` in `logseq/config.edn`
2. Put them under a single `"default"` profile; PDF resolution and the importer ignore other profiles
3. The macros read the config through a subscription (`zotero.cljs:29-33`), so re-render the page after saving `config.edn`

### PDFs Not Opening

**Problem**: Clicking a Zotero PDF doesn't open the file

**Solutions**:

1. Verify the PDF exists at the resolved path (`<data-directory>/storage/<item-key>/<filename>`)
2. Set `:zotero-data-directory` in `config.edn` so Logseq can resolve imported file paths
3. For linked attachments, set `:zotero-linked-attachment-base-directory`
4. Check the loader error (missing, corrupted or password-protected; see [PDF Annotations — Troubleshooting](../../reference/features/pdf-annotations.md#troubleshooting))
5. On mobile, the PDF viewer is not mounted; use desktop

### Citation Keys Not Working

**Problem**: Pages end up with full titles instead of citation keys

**Solutions**:

1. Install [Better BibTeX for Zotero](https://retorque.re/zotero-better-bibtex/)
2. Ensure Better BibTeX has generated citation keys for your items (check the Extra field in Zotero item info)
3. Name pages from the citekey when you create them; Logseq does not do this for you

---

## Alternative Tools

### Pandoc for Citations

**Use Case**: Generate formatted bibliographies from Markdown

**Setup**:

1. Install Pandoc: [https://pandoc.org](https://pandoc.org)
2. Export Zotero library as BibTeX (`.bib` file)
3. In Markdown, use citation syntax: `[@smith2024]`
4. Convert with Pandoc:
   ```bash
   pandoc input.md --bibliography=library.bib --csl=apa.csl -o output.pdf
   ```

---

## Best Practices

### Keep Zotero as Source of Truth

**Do**:

- Maintain complete bibliographic data in Zotero
- Bring items into Logseq for note-taking through a plugin or export, and keep Logseq pages thin
- Use Zotero for PDF organization, and point Logseq at it with `:zotero-data-directory`

**Don't**:

- Duplicate metadata manually (error-prone)
- Store PDFs in both places unnecessarily (but upload a copy when you want saved Logseq highlights)

### Consistent Naming

**Citation Keys**: Use Better BibTeX auto-generation and name your Logseq pages from the generated key.

- `smith2024` (lowercase, consistent format)
- Avoid manual keys (leads to conflicts)

**Page Prefix**: An `@` prefix (e.g., `@smith2024`), the old importer's default, keeps reference pages distinct from regular pages.

### Regular Syncs

**Zotero Syncing**:

- Logseq reads Zotero files from disk, so Zotero must have downloaded the attachments locally
- If you use a plugin that reads the Zotero Web API, keep your library synced so the plugin sees current items

### Backup Both Tools

**Zotero**: Settings > Advanced > Files and Folders > Data Directory Location

- Back up this folder (includes database + PDFs)

**Logseq**: Use **Export both SQLite DB and assets** (`src/resources/dicts/en.edn:666`). Zotero files referenced by path or `zotero://` are not inside the graph, so back them up with Zotero.

---

## Zotero Settings Reference

Zotero settings live in your graph's `logseq/config.edn` under `:zotero/settings-v2`, keyed by profile name. Defaults come from `src/main/frontend/extensions/zotero.cljs:35-51`.

| Setting | Description (old importer) | Default | Still read? |
|---------|-------------|---------|-------------|
| `:zotero-data-directory` | Path to local Zotero data folder | `""` | Yes |
| `:zotero-linked-attachment-base-directory` | Path for linked file attachments | `""` | Yes |
| `:type` | Library type: `:user` or `:group` | `:user` | No |
| `:prefer-citekey?` | Use Better BibTeX citation key as page name | `true` | No |
| `:include-attachments?` | Import attachment links | `true` | No |
| `:attachments-block-text` | Heading text for attachments block | localized `:zotero/attachments`, `"Attachments"` in English | No |
| `:include-notes?` | Import Zotero notes | `true` | No |
| `:notes-block-text` | Heading text for notes block | localized `:zotero/notes`, `"Notes"` in English | No |
| `:overwrite-mode?` | Overwrite existing page data on re-import | `false` | No |
| `:extra-tags` | Additional tags for imported items | `""` | No |
| `:page-insert-prefix` | Prefix for imported page names | `"@"` | No |

"Still read?" reflects `be800f1711`. Only the two directory paths are consumed, by `zotero.cljs` (`:74`, `:92`), `pdf/assets.cljs` (`:220-222`) and the file-graph importer (`exporter.cljs:1268`, `:1813`). The rest are defaults left from the removed importer. `:zotero/api-key-v2` and `:zotero/setting-profile` still appear in the local-storage spec (`src/main/frontend/spec/storage.cljc:18-19`, `:61-62`), but nothing reads the API key and nothing writes the profile.

---

## Related Documentation

**How-To Guides**:

- [Asset Management](../multimedia/asset-management.md) — Managing PDF files
- [Research Workflow](../workflows/research-workflow.md) — Literature notes, properties and templates in DB graphs

**References**:

- [PDF Annotations](../../reference/features/pdf-annotations.md) — Annotating PDFs in Logseq

**Tutorials**:

- [Basic Editing](../../tutorials/getting-started/03-basic-editing.md) — Creating and linking pages

**External Resources**:

- [Zotero Documentation](https://www.zotero.org/support/)
- [Better BibTeX Guide](https://retorque.re/zotero-better-bibtex/)

---

**Status**: Reduced scope — file resolution only; the file-graph-only Zotero Web API importer was removed in `bcc478b5f7` (#12276)
**Feedback**: Report issues on [Logseq GitHub](https://github.com/logseq/logseq) or [Logseq Forums](https://discuss.logseq.com/)
