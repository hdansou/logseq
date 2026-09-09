---
title: 'Integrating Zotero with Logseq'
status: 'Stable (file resolution only)'
version: 'Built-in Zotero Web API integration removed in bcc478b5f7 (#12276); file/path integration remains'
quadrant: 'How-To Guide'
platform: 'Desktop (macOS/Windows/Linux)'
last-verified: '2026-09-09'
verified-against: 'be800f1711'
related-docs:
  - '../multimedia/asset-management.md'
  - '../../reference/features/pdf-annotations.md'
  - '../../tutorials/getting-started/basic-editing.md'
---

# Integrating Zotero with Logseq

> **Last revised:** 2026-09-09

## Overview

Zotero is a popular reference management tool for researchers and academics. Logseq no longer ships an in-app Zotero **importer**; what remains in the current source is Zotero **file resolution** -- Logseq can open PDFs and other attachments that live in your Zotero data directory, and the file-graph importer converts Zotero attachment links into `zotero-link://` / `zotero-path://` assets.

> **Removed:** The built-in Zotero Web API integration -- the **Settings > Zotero settings** pane, the API key / user ID fields, and the `/Zotero` search-and-import command -- is gone from the current source. Commit `bcc478b5f7` ("refactor: separate og and db version", #12276) deleted `src/main/frontend/extensions/zotero/{api,extractor,handler,schema,setting}.cljs`, leaving only `src/main/frontend/extensions/zotero.cljs` (101 lines) with the file-opening helpers. Do not expect to search or import a Zotero library from inside Logseq at `be800f1711`.

**What This Guide Covers**:

- What the current source still does with Zotero data (file resolution, macros, `zotero://` links)
- Configuring the Zotero paths in `logseq/config.edn`
- Third-party plugin and manual import routes, which are now the only way to get item metadata into a graph
- Syncing PDFs and annotations
- Citation workflows
- Linking references to pages

**Prerequisites**:

- Zotero installed ([https://www.zotero.org](https://www.zotero.org))
- Logseq Desktop app (Zotero file resolution builds `file://` URLs, so it is desktop-only)
- Basic familiarity with both tools
- For the plugin and manual routes only: a Zotero account with [syncing enabled](https://www.zotero.org/support/sync) and, where a plugin asks for it, a [Zotero API key](https://www.zotero.org/settings/keys)

## Findings: New Capabilities

- Zotero settings are profile-aware: `:zotero/settings-v2` in `config.edn` is a map of profile name to settings, and the active profile is stored in local storage under `:zotero/setting-profile` (`src/main/frontend/extensions/zotero.cljs:53-60`).
- That profile awareness is only partial. PDF path resolution and the file-graph importer both read the `"default"` profile unconditionally (`src/main/frontend/extensions/pdf/assets.cljs:220`, `deps/graph-parser/src/logseq/graph_parser/exporter.cljs:1268,1813,3356-3357`), so a non-default profile will not change how attachments resolve.
- PDF handling distinguishes imported Zotero files from linked Zotero files, with separate resolution paths for `zotero-path://` and `zotero-link://` assets (`src/main/frontend/extensions/pdf/assets.cljs:218-232`).
- The settings map still carries citekey preference, attachment and note import toggles, page prefix, extra tags, overwrite mode, and both Zotero directory paths (`src/main/frontend/extensions/zotero.cljs:35-44`) -- but with the importer removed, only the two directory paths are still read by anything.
- Only four Zotero i18n keys survive (`:zotero/attachments`, `:zotero/notes`, `:zotero/imported-file-warning`, `:zotero/linked-file-warning`, `src/resources/dicts/en.edn:2005-2008`), which is a good proxy for how much of the feature is left.

---

## Method 1: Built-in Zotero File Resolution

This is what the current source actually provides. It lets Logseq **open** Zotero-managed files; it does not import bibliographic metadata.

### Setup

There is no Zotero settings pane. Configure the paths directly in your graph's `logseq/config.edn`:

```clojure
:zotero/settings-v2
{"default"
 {:zotero-data-directory                   "/Users/you/Zotero"
  :zotero-linked-attachment-base-directory "/Users/you/Zotero/linked"}}
```

The top-level key is a **profile name**. `zotero.cljs` picks the profile stored in local storage under `:zotero/setting-profile`, falling back to the first profile in the map. Everything else that touches these paths -- PDF resolution and the file-graph importer -- hardcodes `"default"`, so keep your working settings there.

> **Removed:** Step-by-step instructions for **Settings > Zotero settings**, the API key and user ID fields, and the `/Zotero` slash command. None of those exist at `be800f1711`.

> **Removed:** The current Zotero settings UI no longer exposes a built-in **Add all** bulk-import control.

> **Removed:** The current source does not document an automatic append-on-reimport behavior. If you need a fresh import, delete the existing page first and import it again.

### Usage

Two macros render an **Open** button for a Zotero attachment (`src/main/frontend/components/block.cljs:1887-1894`):

```text
{{zotero-imported-file <item-key> "<filename>"}}
{{zotero-linked-file "<relative-path>"}}
```

- `{{zotero-imported-file}}` resolves to `<zotero-data-directory>/storage/<item-key>/<filename>`.
- `{{zotero-linked-file}}` resolves under `<zotero-linked-attachment-base-directory>`, stripping a leading `attachments:` prefix.
- If the matching directory setting is blank, Logseq renders a warning instead of the button ("This is a Zotero imported file, set Zotero data directory to open the file in Logseq.").
- PDFs open in Logseq's own PDF viewer; anything else opens in a new tab.

Asset paths beginning with `zotero-link://` or `zotero-path://`, and asset external URLs beginning with `zotero://`, are resolved the same way (`src/main/frontend/extensions/pdf/assets.cljs:218-232`). The file-graph importer produces those prefixes when it converts a Zotero attachment link (`deps/graph-parser/src/logseq/graph_parser/exporter.cljs:1727-1730`), so a graph imported from a file graph that used the old Zotero integration keeps working.

### Getting metadata in

Because the importer is gone, use one of the routes below -- a third-party plugin, a CSV export, or Better Notes Markdown export -- to create the reference pages themselves, and use the macros above (or a plain `file://` link) to attach the PDF.

### Optional: Better BibTeX

For consistent citation keys (e.g., `smith2024` instead of the full item title):

1. Install [Better BibTeX for Zotero](https://retorque.re/zotero-better-bibtex/)
2. Configure your preferred citation key format (e.g., `[auth:lower][year]`)
3. Use those keys as your Logseq page names when you create reference pages

The `:prefer-citekey?` setting still exists in the settings map but is no longer read by anything in the app, since the importer that consumed it was removed.

---

## Method 2: Third-Party Plugins

With the built-in importer removed, a community plugin is the closest replacement for searching a Zotero library from inside Logseq. Check each plugin's compatibility with the current Logseq version before relying on it.

### Logseq Citation Manager

A plugin-based approach that reads a local Zotero library:

- **Plugin**: [logseq-citation-manager](https://github.com/sawhney17/logseq-citation-manager/) by sawhney17
- Supports local Zotero libraries
- Search for "Citation Manager" in the Logseq Marketplace

### Other Options

- **zotero-mdnotes**: A Zotero plugin that exports item metadata and notes as Markdown files, which can then be placed in your Logseq graph's `pages/` folder. See [zotero-mdnotes](https://argentinaos.com/zotero-mdnotes/).
- Search the Logseq Marketplace for other Zotero-related plugins; check reviews and compatibility before installing.

---

## Method 3: Manual CSV Import

### Export from Zotero

1. In Zotero, select items to export
2. Right-click > **Export Items**
3. Format: **CSV**
4. Save to file (e.g., `zotero-export.csv`)

### Import to Logseq

**Option A: Create Pages Manually**

For each reference, create a page:

```markdown
- [[smith2024]]
  - title:: Machine Learning for Everyone
  - author:: [[John Smith]], [[Jane Doe]]
  - year:: 2024
  - type:: Book
  - publisher:: MIT Press
  - url:: https://example.com/book
  - pdf:: ![](../assets/smith2024.pdf)
  - tags:: #research #ML #book
```

**Option B: Use CSV Import Plugin**

1. Install CSV Import plugin from marketplace
2. Open Logseq
3. Run command: `/Import CSV`
4. Select Zotero CSV export
5. Map columns to properties:
   - `Title` > `title`
   - `Author` > `author`
   - `Year` > `year`
   - etc.
6. Click **Import**

---

## Method 4: Zotero Better Notes + Markdown Export

### Setup

1. Install **Zotero Better Notes** plugin:
   - Download: [https://github.com/windingwind/zotero-better-notes](https://github.com/windingwind/zotero-better-notes)
   - In Zotero: Tools > Add-ons > Install from file
2. Configure export template:
   - Tools > Better Notes > Preferences > Export Templates
   - Create Markdown template:

     ```markdown
     # {{title}}

     author:: {{author}}
     year:: {{year}}
     url:: {{url}}

     ## Notes

     {{notes}}
     ```

### Export to Logseq

1. Select item in Zotero
2. Right-click > Better Notes > Export to Markdown
3. Save to Logseq graph `pages/` folder
4. File automatically appears in Logseq

---

## Syncing PDFs

### Zotero Data Directory

Logseq can open PDFs stored in Zotero's local data directory. Set `:zotero-data-directory` under `:zotero/settings-v2` in `logseq/config.edn` so Logseq can locate imported file attachments.

**Default Zotero Data Directory Locations**:

- **macOS**: `~/Zotero/`
- **Windows**: `C:\Users\YourName\Zotero\`
- **Linux**: `~/Zotero/`

For linked file attachments (as opposed to imported/stored attachments), set `:zotero-linked-attachment-base-directory`.

### Copy PDFs to Logseq

If you prefer to keep PDFs within your Logseq graph:

**Find PDF**:

1. Right-click item in Zotero
2. Select **Show File**
3. Reveals PDF in file manager

**Copy to Logseq**:

```bash
# macOS/Linux
cp ~/Zotero/storage/ABC123/paper.pdf /path/to/logseq-graph/assets/

# Windows (PowerShell)
Copy-Item "C:\Users\YourName\Zotero\storage\ABC123\paper.pdf" "C:\path\to\logseq-graph\assets\"
```

**Link in Logseq**:

```markdown
- [[smith2024]]
  - pdf:: ![](../assets/paper.pdf)
```

### Zotero Link (Alternative)

Instead of copying, point at Zotero storage. Either use the macro, which follows your configured data directory:

```text
{{zotero-imported-file ABC123 "paper.pdf"}}
```

Or hard-code an absolute path:

```markdown
- [[smith2024]]
  - pdf:: file:///Users/yourname/Zotero/storage/ABC123/paper.pdf
```

Note: `file://` links are desktop-only and break if the Zotero storage path changes. The macro form survives a move, as long as you update `:zotero-data-directory`.

---

## Annotation Workflows

### Workflow 1: Annotate in Zotero, Export to Logseq

1. **Annotate in Zotero**:

   - Open PDF in Zotero reader
   - Highlight text
   - Add notes

2. **Export Annotations**:

   - Right-click item > Create Note from Annotations
   - Copy note content

3. **Paste in Logseq**:
   ```markdown
   - [[smith2024]]
     - Annotations from Zotero:
       - "Key insight here" (p. 42) #highlight
         - **My note**: This relates to [[Other Paper]]
   ```

### Workflow 2: Annotate in Logseq

1. **Copy PDF to Logseq** (see above)
2. **Open in Logseq PDF viewer**
3. **Highlight & annotate** directly (see [PDF Annotations Reference](../../reference/features/pdf-annotations.md))
4. **Annotations stored as blocks** in Logseq

**Advantage**: Annotations are native Logseq blocks, can be linked and queried.

**Disadvantage**: Annotations not synced back to Zotero.

---

## Citation Management

### BibTeX Keys

**Generate Citation Keys** (with Better BibTeX):

- Format: `smith2024` (author + year)
- Unique keys prevent collisions: `smith2024a`, `smith2024b`

**Use in Logseq**. The old importer prefixed page names with `@` by default (`:page-insert-prefix`); that convention is still a good one to keep by hand:

```markdown
- According to [[@smith2024]], the results show...
```

A namespace prefix works equally well for hierarchical organization:

```markdown
- According to [[zotero/smith2024]], the results show...
```

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

**Generate Formatted Citations**:

1. In Zotero: Right-click item > Create Bibliography from Item
2. Select style (APA, Chicago, MLA, etc.)
3. Copy formatted citation
4. Paste in Logseq

---

## Organizing Research

### Literature Note Template

```markdown
- [[@smith2024]]

  - **Bibliographic Info**

    - title:: Machine Learning for Everyone
    - author:: [[John Smith]], [[Jane Doe]]
    - year:: 2024
    - type:: Book
    - publisher:: MIT Press
    - url:: https://example.com/book
    - pdf:: ![](../assets/smith2024.pdf)

  - **Summary**

    - Main argument: [your summary]
    - Key findings:
      - Finding 1
      - Finding 2

  - **Key Quotes**

    - "Quote 1" (p. 42) #quote
    - "Quote 2" (p. 58) #quote

  - **My Thoughts**

    - This connects to [[Related Paper]]
    - Potential application: [[Project X]]

  - **Tags**: #research #ML #book #reading/completed
```

### Create Template in Logseq

1. Create page: `[[Templates/Literature Note]]`
2. Paste template above
3. Use via command: `/Template` > Select "Literature Note"

### Reading Workflow

**1. Create the Reference Page**

- Create the page by hand, via a citation plugin, or from a CSV / Better Notes export
- Name it with the citation key and a prefix, e.g. `[[@smith2024]]`

**2. Add PDF**

- If `:zotero-data-directory` is configured, use `{{zotero-imported-file <item-key> "<filename>"}}`
- Or copy PDF to `assets/smith2024.pdf` and link: `pdf:: ![](../assets/smith2024.pdf)`

**3. First Pass Reading**

- Annotate PDF with Logseq's highlighter
- Tag passages: #important, #definition, #quote

**4. Extract Notes**

- Review annotations
- Write summary in own words
- Connect to existing notes: `[[Related Concept]]`

**5. Synthesize**

- Create synthesis page: `[[Research Topic X]]`
- Link relevant papers: `See [[@smith2024]] for background`
- Query all notes on topic:
  ```clojure
  {{query (and [[Research Topic X]] #research)}}
  ```

---

## Querying References (Database Graphs)

### All References

```clojure
{{query (property :type "Book")}}
```

### By Author

```clojure
{{query (property :author [[John Smith]])}}
```

### By Year

```clojure
{{query (property :year 2024)}}
```

### Unread Papers

```clojure
{{query (and (property :type "Paper")
             (not [[reading/completed]]))}}
```

### References with PDFs

```clojure
{{query (property :pdf)}}
```

---

## Troubleshooting

### `/Zotero` Does Nothing

**Problem**: There is no `/Zotero` slash command and no Zotero pane in Settings.

**Cause**: Expected. The Web API integration was removed in `bcc478b5f7` (#12276). Use a community plugin or one of the manual import routes below instead.

### "This is a Zotero imported file" Warning Instead of an Open Button

**Problem**: A `{{zotero-imported-file}}` or `{{zotero-linked-file}}` macro renders a warning.

**Solutions**:

1. Set `:zotero-data-directory` (imported files) or `:zotero-linked-attachment-base-directory` (linked files) under `:zotero/settings-v2` in `logseq/config.edn`
2. Put them under the `"default"` profile -- PDF resolution and the importer ignore other profiles
3. Reload the graph after editing `config.edn`

### PDFs Not Opening

**Problem**: Clicking PDF link doesn't open file

**Solutions**:

1. Verify PDF exists at specified path
2. Set `:zotero-data-directory` in `config.edn` so Logseq can resolve imported file paths
3. For linked attachments, set `:zotero-linked-attachment-base-directory`
4. Use relative paths: `../assets/file.pdf` (not absolute) for PDFs stored in the graph
5. Check file permissions (read access required)

### Citation Keys Not Working

**Problem**: Pages end up with full titles instead of citation keys

**Solutions**:

1. Install [Better BibTeX for Zotero](https://retorque.re/zotero-better-bibtex/)
2. Ensure Better BibTeX has generated citation keys for your items (check the Extra field in Zotero item info)
3. Name pages from the citekey when you create them; Logseq no longer does this for you

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
- Store PDFs in both places unnecessarily

### Consistent Naming

**Citation Keys**: Use Better BibTeX auto-generation and name your Logseq pages from the generated key.

- `smith2024` (lowercase, consistent format)
- Avoid manual keys (leads to conflicts)

**Page Prefix**: An `@` prefix (e.g., `@smith2024`) -- the old importer's default -- keeps reference pages distinct from regular pages.

### Regular Syncs

**Zotero Syncing**:

- Logseq reads Zotero files from disk, so what matters is that Zotero has actually downloaded the attachments locally
- If you use a plugin that reads the Zotero Web API, keep your library synced so the plugin sees current items

### Backup Both Tools

**Zotero**: Settings > Advanced > Files and Folders > Data Directory Location

- Backup this folder (includes database + PDFs)

**Logseq**: Backup entire graph folder (includes `assets/` with PDFs)

---

## Zotero Settings Reference

Zotero settings live in your graph's `logseq/config.edn` under `:zotero/settings-v2`, keyed by profile name. Defaults come from `src/main/frontend/extensions/zotero.cljs:35-48`.

| Setting | Description | Default | Still read? |
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

The "Still read?" column reflects `be800f1711`: only the two directory paths are consumed, by `zotero.cljs`, `pdf/assets.cljs`, and the file-graph importer. The rest are leftovers from the removed importer. `:zotero/api-key-v2` and `:zotero/setting-profile` also still appear in the local-storage spec (`src/main/frontend/spec/storage.cljc:18-19`), but nothing reads the API key.

---

## Related Documentation

**How-To Guides**:

- [Asset Management](../multimedia/asset-management.md) -- Managing PDF files
- [PDF Annotations](../../reference/features/pdf-annotations.md) -- Annotating PDFs in Logseq

**Tutorials**:

- [Basic Editing](../../tutorials/getting-started/basic-editing.md) -- Creating and linking pages

**External Resources**:

- [Zotero Documentation](https://www.zotero.org/support/)
- [Better BibTeX Guide](https://retorque.re/zotero-better-bibtex/)
- [Logseq Citation Manager Plugin](https://github.com/sawhney17/logseq-citation-manager/)

---

**Status**: Reduced scope -- file resolution only; the built-in Zotero Web API importer was removed in `bcc478b5f7` (#12276)
**Feedback**: Report issues on [Logseq GitHub](https://github.com/logseq/logseq) or [Logseq Forums](https://discuss.logseq.com/)
