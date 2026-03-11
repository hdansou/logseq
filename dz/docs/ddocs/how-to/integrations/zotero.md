---
title: 'Integrating Zotero with Logseq'
status: 'Stable'
version: 'Built-in since v0.4.0; enhanced with plugins'
quadrant: 'How-To Guide'
platform: 'Desktop (macOS/Windows/Linux)'
related-docs:
  - '../multimedia/asset-management.md'
  - '../../reference/features/pdf-annotations.md'
  - '../../tutorials/getting-started/basic-editing.md'
---

# Integrating Zotero with Logseq

> **Last revised:** 2026-03-10

## Overview

Zotero is a popular reference management tool for researchers and academics. Logseq includes a **built-in Zotero integration** that connects to the Zotero Web API, allowing you to search and import Zotero items directly as Logseq pages. You can also enhance this workflow with third-party plugins and manual import methods.

**What This Guide Covers**:

- Built-in Zotero integration via the Zotero Web API
- Importing Zotero libraries and individual items
- Syncing PDFs and annotations
- Citation workflows
- Linking references to pages

**Prerequisites**:

- Zotero installed ([https://www.zotero.org](https://www.zotero.org))
- A Zotero account with [syncing enabled](https://www.zotero.org/support/sync)
- A Zotero API key (create one at [https://www.zotero.org/settings/keys](https://www.zotero.org/settings/keys))
- Logseq Desktop app
- Basic familiarity with both tools

---

## Method 1: Built-in Zotero Integration (Recommended)

Logseq has a built-in Zotero integration that communicates directly with the Zotero Web API. This does **not** require any plugin installation.

### Setup

**Step 1: Enable Zotero Syncing**

The built-in integration requires Zotero cloud syncing to be enabled, since it connects to the Zotero Web API.

1. In Zotero, go to Edit (or Zotero menu on macOS) > Preferences > Sync
2. Sign in with your Zotero account
3. Enable syncing

**Step 2: Create a Zotero API Key**

1. Go to [https://www.zotero.org/settings/keys](https://www.zotero.org/settings/keys)
2. Note your **userID** (displayed on that page; this is a number, not your username)
3. Click **Create new private key**
4. Grant at least **Allow library access** permission
5. If you want to include notes, select **Allow notes access**
6. Save the key

**Step 3: Configure Zotero Settings in Logseq**

1. Open Logseq
2. Go to **Settings** > **Zotero settings**
3. Enter your **API key** and **User ID**
4. Configure additional options:
   - **Type**: `user` (for personal library) or `group` (for group libraries; use your group ID instead)
   - **Prefer citekey**: If you have [Better BibTeX for Zotero](https://retorque.re/zotero-better-bibtex/) installed, Logseq will use the citation key as the page name; otherwise it uses the item title
   - **Include attachments**: Whether to import attachment links
   - **Include notes**: Whether to import Zotero notes
   - **Page insert prefix**: The prefix for imported pages (default: `@`). You can set it to a namespace like `zotero/` for hierarchical organization
   - **Extra tags**: Additional tags to add to imported items
   - **Zotero data directory**: Path to your local Zotero data folder (enables opening imported PDFs directly)
   - **Zotero linked attachment base directory**: Path for Zotero linked file attachments

### Usage

**Import Individual Items**:

1. In Logseq, type the `/Zotero` slash command
2. Search for a Zotero item by title, author, or any text
3. Select an item from the results
4. Logseq creates a page with the item's metadata (title, authors, tags, etc.)

**Import All Items**:

1. Go to **Settings** > **Zotero settings**
2. Click **Add all** to import all Zotero items as Logseq pages

**Re-importing**: If you import the same item twice, Logseq appends data to the existing page rather than overwriting it. To get a fresh import, delete the page first and re-import.

### Optional: Better BibTeX

For consistent citation keys (e.g., `smith2024` instead of the full item title):

1. Install [Better BibTeX for Zotero](https://retorque.re/zotero-better-bibtex/)
2. Configure your preferred citation key format (e.g., `[auth:lower][year]`)
3. In Logseq Zotero settings, enable **Prefer citekey**
4. Logseq will now use the Better BibTeX citation key as the page name

---

## Method 2: Third-Party Plugins

If you do not have Zotero syncing enabled, or need additional features, you can use community plugins.

### Logseq Citation Manager

If you prefer a plugin-based approach or don't use Zotero cloud syncing:

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

The built-in integration can open PDFs stored in Zotero's local data directory. Configure the **Zotero data directory** path in Logseq's Zotero settings so that Logseq can locate imported file attachments.

**Default Zotero Data Directory Locations**:

- **macOS**: `~/Zotero/`
- **Windows**: `C:\Users\YourName\Zotero\`
- **Linux**: `~/Zotero/`

For linked file attachments (as opposed to imported/stored attachments), set the **Zotero linked attachment base directory** in settings.

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

Instead of copying, link directly to Zotero storage. Logseq supports the `zotero://` protocol:

```markdown
- [[smith2024]]
  - pdf:: file:///Users/yourname/Zotero/storage/ABC123/paper.pdf
```

Note: `file://` links are desktop-only and break if the Zotero storage path changes.

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

**Use in Logseq** (with the default `@` prefix):

```markdown
- According to [[@smith2024]], the results show...
```

If you changed the page insert prefix (e.g., to `zotero/`):

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

**1. Import Reference**

- Use `/Zotero` command to search and import from your synced library
- Creates page with `@` prefix (e.g., `[[@smith2024]]`)

**2. Add PDF**

- If Zotero data directory is configured, imported file attachments are accessible directly
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

### Built-in Integration Not Working

**Problem**: `/Zotero` command does not return results.

**Solutions**:

1. Verify Zotero cloud syncing is enabled and items are synced
2. Check API key and User ID in Settings > Zotero settings (User ID is a number, not your username)
3. Ensure the API key has at least "Allow library access" permission
4. For group libraries, use the group ID and set type to `group`
5. Restart Logseq

### PDFs Not Opening

**Problem**: Clicking PDF link doesn't open file

**Solutions**:

1. Verify PDF exists at specified path
2. Set the **Zotero data directory** in Zotero settings so Logseq can resolve imported file paths
3. For linked attachments, set the **Zotero linked attachment base directory**
4. Use relative paths: `../assets/file.pdf` (not absolute) for PDFs stored in the graph
5. Check file permissions (read access required)

### Citation Keys Not Working

**Problem**: Pages are created with full titles instead of citation keys

**Solutions**:

1. Install [Better BibTeX for Zotero](https://retorque.re/zotero-better-bibtex/)
2. Enable **Prefer citekey** in Logseq Zotero settings
3. Ensure Better BibTeX has generated citation keys for your items (check the Extra field in Zotero item info)

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
- Use the built-in integration to import items to Logseq for note-taking
- Use Zotero for PDF organization

**Don't**:

- Duplicate metadata manually (error-prone)
- Store PDFs in both places unnecessarily

### Consistent Naming

**Citation Keys**: Use Better BibTeX auto-generation with the "Prefer citekey" option enabled in Logseq.

- `smith2024` (lowercase, consistent format)
- Avoid manual keys (leads to conflicts)

**Page Prefix**: The default `@` prefix (e.g., `@smith2024`) keeps Zotero-imported pages distinct from regular pages.

### Regular Syncs

**Zotero Syncing**:

- The built-in integration reads from the Zotero Web API, so ensure your Zotero library is synced
- Changes in Zotero are reflected when you next search or import

### Backup Both Tools

**Zotero**: Settings > Advanced > Files and Folders > Data Directory Location

- Backup this folder (includes database + PDFs)

**Logseq**: Backup entire graph folder (includes `assets/` with PDFs)

---

## Zotero Settings Reference

The built-in Zotero settings are stored in your graph's `logseq/config.edn` under the `:zotero/settings-v2` key. The available settings are:

| Setting | Description | Default |
|---------|-------------|---------|
| `:type` | Library type: `:user` or `:group` | `:user` |
| `:prefer-citekey?` | Use Better BibTeX citation key as page name | `true` |
| `:include-attachments?` | Import attachment links | `true` |
| `:attachments-block-text` | Heading text for attachments block | `"[[Attachments]]"` |
| `:include-notes?` | Import Zotero notes | `true` |
| `:overwrite-mode?` | Overwrite existing page data on re-import | `false` |
| `:notes-block-text` | Heading text for notes block | `"[[Notes]]"` |
| `:zotero-data-directory` | Path to local Zotero data folder | `""` |
| `:zotero-linked-attachment-base-directory` | Path for linked file attachments | `""` |
| `:extra-tags` | Additional tags for imported items | `""` |
| `:page-insert-prefix` | Prefix for imported page names | `"@"` |

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

**Status**: Stable (built-in feature)
**Feedback**: Report issues on [Logseq GitHub](https://github.com/logseq/logseq) or [Logseq Forums](https://discuss.logseq.com/)
