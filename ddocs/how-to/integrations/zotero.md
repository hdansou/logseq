---
title: 'Integrating Zotero with Logseq'
status: '🟢 Stable'
version: 'Since v0.7.0 (via plugins)'
quadrant: 'How-To Guide'
platform: 'Desktop (macOS/Windows/Linux)'
related-docs:
  - '../multimedia/asset-management.md'
  - '../../reference/features/pdf-annotations.md'
  - '../../tutorials/getting-started/basic-editing.md'
---

# Integrating Zotero with Logseq

## Overview

Zotero is a popular reference management tool for researchers and academics. While Logseq doesn't have built-in Zotero integration, you can connect the two tools using plugins or manual workflows to manage citations, PDFs, and research notes in your graph.

**What This Guide Covers**:

- 🔌 Using Logseq-Zotero plugins
- 📚 Importing Zotero libraries
- 📄 Syncing PDFs and annotations
- 📝 Citation workflows
- 🔗 Linking references to pages

**Prerequisites**:

- Zotero installed ([https://www.zotero.org](https://www.zotero.org))
- Logseq Desktop app
- Basic familiarity with both tools

---

## Method 1: Logseq-Zotero Plugin (Recommended)

### Installation

**Step 1: Install Better BibTeX for Zotero**

1. Download Better BibTeX: [https://retorque.re/zotero-better-bibtex/](https://retorque.re/zotero-better-bibtex/)
2. In Zotero: Tools → Add-ons
3. Click gear icon → Install Add-on From File
4. Select downloaded `.xpi` file
5. Restart Zotero

**Step 2: Install Logseq-Zotero Plugin**

1. Open Logseq
2. Click **Settings** (⚙️) → **Plugins**
3. Click **Marketplace**
4. Search "Zotero"
5. Find **"logseq-plugin-zotero"** by [e-zotero]
6. Click **Install**

**Step 3: Configure Plugin**

1. In Zotero: Edit → Preferences → Better BibTeX → Export
   - Citation key format: `[auth:lower][year]` (or customize)
   - Enable "On Change" export
2. In Zotero: File → Export Library
   - Format: **Better CSL JSON**
   - Save to: `~/Zotero/library.json` (or preferred location)
   - Check "Keep updated"
3. In Logseq: Settings → Plugins → Zotero Settings
   - **Library JSON Path**: `/path/to/library.json`
   - **PDF Folder**: `/path/to/Zotero/storage/` (optional)
   - Click **Save**

---

## Method 2: Manual CSV Import

### Export from Zotero

1. In Zotero, select items to export
2. Right-click → **Export Items**
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
   - `Title` → `title`
   - `Author` → `author`
   - `Year` → `year`
   - etc.
6. Click **Import**

---

## Method 3: Zotero Better Notes + Markdown Export

### Setup

1. Install **Zotero Better Notes** plugin:
   - Download: [https://github.com/windingwind/zotero-better-notes](https://github.com/windingwind/zotero-better-notes)
   - In Zotero: Tools → Add-ons → Install from file
2. Configure export template:
   - Tools → Better Notes → Preferences → Export Templates
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
2. Right-click → Better Notes → Export to Markdown
3. Save to Logseq graph `pages/` folder
4. File automatically appears in Logseq

---

## Syncing PDFs

### Copy PDFs to Logseq

**Zotero Storage Location**:

- **macOS**: `~/Zotero/storage/`
- **Windows**: `C:\Users\YourName\Zotero\storage\`
- **Linux**: `~/Zotero/storage/`

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

Instead of copying, link directly to Zotero storage:

```markdown
- [[smith2024]]
  - pdf:: file:///Users/yourname/Zotero/storage/ABC123/paper.pdf
```

⚠️ **Note**: `file://` links are desktop-only and break if Zotero storage path changes.

---

## Annotation Workflows

### Workflow 1: Annotate in Zotero → Export to Logseq

1. **Annotate in Zotero**:

   - Open PDF in Zotero reader
   - Highlight text
   - Add notes

2. **Export Annotations**:

   - Right-click item → Create Note from Annotations
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

**Use in Logseq**:

```markdown
- According to [[smith2024]], the results show...
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

1. In Zotero: Right-click item → Create Bibliography from Item
2. Select style (APA, Chicago, MLA, etc.)
3. Copy formatted citation
4. Paste in Logseq

### Quick Cite (Plugin Feature)

**Using Logseq-Zotero Plugin**:

1. Type `/Zotero`
2. Select **Quick Cite**
3. Search for reference by title/author
4. Select item
5. Citation inserted: `[@smith2024]` or formatted text

**Configure Citation Style**:

- Plugin settings → Citation Style → Select (APA, Chicago, etc.)

---

## Organizing Research

### Literature Note Template

```markdown
- [[Literature Notes/smith2024]]

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
3. Use via command: `/Template` → Select "Literature Note"

### Reading Workflow

**1. Import Reference**

- Export from Zotero → Import to Logseq
- Create page: `[[smith2024]]`

**2. Add PDF**

- Copy PDF to `assets/smith2024.pdf`
- Link: `pdf:: ![](../assets/smith2024.pdf)`

**3. First Pass Reading**

- Annotate PDF with Logseq's highlighter
- Tag passages: #important, #definition, #quote

**4. Extract Notes**

- Review annotations
- Write summary in own words
- Connect to existing notes: `[[Related Concept]]`

**5. Synthesize**

- Create synthesis page: `[[Research Topic X]]`
- Link relevant papers: `See [[smith2024]] for background`
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

### Plugin Not Finding Zotero Library

**Problem**: Logseq-Zotero plugin shows "Library not found"

**Solutions**:

1. Verify Better BibTeX installed in Zotero
2. Export library as Better CSL JSON (File → Export Library)
3. Check export path in plugin settings
4. Ensure "Keep updated" enabled in export dialog
5. Restart Logseq

### PDFs Not Opening

**Problem**: Clicking PDF link doesn't open file

**Solutions**:

1. Verify PDF exists at specified path
2. Use relative paths: `../assets/file.pdf` (not absolute)
3. Check file permissions (read access required)
4. For `file://` links: ensure Zotero storage path correct

### Citation Keys Not Working

**Problem**: Cannot insert citations via plugin

**Solutions**:

1. Check Better BibTeX citation key format (Edit → Preferences → Better BibTeX)
2. Refresh library export (File → Export Library → re-export)
3. Restart Logseq plugin
4. Verify JSON file is valid (open in text editor)

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

### Logseq-Scholar Plugin

**Alternative Zotero Plugin**:

- Search marketplace for "Scholar" or "Citations"
- Some plugins offer direct Zotero API integration
- Check reviews and compatibility before installing

---

## Best Practices

### Keep Zotero as Source of Truth

✅ **Do**:

- Maintain complete bibliographic data in Zotero
- Export to Logseq for note-taking
- Use Zotero for PDF organization

❌ **Don't**:

- Duplicate metadata manually (error-prone)
- Store PDFs in both places (waste of space)

### Consistent Naming

**Citation Keys**: Use Better BibTeX auto-generation

- `smith2024` (lowercase, consistent format)
- Avoid manual keys (leads to conflicts)

**PDF Files**: Match citation keys

- Zotero: `smith2024.pdf`
- Logseq: `../assets/smith2024.pdf`

### Regular Syncs

**Export Frequency**:

- Set Zotero to auto-export (Better BibTeX "On Change")
- Or manual export weekly
- Refresh Logseq plugin after export

### Backup Both Tools

**Zotero**: Settings → Advanced → Files and Folders → Data Directory Location

- Backup this folder (includes database + PDFs)

**Logseq**: Backup entire graph folder (includes `assets/` with PDFs)

---

## Related Documentation

**How-To Guides**:

- [Asset Management](../multimedia/asset-management.md) — Managing PDF files
- [PDF Annotations](../../reference/features/pdf-annotations.md) — Annotating PDFs in Logseq

**Tutorials**:

- [Basic Editing](../../tutorials/getting-started/basic-editing.md) — Creating and linking pages

**External Resources**:

- [Zotero Documentation](https://www.zotero.org/support/)
- [Better BibTeX Guide](https://retorque.re/zotero-better-bibtex/)
- [Logseq-Zotero Plugin](https://github.com/alanzchen/logseq-plugin-zotero)

---

**Last Updated**: January 2026  
**Status**: 🟢 Stable (via plugins)  
**Feedback**: Report Zotero integration issues on plugin GitHub repos or [Logseq Forums](https://discuss.logseq.com/)

---

**Note**: This guide describes community-maintained plugins and workflows. Official Zotero integration is not built into Logseq core. For the latest plugin compatibility, check the Logseq marketplace.
