---
title: 'Import and Export Guide'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'How-To'
platform: 'Desktop + Mobile'
related-docs:
  - '[Backup Strategies](./backup-strategies.md)'
  - '[File vs Database Graphs](../../explanation/concepts/file-vs-database.md)'
  - '[Migration Between Formats](./migration-guide.md)'
tags: ['import', 'export', 'migration', 'data-portability', 'backup']
---

# Import and Export Guide

This guide shows you how to import data into Logseq and export your content to various formats. Learn to migrate between note-taking apps, back up your data, and share your knowledge base.

**What You'll Learn:**

- Importing from other apps (Roam, Notion, Obsidian, etc.)
- Exporting to different formats (JSON, EDN, Markdown, HTML)
- Database graph migration
- Batch operations and automation
- Data validation and troubleshooting

**Time Required:** 15-45 minutes depending on graph size

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

**Access:** Click **⋯ (More)** → **Export graph**

| Format       | Best For                     | File-Based | Database |
| ------------ | ---------------------------- | ---------- | -------- |
| **JSON**     | Data backup, API integration | ✅         | ✅       |
| **EDN**      | Logseq-specific backup       | ✅         | ✅       |
| **Markdown** | Portability, other tools     | ✅         | ✅       |
| **HTML**     | Static website, sharing      | ✅         | ✅       |
| **OPML**     | Outliner apps                | ✅         | ✅       |

### When to Export

**Regular Backups:**

- Before major changes
- Before migrations
- Weekly/monthly routine

**Sharing:**

- Publish knowledge base
- Share with team
- Portfolio/documentation

**Migration:**

- Switching to another tool
- Changing graph types

---

## Import Overview

### Supported Import Sources

| Source            | Format              | Notes                    |
| ----------------- | ------------------- | ------------------------ |
| **Roam Research** | JSON, EDN, Markdown | Best compatibility       |
| **Notion**        | Markdown, HTML      | Export from Notion first |
| **Obsidian**      | Markdown            | Direct folder import     |
| **Evernote**      | ENEX → Markdown     | Use converter            |
| **Bear**          | Markdown            | Export notes first       |
| **Apple Notes**   | Markdown            | Use exporter             |
| **OneNote**       | Markdown            | Use converter            |
| **Google Keep**   | Markdown            | Use converter            |
| **Plain Files**   | Markdown, Org       | Direct import            |

### Import Methods

**Method 1: Direct Import**

- File → Import → Choose format
- Best for: Small-medium graphs

**Method 2: File System (File-Based)**

- Copy files to graph folder
- Best for: Markdown/Org files

**Method 3: Database Import**

- Import EDN file
- Best for: Database graphs

---

## Importing from Apps

### From Roam Research

**Step 1: Export from Roam**

1. Click **⋯ (More)** in Roam
2. Select **Export All**
3. Choose format:
   - **JSON** (recommended - preserves all data)
   - **Markdown** (more portable)
   - **EDN** (Logseq-specific)
4. Download `.zip` file

**Step 2: Import to Logseq**

```markdown
Method A: Direct Import

1. Logseq → File → Import
2. Select "Roam JSON"
3. Choose downloaded .zip file
4. Wait for processing
5. Review imported content

Method B: EDN Import (Database graphs)

1. Extract .zip file
2. Find .edn file
3. Logseq → Import → EDN
4. Select .edn file
```

**What's Preserved:**

- ✅ Pages and hierarchies
- ✅ Block references
- ✅ Tags and links
- ✅ TODO states
- ✅ Timestamps
- ⚠️ Custom CSS (needs manual migration)
- ⚠️ Queries (may need adjustment)

---

### From Notion

**Step 1: Export from Notion**

1. Settings → **Export all workspace content**
2. Format: **Markdown & CSV**
3. Include: **Everything**
4. Download export

**Step 2: Prepare Files**

```bash
# Extract export
unzip Notion-Export.zip -d notion-export

# Structure check
cd notion-export
ls -R  # Review folder structure
```

**Step 3: Import to Logseq**

```markdown
Method A: File-Based Graph

1. Create new Logseq graph
2. Copy Notion folders to: LogseqGraph/pages/
3. Open Logseq → Re-index
4. Fix links (see cleanup below)

Method B: Import Feature

1. Logseq → File → Import
2. Select "Notion Markdown"
3. Choose notion-export folder
4. Let Logseq process
```

**Cleanup Required:**

```markdown
Notion links: [Page](Page.md)
Logseq format: [[Page]]

Find and replace:

- Remove .md extensions from links
- Convert [Link](file.md) → [[Link]]
- Fix nested page paths
```

**Script to Fix Links:**

```bash
#!/bin/bash
# fix-notion-links.sh

for file in pages/*.md; do
  # Remove .md from links
  sed -i '' 's/\[([^]]*)\](\([^)]*\)\.md)/[[\1]]/g' "$file"

  # Fix nested paths
  sed -i '' 's/\[\[.*\/\([^]]*\)\]\]/[[\1]]/g' "$file"
done
```

---

### From Obsidian

**Step 1: Prepare Obsidian Vault**

```bash
# Obsidian vault structure
ObsidianVault/
├── Daily Notes/
├── Projects/
├── .obsidian/
└── attachments/
```

**Step 2: Import to Logseq**

```markdown
Method A: File-Based Graph (Easiest)

1. Copy entire vault folder
2. Rename to LogseqGraph
3. Open in Logseq
4. Logseq will auto-detect markdown files

Method B: Import Feature

1. Logseq → File → Import
2. Select "Obsidian Vault"
3. Choose vault folder
4. Logseq converts to Logseq format
```

**Compatibility:**

| Feature     | Obsidian    | Logseq               | Notes                  |
| ----------- | ----------- | -------------------- | ---------------------- |
| Wiki links  | `[[Page]]`  | `[[Page]]`           | ✅ Compatible          |
| Tags        | `#tag`      | `#tag`               | ✅ Compatible          |
| Frontmatter | YAML        | Properties           | ⚠️ May need conversion |
| Code blocks | ` ```lang ` | ` ```lang `          | ✅ Compatible          |
| Embeds      | `![[Page]]` | `{{embed [[Page]]}}` | ❌ Needs conversion    |
| Dataview    | Custom      | Queries              | ❌ Needs rewrite       |

**Convert Obsidian Embeds:**

```bash
# Find embeds
grep -r "!\[\[" ObsidianVault/

# Replace with Logseq format
sed -i '' 's/!\[\[\([^]]*\)\]\]/{{embed [[\1]]}}/g' pages/*.md
```

---

### From Evernote

**Step 1: Export from Evernote**

1. File → **Export Notes**
2. Format: **ENEX** (Evernote Export)
3. Save `.enex` file

**Step 2: Convert ENEX to Markdown**

**Option A: Use evernote2md (recommended)**

```bash
# Install converter
npm install -g evernote2md

# Convert
evernote2md export.enex output-folder

# Result structure
output-folder/
├── Note 1.md
├── Note 2.md
└── attachments/
```

**Option B: Use Yarle**

```bash
# Install
npm install -g yarle

# Convert
yarle --enex-file export.enex --output-dir output-folder
```

**Step 3: Import to Logseq**

```bash
# Copy converted files
cp -r output-folder/* ~/LogseqGraph/pages/

# Open Logseq and re-index
```

**Cleanup:**

```markdown
- Convert Evernote tags to Logseq tags
- Fix internal links
- Move attachments to assets/
- Update image paths
```

---

## Export Formats

### JSON Export

**Best For:** Complete backups, API integration

**Export:**

```markdown
1. ⋯ → Export graph
2. Format: JSON
3. Choose location
4. Save: my-graph-2026-01-22.json
```

**Contents:**

```json
{
  "version": "0.10.9",
  "pages": [
    {
      "name": "Page Title",
      "original-name": "Page Title",
      "blocks": [...]
    }
  ],
  "blocks": [...],
  "assets": [...]
}
```

**Use Cases:**

- Backup before migrations
- Data analysis (parse JSON)
- API integration
- Custom processing

---

### EDN Export (Database Graphs)

**Best For:** Database graph backups, version control

**Export:**

```markdown
1. ⋯ → Export graph
2. Format: EDN
3. Save: my-graph-2026-01-22.edn
```

**Contents:**

```clojure
{:schema-version 65.16
 :pages [{:block/uuid #uuid "..."
          :block/title "Page Title"
          :block/properties {...}}]
 :blocks [...]
 :transactions [...]}
```

**Use Cases:**

- Database graph backups
- Restore specific version
- RTC sync troubleshooting
- Schema migration testing

**Import EDN:**

```markdown
1. ⋯ → Import
2. Format: EDN
3. Select .edn file
4. Choose: Create new graph OR Import to existing
```

---

### Markdown Export

**Best For:** Portability, publishing, other tools

**Export:**

```markdown
1. ⋯ → Export graph
2. Format: Markdown
3. Options:
   - Include assets: Yes/No
   - Include journals: Yes/No
   - Include pages: Yes/No
4. Export
```

**Output Structure:**

```
export-2026-01-22/
├── pages/
│   ├── Page 1.md
│   ├── Page 2.md
│   └── Projects___Alpha.md
├── journals/
│   └── 2026-01-22.md
└── assets/
    └── images/
```

**Markdown Format:**

```markdown
# Page Title

- Block content
  - Nested block
  - property:: value

## Another Section

- More content
```

**Use Cases:**

- Switch to other markdown tools
- Static site generation (Hugo, Jekyll)
- Version control (Git)
- Sharing with non-Logseq users

---

### HTML Export

**Best For:** Publishing, sharing, static websites

**Export:**

```markdown
1. ⋯ → Export graph
2. Format: HTML
3. Options:
   - Theme: Light/Dark/Auto
   - Include search: Yes/No
   - Include graph view: Yes/No
4. Export
```

**Output:**

```
export-html/
├── index.html
├── pages/
│   ├── page1.html
│   └── page2.html
├── assets/
│   ├── css/
│   ├── js/
│   └── images/
└── graph.json (if graph view enabled)
```

**Hosting:**

```bash
# Deploy to Netlify/Vercel
cd export-html
netlify deploy --prod

# Or GitHub Pages
git init
git add .
git commit -m "Initial commit"
git push -u origin main
```

**Customization:**

```css
/* Custom CSS for HTML export */
/* Add to logseq/custom.css before exporting */

.page-title {
  color: #2563eb;
  font-size: 2rem;
}

.block-content {
  line-height: 1.8;
}
```

---

## Database Graph Operations

### Export Database Graph

**Full Export (EDN):**

```markdown
1. ⋯ → Export graph
2. Format: EDN (recommended)
3. Save location
4. Verify file size (should be MB+)
```

**Partial Export (Markdown):**

```markdown
1. ⋯ → Export graph
2. Format: Markdown
3. Use for: Sharing, publishing
4. Note: Loses database-specific features
```

### Import to Database Graph

**From EDN:**

```markdown
1. Create new database graph
2. ⋯ → Import
3. Select: EDN file
4. Wait for import (may take minutes for large graphs)
5. Verify: Check schema version
```

**From Markdown:**

```markdown
1. Create file-based graph first
2. Add markdown files
3. Convert to database graph:
   - ⋯ → Convert to database graph
   - Confirm conversion
   - Wait for processing
```

### Migrate File-Based → Database

**Process:**

```markdown
Step 1: Backup current graph

- Export as JSON + Markdown
- Copy entire graph folder

Step 2: Create database graph

- File → New graph
- Type: Database
- Name: [Same as file-based]

Step 3: Import content

- ⋯ → Import
- Select: File-based graph folder
- Wait for conversion

Step 4: Verify

- Check page count
- Verify assets loaded
- Test queries
- Check properties
```

**What Changes:**

| Feature         | File-Based     | Database          |
| --------------- | -------------- | ----------------- |
| **Storage**     | `.md` files    | `.db` file        |
| **Properties**  | `key:: value`  | Native properties |
| **Performance** | Slower (large) | Faster            |
| **Sync**        | Git/cloud      | Logseq Sync       |
| **Queries**     | Simple         | Advanced          |

---

## Automation

### Batch Export Script

```bash
#!/bin/bash
# auto-export.sh - Export all formats

GRAPH_NAME="my-graph"
DATE=$(date +%Y-%m-%d)
BACKUP_DIR="$HOME/Backups/Logseq"

# Create backup directory
mkdir -p "$BACKUP_DIR/$DATE"

# Export JSON
logseq-cli export \
  --graph "$GRAPH_NAME" \
  --format json \
  --output "$BACKUP_DIR/$DATE/$GRAPH_NAME.json"

# Export Markdown
logseq-cli export \
  --graph "$GRAPH_NAME" \
  --format markdown \
  --output "$BACKUP_DIR/$DATE/$GRAPH_NAME-md/"

# Export EDN (if database graph)
logseq-cli export \
  --graph "$GRAPH_NAME" \
  --format edn \
  --output "$BACKUP_DIR/$DATE/$GRAPH_NAME.edn"

echo "Export completed: $DATE"
```

**Schedule with Cron:**

```bash
# Run daily at 2 AM
0 2 * * * /path/to/auto-export.sh >> ~/logseq-export.log 2>&1
```

### Programmatic Export (Plugin)

```typescript
// Export via plugin API
async function exportGraph() {
  const graph = await logseq.App.getCurrentGraph()

  // Export as JSON
  await logseq.api.export_graph({
    format: 'json',
    path: `/backups/${graph.name}-${Date.now()}.json`,
  })

  console.log('Export complete')
}

// Register command
logseq.App.registerCommand('export-backup', exportGraph)
```

---

## Troubleshooting

### Import Issues

**Problem: Import fails with error**

**Solutions:**

```markdown
1. Check file format

   - Verify .json/.edn/.md format
   - Validate JSON structure
   - Check file encoding (UTF-8)

2. Check file size

   - Large files (>100MB) may timeout
   - Split into smaller chunks
   - Import pages incrementally

3. Validate content
   - Check for special characters
   - Fix malformed markdown
   - Remove invalid properties
```

**Problem: Missing pages after import**

```markdown
1. Check import log

   - Look for errors/warnings
   - Identify failed pages

2. Verify file names

   - Check special characters
   - Ensure proper encoding
   - Fix Windows path issues

3. Re-import missing pages
   - Export individual pages
   - Import one by one
   - Check each import
```

**Problem: Links broken after import**

```markdown
1. Fix link format
   Before: [Link](file.md)
   After: [[Link]]

2. Update internal refs

   - Remove .md extensions
   - Fix namespaces (/ separator)
   - Update block references

3. Run link checker
   - Search for broken links
   - Update systematically
```

### Export Issues

**Problem: Export file too large**

```bash
# Export subsets
## Export specific namespace
logseq-cli export --namespace "Projects/Work"

## Export date range
logseq-cli export --from 2026-01-01 --to 2026-01-22

## Exclude journals
logseq-cli export --exclude-journals
```

**Problem: HTML export missing assets**

```markdown
1. Check asset paths

   - Ensure relative paths
   - Fix broken image links

2. Re-export with assets

   - Enable "Include assets"
   - Verify output folder

3. Manual copy
   - Copy assets/ folder
   - Update HTML references
```

**Problem: EDN export fails (database graph)**

```markdown
1. Check database integrity

   - Run integrity check
   - Fix validation errors

2. Export as JSON instead

   - More forgiving format
   - Can convert to EDN

3. Export subsets
   - Export pages individually
   - Combine later
```

---

## Best Practices

### Before Export/Import

**Checklist:**

- [ ] Backup current graph
- [ ] Test on small sample first
- [ ] Verify format compatibility
- [ ] Check available disk space
- [ ] Close other applications
- [ ] Update Logseq to latest

### During Migration

**Steps:**

1. Export from source (JSON + Markdown)
2. Test import on sample
3. Full import to new graph
4. Verify all content present
5. Fix broken links/assets
6. Test queries and features
7. Keep old graph for 30 days

### After Import

**Validation:**

```markdown
1. Check page count

   - Compare source vs. destination
   - List missing pages

2. Verify assets

   - Check images load
   - Test PDF attachments
   - Verify videos play

3. Test features

   - Block references work
   - Queries execute
   - Templates insert
   - Plugins compatible

4. Performance check
   - Graph loads quickly
   - Search responsive
   - Sync works (if database)
```

---

## Next Steps

**Complete Your Migration:**

1. Choose import/export method
2. Test on sample data
3. Execute full migration
4. Verify and fix issues
5. Document your process

**Related Guides:**

- [Backup Strategies](./backup-strategies.md)
- [File vs Database](../../explanation/concepts/file-vs-database.md)
- [Graph Migration](./migration-guide.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
