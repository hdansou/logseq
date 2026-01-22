---
title: 'File Organization and Namespaces'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'Explanation'
platform: 'Desktop + Mobile'
related-docs:
  - '[File-Based vs Database Graphs](./file-vs-database.md)'
  - '[Graph Structure](./graph-visualization.md)'
  - '[Folder Structure](../../reference/storage/folder-structure.md)'
tags: ['organization', 'namespaces', 'structure', 'file-system']
---

# File Organization and Namespaces

This document explains how Logseq organizes your content—both the logical structure (namespaces, hierarchies) and physical structure (files, folders). Understanding these concepts helps you create scalable, maintainable knowledge bases.

**What You'll Learn:**

- Logical vs physical organization
- Namespace hierarchies
- File structure for file-based graphs
- Database organization
- Organizing strategies

**Prerequisites:**

- Basic understanding of pages and blocks
- Familiarity with graph types (file-based vs database)

---

## Table of Contents

1. [Logical vs Physical Organization](#logical-vs-physical-organization)
2. [Namespace System](#namespace-system)
3. [File-Based Graph Structure](#file-based-graph-structure)
4. [Database Graph Structure](#database-graph-structure)
5. [Organization Strategies](#organization-strategies)
6. [Best Practices](#best-practices)

---

## Logical vs Physical Organization

### Logical Organization (What You See)

**In Logseq UI:**

```
📄 Projects
  📄 Projects/Work
    📄 Projects/Work/Alpha
    📄 Projects/Work/Beta
  📄 Projects/Personal
    📄 Projects/Personal/Website
```

**This is logical** - organized by namespace hierarchy, independent of file system.

### Physical Organization (File System)

**File-Based Graph:**

```
LogseqGraph/
├── pages/
│   ├── Projects.md
│   ├── Projects___Work.md
│   ├── Projects___Work___Alpha.md
│   └── Projects___Personal___Website.md
└── journals/
    └── 2026_01_21.md
```

**Database Graph:**

```
LogseqApp/
└── graphs/
    └── my-graph.db  (SQLite - all content in one file)
```

**Key Insight:** Logical hierarchy ≠ Folder hierarchy

---

## Namespace System

### What are Namespaces?

**Namespaces** create hierarchical organization using `/` separator:

```markdown
[[Projects]] # Top level
[[Projects/Work]] # One level down
[[Projects/Work/Alpha]] # Two levels down
[[Projects/Personal/Website]] # Different branch
```

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

### Creating Namespaces

**Method 1: Direct Creation**

```markdown
Create page: [[Projects/Work/NewProject]]
```

Automatically creates:

- `[[Projects]]` (if doesn't exist)
- `[[Projects/Work]]` (if doesn't exist)
- `[[Projects/Work/NewProject]]` (your page)

**Method 2: Manual Hierarchy**

```markdown
# [[Projects]]

- Sub-pages:
  - [[Projects/Work]]
  - [[Projects/Personal]]
```

### Namespace Properties

**Auto-generated Parent Links:**

When you create `[[Projects/Work/Alpha]]`:

```markdown
# Projects/Work/Alpha

← Projects/Work (auto-generated breadcrumb)
```

**Namespace Page Auto-lists Children:**

```markdown
# Projects/Work

## Linked References

- [[Projects/Work/Alpha]]
- [[Projects/Work/Beta]]
- [[Projects/Work/Gamma]]
```

---

## File-Based Graph Structure

### Root Folder Structure

```
MyLogseqGraph/
├── assets/              # Images, PDFs, attachments
├── draws/               # Whiteboard/TLDraw files
├── journals/            # Daily notes
├── logseq/              # Configuration & metadata
│   ├── bak/            # Backup files
│   ├── .recycle/       # Deleted items
│   ├── config.edn      # Settings
│   └── custom.css      # Custom styles
└── pages/               # All your pages
    ├── contents.md     # Default home page
    └── *.md            # Your pages
```

### Page Files

**File Naming:**

```
Logical Name → Physical File

[[My Page]]        → pages/My Page.md
[[Projects/Work]]  → pages/Projects___Work.md
[[2026-01-21]]     → journals/2026_01_21.md
```

**Encoding Rules:**

| Character     | Replacement | Example                                |
| ------------- | ----------- | -------------------------------------- |
| `/`           | `___`       | `Projects/Work` → `Projects___Work.md` |
| Space         | ` `         | `My Page` → `My Page.md`               |
| Special chars | URL-encoded | `Question?` → `Question%3F.md`         |

**File Content:**

```markdown
# Page Title

- Block content
  - Nested block
- Another block
  property:: value
```

### Assets Organization

**Default Structure:**

```
assets/
├── 2026-01-21_image_1738.png
├── document_abc123.pdf
└── recording_xyz.mp3
```

**Custom Organization:**

```
assets/
├── images/
│   ├── projects/
│   │   └── alpha-screenshot.png
│   └── personal/
│       └── photo.jpg
├── documents/
│   └── report.pdf
└── media/
    └── recording.mp3
```

**Link to Asset:**

```markdown
![Image](../assets/images/projects/alpha-screenshot.png)
[PDF](../assets/documents/report.pdf)
```

### Configuration Files

**logseq/config.edn:**

```clojure
{:preferred-format :markdown
 :journal/page-title-format "yyyy-MM-dd"
 :journal/file-name-format "yyyy_MM_dd"
 :pages-directory "pages"
 :journals-directory "journals"
 :org-mode/insert-file-link? false}
```

**Key Settings:**

| Setting                      | Purpose                | Options                      |
| ---------------------------- | ---------------------- | ---------------------------- |
| `:preferred-format`          | File format            | `:markdown` or `:org`        |
| `:journal/page-title-format` | Journal naming         | `yyyy-MM-dd`, `MMM do, yyyy` |
| `:pages-directory`           | Custom pages folder    | Default: `pages`             |
| `:journals-directory`        | Custom journals folder | Default: `journals`          |

---

## Database Graph Structure

### Single-File Storage

**Everything in One Database:**

```
Application Support/Logseq/
└── graphs/
    └── my-graph.db  (SQLite)
        ├── entities (pages, blocks, properties)
        ├── transactions (change log)
        └── assets (metadata only)
```

**Assets Still Separate:**

```
My Graph/
├── assets/          # Files still stored separately
│   └── images/
└── (database content in app data folder)
```

### Logical Organization Only

**Database graphs don't have:**

- ❌ Individual page files
- ❌ Folder hierarchies for pages
- ❌ File system organization

**Database graphs do have:**

- ✅ Namespace hierarchies (logical)
- ✅ Properties and classes
- ✅ Query-based organization
- ✅ RTC sync structure

### Export Structure

**EDN Export:**

```clojure
;; Single .edn file with all data
{:schema-version 65.16
 :pages [...]
 :blocks [...]
 :assets [...]
 :transactions [...]}
```

**Markdown Export:**

```
export/
├── pages/
│   ├── Projects.md
│   └── Projects___Work.md
└── assets/
    └── ...
```

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
- ✅ Good for exporting subsets

**Cons:**

- ❌ Rigid structure
- ❌ Pages can only be in one namespace
- ❌ Can become too nested

### Strategy 2: Flat with Tags

**Use Case:** Flexible, multi-dimensional organization

**Example:**

```
Pages (flat):
- [[Project Alpha]]
  tags: #work #active #client-x #engineering
- [[Project Beta]]
  tags: #work #planning #client-y #design
- [[Weekend Hike]]
  tags: #personal #health #outdoors
```

**Pros:**

- ✅ Flexible (multiple tags)
- ✅ Cross-cutting concerns
- ✅ Easy to reorganize

**Cons:**

- ❌ No hierarchy
- ❌ Can be chaotic
- ❌ Requires discipline

### Strategy 3: Hybrid Approach

**Use Case:** Balance structure and flexibility

**Example:**

```
Namespaces for domains:
- Work/Projects/Alpha
  tags: #active #client-x #q1
- Personal/Goals/2026
  tags: #health #finance #learning

Flat for cross-cutting:
- [[Weekly Review]]
  tags: #process #routine
  references: Work/*, Personal/*
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

Content:
- [[202601210930]] First Note
  links: [[202601210945]], [[Topic X]]
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

**2. Descriptive Names**

```markdown
✅ Good:

- [[Client Onboarding Process]]
- [[Q1 2026 Planning]]

❌ Vague:

- [[Process]]
- [[Planning]]
```

**3. Avoid Special Characters**

```markdown
✅ Good:

- [[Question About Feature X]]
- [[Bug - Login Issue]]

❌ Problematic:

- [[Question? Feature X]] (? may cause issues)
- [[Bug: Login]] (: may cause issues on Windows)
```

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

### File Hygiene

**Regular Cleanup:**

1. **Weekly:**

   - Review orphan pages
   - Merge duplicates
   - Archive old content

2. **Monthly:**

   - Clean assets folder
   - Remove unused images
   - Archive old journals

3. **Quarterly:**
   - Reorganize namespaces if needed
   - Export archives
   - Backup entire graph

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

```bash
# Rename pages (file-based)
mv pages/"Project Alpha.md" pages/"Projects___Work___Alpha.md"

# Or use Logseq UI
1. Open [[Project Alpha]]
2. Page menu → Rename
3. Enter: [[Projects/Work/Alpha]]
```

**From Namespaced to Flat:**

```bash
# Remove namespace separators
mv pages/"Projects___Work___Alpha.md" pages/"Alpha.md"
```

⚠️ **Warning:** Update all links after renaming!

---

## Platform Differences

### File-Based Graphs

**Advantages:**

- ✅ Direct file access
- ✅ Git-friendly
- ✅ Transparent storage
- ✅ Easy backup (copy folder)

**Limitations:**

- ❌ File name encoding issues
- ❌ Platform-specific path limits
- ❌ Sync conflicts (Dropbox, iCloud)

### Database Graphs

**Advantages:**

- ✅ No file naming issues
- ✅ Better performance (large graphs)
- ✅ Atomic operations
- ✅ Built-in sync (RTC)

**Limitations:**

- ❌ Opaque storage
- ❌ Harder to backup (requires export)
- ❌ No Git versioning
- ❌ Cannot edit externally

---

## Troubleshooting

**Page not found after rename:**

- Check file name encoding
- Search for old name in backlinks
- Update references manually

**Namespace not showing hierarchy:**

- Ensure using `/` separator
- Check for typos in namespace path
- Reload graph (⌘ R)

**File sync conflicts:**

- Use Git instead of cloud sync
- Enable conflict resolution in config
- Manually merge conflicts

**Asset files too large:**

- Store large files externally
- Link to cloud storage URLs
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
