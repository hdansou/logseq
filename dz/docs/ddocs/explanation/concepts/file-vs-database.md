---
title: 'File-Based vs Database Graphs'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'Explanation'
platform: 'Desktop + Mobile'
related-docs:
  - '[Local-First Architecture](./local-first-architecture.md)'
  - '[File Organization](./file-organization.md)'
  - '[Database Integrity](../architecture/database-integrity.md)'
tags: ['graphs', 'architecture', 'file-based', 'database', 'storage']
---

# File-Based vs Database Graphs

Understand the two graph storage modes in Logseq. This guide explains the differences, helps you choose the right mode, and shows how to work with each effectively.

**What You'll Understand:**

- Two graph storage architectures
- Key differences and trade-offs
- When to use each mode
- Migration between modes
- Performance characteristics
- Sync and collaboration implications

---

## Table of Contents

1. [Overview](#overview)
2. [File-Based Graphs](#file-based-graphs)
3. [Database Graphs](#database-graphs)
4. [Key Differences](#key-differences)
5. [When to Use Each](#when-to-use-each)
6. [Performance Comparison](#performance-comparison)
7. [Sync & Collaboration](#sync--collaboration)
8. [Migration](#migration)
9. [Best Practices](#best-practices)

---

## Overview

### Two Storage Modes

**Logseq supports two graph storage architectures:**

```markdown
File-Based Graph:
└── Your knowledge stored as plain Markdown files

Database Graph:
└── Your knowledge stored in SQLite database
```

**Both modes:**

- Support full Logseq features
- Work offline-first
- Preserve local-first principles
- Maintain bidirectional links
- Enable powerful queries

**Different in:**

- Storage format
- Performance characteristics
- Sync capabilities
- File system interaction
- Mobile support

### Visual Comparison

**File-Based:**

```markdown
LogseqGraph/
├── journals/
│ ├── 2026-01-22.md
│ ├── 2026-01-21.md
│ └── 2026-01-20.md
├── pages/
│ ├── Projects\_\_\_Alpha.md
│ ├── Meeting Notes.md
│ └── Task Management.md
├── assets/
│ ├── image.png
│ └── document.pdf
└── logseq/
├── config.edn
└── custom.css

Files: Human-readable Markdown
```

**Database:**

```markdown
LogseqGraph/
├── logseq.db (SQLite database)
├── version-files/
│ ├── graph-1737504000.transit
│ └── graph-1737590400.transit
├── assets/
│ ├── image.png
│ └── document.pdf
└── logseq/
├── config.edn
└── custom.css

Database: Optimized binary format
Transit exports: Backup snapshots
```

---

## File-Based Graphs

### What Are File-Based Graphs?

**Storage:** Plain Markdown files in your filesystem

```markdown
Each page = One .md file
Each journal = One .md file

Example page file:

# pages/Projects\_\_\_Alpha.md

- [[Project Alpha]]
  status:: active
  owner:: [[Person/Sarah]]

  ## Goals

  - Launch by Q2 2026

  ## Tasks

  - TODO Complete API design
    DEADLINE: <2026-01-30 Wed>
```

### Architecture

**How It Works:**

```markdown
1. Startup:
   - Scan directory for .md files
   - Parse each file to blocks
   - Build in-memory graph (DataScript)
   - Extract links, properties, refs
2. Runtime:
   - All operations in memory
   - Queries run on DataScript
   - Changes tracked in memory
3. Save:
   - Detect modified files
   - Write back to .md files
   - Preserve formatting where possible
   - Update timestamps
```

**In-Memory Database:**

```clojure
;; DataScript - in-memory database
(def conn (d/create-conn schema))

;; Load all files
(doseq [file markdown-files]
  (let [blocks (parse-markdown file)]
    (d/transact! conn blocks)))

;; Query in memory
(d/q '[:find ?title
       :where [?p :block/name ?title]]
     @conn)

;; Fast because all in RAM
```

### File Structure

**Naming Conventions:**

```markdown
Journals:
2026-01-22.md → Daily journal

Pages:
Simple.md → Page "Simple"
My Page.md → Page "My Page"
Projects\_\_\_Alpha.md → Page "Projects/Alpha"

Rules:

- Namespace separator / → \_\_\_
- Spaces allowed in filename
- Case preserved
```

**File Content:**

```markdown
# pages/Task Management.md

- [[Task Management System]]

  ## Overview

  How we track tasks in Logseq.

  ## Active Tasks

  #+BEGIN_QUERY
  {:title "TODOs"
  :query [:find (pull ?b [*])
  :where [?b :block/marker "TODO"]]}
  #+END_QUERY

  ## Related

  - [[GTD Method]]
  - [[PARA System]]

Internal structure:

- Outline format (bullets)
- Properties (key:: value)
- Queries (#+BEGIN_QUERY)
- Links ([[Page]])
- Block refs (((uuid)))
```

### Advantages

**✅ Human-Readable:**

```markdown
Open any .md file:

- Read in any text editor
- Vim, Emacs, VS Code, Notepad
- No special tools needed
- Plain text forever
```

**✅ Version Control Friendly:**

```bash
# Git works perfectly
git init
git add .
git commit -m "Daily notes"
git push

# See file diffs
git diff pages/Project.md

# Merge changes
git merge other-branch

# File-based = Git-friendly
```

**✅ Tool Agnostic:**

```markdown
Use with other tools:

- Obsidian (mostly compatible)
- VS Code with Markdown plugins
- Plain text editors
- Grep/sed/awk for processing
- Custom scripts

No lock-in to Logseq format
```

**✅ Transparent:**

```markdown
You see exactly what's stored:

- Open file manager
- See all your files
- Know where data lives
- Easy to backup (copy folder)
- Easy to inspect
```

**✅ Selective Sync:**

```bash
# Sync only specific folders
rsync -av --exclude='private/' LogseqGraph/ backup/

# Or specific files
cp pages/Public*.md shared-folder/

# File-level granularity
```

### Disadvantages

**❌ Performance at Scale:**

```markdown
Large graphs (10,000+ blocks):

- Slower startup (parse all files)
- More memory usage
- Queries can be slower
- File I/O overhead

Reason: Parse every file on startup
```

**❌ Sync Conflicts:**

```markdown
When using Dropbox/iCloud/Git:

Scenario:

1. Edit on Device A
2. Edit same page on Device B
3. Both sync

Result:

- Conflict files created
- Manual resolution needed
- "conflicted copy" files
- Data not automatically merged
```

**❌ Mobile Limitations:**

```markdown
iOS/Android challenges:

- Limited file system access
- No direct folder picking (iOS)
- Slower file scanning
- Battery impact from I/O

Better on Android than iOS
```

**❌ No Real-Time Collaboration:**

```markdown
Can't have:

- Multiple users editing simultaneously
- Live cursor positions
- Instant updates
- Conflict-free merges

File-based = One editor at a time
```

---

## Database Graphs

### What Are Database Graphs?

**Storage:** SQLite database file

```markdown
All data in: logseq.db

Benefits:

- Optimized queries
- ACID transactions
- Better performance
- Schema validation
```

### Architecture

**How It Works:**

```markdown
1. Startup:
   - Open SQLite database
   - Load schema
   - Ready to query
2. Runtime:
   - SQL queries for reads
   - Transactions for writes
   - Indexes for speed
3. Save:
   - Immediate (every change)
   - Transactional (ACID)
   - Automatic
4. Export:
   - Periodic transit exports
   - For backup/sync
   - Compressed format
```

**SQLite Database:**

```sql
-- Simplified schema
CREATE TABLE blocks (
  id INTEGER PRIMARY KEY,
  uuid TEXT UNIQUE NOT NULL,
  content TEXT,
  parent_id INTEGER,
  page_id INTEGER,
  properties JSON,
  marker TEXT,
  priority TEXT,
  deadline INTEGER,
  scheduled INTEGER,
  created_at INTEGER,
  updated_at INTEGER,
  FOREIGN KEY (parent_id) REFERENCES blocks(id),
  FOREIGN KEY (page_id) REFERENCES blocks(id)
);

CREATE INDEX idx_blocks_marker ON blocks(marker);
CREATE INDEX idx_blocks_deadline ON blocks(deadline);
CREATE INDEX idx_blocks_page ON blocks(page_id);
CREATE INDEX idx_blocks_uuid ON blocks(uuid);

-- Optimized for queries
```

### Transit Exports

**Version Files:**

```markdown
version-files/
├── graph-1737504000.transit
├── graph-1737590400.transit
└── graph-1737676800.transit

Purpose:

- Backup snapshots
- Sync format
- Point-in-time exports
- Compressed (smaller than files)

Format: Transit (EDN-based)

- Human-readable (with effort)
- Preserves all data
- Logseq can import
```

**Export Schedule:**

```markdown
Automatic exports:

- On app close
- Every 30 minutes (if changes)
- Before major operations
- On sync

Manual export:

- File → Export → Database (Transit)
```

### Advantages

**✅ Performance:**

```markdown
Large graphs excel:

- Fast startup
- Efficient queries
- Indexed lookups
- Low memory usage

Query example:
File: 500ms to find 1000 TODOs
Database: 10ms to find 1000 TODOs

50x faster!
```

**✅ Schema Validation:**

```markdown
Database enforces:

- Property types
- Allowed values
- References exist
- Data integrity

Example:
priority:: high ✓ Valid
priority:: urgent ✗ Invalid (not allowed)

Prevents data errors
```

**✅ Real-Time Collaboration (with Logseq Sync):**

```markdown
CRDTs + Database = Real-time editing:

- Multiple users simultaneously
- Conflict-free merges
- Live updates
- Cursor positions

File-based can't do this
```

**✅ Better Mobile Support:**

```markdown
Mobile benefits:

- Faster app launch
- Lower battery usage
- Less file I/O
- Better responsiveness

Especially important on iOS
```

**✅ Advanced Features:**

```markdown
Database-only features:

- Real-time collaboration
- Better schema support
- Property classes
- Type checking
- Advanced validation

Future features likely database-only
```

### Disadvantages

**❌ Not Human-Readable:**

```markdown
SQLite is binary:

- Can't open in text editor
- Need SQLite tools to inspect
- Less transparent
- Requires export for reading

Transit readable but not convenient
```

**❌ Tool Lock-In:**

```markdown
Need Logseq to access data:

- Can't use other markdown tools
- No grep/sed/awk
- Export required for portability

Mitigation:

- Regular transit exports
- Export to markdown option
- Open database format (SQLite)
```

**❌ Git Unfriendly:**

```markdown
Binary database in git:

- No useful diffs
- Large binary commits
- Merge conflicts unsolvable
- History less useful

Better: Git the transit exports
```

**❌ Requires Logseq Sync for Multi-Device:**

```markdown
Free sync options limited:

- Dropbox doesn't work well (binary)
- iCloud has issues
- Git not ideal

Best option: Logseq Sync ($5/mo)
```

---

## Key Differences

### Comparison Table

| Aspect                         | File-Based             | Database                 |
| ------------------------------ | ---------------------- | ------------------------ |
| **Storage Format**             | Markdown files         | SQLite database          |
| **Human Readable**             | ✅ Yes                 | ❌ No (binary)           |
| **Startup Speed**              | Slow (large graphs)    | Fast                     |
| **Query Performance**          | Slower                 | Faster                   |
| **Memory Usage**               | Higher                 | Lower                    |
| **Git Friendly**               | ✅ Yes                 | ❌ No                    |
| **Tool Agnostic**              | ✅ Yes                 | ❌ No                    |
| **Mobile Performance**         | Slower                 | Faster                   |
| **Real-Time Collab**           | ❌ No                  | ✅ Yes (with Sync)       |
| **Schema Validation**          | Basic                  | ✅ Advanced              |
| **Sync Options**               | Many (Git/Dropbox/etc) | Logseq Sync best         |
| **Data Portability**           | ✅ Excellent           | ⚠️ Requires export       |
| **Conflict Handling**          | Manual                 | ⚠️ Automatic (with Sync) |
| **Large Graphs (10k+ blocks)** | ⚠️ Struggles           | ✅ Handles well          |
| **Learning Curve**             | Easy                   | Easy                     |
| **Future Features**            | Limited                | Full support             |

### Performance Benchmarks

**Startup Time:**

| Graph Size    | File-Based | Database |
| ------------- | ---------- | -------- |
| 1,000 blocks  | 2s         | 0.5s     |
| 5,000 blocks  | 8s         | 1s       |
| 10,000 blocks | 20s        | 1.5s     |
| 50,000 blocks | 120s       | 3s       |

**Query Performance:**

| Operation        | File-Based | Database |
| ---------------- | ---------- | -------- |
| Find all TODOs   | 100ms      | 5ms      |
| Complex query    | 500ms      | 20ms     |
| Full-text search | 300ms      | 50ms     |
| Load page        | 50ms       | 10ms     |

**Memory Usage:**

| Graph Size    | File-Based | Database |
| ------------- | ---------- | -------- |
| 1,000 blocks  | 100 MB     | 50 MB    |
| 10,000 blocks | 500 MB     | 150 MB   |
| 50,000 blocks | 2 GB       | 400 MB   |

---

## When to Use Each

### Choose File-Based If:

**✅ You value transparency and portability:**

```markdown
- Want to see your raw files
- Use other Markdown tools
- Need data portability
- Want no vendor lock-in
```

**✅ You use version control (Git):**

```markdown
- Track changes with git
- Collaborate via GitHub/GitLab
- Want meaningful diffs
- Need branching/merging
```

**✅ Your graph is small-medium:**

```markdown
- Under 5,000 blocks
- Performance is acceptable
- Startup time tolerable
- Memory usage fine
```

**✅ You use multiple sync solutions:**

```markdown
- Dropbox/iCloud/OneDrive
- Syncthing
- Your own rsync setup
- Want flexibility
```

**✅ You work solo:**

```markdown
- No collaboration needed
- Single device mostly
- Occasional multi-device
- Manual conflict resolution OK
```

### Choose Database If:

**✅ You have a large graph:**

```markdown
- Over 10,000 blocks
- Performance matters
- Fast startup important
- Memory constrained
```

**✅ You need real-time collaboration:**

```markdown
- Team using Logseq
- Simultaneous editing
- Shared knowledge base
- Live updates desired
```

**✅ You're primarily on mobile:**

```markdown
- iOS/Android main device
- Performance critical
- Battery life matters
- Responsiveness important
```

**✅ You want future features:**

```markdown
- Advanced schema
- Property validation
- Type checking
- New capabilities
```

**✅ You're OK with Logseq Sync:**

```markdown
- $5/mo acceptable
- Trust Logseq service
- Want easy sync
- Value convenience
```

### Migration Scenarios

**Start File-Based, Switch to Database When:**

```markdown
✓ Graph grows beyond 10,000 blocks
✓ Performance becomes issue
✓ Need collaboration
✓ Primarily use mobile
✓ Want advanced features
```

**Start Database, Switch to File-Based When:**

```markdown
✓ Need Git integration
✓ Want tool flexibility
✓ Privacy concerns (no cloud)
✓ Prefer self-hosted sync
✓ Want data portability
```

---

## Migration

### File-Based → Database

**Process:**

```markdown
1. Backup current graph

   - Copy entire folder
   - Verify backup

2. In Logseq:

   - File → Convert to database
   - Wait for conversion
   - Verify data integrity

3. New structure:

   - logseq.db created
   - Files moved to backup/
   - Transit exports in version-files/

4. Test thoroughly:
   - Open pages
   - Check queries
   - Verify links

Time: 5-30 minutes depending on size
```

**What Happens:**

```markdown
Before:
LogseqGraph/
├── journals/ (many .md files)
├── pages/ (many .md files)
└── logseq/

After:
LogseqGraph/
├── logseq.db (new)
├── version-files/ (new)
│ └── graph-xxx.transit
├── backup/
│ ├── journals/ (original files)
│ └── pages/ (original files)
└── logseq/

Original files preserved in backup/
```

### Database → File-Based

**Process:**

```markdown
1. Export database:

   - File → Export → Markdown
   - Choose location
   - Wait for export

2. Create new file-based graph:

   - Open exported folder
   - Logseq detects markdown files
   - Indexes all files

3. Verify:
   - Check all pages
   - Test queries
   - Verify links

Time: 10-60 minutes depending on size
```

**Export Options:**

```markdown
Export formats:

1. Markdown (for file-based graph)

   - Standard markdown
   - Preserves structure
   - Ready for Logseq

2. JSON (for backup/processing)

   - Complete data
   - Machine-readable
   - Can be parsed

3. EDN (for developers)
   - Clojure format
   - Full fidelity
   - Technical
```

---

## Sync & Collaboration

### File-Based Sync Options

**Git (Best for Solo/Teams):**

```bash
# Setup
cd LogseqGraph
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/you/graph.git
git push -u origin main

# Daily workflow
git pull  # Before work
git add .
git commit -m "Update"
git push  # After work

# Automation with cron
0 */6 * * * cd ~/LogseqGraph && git add . && git commit -m "Auto backup" && git push
```

**Dropbox/iCloud (Easy but Conflicts):**

```markdown
Setup:

1. Move graph to Dropbox/iCloud folder
2. Open in Logseq
3. Auto-sync enabled

Pros:

- Automatic
- Easy
- Fast

Cons:

- Conflict files possible
- Manual resolution
- Timing issues
```

**Syncthing (P2P, Private):**

```markdown
Setup:

1. Install Syncthing
2. Add graph folder
3. Connect devices
4. Auto-sync

Pros:

- Peer-to-peer
- No cloud
- Fast local network
- Free

Cons:

- Setup complexity
- Both devices online
- Conflicts possible
```

### Database Sync Options

**Logseq Sync (Recommended):**

```markdown
Setup:

1. Settings → Sync
2. Enable Logseq Sync
3. Login/subscribe
4. All devices auto-sync

Features:

- Real-time sync
- Conflict-free (CRDT)
- End-to-end encrypted
- Mobile support
- Version history

Cost: $5/month
```

**Alternative (Transit Export to Git):**

```bash
# Manual process
1. Export transit file
2. Commit to git
3. Push
4. On other device: pull, import

Not ideal but works
```

---

## Best Practices

### File-Based Best Practices

**✅ Regular Backups:**

```bash
# Automated backup script
#!/bin/bash
DATE=$(date +%Y-%m-%d)
tar -czf "backup-${DATE}.tar.gz" LogseqGraph/
cp "backup-${DATE}.tar.gz" /path/to/backup/
```

**✅ Use Git:**

```bash
# Add to .gitignore
.DS_Store
.cache/
node_modules/

# Commit regularly
git add .
git commit -m "Daily update"
git push
```

**✅ Structure with Namespaces:**

```markdown
Use namespaces for organization:

- Projects/Alpha
- Projects/Beta
- Areas/Health
- Areas/Career

Files created:

- Projects\_\_\_Alpha.md
- Projects\_\_\_Beta.md
- Areas\_\_\_Health.md
- Areas\_\_\_Career.md

Easier to find, manage
```

### Database Best Practices

**✅ Regular Exports:**

```markdown
Schedule exports:

- Weekly: Full markdown export
- Daily: Transit export
- Before major changes: Export

Automate if possible
```

**✅ Verify Integrity:**

```markdown
Monthly check:
Settings → Database → Check integrity

Fixes issues:

- Orphaned blocks
- Invalid references
- Schema violations
```

**✅ Use Logseq Sync:**

```markdown
For multi-device + database:

- Logseq Sync is best option
- End-to-end encrypted
- Conflict-free
- Worth $5/month
```

### Universal Best Practices

**✅ Consistent Naming:**

```markdown
Use consistent conventions:

- Page names: Title Case
- Properties: lowercase-dash
- Namespaces: Noun/Detail
- Dates: YYYY-MM-DD

Reduces confusion
```

**✅ Monitor Performance:**

```markdown
Watch for issues:

- Slow startup
- Laggy typing
- Slow queries

May need:

- Re-index
- Database mode
- Cleanup
```

**✅ Document Your Choice:**

```markdown
In your graph, note:

- Why file-based or database
- Sync strategy
- Backup schedule
- Contact for questions (team)

Future you will thank you
```

---

## Troubleshooting

### File-Based Issues

**Slow Startup:**

```markdown
Solutions:

1. Reduce file count (archive old)
2. Split large pages
3. Clean up assets
4. Consider database mode

Check:
Settings → File count
If > 10,000, consider database
```

**Sync Conflicts:**

```markdown
Dropbox conflict files:

1. Open both versions
2. Compare changes
3. Merge manually
4. Delete conflict file

Prevention:

- Use Git instead
- Single editor at a time
- Close Logseq before syncing
```

**Encoding Issues:**

```markdown
If special characters broken:

1. Check file encoding (UTF-8)
2. Re-save files as UTF-8
3. Update Logseq

Command line:
iconv -f ISO-8859-1 -t UTF-8 input.md > output.md
```

### Database Issues

**Corruption:**

```markdown
If database corrupted:

1. Settings → Database → Check integrity
2. If fails, restore from transit export
3. File → Import → Transit file
4. Verify data

Prevention:

- Regular exports
- Don't force quit Logseq
- Let saves complete
```

**Performance Degradation:**

```markdown
If database slows down:

1. Settings → Database → Vacuum
2. Re-index graph
3. Export and re-import
4. Check disk space

Vacuum recovers space
Re-index rebuilds indexes
```

**Cannot Open:**

```markdown
If database won't open:

1. Check file permissions
2. Look for .db-wal or .db-shm files
3. Close other Logseq instances
4. Restore from backup

SQLite lockfile issues common
```

---

## Future Considerations

### Trends

**Database Mode Becoming Default:**

```markdown
Logseq direction:

- Future features database-first
- Better mobile support
- Real-time collaboration
- Advanced schema

File-based still supported
But database is future
```

**Hybrid Approaches:**

```markdown
Possible future:

- Database for performance
- Export to markdown on demand
- Best of both worlds
- Transparent sync

Not yet available
```

### Making the Decision

**Start with File-Based if:**

- New to Logseq
- Small graph
- Want to explore
- Like transparency

**Switch to Database when:**

- Performance matters
- Graph is large
- Need collaboration
- Primarily mobile

**Both are valid choices.** Choose based on your needs, not hype.

---

## Summary

**File-Based:**

- Transparent, portable, Git-friendly
- Best for: Small-medium graphs, solo users, Git lovers
- Trade-off: Performance at scale

**Database:**

- Fast, scalable, collaboration-ready
- Best for: Large graphs, mobile users, teams
- Trade-off: Less transparent, needs Logseq

**Both:**

- Support all core features
- Work offline-first
- Preserve your data
- Can migrate between

**Choose based on your needs. You can always migrate later.**

---

## Next Steps

**Evaluate Your Needs:**

1. Assess current graph size
2. Consider growth trajectory
3. Identify primary use case
4. Choose storage mode
5. Set up appropriate sync

**Implement:**

1. If new: Start with file-based
2. If large/mobile: Use database
3. Configure sync strategy
4. Document your setup
5. Schedule backups

**Related:**

- [Local-First Architecture](./local-first-architecture.md)
- [File Organization](./file-organization.md)
- [Database Integrity](../architecture/database-integrity.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
