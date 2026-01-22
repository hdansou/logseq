---
title: 'Performance Optimization Guide'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'How-To'
platform: 'Desktop + Mobile'
related-docs:
  - '[Database Integrity](../../explanation/developer/database-integrity.md)'
  - '[File Organization](../../explanation/concepts/file-organization.md)'
  - '[Backup Strategies](../organization/backup-strategies.md)'
tags: ['performance', 'optimization', 'speed', 'troubleshooting']
---

# Performance Optimization Guide

This guide helps you optimize Logseq performance for faster loading, smoother editing, and better responsiveness. Learn to identify bottlenecks and apply targeted improvements.

**What You'll Learn:**

- Measure current performance
- Optimize graph structure
- Improve indexing speed
- Reduce memory usage
- Troubleshoot slow operations
- Platform-specific optimizations

**Time Required:** 30-60 minutes for full optimization

---

## Table of Contents

1. [Performance Benchmarks](#performance-benchmarks)
2. [Quick Wins](#quick-wins)
3. [Graph Optimization](#graph-optimization)
4. [Database Tuning](#database-tuning)
5. [Asset Management](#asset-management)
6. [Memory Optimization](#memory-optimization)
7. [Platform-Specific Tips](#platform-specific-tips)
8. [Troubleshooting Slow Performance](#troubleshooting-slow-performance)

---

## Performance Benchmarks

### Measure Your Performance

**Graph Load Time:**

```markdown
1. Close Logseq completely
2. Note start time
3. Open Logseq
4. Graph fully loaded when:
   - Page list visible
   - Search functional
   - Recent pages render
5. Calculate: time to full load
```

**Target Times:**

| Graph Size      | Expected Load Time | Status                |
| --------------- | ------------------ | --------------------- |
| < 1,000 blocks  | < 3 seconds        | ✅ Excellent          |
| 1,000 - 10,000  | 3-10 seconds       | ✅ Good               |
| 10,000 - 50,000 | 10-30 seconds      | ⚠️ Acceptable         |
| 50,000+ blocks  | 30-60 seconds      | 🔴 Needs optimization |

**Key Metrics:**

```markdown
Check in Settings → General:

- Total blocks: [Your number]
- Total pages: [Your number]
- Assets count: [Your number]
- Database size: [MB/GB]
```

**Performance Test Commands:**

```clojure
;; Run in Developer Console (Cmd+Opt+I)

;; Count blocks
(count (d/datoms @conn :avet :block/uuid))

;; Count pages
(count (d/datoms @conn :avet :block/name))

;; Check index size
(.getAll (.-storage js/indexedDB))
```

---

## Quick Wins

### Immediate Improvements

**1. Re-index Graph**

```markdown
⋯ → Settings → Advanced → Re-index

When: After bulk imports, before measuring performance
Effect: Rebuilds search index, fixes inconsistencies
Time: 1-5 minutes depending on graph size
```

**2. Clear Cache**

```markdown
⋯ → Settings → Advanced → Clear cache

Effect: Removes cached data, fresh start
When: Monthly or when experiencing slowness
```

**3. Update Logseq**

```markdown
Check: Help → About → Check for updates
Latest: v0.10.9 (always use latest stable)
```

**4. Disable Unused Plugins**

```markdown
⋯ → Plugins → Disable unused plugins

Impact:

- 3-5 plugins: Minimal
- 10+ plugins: Noticeable
- 20+ plugins: Significant slowdown
```

**5. Reduce Auto-Save Frequency**

```markdown
Settings → Editor → Auto-save delay

Default: 2000ms (2 seconds)
Recommended: 5000ms (5 seconds) for large graphs
Trade-off: Less saving = less disk I/O, but more data loss risk
```

---

## Graph Optimization

### Structure Your Content

**1. Split Large Pages**

**Problem:** Pages with 500+ blocks slow rendering

**Solution:**

```markdown
Before:

- [[Projects]] (2000 blocks, very slow)

After:

- [[Projects]]
  - [[Projects/Work]] (600 blocks)
  - [[Projects/Personal]] (400 blocks)
  - [[Projects/Learning]] (500 blocks)

Impact: 3-5x faster page rendering
```

**2. Use Namespaces Wisely**

**Good Structure:**

```markdown
Projects/Work/ClientA
Projects/Work/ClientB
Projects/Personal/Home
Projects/Personal/Health

Benefits:

- Logical grouping
- Lazy loading (only load visible pages)
- Better organization
```

**Avoid:**

```markdown
Projects/Work/ClientA/ProjectX/PhaseY/TaskZ/SubtaskA

Issues:

- Deep nesting (>5 levels)
- Slow tree expansion
- Complex queries
```

**3. Optimize Queries**

**Slow Query:**

```clojure
#+BEGIN_QUERY
{:title "All blocks with content"
 :query [:find (pull ?b [*])
         :where [?b :block/content]]}  ;; Returns 50,000+ blocks
#+END_QUERY
```

**Fast Query:**

```clojure
#+BEGIN_QUERY
{:title "Recent TODOs"
 :query [:find (pull ?b [*])
         :in $ ?start
         :where
         [?b :block/marker "TODO"]
         [?b :block/created-at ?t]
         [(> ?t ?start)]]
 :inputs [:7d-before]}  ;; Only last 7 days
#+END_QUERY
```

**Query Performance Tips:**

```markdown
✅ Do:

- Add time filters (:7d-before, :30d-before)
- Filter by marker (TODO, DOING)
- Limit results: :limit 50
- Use simple-query? true for basic queries

❌ Avoid:

- Queries returning 1000+ blocks
- Complex joins without filters
- Nested pull patterns
- Queries in every page
```

**4. Archive Old Content**

```markdown
Move old journals to archive:

1. Create [[Archive]] namespace
2. Move pages: journals/2023-\* → Archive/2023/
3. Reduces active graph size
4. Keeps data accessible

Structure:

- Archive/
  - 2023/
    - 2023-01.md (monthly combined)
    - 2023-02.md
  - 2024/
```

**Script:**

```bash
#!/bin/bash
# archive-old-journals.sh

YEAR=2023
ARCHIVE_DIR="$HOME/LogseqGraph/pages/Archive/$YEAR"

mkdir -p "$ARCHIVE_DIR"

# Combine monthly journals
for month in {01..12}; do
  cat journals/$YEAR-$month-*.md > "$ARCHIVE_DIR/$YEAR-$month.md"
  rm journals/$YEAR-$month-*.md
done

echo "Archived $YEAR journals"
```

---

## Database Tuning

### Database Graph Performance

**1. Vacuum Database**

```markdown
⋯ → Settings → Advanced → Vacuum database

What it does:

- Compacts SQLite database
- Removes deleted data
- Rebuilds indexes

When: Monthly or after large deletions
Time: 1-10 minutes
Effect: 20-40% size reduction
```

**2. Check Integrity**

```markdown
⋯ → Settings → Advanced → Check database integrity

Runs: PRAGMA integrity_check
Fixes: Corrupted indexes, inconsistencies
When: After crashes, before exports
```

**3. Optimize Schema**

**Check Schema Version:**

```clojure
;; Developer Console
@(get-in @state/state [:db/schema-version])
;; Should be 65.16 or higher
```

**Migration:**

```markdown
If old schema detected:

1. ⋯ → Settings → Advanced → Migrate schema
2. Wait for completion (may take 10-30 min)
3. Verify queries still work
```

**4. Reduce Transaction Size**

**Problem:** Large pastes/imports cause lag

**Solution:**

```markdown
Settings → Editor → Batch size

Default: 100 blocks
Large graph: 50 blocks
Very large: 25 blocks

Trade-off: Smaller batches = slower imports but smoother UI
```

---

## Asset Management

### Optimize Media Files

**1. Compress Images**

**Before Upload:**

```bash
# Compress PNG (70% quality)
for img in *.png; do
  pngquant --quality=60-80 --ext .png --force "$img"
done

# Compress JPEG
for img in *.jpg; do
  jpegoptim --max=85 "$img"
done

# Result: 50-80% size reduction
```

**2. Use Web Formats**

```markdown
Replace:

- PNG → WebP (50% smaller)
- JPEG → WebP or AVIF
- GIF → MP4 (10x smaller)

Tools:

- cwebp (PNG/JPEG → WebP)
- ffmpeg (GIF → MP4)
```

**3. External Asset Hosting**

**Problem:** Large assets slow graph loading

**Solution:**

```markdown
Host on CDN/cloud:

1. Upload to: Imgur, Cloudinary, S3
2. Reference by URL:
   ![](https://i.imgur.com/abc123.png)

Benefits:

- Faster loading (CDN)
- Smaller graph size
- Parallel downloads

Trade-off: Requires internet, external dependency
```

**4. Clean Unused Assets**

```bash
#!/bin/bash
# find-unused-assets.sh

cd "$HOME/LogseqGraph"

# Find all referenced assets
grep -roh '\.\./assets/[^)]*' pages journals | sort -u > used-assets.txt

# Find all actual assets
find assets -type f > all-assets.txt

# Find unused (not in used-assets.txt)
comm -13 <(sort used-assets.txt) <(sort all-assets.txt) > unused-assets.txt

echo "Unused assets: $(wc -l < unused-assets.txt)"
```

**5. Asset Location**

```markdown
Best: assets/ in graph root
Avoid: Multiple asset folders
Why: Single location = faster indexing

Structure:
assets/
├── images/
├── pdfs/
├── videos/
└── audio/
```

---

## Memory Optimization

### Reduce Memory Usage

**1. Check Memory Usage**

```markdown
macOS: Activity Monitor → Logseq
Windows: Task Manager → Logseq.exe
Linux: htop → logseq

Typical:

- Small graph (<10k blocks): 200-400 MB
- Medium (10-50k): 400-800 MB
- Large (50k+): 800-1500 MB
- Very large (100k+): 1.5-3 GB
```

**2. Reduce Loaded Pages**

```markdown
Settings → Editor → Number of loaded pages

Default: 20 pages
Optimized: 10 pages
Low memory: 5 pages

Effect: Less memory, but more page loads
```

**3. Disable Features**

```markdown
Turn off if not needed:

Settings → Features:
□ Graph view (saves 100-200 MB)
□ Whiteboards (saves 50-100 MB)
□ PDF annotations (saves 50-100 MB)
□ Full-text search (saves 100-300 MB)
```

**4. Restart Regularly**

```markdown
Memory leak indicators:

- Logseq using >2 GB
- Slow after hours of use
- Frequent freezing

Solution: Restart Logseq daily/weekly
```

**5. Close Unused Tabs**

```markdown
Right sidebar tabs consume memory:

Close unused:

- Right-click tab → Close
- Or: Cmd+W (close active tab)

Each tab: 20-50 MB
10+ tabs: 200-500 MB overhead
```

---

## Platform-Specific Tips

### macOS

**1. Disable Spotlight Indexing**

```bash
# Prevent Spotlight indexing Logseq graph
cd ~/LogseqGraph
touch .metadata_never_index

# Or system-wide:
sudo mdutil -i off /
```

**2. Increase File Limits**

```bash
# Check current limits
ulimit -n  # Should be 10000+

# Increase (add to ~/.zshrc)
ulimit -n 10000
```

**3. Use APFS Snapshots**

```bash
# Create snapshot before major changes
tmutil localsnapshot

# Restore if needed
tmutil listlocalsnapshots /
tmutil deletelocalsnapshots
```

### Windows

**1. Disable Windows Defender Real-Time Scanning**

```powershell
# Exclude Logseq graph from scanning
Add-MpPreference -ExclusionPath "C:\Users\YourName\LogseqGraph"

# Exclude Logseq app
Add-MpPreference -ExclusionProcess "Logseq.exe"
```

**2. Increase Page File**

```markdown
1. System Properties → Advanced → Performance Settings
2. Advanced → Virtual memory → Change
3. Set custom size:
   - Initial: 4096 MB
   - Maximum: 8192 MB
4. Restart
```

**3. Disable Search Indexing**

```markdown
1. Services → Windows Search → Stop
2. Or exclude:
   Indexing Options → Modify → Uncheck LogseqGraph folder
```

### Linux

**1. Increase inotify Limits**

```bash
# Check current limits
cat /proc/sys/fs/inotify/max_user_watches

# Increase (add to /etc/sysctl.conf)
fs.inotify.max_user_watches=524288
fs.inotify.max_user_instances=512

# Apply
sudo sysctl -p
```

**2. Use Fast Filesystem**

```bash
# Check filesystem
df -T ~/LogseqGraph

# Recommended: ext4, btrfs, zfs
# Avoid: NFS, FUSE (slower)
```

**3. RAM Disk for Cache**

```bash
# Create RAM disk (tmpfs)
sudo mkdir /mnt/ramdisk
sudo mount -t tmpfs -o size=512M tmpfs /mnt/ramdisk

# Point Logseq cache to RAM disk
ln -s /mnt/ramdisk ~/.logseq/cache
```

---

## Troubleshooting Slow Performance

### Common Issues

**1. Slow Graph Loading**

**Causes:**

- Large graph (50k+ blocks)
- Too many plugins
- Slow disk (HDD vs SSD)
- Outdated Logseq version

**Solutions:**

```markdown
1. Measure load time (see benchmarks)
2. Re-index graph
3. Disable plugins one-by-one
4. Move graph to SSD
5. Update Logseq
6. Split large pages
```

**2. Laggy Typing**

**Causes:**

- Auto-save too frequent
- Large page (500+ blocks)
- Heavy queries on page
- Memory leak

**Solutions:**

```markdown
1. Increase auto-save delay: 5000ms
2. Split page into smaller pages
3. Remove/optimize queries
4. Restart Logseq
5. Check memory usage
```

**3. Slow Search**

**Causes:**

- Large graph
- Fuzzy search enabled
- Old index
- No full-text search index

**Solutions:**

```markdown
1. Re-index graph
2. Enable full-text search:
   Settings → Editor → Enable full-text search
3. Use exact search (disable fuzzy)
4. Build search index:
   ⋯ → Settings → Advanced → Build search index
```

**4. Slow Page Rendering**

**Causes:**

- 500+ blocks on page
- Complex queries
- Many embeds
- Large assets

**Solutions:**

```markdown
1. Split page (<200 blocks per page)
2. Optimize queries (add filters)
3. Replace embeds with links
4. Compress/remove large images
```

**5. High CPU Usage**

**Causes:**

- Infinite query loop
- Plugin bug
- Sync conflict resolution
- Re-indexing

**Solutions:**

```markdown
1. Check queries for loops
2. Disable plugins
3. Pause sync temporarily
4. Wait for re-index to finish
```

---

## Performance Checklist

**Before Optimization:**

- [ ] Measure current performance (load time, memory)
- [ ] Note graph size (blocks, pages, assets)
- [ ] Identify slowest operations
- [ ] Backup graph

**Quick Wins:**

- [ ] Re-index graph
- [ ] Clear cache
- [ ] Update Logseq
- [ ] Disable unused plugins
- [ ] Increase auto-save delay

**Graph Structure:**

- [ ] Split pages >500 blocks
- [ ] Use namespaces (3-4 levels max)
- [ ] Optimize queries (add filters)
- [ ] Archive old journals
- [ ] Remove duplicate pages

**Database (if applicable):**

- [ ] Vacuum database
- [ ] Check integrity
- [ ] Update schema
- [ ] Reduce transaction size

**Assets:**

- [ ] Compress images (WebP)
- [ ] Remove unused assets
- [ ] Use external hosting for large files
- [ ] Organize in assets/ folder

**Memory:**

- [ ] Reduce loaded pages
- [ ] Close unused tabs
- [ ] Disable unused features
- [ ] Restart Logseq regularly

**Platform-Specific:**

- [ ] Disable antivirus scanning (graph folder)
- [ ] Exclude from file indexing
- [ ] Increase file limits (Linux/macOS)
- [ ] Use SSD storage

**After Optimization:**

- [ ] Measure performance again
- [ ] Compare before/after
- [ ] Document improvements
- [ ] Schedule regular maintenance

---

## Best Practices

### Daily Habits

```markdown
1. Close Logseq when not in use
2. Restart weekly
3. Keep graph organized
4. Regular cleanups
```

### Weekly Maintenance

```markdown
1. Check graph size
2. Archive old journals
3. Remove unused assets
4. Optimize slow queries
```

### Monthly Tasks

```markdown
1. Re-index graph
2. Clear cache
3. Vacuum database (if DB graph)
4. Update Logseq
5. Review plugin usage
```

---

## Next Steps

**Optimize Your Graph:**

1. Run performance benchmarks
2. Apply quick wins (15 min)
3. Implement graph optimizations (30 min)
4. Platform-specific tuning (15 min)
5. Monitor improvements

**Related:**

- [Database Integrity](../../explanation/developer/database-integrity.md)
- [File Organization](../../explanation/concepts/file-organization.md)
- [Backup Strategies](../organization/backup-strategies.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
