---
title: 'Troubleshooting Guide'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'How-To'
platform: 'All Platforms'
related-docs:
  - '[Performance Guide](../optimization/performance-guide.md)'
  - '[Sync Issues](./sync-issues.md)'
  - '[File System Errors](./file-system-errors.md)'
tags: ['troubleshooting', 'debugging', 'errors', 'problems']
---

# Troubleshooting Guide

Diagnose and fix common Logseq problems. This comprehensive guide covers startup issues, sync problems, performance degradation, data integrity, and more.

**What This Covers:**

- Startup and loading issues
- Sync and storage problems
- Performance and slowness
- Data loss and corruption
- Plugin and theme issues
- Platform-specific problems

**When to Use:**

- Logseq won't start or load
- Experiencing crashes or freezes
- Sync failing or conflicts occurring
- Data missing or corrupted
- Plugins not working
- Performance degraded

---

## Table of Contents

1. [Quick Diagnostics](#quick-diagnostics)
2. [Startup Issues](#startup-issues)
3. [Sync Problems](#sync-problems)
4. [Performance Issues](#performance-issues)
5. [Data Problems](#data-problems)
6. [Plugin Issues](#plugin-issues)
7. [Platform-Specific](#platform-specific)
8. [Advanced Recovery](#advanced-recovery)

---

## Quick Diagnostics

### First Steps

**Before diving into specific issues:**

```markdown
1. **Check Logseq version:**

   - Settings → About
   - Ensure you're on latest stable version
   - Update if behind

2. **Check system requirements:**

   - Desktop: 4GB+ RAM recommended
   - Mobile: iOS 14+, Android 8+
   - Storage: 500MB+ free space

3. **Reproduce the issue:**

   - Can you consistently trigger it?
   - Does it happen in safe mode?
   - Is it graph-specific or global?

4. **Check logs:**
   - Help → Developer tools → Console
   - Look for error messages
   - Note timestamps and patterns
```

### Safe Mode

**Test in clean environment:**

```markdown
1. **Enable Safe Mode:**

   - Settings → Advanced → Safe mode
   - Disables all plugins and custom CSS
   - Helps isolate issue

2. **If problem disappears:**

   - Issue is plugin or theme related
   - Re-enable plugins one by one
   - Identify culprit

3. **If problem persists:**
   - Core Logseq issue
   - Continue troubleshooting below
```

### Diagnostic Checklist

```markdown
## Initial Diagnostic

- [ ] Logseq version: \***\*\_\*\***
- [ ] Operating system: \***\*\_\*\***
- [ ] Graph type: File-based / Database
- [ ] Sync enabled: Yes / No
- [ ] Plugins installed: \***\*\_\*\***
- [ ] Custom CSS: Yes / No
- [ ] Issue reproducible: Yes / No
- [ ] Affects all graphs: Yes / No
- [ ] Started after: \***\*\_\*\***
```

---

## Startup Issues

### Logseq Won't Start

**Symptoms:** Application doesn't launch, crashes immediately, white screen.

**Solution 1: Clear Cache**

```bash
# macOS
rm -rf ~/Library/Application\ Support/Logseq
rm -rf ~/Library/Caches/Logseq

# Windows
# Delete folders:
# %APPDATA%/Logseq
# %LOCALAPPDATA%/Logseq

# Linux
rm -rf ~/.config/Logseq
rm -rf ~/.cache/Logseq
```

⚠️ **Warning:** This removes all local data. Ensure graphs are synced or backed up.

**Solution 2: Reinstall Logseq**

```markdown
1. **Uninstall completely:**

   - macOS: Delete from Applications
   - Windows: Uninstall from Control Panel
   - Linux: Remove package

2. **Clear application data** (as above)

3. **Download fresh installer:**

   - Visit https://logseq.com/downloads
   - Get latest stable version
   - Install

4. **Re-add graphs:**
   - Open existing graph folder
   - Or restore from sync
```

**Solution 3: Check Permissions**

```bash
# macOS: Reset permissions
sudo chown -R $(whoami) ~/Library/Application\ Support/Logseq

# Linux: Fix ownership
sudo chown -R $USER:$USER ~/.config/Logseq
```

### White Screen / Blank Page

**Symptoms:** Logseq opens but shows blank white screen.

**Solution 1: Force Reload**

```markdown
- Desktop: Ctrl+R (Windows/Linux) or Cmd+R (macOS)
- Or: Help → Reload
```

**Solution 2: Disable GPU Acceleration**

```markdown
1. Start Logseq with flag:

   macOS/Linux:
   open -a Logseq --args --disable-gpu

   Windows:
   Logseq.exe --disable-gpu

2. If fixed, make permanent:
   - Settings → Advanced → Disable GPU acceleration
```

**Solution 3: Reset Database**

```markdown
1. Go to graph folder
2. Delete `.logseq/` directory
3. Restart Logseq
4. Reindex graph
```

### Stuck on Loading Screen

**Symptoms:** "Loading..." screen never completes.

**Solution 1: Check Graph Integrity**

```markdown
1. Navigate to graph folder in file manager
2. Look for:

   - Corrupted files (0 bytes, weird characters)
   - Very large files (>50MB)
   - Symlinks or aliases

3. Move suspicious files to temporary location
4. Try loading again
```

**Solution 2: Incremental Loading**

```markdown
1. Create new empty graph
2. Gradually copy files from old graph:
   - Start with 10-20 files
   - Load and verify
   - Add more in batches
3. Identify problematic file
```

**Solution 3: Check Database Size**

For database graphs:

```markdown
1. Check .logseq/graphs/ folder
2. If .db file is very large (>500MB):
   - May be corrupted or bloated
   - Consider exporting to file-based
   - Or: Delete .db and re-import from transit
```

### Graph Won't Open

**Symptoms:** Specific graph fails to load, others work fine.

**Solution 1: Repair Graph**

```markdown
1. **File-based graph:**

   - Open graph folder
   - Look for .logseq/config.edn
   - Validate EDN syntax
   - Fix or delete if corrupted

2. **Database graph:**
   - Check .logseq/graphs/<graph-id>.db
   - If large or corrupted, delete
   - Re-import from latest .transit export
```

**Solution 2: Remove Problematic Files**

```markdown
1. Open DevTools (Ctrl+Shift+I)
2. Look at Console for error messages
3. Errors often point to specific files
4. Move those files out temporarily
5. Retry loading
```

**Solution 3: Reindex**

```markdown
1. Settings → Advanced → Re-index graph
2. Or: Delete .logseq/graphs/<graph>.db
3. Restart Logseq
4. Wait for full reindex (may take time for large graphs)
```

---

## Sync Problems

### Sync Not Working

**Symptoms:** Changes not syncing, "sync failed" errors.

**Solution 1: Check Connectivity**

```markdown
1. **Verify internet connection:**

   - Test other websites
   - Check firewall settings
   - Disable VPN temporarily

2. **Check Logseq servers:**

   - Visit https://status.logseq.com
   - Check for outages

3. **Test network:**
   - Help → Developer tools → Network tab
   - Look for failed requests
```

**Solution 2: Re-authenticate**

```markdown
1. Settings → Sync → Sign out
2. Sign out completely
3. Restart Logseq
4. Settings → Sync → Sign in
5. Re-authorize graph
```

**Solution 3: Clear Sync Cache**

```markdown
1. Settings → Sync → Advanced
2. Click "Clear sync cache"
3. Restart Logseq
4. Wait for full re-sync
```

**Solution 4: Check Graph Size**

```markdown
Sync limitations:

- File-based: No explicit limit
- Database: Check plan limits
- Large assets (videos) may cause issues

Fix:

1. Check total graph size
2. Remove large unnecessary files
3. Use external storage for big assets
```

### Sync Conflicts

**Symptoms:** "Conflict detected" messages, duplicate content.

**Solution 1: Understand Conflicts**

```markdown
Conflicts occur when:

- Same block edited on multiple devices
- Offline edits synced later
- Network interruption during sync

Logseq creates:

- Conflict blocks marked with timestamp
- Both versions preserved
```

**Solution 2: Resolve Manually**

```markdown
1. **Find conflict blocks:**

   - Search: `{{query (page "conflict-resolution")}}`
   - Or manually scan recent edits

2. **Merge content:**

   - Review both versions
   - Keep correct version
   - Delete duplicate

3. **Mark resolved:**
   - Remove conflict marker
   - Add resolution note
```

**Solution 3: Prevent Conflicts**

```markdown
Best practices:

- Close Logseq on one device before using another
- Wait for sync to complete before closing
- Check sync status indicator
- Use one device at a time when possible
```

**Solution 4: Reset Sync State**

If conflicts are persistent:

```markdown
1. **Export graph:**

   - Settings → Export graph
   - Save JSON or EDN

2. **Clear sync:**

   - Settings → Sync → Clear sync cache
   - Delete all synced data

3. **Re-import:**
   - Create fresh graph
   - Import from export
   - Enable sync again
```

### Sync Slow or Stalled

**Symptoms:** Sync takes forever, progress bar stuck.

**Solution 1: Check Network Speed**

```markdown
1. Test internet speed
2. Large graphs need good upload speed
3. Consider:
   - Using wired connection
   - Switching network
   - Syncing during off-peak hours
```

**Solution 2: Reduce Graph Size**

```markdown
1. **Remove large assets:**

   - Videos
   - Large PDFs
   - High-res images

2. **Archive old content:**

   - Move to separate graph
   - Or delete unused pages

3. **Clean up:**
   - Delete old journals
   - Remove temp pages
```

**Solution 3: Force Sync**

```markdown
1. Settings → Sync → Force sync
2. Or restart Logseq
3. Check sync progress in status bar
```

---

## Performance Issues

### Logseq is Slow

**Symptoms:** Laggy typing, slow navigation, delayed actions.

**Solution 1: Optimize Graph**

```markdown
1. **Check graph size:**

   - Large graphs (>10,000 blocks) may slow down
   - Consider splitting into multiple graphs

2. **Reduce linked references:**

   - Too many backlinks slow rendering
   - Archive old content
   - Be selective with links

3. **Simplify queries:**
   - Complex queries impact performance
   - Cache query results where possible
   - Limit results with :limit
```

**Solution 2: Disable Features**

```markdown
1. **Turn off expensive features:**

   - Settings → Auto-sync (if not needed)
   - Settings → Real-time collaboration
   - Settings → Auto-save frequency (increase interval)

2. **Reduce plugins:**
   - Disable unused plugins
   - Some plugins impact performance
```

**Solution 3: Hardware Optimization**

```markdown
1. **Increase RAM allocation:**

   - Close other applications
   - Restart computer

2. **Use SSD storage:**

   - Faster disk access
   - Move graph to SSD

3. **Disable GPU acceleration** (if causing issues):
   - Settings → Advanced → Disable GPU
```

**Solution 4: Database Maintenance**

For database graphs:

```markdown
1. **Vacuum database:**

   - Settings → Advanced → Optimize database
   - Or: Export → Delete → Re-import

2. **Rebuild indexes:**
   - Settings → Advanced → Re-index
   - Wait for completion
```

### High Memory Usage

**Symptoms:** Logseq uses excessive RAM, system slows down.

**Solution 1: Monitor Memory**

```markdown
1. Help → Developer tools → Memory tab
2. Take heap snapshot
3. Look for memory leaks
4. Identify large objects
```

**Solution 2: Restart Regularly**

```markdown
- Long-running sessions accumulate memory
- Restart Logseq daily
- Close when not in use
```

**Solution 3: Reduce Cache**

```markdown
1. Settings → Advanced → Clear cache
2. Restart Logseq
3. May need to reindex
```

**Solution 4: Limit Open Pages**

```markdown
- Close unused tabs
- Limit right sidebar panels
- Use fewer concurrent queries
```

### Slow Sync

See [Sync Slow or Stalled](#sync-slow-or-stalled) above.

---

## Data Problems

### Missing Content

**Symptoms:** Blocks, pages, or entire sections disappeared.

**Solution 1: Check Version History**

```markdown
1. **File-based graphs:**

   - Check file system (Finder/Explorer)
   - Look in graph folder
   - Search for .md files

2. **Database graphs:**

   - Check .logseq/graphs/<graph>.transit exports
   - Restore from backup

3. **Use file history:**
   - macOS: Time Machine
   - Windows: File History
   - Linux: Check backup tool
```

**Solution 2: Search Thoroughly**

```markdown
1. Use full-text search
2. Try different keywords
3. Check:
   - Archive pages
   - Old journals
   - Trash (if plugin installed)
```

**Solution 3: Restore from Backup**

```markdown
1. **Automatic backups:**

   - Check .logseq/bak/ folder
   - Dated backup files
   - Restore from recent backup

2. **Sync history:**

   - If using Logseq sync
   - May have version history
   - Contact support for recovery

3. **Manual backups:**
   - Check your backup location
   - Restore from known good state
```

**Solution 4: Recovery Tools**

```markdown
1. **For file-based:**

   - Use file recovery software
   - Recuva (Windows)
   - TestDisk (cross-platform)
   - PhotoRec

2. **For database:**
   - Check transit exports
   - Database recovery tools
   - SQLite recovery utilities
```

### Corrupted Data

**Symptoms:** Garbled text, encoding errors, unreadable content.

**Solution 1: Fix Encoding**

````markdown
1. **Check file encoding:**

   - Should be UTF-8
   - Open in text editor
   - Save with correct encoding

2. **Convert if needed:**
   ```bash
   iconv -f ISO-8859-1 -t UTF-8 input.md > output.md
   ```
````

````

**Solution 2: Repair Files**

```markdown
1. **For markdown files:**
   - Open in text editor
   - Look for malformed markdown
   - Fix syntax errors

2. **For database:**
   - Export to transit
   - Check transit file validity
   - Re-import to new graph
````

**Solution 3: Isolate Corruption**

```markdown
1. Create new graph
2. Import pages one by one
3. Identify corrupted pages
4. Repair or recreate manually
```

### Duplicate Content

**Symptoms:** Same blocks appearing multiple times, duplicate pages.

**Solution 1: Merge Duplicates**

```markdown
1. **Find duplicates:**

   - Manual search
   - Or use plugin (Duplicate Finder)

2. **Merge manually:**

   - Keep best version
   - Preserve unique content
   - Delete extras

3. **Fix references:**
   - Update links to merged page
   - Use block references for shared content
```

**Solution 2: Deduplicate Database**

```markdown
1. Export graph
2. Run deduplication script (if available)
3. Re-import cleaned data
```

**Solution 3: Prevent Future Duplicates**

```markdown
- Use consistent naming
- Check for existing pages before creating
- Use templates for standard pages
- Enable "Show existing pages" in autocomplete
```

---

## Plugin Issues

### Plugin Won't Install

**Symptoms:** Plugin fails to install from marketplace or manually.

**Solution 1: Check Compatibility**

```markdown
1. Verify plugin supports your Logseq version
2. Check plugin requirements
3. Read installation instructions
```

**Solution 2: Manual Installation**

```markdown
1. Download plugin ZIP from GitHub
2. Extract to folder
3. Settings → Plugins → Load unpacked plugin
4. Select extracted folder
```

**Solution 3: Clear Plugin Cache**

```markdown
1. Close Logseq
2. Delete plugins cache:
   - macOS: ~/Library/Application Support/Logseq/plugins
   - Windows: %APPDATA%/Logseq/plugins
   - Linux: ~/.config/Logseq/plugins
3. Restart and reinstall
```

### Plugin Not Working

**Symptoms:** Plugin installed but not functioning.

**Solution 1: Check Plugin Status**

```markdown
1. Settings → Plugins
2. Verify plugin is enabled
3. Check for error icon
4. Click for error details
```

**Solution 2: Review Console Logs**

```markdown
1. Help → Developer tools → Console
2. Look for plugin-related errors
3. Note error messages
4. Report to plugin developer
```

**Solution 3: Update Plugin**

```markdown
1. Settings → Plugins
2. Check for updates
3. Install latest version
4. Restart if needed
```

**Solution 4: Reinstall Plugin**

```markdown
1. Uninstall plugin
2. Restart Logseq
3. Reinstall from marketplace
4. Configure settings again
```

### Plugin Conflicts

**Symptoms:** Multiple plugins interfering with each other.

**Solution 1: Isolate Conflict**

```markdown
1. Disable all plugins
2. Enable one at a time
3. Test after each
4. Identify conflicting pair
```

**Solution 2: Check Load Order**

```markdown
Some plugins depend on load order:

1. Check plugin documentation
2. Adjust order in plugin settings
3. Restart Logseq
```

**Solution 3: Report to Developers**

```markdown
1. Document conflict details
2. List both plugins and versions
3. Report to both plugin developers
4. May need to choose one plugin
```

### Plugin Performance Issues

**Symptoms:** Specific plugin causing slowness or crashes.

**Solution 1: Profile Plugin**

```markdown
1. Disable plugin
2. Test performance without it
3. If improved, plugin is culprit
4. Report to developer or find alternative
```

**Solution 2: Configure Plugin**

```markdown
- Check plugin settings
- Reduce frequency of operations
- Disable non-essential features
- Limit scope (e.g., specific pages only)
```

**Solution 3: Find Alternative**

```markdown
If plugin is essential but problematic:

1. Search marketplace for alternatives
2. Check plugin reviews
3. Test performance impact
```

---

## Platform-Specific

### macOS Issues

**Symptom: Can't Open Logseq (Security)**

```markdown
Solution:

1. Right-click Logseq.app
2. Select "Open"
3. Click "Open" in dialog
4. Or: System Preferences → Security → Allow
```

**Symptom: iCloud Sync Issues**

```markdown
Solution:

1. Don't store graph in iCloud Drive
2. Use Logseq Sync instead
3. If must use iCloud:
   - Ensure Desktop & Documents sync disabled
   - Or move graph to non-synced location
```

**Symptom: Permission Errors**

```markdown
Solution:

1. System Preferences → Security → Files and Folders
2. Grant Logseq full disk access
3. Restart Logseq
```

### Windows Issues

**Symptom: Firewall Blocking Sync**

```markdown
Solution:

1. Windows Defender Firewall → Allow app
2. Add Logseq.exe to allowed list
3. Allow both private and public networks
```

**Symptom: Path Too Long Error**

```markdown
Windows has 260 character path limit.

Solution:

1. Shorten graph folder path
2. Move closer to drive root (e.g., C:\Logseq\)
3. Or enable long paths:
   - gpedit.msc → Computer Config → Administrative Templates
   - System → Filesystem → Enable Win32 long paths
```

**Symptom: OneDrive Conflicts**

```markdown
Solution:

1. Don't store graph in OneDrive
2. Use Logseq Sync
3. Or disable OneDrive for graph folder
```

### Linux Issues

**Symptom: AppImage Won't Run**

```markdown
Solution:

1. Make executable:
   chmod +x Logseq-\*.AppImage

2. Install FUSE:
   sudo apt install fuse libfuse2

3. Run:
   ./Logseq-\*.AppImage
```

**Symptom: Missing Dependencies**

```markdown
Solution:

1. Install required libraries:
   sudo apt install libgtk-3-0 libnotify4 libnss3 libxss1 \
    libxtst6 xdg-utils libatspi2.0-0 libdrm2 libgbm1 libxcb-dri3-0

2. Or use Flatpak/Snap versions
```

**Symptom: Wayland Issues**

```markdown
Solution:

1. Run with X11 backend:
   GDK_BACKEND=x11 ./Logseq-\*.AppImage

2. Or install XWayland
```

### Mobile Issues (iOS/Android)

**Symptom: App Crashes on Mobile**

```markdown
Solution:

1. Update to latest version
2. Clear app cache:
   - iOS: Offload app and reinstall
   - Android: App settings → Clear cache
3. Restart device
```

**Symptom: Sync Not Working on Mobile**

```markdown
Solution:

1. Check mobile data/WiFi
2. Verify sync enabled in app
3. Re-authenticate sync
4. Check background app refresh:
   - iOS: Settings → General → Background App Refresh
   - Android: Settings → Apps → Logseq → Data usage → Background data
```

**Symptom: Storage Full**

```markdown
Solution:

1. Check device storage
2. Clear Logseq cache
3. Remove large graphs
4. Use database graph (more efficient)
```

---

## Advanced Recovery

### Manual Database Repair

**For database graphs:**

```bash
# 1. Locate database
cd ~/.logseq/graphs/

# 2. Backup
cp <graph-id>.db <graph-id>.db.backup

# 3. Check integrity (requires sqlite3)
sqlite3 <graph-id>.db "PRAGMA integrity_check;"

# 4. If corrupted, dump and rebuild
sqlite3 <graph-id>.db ".dump" > dump.sql
sqlite3 <graph-id>-new.db < dump.sql

# 5. Replace original
mv <graph-id>-new.db <graph-id>.db
```

### Export and Clean Import

**Complete graph reset:**

```markdown
1. **Export current graph:**

   - Settings → Export graph
   - Choose JSON format
   - Save to safe location

2. **Create new graph:**

   - Create new empty graph
   - Configure settings

3. **Import exported data:**

   - Settings → Import
   - Select exported JSON
   - Wait for import

4. **Verify data:**

   - Check all pages imported
   - Test queries
   - Verify attachments

5. **Switch to new graph:**
   - Close old graph
   - Use new as primary
```

### Contact Support

**When all else fails:**

```markdown
1. **Gather information:**

   - Logseq version
   - Operating system
   - Graph type (file/database)
   - Error messages
   - Steps to reproduce
   - Screenshots

2. **Export logs:**

   - Help → Export logs
   - Include in report

3. **Contact support:**

   - Discord: https://discord.gg/logseq (#help channel)
   - Forum: https://discuss.logseq.com
   - GitHub: https://github.com/logseq/logseq/issues
   - Email: support@logseq.com (for sync issues)

4. **Be specific:**
   - Describe problem clearly
   - Share relevant logs
   - Mention troubleshooting steps tried
```

### Data Recovery Services

**For critical data loss:**

```markdown
If data is critical and standard recovery fails:

1. **Professional services:**

   - Disk/data recovery specialists
   - Can recover deleted files
   - Costly but effective

2. **Community help:**

   - Logseq Discord #help channel
   - Power users may assist
   - Share anonymized graph (if comfortable)

3. **Prevention:**
   - Enable automatic backups
   - Use version control (Git)
   - Regular manual exports
   - Multiple backup locations
```

---

## Prevention Checklist

**Avoid future problems:**

```markdown
## Daily

- [ ] Close Logseq properly (don't force quit)
- [ ] Wait for sync to complete before closing
- [ ] Check sync status indicator

## Weekly

- [ ] Review recent backups
- [ ] Update Logseq if new version available
- [ ] Check plugin updates
- [ ] Monitor graph size

## Monthly

- [ ] Export graph (full backup)
- [ ] Clean up old journals
- [ ] Archive completed projects
- [ ] Review and optimize queries
- [ ] Check disk space

## Best Practices

- [ ] Use Logseq Sync or reliable sync solution
- [ ] Keep graphs reasonable size (<10k blocks)
- [ ] Use database graphs for better performance
- [ ] Limit plugins to essential ones
- [ ] Keep backups in multiple locations
- [ ] Test backups regularly
- [ ] Document custom configurations
- [ ] Stay informed on Logseq updates
```

---

## Summary

**Quick Reference:**

| Problem          | First Try                         |
| ---------------- | --------------------------------- |
| Won't start      | Clear cache, reinstall            |
| White screen     | Force reload, disable GPU         |
| Sync failing     | Re-authenticate, clear sync cache |
| Slow performance | Optimize graph, disable plugins   |
| Missing data     | Check backups, search thoroughly  |
| Plugin issues    | Update, reinstall, check console  |
| Platform errors  | Check permissions, update OS      |

**Key Principles:**

1. **Diagnose before acting** - Understand the problem
2. **Start simple** - Try basic solutions first
3. **Backup first** - Always backup before major changes
4. **Document steps** - Note what you try
5. **Seek help** - Community is friendly and helpful
6. **Prevent recurrence** - Learn from issues

**Resources:**

- [Logseq Discord](https://discord.gg/logseq)
- [Discussion Forum](https://discuss.logseq.com)
- [GitHub Issues](https://github.com/logseq/logseq/issues)
- [Documentation](https://docs.logseq.com)
- [Status Page](https://status.logseq.com)

**Remember:** Most issues are solvable. Stay calm, work systematically, and don't hesitate to ask for help! 💪
