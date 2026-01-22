# Database Graph Backup Guide

**For:** Logseq Desktop Users (Windows & macOS)  
**Graph Type:** Database Graphs  
**Last Updated:** January 11, 2026

---

## What Are Database Backups?

Logseq automatically creates backup copies of your database graphs to protect your data. Think of backups as snapshots of your graph at different points in time - if something goes wrong, you can restore a previous version.

### Why Backups Matter

- 🛡️ **Protect against data corruption** - If your database gets damaged, you can restore from a backup
- 🔄 **Recover from mistakes** - Accidentally deleted important notes? Restore an older version
- 💾 **Peace of mind** - Your work is automatically saved in multiple places

---

## How Backups Work

### Automatic Backups

Logseq automatically creates backups whenever it saves your database. **You don't need to do anything** - it just works!

**When backups are created:**

- ✅ Every time you manually save (CMD+S / Ctrl+S)
- ✅ Automatically every 30 seconds (if you made changes)
- ✅ When you close Logseq

**How many backups are kept:**

- 12 backup copies total
- 6 recent backups (one per hour)
- 6 older backups (one per day)

**Example timeline:**

```txt
Today 3:30 PM  ← Most recent
Today 2:15 PM
Today 1:45 PM
Today 12:30 PM
Today 11:15 PM
Today 10:00 AM
Yesterday 11:45 PM
2 days ago 6:30 PM
3 days ago 2:15 PM
4 days ago 9:45 AM
5 days ago 4:30 PM
6 days ago 8:15 PM  ← Oldest kept
```

### Where Backups Are Stored

**Backup Location:**

- **Windows:** `C:\Users\<YourName>\.logseq\graphs\<YourGraph>\backups\`
- **macOS:** `/Users/<YourName>/.logseq/graphs/<YourGraph>/backups/`

**Important:**

- `.logseq` folder is hidden by default
- Don't manually edit or delete files in this folder
- Each backup is a complete copy of your database

---

## Viewing Your Backups

### On Windows

1. Open File Explorer
2. Type in the address bar: `%USERPROFILE%\.logseq\graphs`
3. Press Enter
4. Find your graph folder (named after your graph)
5. Open the `backups` folder

**Tip:** You may need to show hidden files/folders:

- View menu → Show → Hidden items

### On macOS

1. Open Finder
2. Press **CMD+Shift+G** (Go to Folder)
3. Type: `~/.logseq/graphs`
4. Press Enter
5. Find your graph folder
6. Open the `backups` folder

**Tip:** Hidden files/folders appear dimmed in Finder.

### Understanding Backup Names

Backup files are named with timestamps:

```text
2026-01-11T14_30_45.123Z.sqlite
└─┬─┘ └┬┘ └┬┘ └─┬──┘
  │    │   │     │
  │    │   │     └─ Milliseconds
  │    │   └─ Time (14:30:45)
  │    └─ Month and Day (Jan 11)
  └─ Year (2026)
```

**Reading the timestamp:**

- `2026-01-11` = January 11, 2026
- `14_30_45` = 2:30:45 PM (underscores replace colons)

---

## Restoring from a Backup

### ⚠️ Before You Start

**Important safety steps:**

1. **Close Logseq completely** - Don't skip this!
2. **Backup your current database** - Copy `db.sqlite` somewhere safe
3. **Know what you're doing** - Restoring replaces your current data

### Step-by-Step Restoration

#### Step 1: Close Logseq

Make sure Logseq is completely closed (not just minimized):

- **Windows:** Check system tray, right-click Logseq icon → Exit
- **macOS:** Quit Logseq (CMD+Q)

#### Step 2: Navigate to Your Graph Folder

- **Windows:** `%USERPROFILE%\.logseq\graphs\<YourGraph>\`
- **macOS:** `~/.logseq/graphs/<YourGraph>/`

You should see:

```text
db.sqlite          ← Your current database
db.sqlite-wal      ← May be present
db.sqlite-shm      ← May be present
backups/           ← Backup folder
```

#### Step 3: Backup the Current Database (Optional but Recommended)

Before restoring, save your current database:

1. Copy `db.sqlite` to your Desktop or Documents folder
2. Rename it to something like `db-backup-before-restore.sqlite`

#### Step 4: Choose a Backup to Restore

1. Open the `backups` folder
2. Find the backup you want (look at the timestamp)
3. Copy that file (e.g., `2026-01-11T10_30_45.123Z.sqlite`)

#### Step 5: Restore the Backup

1. Go back to the graph folder (where `db.sqlite` is)
2. Delete or rename the current `db.sqlite`
3. Paste the backup file you copied
4. Rename it to exactly: `db.sqlite`

#### Step 6: Clean Up (Optional)

If you see these files, delete them:

- `db.sqlite-wal`
- `db.sqlite-shm`

These are temporary files that will be recreated.

#### Step 7: Open Logseq

1. Launch Logseq
2. Open your graph
3. Check that your data looks correct

---

## Manual Backup Best Practices

While Logseq backs up automatically, you should also create your own backups for extra safety.

### Option 1: Export to SQLite File (Recommended)

**Best for:** Regular backups, sharing graphs

1. Open Logseq
2. Click the **⋯** menu (top-right)
3. Select **Export graph**
4. Choose **Export SQLite DB**
5. Save to a safe location (external drive, cloud storage)

**Advantages:**

- Single file, easy to manage
- Can be opened on any computer with Logseq
- Perfect for sharing or archiving

### Option 2: Scheduled Folder Backup (Advanced)

**Best for:** Automated external backups

1. Go to **Settings** → **Export**
2. Under "Schedule backup", click **Set backup folder first**
3. Choose a location (external drive, network drive)
4. Click **Backup now** to create initial backup
5. Backups will run automatically every hour

**Advantages:**

- Automatic (runs every hour)
- Stores outside Logseq folder
- Protected from disk failures

**Requirements:**

- Chromium-based browser (Chrome, Edge, Brave)
- File System Access API support

### Option 3: Copy the Entire Graph Folder

**Best for:** Complete backup with all history

1. Close Logseq
2. Navigate to `~/.logseq/graphs/`
3. Copy your entire graph folder
4. Paste to external drive or cloud storage

**Advantages:**

- Includes all 12 automatic backups
- Complete restoration possible
- Simple to understand

**Disadvantages:**

- Large file size (13× your database size)
- Must close Logseq first

---

## Disk Space Management

### How Much Space Do Backups Use?

Each backup is a complete copy of your database:

```text
Database size: 50 MB
× 12 backups
= 600 MB total for backups
+ 50 MB for current database
= 650 MB total
```

**Example sizes:**

- Small graph (10 MB): ~130 MB total
- Medium graph (50 MB): ~650 MB total
- Large graph (200 MB): ~2.6 GB total

### Checking Your Backup Space

**Find your graph size:**

1. Navigate to `~/.logseq/graphs/<YourGraph>/`
2. Right-click `db.sqlite` → Properties (Windows) / Get Info (macOS)
3. Look at the file size
4. Multiply by 13 for total space used

### What If I'm Running Out of Space?

### Option 1: Export and Clean

1. Export your graph (Settings → Export → Export SQLite DB)
2. Save the export somewhere safe
3. Clear the backups folder (Logseq will rebuild it)

**Option 2: Move to Larger Drive**
The `.logseq` folder can't be moved easily, but you can:

- Use external backups (scheduled backup feature)
- Export regularly to external drive
- Free up space on your main drive

**Option 3: Split Your Graphs**
If your graph is very large:

- Consider splitting into multiple smaller graphs
- Archive old content to separate graphs
- Keep active work in a smaller graph

---

## Troubleshooting

### Problem: Can't Find the .logseq Folder

**Windows:**

1. Open File Explorer
2. Click **View** menu
3. Check ☑️ **Hidden items**
4. Now you can see `.logseq`

**macOS:**

1. Open Finder
2. Press **CMD+Shift+.** (dot)
3. Hidden files now appear dimmed

### Problem: No Backups Folder

**Cause:** You've never saved changes to this graph

**Solution:**

1. Make any edit in your graph
2. Wait 30 seconds or press CMD+S
3. The `backups` folder will be created automatically

### Problem: Backup Folder Is Empty

**Possible causes:**

- This is a brand new graph (never saved)
- Less than 1 hour has passed since graph creation
- Someone manually deleted the backups

**Solution:**

- Make an edit and save
- Backups will start accumulating
- Future edits will be protected

### Problem: Restored Backup but Data Still Wrong

**Check:**

1. Did you close Logseq completely before restoring?
2. Did you rename the file to exactly `db.sqlite`?
3. Did you delete the `-wal` and `-shm` files?

**Try:**

1. Close Logseq again
2. Verify file name is correct
3. Delete `db.sqlite-wal` and `db.sqlite-shm` if present
4. Reopen Logseq

### Problem: "Graph Not Found" After Restore

**Cause:** File name is incorrect

**Solution:**

1. Close Logseq
2. Go to graph folder
3. Check that file is named exactly: `db.sqlite` (lowercase, no typo)
4. Not `db.sqlite.sqlite` or `DB.sqlite` or `db.SQLite`

### Problem: Backup Process Slowing Down My Computer

**Cause:** Large database takes time to copy

**Solutions:**

1. This is normal for large graphs (>100 MB)
2. Backups happen in background, shouldn't affect regular use
3. If severely impacted, consider splitting your graph

---

## Understanding Backup Limitations

### What Backups DON'T Protect Against

❌ **Hard drive failure** - Backups are on same drive as original  
❌ **Computer loss/theft** - Everything is on your computer  
❌ **Ransomware** - May encrypt backups too  
❌ **Accidental deletion of .logseq folder** - All backups gone

### What You Should Also Do

✅ **Cloud storage backup** - Dropbox, Google Drive, iCloud  
✅ **External drive backup** - USB drive, Time Machine, etc.  
✅ **Remote sync** - Logseq Sync (paid service)  
✅ **Version control** - Git (advanced users)

### Recommended Backup Strategy

**Daily workflow:**

- Let automatic backups work (no action needed)
- Make edits normally
- Logseq handles it

**Weekly:**

- Export to SQLite file
- Save to external drive or cloud storage

**Monthly:**

- Verify backups exist and are recent
- Check disk space
- Test a restore (optional but recommended)

**Before major changes:**

- Manually export (Settings → Export → Export SQLite DB)
- Keep the export until you're sure changes are good

---

## Advanced: Backup File Technical Details

### File Format

Backups are standard SQLite database files:

- Can be opened with SQLite tools
- Binary format (not human-readable)
- Complete snapshot of all your data

### Backup Frequency Rules

**Throttling:** Maximum 1 backup per hour

- If you save at 2:10 PM and 2:30 PM, only 2:10 PM is backed up
- Next backup can be created after 3:10 PM
- Prevents creating hundreds of backups

**Why hourly?**

- Balances protection vs. disk space
- Most users don't need minute-by-minute backups
- Can still recover recent work (auto-save protects last 30 seconds)

### Retention Algorithm

**Phase 1 - Hourly (newest 6 backups):**

- Keeps one backup per hour
- Example: 3 PM, 2 PM, 1 PM, 12 PM, 11 AM, 10 AM

**Phase 2 - Daily (older 6 backups):**

- Keeps one backup per day
- Example: Yesterday, 2 days ago, 3 days ago, etc.

**Why this strategy?**

- Recent changes: More granular (hourly)
- Older changes: Less granular (daily)
- Balances space vs. recovery options

---

## FAQs

### Q: Do backups work if Logseq crashes?

**A:** Yes! The current database is always saved before backup. If Logseq crashes:

- Your last auto-save (within 30 seconds) is in the database
- Previous backups remain intact
- You can restore if database is corrupted

### Q: Can I disable automatic backups?

**A:** No, backups are automatic and cannot be disabled. This is by design to protect your data.

### Q: Do backups sync with Logseq Sync?

**A:** No, backups are local only. Logseq Sync provides separate cloud backup. The two systems work independently.

### Q: Can I change how many backups are kept?

**A:** Not currently. The 12-backup limit is fixed (6 hourly + 6 daily).

### Q: What happens if my backup folder grows too large?

**A:** Backups are automatically deleted when they exceed the retention policy. The folder will never grow beyond 12 copies.

### Q: Can I restore a backup on a different computer?

**A:** Yes! Copy the backup file to the new computer:

1. Install Logseq on new computer
2. Place backup file in appropriate `.logseq/graphs/<name>/` folder
3. Rename to `db.sqlite`
4. Open Logseq

### Q: Are backups encrypted?

**A:** Only if you use full-disk encryption (BitLocker, FileVault). Logseq backups inherit your system's encryption settings.

### Q: Can I access backups if I forgot my password?

**A:** Backups don't have separate passwords. If your graph is encrypted, you need the encryption password to access both database and backups.

---

## Quick Reference

### Backup Locations

| OS          | Location                                          |
| ----------- | ------------------------------------------------- |
| **Windows** | `C:\Users\<Name>\.logseq\graphs\<Graph>\backups\` |
| **macOS**   | `/Users/<Name>/.logseq/graphs/<Graph>/backups/`   |

### Common Tasks

| Task               | Steps                                              |
| ------------------ | -------------------------------------------------- |
| **View backups**   | Navigate to backups folder using paths above       |
| **Export graph**   | ⋯ menu → Export graph → Export SQLite DB           |
| **Restore backup** | Close Logseq → Copy backup → Rename to `db.sqlite` |
| **Manual save**    | CMD+S (macOS) or Ctrl+S (Windows)                  |
| **Check space**    | Right-click `db.sqlite` → Properties/Get Info      |

### When to Restore

| Situation               | Action                                    |
| ----------------------- | ----------------------------------------- |
| **Accidental deletion** | Restore most recent backup                |
| **Data corruption**     | Restore last known good backup            |
| **Bad import**          | Restore backup from before import         |
| **Testing restore**     | Practice quarterly to ensure you know how |

---

## Getting Help

### If You Need Assistance

1. **Logseq Discord** - Community support, #help channel
2. **Logseq Forum** - Post detailed questions with screenshots
3. **GitHub Issues** - Report bugs or problems

### What to Include When Asking for Help

- Operating system and version
- Logseq version (Help → About)
- What you were trying to do
- What happened instead
- Screenshot of error message (if any)
- Graph size and number of backups

### Before Asking for Restoration Help

**⚠️ DO NOT DELETE YOUR BACKUPS!**

If your database is corrupted:

1. Don't panic
2. Don't delete anything
3. Don't try multiple restore attempts
4. Take screenshots
5. Ask for help FIRST

---

## Summary

**Key Takeaways:**

✅ Backups happen automatically - you don't need to do anything  
✅ 12 backups are kept (6 hourly, 6 daily)  
✅ Backups are stored in `.logseq/graphs/<name>/backups/`  
✅ You can restore by copying a backup file over `db.sqlite`  
✅ Also create external backups (export weekly)  
✅ Backups protect against corruption and mistakes  
✅ They DON'T protect against drive failure or computer loss

**Remember:** Backups are your safety net. Understanding how they work helps you sleep better at night!

---

## Related Guides

- [Export Guide](../03-features/database-graphs/export-guide.md) - How to export your graph
- [Database Graphs Overview](../03-features/database-graphs/README.md) - Understanding database mode
- [CLI User Guide](./cli-user-guide.md) - Command-line export and backup tools

---

**Last Updated:** January 11, 2026  
**Version:** 1.0  
**Investigation Source:** [Database Backup Investigation](../07-investigations/database-backup-electron.md)
