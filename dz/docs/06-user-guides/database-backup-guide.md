# Database Graph Backup Guide

**For:** Logseq Desktop Users (Windows & macOS)  
**Graph Type:** Database Graphs

> **Last revised:** 2026-05-29

---

## What Are Database Backups?

Logseq Desktop creates backup snapshots of database graphs to protect your data. Each graph backup is stored as a directory that contains a SQLite copy of the graph plus metadata about when and how it was created.
Desktop and CLI graph backups use the same on-disk `backup/` layout.

### Why Backups Matter

- 🛡️ **Protect against data corruption** - If your database gets damaged, you can restore from a backup
- 🔄 **Recover from mistakes** - Accidentally deleted important notes? Restore an older version
- 💾 **Peace of mind** - Your work is automatically saved in multiple places

---

## How Backups Work

### Automatic Backups

Logseq Desktop creates hourly automatic backups for active graphs. The backup is produced through the db-worker-backed SQLite backup flow, so the live database and the backup snapshot are separate files.

**When a backup can be created:**

- ✅ Every hour for active Desktop graphs
- ✅ When you use the save shortcut and Logseq forces a backup
- ✅ Through the CLI `graph backup` commands, which share the same storage layout

**How many backups are kept:**

- 12 automatic Desktop backups per source
- Manual and CLI backups are stored separately from the automatic-retention bucket

> **Removed:** Backups are not created on every manual save, every 30 seconds, or when you close Logseq.

> **Removed:** The old 6-hourly + 6-daily retention split does not match the current graph backup code.

### Where Backups Are Stored

**Backup Location:**

- **Windows:** `C:\Users\<YourName>\.logseq\graphs\<YourGraph>\backup\`
- **macOS:** `/Users/<YourName>/.logseq/graphs/<YourGraph>/backup/`

Each listable backup lives in its own directory:

```text
~/.logseq/graphs/<encoded-graph-dir>/
├── db.sqlite
└── backup/
    └── <backup-name>/
        ├── db.sqlite
        └── metadata.edn
```

**Important:**

- `backup/` is the graph-backup root
- `backups/` is only used by the separate emergency write-failure backup helper
- Do not edit `db.sqlite` or `metadata.edn` by hand unless you are restoring on purpose

---

## Findings: New Capabilities

- Graph backups now store `metadata.edn` alongside `db.sqlite`, so the backup system can track source, creation time, and path metadata.
- Desktop automatic backups and CLI-created backups share the same `backup/` layout.
- The CLI can list, create, restore, and remove backups from the same storage tree.
- Source-scoped retention means automatic backups can be pruned without removing CLI or manual backups.

---

## Viewing Your Backups

### On Windows

1. Open File Explorer
2. Type in the address bar: `%USERPROFILE%\.logseq\graphs`
3. Press Enter
4. Find your graph folder (named after your graph)
5. Open the `backup` folder

**Tip:** You may need to show hidden files/folders:

- View menu → Show → Hidden items

### On macOS

1. Open Finder
2. Press **CMD+Shift+G** (Go to Folder)
3. Type: `~/.logseq/graphs`
4. Press Enter
5. Find your graph folder
6. Open the `backup` folder

**Tip:** Hidden files/folders appear dimmed in Finder.

### Understanding Backup Names

Backup directories are named automatically and encoded for filesystem safety.

Examples:

```text
my-graph-20260529T143045Z
my-graph-nightly-20260529T143045Z
```

- The timestamp is UTC
- Optional labels may appear in CLI-created backups
- The actual SQLite file inside the directory is always `db.sqlite`

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
backup/            ← Backup folder
```

#### Step 3: Backup the Current Database (Optional but Recommended)

Before restoring, save your current database:

1. Copy `db.sqlite` to your Desktop or Documents folder
2. Rename it to something like `db-backup-before-restore.sqlite`

#### Step 4: Choose a Backup to Restore

1. Open the `backup` folder
2. Find the backup you want (look at the timestamp or label)
3. Copy that backup directory's `db.sqlite` file

#### Step 5: Restore the Backup

1. Go back to the graph folder (where `db.sqlite` is)
2. Delete or rename the current `db.sqlite`
3. Paste the backup file you copied from `backup/<backup-name>/db.sqlite`
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

> **Removed:** The old instructions that referenced restoring from a flat `backups/` file list no longer match the current graph backup layout.

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

> **Removed:** The old "Schedule backup" folder workflow is not present in the current source.

### Option 2: Copy the Entire Graph Folder

**Best for:** Complete backup with all history

1. Go to **Settings** → **Export**
2. Close Logseq
3. Navigate to `~/.logseq/graphs/<YourGraph>/`
4. Copy your entire graph folder to an external drive or cloud storage

**Advantages:**

- Includes the live `db.sqlite` plus the `backup/` directory
- Simple to understand and restore
- Keeps every backup you copied

---

## Disk Space Management

### How Much Space Do Backups Use?

Each automatic Desktop backup is a complete copy of your database:

```text
Database size: 50 MB
× 12 automatic backups
= 600 MB total for automatic backups
+ 50 MB for current database
= 650 MB total
```

**Example sizes:**

- Small graph (10 MB): ~130 MB total
- Medium graph (50 MB): ~650 MB total
- Large graph (200 MB): ~2.6 GB total

Manual and CLI-created backups add to that total until you remove them.

### Checking Your Backup Space

**Find your graph size:**

1. Navigate to `~/.logseq/graphs/<YourGraph>/`
2. Right-click `db.sqlite` → Properties (Windows) / Get Info (macOS)
3. Look at the file size
4. Multiply by 13 for a rough automatic-backup estimate

**Note:** manual backups you create are additional to that estimate.

**Note:** CLI-created backups are additional to that estimate too.

### What If I'm Running Out of Space?

### Option 1: Export and Clean

1. Export your graph (Settings → Export → Export SQLite DB)
2. Save the export somewhere safe
3. Remove old backup directories you no longer need

**Option 2: Move to Larger Drive**
The `.logseq` folder can't be moved easily, but you can:

- Keep exports on an external drive
- Copy the graph folder to external storage
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

### Problem: No Backup Directory

**Cause:** No backup has been created yet

**Solution:**

1. Make any edit in your graph
2. Press the save shortcut or wait for the next hourly auto-backup
3. The `backup` folder will be created automatically

### Problem: Backup Folder Is Empty

**Possible causes:**

- This is a brand new graph (never saved)
- No hourly auto-backup has run yet
- No manual save backup or CLI backup has been created yet
- Someone manually deleted the backup directories

**Solution:**

- Make an edit and save
- Wait for the next hourly backup if you only rely on automatic backups
- Backups will start accumulating once the first backup completes

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
2. Backups happen in the background and should not block normal editing for long
3. If severely impacted, consider splitting your graph

---

## Understanding Backup Limitations

### What Backups DON'T Protect Against

❌ **Hard drive failure** - Backups are usually on the same drive as the original graph
❌ **Computer loss/theft** - Everything is on your computer  
❌ **Ransomware** - May encrypt backups too  
❌ **Accidental deletion of `.logseq` folder** - All local backups are gone

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

> **Removed:** The old hourly/daily bucket retention description does not apply to the current graph backup code.

---

## Advanced: Backup File Technical Details

### File Format

Backups are standard SQLite database files:

- Can be opened with SQLite tools
- Binary format (not human-readable)
- Complete snapshot of all your data

### Backup Frequency Rules

**Throttling:** Maximum 1 automatic backup per hour

- If an automatic backup was created at 2:10 PM, the next automatic backup can be skipped until 3:10 PM
- Manual forced backups can bypass the hourly throttle
- This prevents creating hundreds of automatic backups

**Why hourly?**

- Balances protection vs. disk space
- Most users don't need minute-by-minute backups
- Manual saves can still force a backup when needed

### Retention Algorithm

**Automatic source retention:**

- Keeps the newest 12 automatic backups
- Retention is source-scoped, so automatic and manual backups are tracked separately

> **Removed:** The old hourly-plus-daily dedupe strategy no longer applies to graph backups.

---

## FAQs

### Q: Do backups work if Logseq crashes?

**A:** Yes. The backup system writes a separate SQLite snapshot, so a crash does not overwrite your most recent backup. If Logseq crashes:

- Your live database remains separate from the backup snapshots
- Previous backups remain intact
- You can restore if the current database is corrupted

### Q: Can I disable automatic backups?

**A:** There is no user-facing setting in current source to turn off the hourly Desktop auto-backup timer.

### Q: Do backups sync with Logseq Sync?

**A:** No. Backups are local snapshots. Logseq Sync is a separate cloud feature.

### Q: Can I change how many backups are kept?

**A:** Not from the UI. Desktop automatic backups keep 12 snapshots by default.

### Q: What happens if my backup folder grows too large?

**A:** Automatic backups are pruned to the retention limit, but any manual backups you create add to the total until you remove them.

### Q: Can I restore a backup on a different computer?

**A:** Yes. Copy the backup directory or an exported SQLite file to the new computer:

1. Install Logseq on new computer
2. Copy the graph backup directory into `.logseq/graphs/<name>/backup/`
3. Or use an exported SQLite file and restore it as `db.sqlite`
4. Open Logseq

### Q: Are backups encrypted?

**A:** Only if you use full-disk encryption (BitLocker, FileVault). Logseq backups inherit your system's encryption settings.

### Q: Can I access backups if I forgot my password?

**A:** Backups don't have separate passwords. If your graph is encrypted, you need the encryption password to access both database and backups.

---

## Quick Reference

### Backup Locations

| OS          | Location                                         |
| ----------- | ------------------------------------------------ |
| **Windows** | `C:\Users\<Name>\.logseq\graphs\<Graph>\backup\` |
| **macOS**   | `/Users/<Name>/.logseq/graphs/<Graph>/backup/`   |

### Common Tasks

| Task               | Steps                                                                 |
| ------------------ | --------------------------------------------------------------------- |
| **View backups**   | Navigate to the `backup` folder using paths above                     |
| **Export graph**   | ⋯ menu → Export graph → Export SQLite DB                              |
| **Restore backup** | Close Logseq → Copy `backup/<name>/db.sqlite` → Rename to `db.sqlite` |
| **Manual save**    | Save shortcut, or use the menu action that forces a backup            |
| **Check space**    | Right-click `db.sqlite` in a backup directory → Properties/Get Info   |

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

✅ Desktop graph backups run hourly for active graphs
✅ 12 automatic backups are retained by default
✅ Backups are stored in `.logseq/graphs/<name>/backup/`
✅ Restore by copying `backup/<name>/db.sqlite` over the live `db.sqlite`
✅ Export SQLite files or copy the whole graph folder for extra safety
✅ Backups protect against corruption and mistakes  
✅ They don't protect against drive failure or computer loss

**Remember:** Backups are your safety net. Understanding how they work helps you sleep better at night!

---

## Related Guides

- [Export Guide](../03-features/database-graphs/export-guide.md) - How to export your graph
- [Database Graphs Overview](../03-features/database-graphs/README.md) - Understanding database mode
- [CLI User Guide](./cli-user-guide.md) - Command-line export and backup tools

---

**Last revised:** 2026-05-29
**Version:** 1.0  
**Investigation Source:** [Database Backup Investigation](../07-investigations/database-backup-electron.md)
