---
title: 'Backup and Restore'
description: 'How to backup and restore your Logseq database graphs'
status: stable
lastVerified: '2026-03-10'
logseqVersion: '0.11.x'
platforms: [desktop]
graphType: [db]
relatedDocs:
  explanation:
    - '[File vs DB Graphs](../../explanation/concepts/file-vs-db.md)'
  howTo:
    - '[CLI Basics](../integrations/cli-basic.md)'
---

# Backup and Restore

> **Last revised:** 2026-03-10

> How to protect your Logseq database graphs with backups and restore when needed.

## How Automatic Backups Work

Logseq automatically creates backup copies of your database graphs. No action required.

### When Backups Are Created

Logseq persists your database to disk every 30 seconds (if there are changes) and on manual save (Cmd+S / Ctrl+S). Each persist creates a backup of the previous database file, but a **minimum 1-hour interval** is enforced between backups to avoid excessive copies.

- A backup is created at most **once per hour** during normal use
- The backup captures the state _before_ the latest save, not after

### How Many Backups Are Kept

- **12 total backups**
  - 6 recent (one per hour)
  - 6 older (one per day)

---

## Find Your Backups

### Backup Location

| Platform    | Path                                                |
| ----------- | --------------------------------------------------- |
| **macOS**   | `~/logseq/graphs/<GraphName>/backups/`             |
| **Windows** | `%USERPROFILE%\logseq\graphs\<GraphName>\backups\` |
| **Linux**   | `~/logseq/graphs/<GraphName>/backups/`             |

### Navigate to Backups

**macOS:**

1. Open Finder
2. Press `Cmd+Shift+G`
3. Type: `~/logseq/graphs`
4. Open your graph folder → `backups`

**Windows:**

1. Open File Explorer
2. Type in address bar: `%USERPROFILE%\logseq\graphs`
3. Open your graph folder → `backups`

### Understand Backup Names

```
2026-01-29T14_30_45.123Z.sqlite
│         │         │
│         │         └── Milliseconds
│         └── Time (14:30:45 = 2:30 PM)
└── Date (Jan 29, 2026)
```

---

## Restore from Backup

### ⚠️ Before You Start

1. **Close Logseq completely** (not just minimized)
2. **Save current database** as backup before restoring
3. **Know which backup** you want to restore

### Step-by-Step

#### 1. Close Logseq

- **macOS**: Cmd+Q (check Dock for icon)
- **Windows**: Check system tray → Exit

#### 2. Go to Graph Folder

Navigate to:

```
~/logseq/graphs/<YourGraph>/
```

You'll see:

```
db.sqlite          ← Current database
db.sqlite-wal      ← May exist
db.sqlite-shm      ← May exist
backups/           ← Backup folder
```

#### 3. Backup Current Database (Safety)

Copy `db.sqlite` to your Desktop:

```bash
cp db.sqlite ~/Desktop/db-before-restore.sqlite
```

#### 4. Choose Backup to Restore

Open `backups/` folder and find the backup by timestamp.

#### 5. Restore

```bash
# Remove current database
rm db.sqlite db.sqlite-wal db.sqlite-shm 2>/dev/null

# Copy backup as new database
cp backups/2026-01-29T10_30_45.123Z.sqlite db.sqlite
```

#### 6. Open Logseq

Launch Logseq and verify your data.

---

## Manual Backup Methods

### Method 1: Export SQLite (Recommended)

Best for: Regular backups, safe keeping

1. Open Logseq
2. Click **⋯** menu (top-right)
3. Select **Export graph**
4. Choose **Export SQLite DB**
5. Save to external drive or cloud

### Method 2: CLI Export

Best for: Automation, scheduled backups

```bash
# Export to Markdown (produces a .zip file)
logseq export -g my-graph -f ~/backups/my-graph.zip

# Export to EDN (separate command)
logseq export-edn -g my-graph -f ~/backups/my-graph.edn
```

### Method 3: Copy Database File

Best for: Quick local backup

```bash
# macOS/Linux
cp ~/logseq/graphs/my-graph/db.sqlite ~/backups/my-graph-$(date +%Y%m%d).sqlite

# Windows (PowerShell)
Copy-Item "$env:USERPROFILE\logseq\graphs\my-graph\db.sqlite" "C:\backups\my-graph-$(Get-Date -Format yyyyMMdd).sqlite"
```

---

## Automated Backup Script

### macOS/Linux

Create `~/scripts/backup-logseq.sh`:

```bash
#!/bin/bash
GRAPH_NAME="my-graph"
BACKUP_DIR="$HOME/Dropbox/logseq-backups"
DATE=$(date +%Y-%m-%d_%H%M)

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Copy database
cp ~/logseq/graphs/$GRAPH_NAME/db.sqlite "$BACKUP_DIR/$GRAPH_NAME-$DATE.sqlite"

# Keep only last 30 backups
ls -t "$BACKUP_DIR"/*.sqlite | tail -n +31 | xargs rm -f 2>/dev/null

echo "Backup created: $GRAPH_NAME-$DATE.sqlite"
```

Schedule with cron:

```bash
# Run daily at 6 PM
0 18 * * * ~/scripts/backup-logseq.sh
```

### Windows (PowerShell)

Create `backup-logseq.ps1`:

```powershell
$GraphName = "my-graph"
$BackupDir = "$env:USERPROFILE\OneDrive\logseq-backups"
$Date = Get-Date -Format "yyyy-MM-dd_HHmm"

# Create backup directory
New-Item -ItemType Directory -Force -Path $BackupDir

# Copy database
Copy-Item "$env:USERPROFILE\logseq\graphs\$GraphName\db.sqlite" "$BackupDir\$GraphName-$Date.sqlite"

Write-Host "Backup created: $GraphName-$Date.sqlite"
```

Schedule with Task Scheduler.

---

## Cloud Backup Strategies

### Option 1: Sync Backup Copies (Recommended)

Sync your _backup copies_ (not the live database) to a cloud folder. Copy database files into a synced directory using the automated backup script above, pointing `BACKUP_DIR` to your cloud-synced folder.

> **Warning:** Do not symlink or sync the live graph folder (containing `db.sqlite`) with cloud sync services like Dropbox, iCloud, or OneDrive. SQLite databases can become corrupted when multiple sync clients modify the same file. Instead, sync backup copies only.

### Option 2: Backup to Cloud CLI

```bash
# AWS S3
aws s3 cp ~/logseq/graphs/my-graph/db.sqlite s3://my-bucket/logseq/

# Google Cloud Storage
gsutil cp ~/logseq/graphs/my-graph/db.sqlite gs://my-bucket/logseq/

# Backblaze B2
b2 upload-file my-bucket ~/logseq/graphs/my-graph/db.sqlite logseq/db.sqlite
```

---

## Troubleshooting

### "Database is locked"

Close all Logseq instances before copying db.sqlite.

```bash
# Check for running processes
ps aux | grep -i logseq

# Force close (macOS)
killall "Logseq"
```

### "Backup file is empty"

The backup may be corrupted. Try another backup file.

### "Graph won't open after restore"

Delete temporary files:

```bash
rm db.sqlite-wal db.sqlite-shm
```

### "Data is missing after restore"

You may have restored an older backup. Try a more recent one.

---

## Best Practices

1. **Test restores regularly** - Don't wait for emergency
2. **Multiple backup locations** - Local + Cloud
3. **Version naming** - Include date in filename
4. **Document your process** - Know your backup schedule
5. **Keep exports too** - Markdown export is human-readable

---

## See Also

- **CLI Integration**: [cli-basic.md](../integrations/cli-basic.md)
- **Graph Types**: [file-vs-db.md](../../explanation/concepts/file-vs-db.md)

---

_Last updated: 2026-03-10_
