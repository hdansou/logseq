---
title: 'Backup and Restore'
description: 'How to back up and restore your Logseq database graphs'
status: stable
lastVerified: '2026-05-30'
logseqVersion: '0.11.x'
platforms: [desktop, cli]
graphType: [db]
relatedDocs:
  explanation:
    - '[File vs DB Graphs](../../explanation/concepts/file-vs-db.md)'
  howTo:
    - '[CLI Basics](../integrations/cli-basic.md)'
---

# Backup and Restore

> **Last revised:** 2026-05-30

How to protect your Logseq database graphs with backups and restore them when needed.

## How Backups Work

Logseq writes graph backups through the shared db-worker SQLite backup flow. Desktop and CLI use the same on-disk layout.

### When Backups Are Created

* Desktop automatic backups run hourly for active graphs.
* Desktop forced backups use the same graph-backup pipeline and can bypass the hourly throttle.
* CLI `graph backup create` creates a backup on demand.
* All of these paths store a SQLite snapshot in the shared `backup/` tree.

> **Removed:** The old "every 30 seconds if there are changes" behavior no longer matches current source.
> **Removed:** Manual save is not the trigger for this backup flow anymore.

### Retention

* Desktop automatic retention keeps the latest 12 `:electron-auto` backups.
* CLI-created backups are stored in the same tree but are not pruned by Desktop automatic retention.
* Manual desktop backups are also kept out of the automatic-retention bucket.

> **Removed:** The old 6 hourly + 6 daily retention split does not match current source behavior.

---

## Find Your Backups

### Backup Location

| Platform    | Path                                                |
| ----------- | --------------------------------------------------- |
| **macOS**   | `~/.logseq/graphs/<GraphName>/backup/`             |
| **Windows** | `%USERPROFILE%\.logseq\graphs\<GraphName>\backup\` |
| **Linux**   | `~/.logseq/graphs/<GraphName>/backup/`             |

### Navigate to Backups

**macOS:**

1. Open Finder
2. Press `Cmd+Shift+G`
3. Type: `~/.logseq/graphs`
4. Open your graph folder → `backup`

**Windows:**

1. Open File Explorer
2. Type in address bar: `%USERPROFILE%\.logseq\graphs`
3. Open your graph folder → `backup`

### Understand Backup Names

```
demo-nightly-20260101T000000Z
│    │       └── UTC timestamp
│    └── Optional label
└── Graph name
```

Backups are stored as directories, not flat `.sqlite` files. Each backup directory contains `db.sqlite` and `metadata.edn`.

> **Removed:** The old flat `backups/` file list is not the current graph-backup layout.

---

## Restore from Backup

### ⚠️ Before You Start

1. **Close Logseq completely**.
2. **Choose the backup** you want to restore.
3. **Copy the current `db.sqlite` somewhere safe** before replacing it.

### Step-by-Step

#### 1. Close Logseq

- **macOS**: Cmd+Q.
- **Windows**: Exit Logseq from the system tray if it is running there.

#### 2. Go to Graph Folder

Navigate to:

```
~/.logseq/graphs/<YourGraph>/
```

You'll see:

```
db.sqlite          ← Current database
db.sqlite-wal      ← May exist
db.sqlite-shm      ← May exist
backup/            ← Backup folder
```

#### 3. Backup Current Database (Safety)

Copy `db.sqlite` to your Desktop:

```bash
cp db.sqlite ~/Desktop/db-before-restore.sqlite
```

#### 4. Choose Backup to Restore

Open `backup/` and find the backup by name or timestamp.

#### 5. Restore

```bash
# Remove current database
rm db.sqlite db.sqlite-wal db.sqlite-shm 2>/dev/null

# Copy backup database as the new live database
cp backup/demo-nightly-20260101T000000Z/db.sqlite db.sqlite
```

#### 6. Open Logseq

Launch Logseq and verify your data.

> **Removed:** The old restore instructions that referenced a flat `backups/` file list no longer match the current graph-backup layout.

---

## Findings: New Capabilities

- Backup metadata now lives alongside each backup in `metadata.edn`.
- Desktop automatic backups and CLI-created backups share the same `backup/` layout.
- Backup retention is source-aware, so Desktop automatic pruning only touches `:electron-auto` backups.
- Restore now uses the shared SQLite import flow after the backup file is located.

## CLI Backup Commands

The current source supports these graph backup commands:

- `logseq graph backup list`
- `logseq graph backup create`
- `logseq graph backup restore`
- `logseq graph backup remove`

Useful examples:

```bash
logseq graph backup list --graph my-graph
logseq graph backup create --graph my-graph --name nightly
logseq graph backup restore --src my-graph-nightly --dst my-graph-restore
logseq graph backup remove --src my-graph-nightly
```

## Backup Metadata

Each `metadata.edn` file currently stores:

- `:schema-version`
- `:name`
- `:repo`
- `:source`
- `:created-at-ms`
- `:db-path`

## Notes

- `list-backups` returns `:name`, `:created-at`, `:size-bytes`, and `:source` when metadata is present.
- The desktop backup-folder shortcut points at the graph's `backup/` directory.

> **Removed:** The old CLI export examples are not part of this backup/restore doc's current command surface.

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
