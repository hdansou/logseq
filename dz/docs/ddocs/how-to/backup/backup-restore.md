---
title: 'Backup and Restore'
description: 'How to back up and restore your Logseq database graphs'
status: stable
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: merged-not-fully-audited
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

> **Last revised:** 2026-09-09 (verified against `be800f1711`)

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

| Platform    | Path                                               |
| ----------- | -------------------------------------------------- |
| **macOS**   | `~/logseq/graphs/<GraphDirName>/backup/`           |
| **Windows** | `%USERPROFILE%\logseq\graphs\<GraphDirName>\backup\` |
| **Linux**   | `~/logseq/graphs/<GraphDirName>/backup/`           |

> **Corrected:** DB graphs live under `~/logseq/graphs`, not `~/.logseq/graphs`. The default is `"~/logseq/graphs"` ([config.cljs:55](../../deps/common/src/logseq/common/config.cljs#L55)), expanded from `$HOME` and overridable with the `LOGSEQ_GRAPHS_DIR` environment variable ([graph.cljs:111-126](../../deps/common/src/logseq/common/graph.cljs#L111-L126)). `~/.logseq` is a different directory, used for plugins and app-level config.

`<GraphDirName>` is the graph name URI-encoded, not the graph name verbatim: `encodeURIComponent` is applied, then `%20` is restored to a space and the remaining `%` escapes become `~` ([graph_dir.cljs:6-12](../../deps/common/src/logseq/common/graph_dir.cljs#L6-L12)). A graph called `my/graph` therefore lives in a directory named `my~2Fgraph`.

### Navigate to Backups

**macOS:**

1. Open Finder
2. Press `Cmd+Shift+G`
3. Type: `~/logseq/graphs`
4. Open your graph folder → `backup`

**Windows:**

1. Open File Explorer
2. Type in address bar: `%USERPROFILE%\logseq\graphs`
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
~/logseq/graphs/<YourGraph>/
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
logseq graph backup restore --graph my-graph --src my-graph-nightly-20260101T000000Z --dst my-graph-restore
logseq graph backup remove --graph my-graph --src my-graph-nightly-20260101T000000Z
```

`--src` takes a backup **name** as reported by `graph backup list` (JSON output: `data.backups[].name`), and `--dst` is the name of the new graph the backup is restored into. `restore` and `remove` also take `--graph` to say which graph's backup tree to look in. Verified against the CLI end-to-end lifecycle case in [cli-e2e/spec/non_sync_cases.edn:182-198](../../cli-e2e/spec/non_sync_cases.edn#L182-L198).

## Backup Metadata

Each `metadata.edn` file currently stores:

- `:schema-version`
- `:name`
- `:repo`
- `:source`
- `:created-at-ms`
- `:db-path`

## Notes

- `list-backups` returns `:name`, `:created-at`, `:size-bytes`, and `:source` when metadata is present ([graph_backup.cljs:161-167](../../src/main/logseq/db_worker/graph_backup.cljs#L161-L167)). `:created-at` comes from the backup `db.sqlite`'s mtime, while retention and throttling sort on `:created-at-ms` from `metadata.edn`.
- Backup directory names are deduplicated rather than overwritten: if `<graph>-<timestamp>` already exists, a `-1`, `-2`, ... suffix is appended ([graph_backup.cljs:192-211](../../src/main/logseq/db_worker/graph_backup.cljs#L192-L211)).
- A failed snapshot leaves nothing behind. The snapshot is written to a temp file inside the reserved directory and renamed into place only on success; on failure the temp file and the empty directory are removed ([graph_backup.cljs:213-219, 260-288](../../src/main/logseq/db_worker/graph_backup.cljs#L260-L288)).
- The browser/web build has a separate, unrelated "auto backup" feature that asks you to pick a folder and writes timestamped copies into a `backups` subdirectory of that handle ([export.cljs:229-267](../../src/main/frontend/handler/export.cljs#L229-L267)). Do not confuse that `backups/` directory with the desktop/CLI `backup/` tree described here.

> **Removed:** The old CLI export examples are not part of this backup/restore doc's current command surface.
> **Removed:** The claim that "the desktop backup-folder shortcut points at the graph's `backup/` directory" -- there is no such shortcut in the desktop UI. The only folder-picker in the source is the web-only `choose-backup-folder` flow above.

---

## Manual Backup Methods

### Method 1: Export SQLite DB (recommended)

Best for: a one-off copy you keep somewhere else.

1. Open the **⋯** menu in the top-right of the app
2. Choose **Export graph** ([header.cljs:227-230](../../src/main/frontend/components/header.cljs#L227-L230))
3. Click **Export SQLite DB** ([export.cljs:126-129](../../src/main/frontend/components/export.cljs#L126-L129))
4. Save the file to an external drive or cloud folder

The same dialog also offers **Export both SQLite DB and assets**, which produces a `.zip`. Prefer it if your graph has assets -- the plain SQLite export does not include them ([en.edn:666, 678-680](../../src/resources/dicts/en.edn#L666)).

### Method 2: CLI backup

Best for: automation and scheduled backups. This uses the same `backup/` tree and metadata as the desktop automatic backups.

```bash
logseq graph backup create --graph my-graph --name nightly
```

Schedule it with cron, for example daily at 6 PM:

```
0 18 * * * logseq graph backup create --graph my-graph --name nightly
```

Because CLI backups are recorded with their own `:source`, they are never pruned by the desktop's 12-backup `:electron-auto` retention. Prune them yourself with `logseq graph backup remove`.

### Method 3: Copy the database file

Best for: a quick local copy. **Close Logseq first** -- copying `db.sqlite` while the app is running can capture a database whose latest transactions are still in the `-wal` sidecar file.

```bash
# macOS/Linux
cp ~/logseq/graphs/my-graph/db.sqlite ~/backups/my-graph-$(date +%Y%m%d).sqlite
```

```powershell
# Windows (PowerShell)
Copy-Item "$env:USERPROFILE\logseq\graphs\my-graph\db.sqlite" "C:\backups\my-graph-$(Get-Date -Format yyyyMMdd).sqlite"
```

---

## Troubleshooting

### "Database is locked"

Close all Logseq instances before copying `db.sqlite`.

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
5. **Keep an assets-inclusive export too** - the `.zip` export carries assets that a bare `.sqlite` file does not

> **Removed:** "Markdown export is human-readable." The DB export dialog offers SQLite, SQLite+assets `.zip`, and EDN -- there is no Markdown option, and the app itself says of the EDN export: "Don't rely on this as a primary backup" ([en.edn:632-635, 661-666](../../src/resources/dicts/en.edn#L632-L635)).

---

## See Also

- **CLI Integration**: [cli-basic.md](../integrations/cli-basic.md)
- **Graph Types**: [file-vs-db.md](../../explanation/concepts/file-vs-db.md)

---

_Last updated: 2026-09-09_
