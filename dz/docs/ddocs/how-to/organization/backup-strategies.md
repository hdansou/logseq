---
title: 'Backup and Recovery Strategies'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'How-To'
platform: 'Desktop + Mobile'
related-docs:
  - '[Logseq Sync Setup](../collaboration/logseq-sync.md)'
  - '[Git Sync](../collaboration/git-sync.md)'
  - '[Export Graph](./export-graph.md)'
tags: ['backup', 'recovery', 'sync', 'export', 'data-safety']
---

# Backup and Recovery Strategies

This guide shows you how to protect your Logseq data with effective backup strategies. Learn multiple backup methods, automation techniques, and recovery procedures to ensure your notes are always safe.

**What You'll Learn:**

- Backup methods for file-based and database graphs
- Automated backup workflows
- Cloud sync strategies
- Recovery procedures
- Disaster recovery planning

**Time Required:** 30-45 minutes for full setup

---

## Table of Contents

1. [Understanding Backup Needs](#understanding-backup-needs)
2. [File-Based Graph Backups](#file-based-graph-backups)
3. [Database Graph Backups](#database-graph-backups)
4. [Automated Backup Workflows](#automated-backup-workflows)
5. [Recovery Procedures](#recovery-procedures)
6. [Best Practices](#best-practices)

---

## Understanding Backup Needs

### Backup Strategy Matrix

| Graph Type       | Primary Backup | Secondary Backup | Frequency         |
| ---------------- | -------------- | ---------------- | ----------------- |
| **File-Based**   | Git sync       | Manual export    | Every commit      |
| **Database**     | Logseq Sync    | EDN export       | Real-time / Daily |
| **Desktop Only** | Local backup   | Cloud backup     | Daily             |
| **Mobile**       | Logseq Sync    | N/A              | Real-time         |

### What Needs Backing Up?

**File-Based Graphs:**

- ✅ Markdown/Org files (all your notes)
- ✅ Assets folder (images, PDFs, attachments)
- ✅ `logseq/` config folder (settings, queries, custom.css)
- ❌ `logseq/.recycle/` (can be excluded)

**Database Graphs:**

- ✅ SQLite database file (`<graph-name>.db`)
- ✅ Transit export file (`.edn` backup)
- ✅ Assets folder
- ✅ Config (part of database)

---

## File-Based Graph Backups

### Method 1: Git Sync (Recommended)

**Best For:** Version control, collaboration, desktop users

**Setup:**

```bash
# Navigate to your graph folder
cd ~/Documents/LogseqGraph

# Initialize Git repository
git init

# Create .gitignore
cat > .gitignore << EOF
.DS_Store
logseq/.recycle/
logseq/bak/
node_modules/
EOF

# Initial commit
git add .
git commit -m "Initial commit"

# Add remote (GitHub, GitLab, etc.)
git remote add origin https://github.com/username/logseq-graph.git
git push -u origin main
```

**Daily Workflow:**

```bash
# Pull changes (if using multiple devices)
git pull

# Add all changes
git add .

# Commit with timestamp
git commit -m "Update $(date '+%Y-%m-%d %H:%M')"

# Push to remote
git push
```

**Automate with Script:**

```bash
#!/bin/bash
# save as: ~/scripts/logseq-backup.sh

GRAPH_DIR="$HOME/Documents/LogseqGraph"
cd "$GRAPH_DIR"

# Pull first
git pull --rebase

# Add and commit
git add .
if git diff-index --quiet HEAD --; then
    echo "No changes to commit"
else
    git commit -m "Auto-backup $(date '+%Y-%m-%d %H:%M')"
    git push
    echo "Backup completed: $(date)"
fi
```

**Schedule with Cron (macOS/Linux):**

```bash
# Edit crontab
crontab -e

# Add line (backup every hour)
0 * * * * /bin/bash ~/scripts/logseq-backup.sh >> ~/logseq-backup.log 2>&1
```

**Schedule with Task Scheduler (Windows):**

1. Open Task Scheduler
2. Create Basic Task → Name: "Logseq Backup"
3. Trigger: Daily at specific time
4. Action: Start a program → `git-backup.bat`

```batch
@echo off
cd C:\Users\YourName\Documents\LogseqGraph
git pull --rebase
git add .
git commit -m "Auto-backup %date% %time%"
git push
```

---

### Method 2: Cloud Sync (Dropbox, iCloud, OneDrive)

**Best For:** Simple setup, automatic sync, mobile access

**Setup:**

1. **Move graph to cloud folder:**

```bash
# macOS - iCloud
mv ~/Documents/LogseqGraph ~/Library/Mobile\ Documents/com~apple~CloudDocs/LogseqGraph

# Windows - OneDrive
move %USERPROFILE%\Documents\LogseqGraph %USERPROFILE%\OneDrive\LogseqGraph

# Linux - Dropbox
mv ~/Documents/LogseqGraph ~/Dropbox/LogseqGraph
```

2. **Open graph from new location in Logseq**

**Pros:**

- ✅ Zero configuration
- ✅ Automatic sync
- ✅ Access from multiple devices

**Cons:**

- ❌ No version history (only recent versions)
- ❌ Potential sync conflicts
- ❌ Requires cloud subscription

---

### Method 3: Manual Export

**Best For:** Offline backups, full control

**Steps:**

1. Open Logseq
2. Click **⋯ (More)** → **Export graph**
3. Choose format:
   - **JSON** - Full export with metadata
   - **Markdown** - Portable plain text
   - **EDN** - Logseq internal format
4. Save to external drive or backup location

**Automate with Plugin:**

Install **Export on Schedule** plugin (if available) to automatically export daily.

---

## Database Graph Backups

### Method 1: Logseq Sync (Recommended)

**Best For:** Real-time backup, E2EE, mobile sync

**Setup:**

1. **Enable Logseq Sync:**

   - Open Settings → **Sync**
   - Click **Enable Sync**
   - Choose plan (free 1GB or paid)

2. **Configure backup retention:**
   - Settings → Sync → **Backup settings**
   - Keep version history: 30 days (recommended)

**Features:**

- ✅ Real-time sync across devices
- ✅ End-to-end encryption
- ✅ Version history (time travel)
- ✅ Automatic conflict resolution
- ✅ Works on mobile

**Recovery:**

- Changes synced automatically
- Access version history: Right-click block → **Version history**
- Restore entire graph: Settings → Sync → **Restore from backup**

---

### Method 2: Transit Export (Manual Backup)

**Best For:** Local backups, migration, disaster recovery

**Export Database:**

1. Click **⋯ (More)** → **Export graph**
2. Choose **EDN (Transit format)**
3. Save `.edn` file to backup location

**Example Filename:**

```
my-graph-2026-01-21-export.edn
```

**Automate with Script (Electron API):**

```javascript
// Plugin code to auto-export daily
async function exportDatabase() {
  const { invoke } = logseq.api
  const repo = await logseq.App.getCurrentGraph()
  const timestamp = new Date().toISOString().slice(0, 10)

  await invoke('export-db', {
    repo: repo.path,
    format: 'edn',
    path: `${repo.path}-${timestamp}-export.edn`,
  })

  console.log(`Exported: ${timestamp}`)
}

// Schedule daily at 11 PM
logseq.App.onScheduler('daily-backup', { hour: 23, minute: 0 }, exportDatabase)
```

---

### Method 3: SQLite Database Backup

**Best For:** Advanced users, database admins

**Locate Database File:**

```bash
# macOS
~/Library/Application Support/Logseq/graphs/<graph-name>.db

# Windows
%APPDATA%\Logseq\graphs\<graph-name>.db

# Linux
~/.config/Logseq/graphs/<graph-name>.db
```

**Manual Backup:**

```bash
# Copy database file
cp ~/Library/Application\ Support/Logseq/graphs/my-graph.db \
   ~/Backups/my-graph-$(date +%Y%m%d).db

# Verify integrity
sqlite3 ~/Backups/my-graph-$(date +%Y%m%d).db "PRAGMA integrity_check;"
```

**⚠️ Warning:**

- Close Logseq before copying database file
- Database may be locked if Logseq is running
- Corrupted database cannot be opened

---

## Automated Backup Workflows

### Workflow 1: 3-2-1 Backup Strategy

**Rule:** 3 copies, 2 different media, 1 offsite

**Implementation:**

1. **Primary (Real-time):** Logseq Sync or Git sync
2. **Secondary (Daily):** Local external drive backup
3. **Tertiary (Weekly):** Cloud storage (encrypted)

**Example Script (macOS):**

```bash
#!/bin/bash
# 3-2-1 backup script

GRAPH_DIR="$HOME/Documents/LogseqGraph"
BACKUP_DIR="/Volumes/ExternalDrive/Backups/Logseq"
CLOUD_DIR="$HOME/Dropbox/Backups/Logseq"
DATE=$(date +%Y-%m-%d)

# Backup 1: Local external drive
rsync -av --delete "$GRAPH_DIR/" "$BACKUP_DIR/$DATE/"

# Backup 2: Cloud (encrypted)
tar -czf "/tmp/logseq-$DATE.tar.gz" -C "$GRAPH_DIR" .
gpg --encrypt --recipient your-email@example.com "/tmp/logseq-$DATE.tar.gz"
mv "/tmp/logseq-$DATE.tar.gz.gpg" "$CLOUD_DIR/"
rm "/tmp/logseq-$DATE.tar.gz"

# Cleanup old backups (keep 30 days)
find "$BACKUP_DIR" -type d -mtime +30 -delete
find "$CLOUD_DIR" -type f -mtime +30 -delete

echo "3-2-1 Backup completed: $DATE"
```

---

### Workflow 2: Hourly Git Snapshots

**Setup with Launchd (macOS):**

Create: `~/Library/LaunchAgents/com.user.logseq-backup.plist`

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.user.logseq-backup</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/bash</string>
        <string>/Users/yourname/scripts/logseq-backup.sh</string>
    </array>
    <key>StartInterval</key>
    <integer>3600</integer>
    <key>RunAtLoad</key>
    <true />
</dict>
</plist>
```

**Load:**

```bash
launchctl load ~/Library/LaunchAgents/com.user.logseq-backup.plist
```

---

### Workflow 3: Pre-Shutdown Backup

**macOS - Add to Shell Profile:**

```bash
# Add to ~/.zshrc or ~/.bash_profile
trap 'backup_logseq' EXIT

backup_logseq() {
    if [ -d "$HOME/Documents/LogseqGraph" ]; then
        cd "$HOME/Documents/LogseqGraph"
        git add . && git commit -m "Auto-backup on shutdown" && git push
    fi
}
```

---

## Recovery Procedures

### Scenario 1: Accidental Block Deletion

**File-Based Graph:**

```bash
# Option 1: Git history
git log --all --full-history -- "path/to/file.md"
git show <commit-hash>:path/to/file.md > recovered-file.md

# Option 2: Check .recycle folder
ls logseq/.recycle/
```

**Database Graph:**

1. Right-click nearby block → **Version history**
2. Find deleted block in timeline
3. Click **Restore this version**

---

### Scenario 2: Corrupted Graph

**File-Based Graph:**

```bash
# Restore from Git
git reset --hard origin/main

# Or restore from backup
cp -r ~/Backups/LogseqGraph-latest/* ~/Documents/LogseqGraph/
```

**Database Graph:**

1. Close Logseq
2. Delete corrupted database:
   ```bash
   rm ~/Library/Application\ Support/Logseq/graphs/<graph-name>.db
   ```
3. Restore from backup:
   ```bash
   cp ~/Backups/<graph-name>-20260115.db \
      ~/Library/Application\ Support/Logseq/graphs/<graph-name>.db
   ```
4. Reopen Logseq

**Or Import from Transit:**

1. Click **⋯** → **Import graph**
2. Choose **EDN file**
3. Select backup `.edn` file
4. Create new graph from backup

---

### Scenario 3: Complete Data Loss

**Recovery Steps:**

1. **Check Logseq Sync:**

   - Login to Logseq
   - Settings → Sync → **Download graph**

2. **Check Git Remote:**

   ```bash
   mkdir ~/LogseqGraph
   cd ~/LogseqGraph
   git clone https://github.com/username/logseq-graph.git .
   ```

3. **Check Cloud Storage:**

   - Download from Dropbox/iCloud/OneDrive

4. **Check Local Backups:**

   - External drives
   - Time Machine (macOS)
   - File History (Windows)

5. **Last Resort - Check `.edn` exports:**
   - Import latest EDN backup
   - Rebuild graph from scratch

---

## Best Practices

### Backup Checklist

**Daily:**

- [ ] Automatic Git commit/push (if using Git)
- [ ] Logseq Sync active and syncing
- [ ] Check backup logs for errors

**Weekly:**

- [ ] Manual EDN export to external drive
- [ ] Verify backup integrity (open backup file)
- [ ] Test restore from backup (on test graph)

**Monthly:**

- [ ] Clean up old backups (keep 90 days)
- [ ] Verify cloud backup accessible
- [ ] Update backup scripts if needed

### Testing Your Backups

**Test Restore Process:**

1. Create test graph: `MyGraph-Test`
2. Restore from backup to test graph
3. Verify all content present
4. Check assets/attachments load
5. Delete test graph

**Why Test?**

- Corrupted backups may not be discovered until needed
- Restore process may have changed
- Validates entire backup pipeline

---

### Security Considerations

**Encrypt Sensitive Graphs:**

```bash
# Encrypt backup with GPG
gpg --encrypt --recipient your-email@example.com my-graph-export.edn

# Decrypt when needed
gpg --decrypt my-graph-export.edn.gpg > my-graph-export.edn
```

**Use Logseq Sync E2EE:**

- Settings → Sync → Enable **End-to-end encryption**
- Store encryption key safely (cannot recover without it)

**Git Private Repositories:**

- Never use public repos for private notes
- Use GitHub private repos or self-hosted Git

---

## Backup Strategy by Use Case

### Student/Researcher

**Recommended:**

- Primary: Git sync (version control for thesis/papers)
- Secondary: Logseq Sync (mobile access)
- Tertiary: Weekly EDN export to external drive

**Why:** Need version history, collaboration, and offline backups

---

### Professional/Work Notes

**Recommended:**

- Primary: Logseq Sync with E2EE (security)
- Secondary: Daily encrypted cloud backup
- Tertiary: Git sync to company GitLab (compliance)

**Why:** Security, compliance, and audit trail required

---

### Personal/Journal

**Recommended:**

- Primary: iCloud/Dropbox sync (convenience)
- Secondary: Logseq Sync (version history)
- Tertiary: Monthly USB backup

**Why:** Simplicity and ease of use priority

---

### Multi-Device Power User

**Recommended:**

- Primary: Logseq Sync (real-time everywhere)
- Secondary: Git sync (desktop version control)
- Tertiary: Automated daily EDN export

**Why:** Need seamless sync + full control over backups

---

## Troubleshooting

**Problem: Git push fails**

```bash
# Pull first to merge changes
git pull --rebase

# Resolve conflicts if any
git add .
git rebase --continue

# Push again
git push
```

**Problem: Logseq Sync stuck**

1. Settings → Sync → **Pause sync**
2. Wait 30 seconds
3. Click **Resume sync**
4. If still stuck: **Re-initialize sync**

**Problem: EDN export file too large**

```bash
# Compress export
gzip my-graph-export.edn

# Results in: my-graph-export.edn.gz (much smaller)
```

**Problem: Cannot find database file**

```bash
# macOS
open ~/Library/Application\ Support/Logseq/graphs/

# Windows
explorer %APPDATA%\Logseq\graphs\

# Linux
xdg-open ~/.config/Logseq/graphs/
```

---

## Next Steps

**Implement Your Strategy:**

1. Choose backup methods from this guide
2. Set up automated workflows
3. Test restore procedure
4. Document your backup process
5. Schedule regular backup verification

**Related Guides:**

- [Logseq Sync Setup](../collaboration/logseq-sync.md)
- [Git Integration](../collaboration/git-sync.md)
- [Export and Import](./export-graph.md)

---

**Questions or Issues?** Visit [Logseq Forum](https://discuss.logseq.com) or [Discord](https://discord.gg/KpN4eHY).
