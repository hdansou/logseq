---
title: 'CLI Basics'
description: 'How to use the Logseq command-line interface for automation and scripting'
status: stable
lastVerified: '2026-01-29'
logseqVersion: '0.11.x'
cliVersion: '0.4.2'
platforms: [desktop]
graphType: [db]
relatedDocs:
  reference:
    - '[HTTP API](../../reference/api/http-api.md)'
    - '[MCP Server](../../reference/api/mcp-server.md)'
---

## CLI Basics

> How to use the Logseq CLI to interact with your graphs from the terminal.
>
> Generation notice:
> This page currently describes the legacy `deps/cli` command surface.
> For the newer db-worker-node-based CLI, see `docs/cli/logseq-cli.md` and `docs/cli/cli-generations.md`.

## Prerequisites

- Node.js 22.17.0 or higher
- Logseq Desktop installed
- Database graph (CLI works with DB graphs only)

---

## Install the CLI

```bash
npm install -g @logseq/cli
```

Verify installation:

```bash
logseq --version
```

---

## List Your Graphs

See all available graphs:

```bash
logseq list
```

**Output:**

```
DB Graphs:
my-notes
work-journal
personal

File Graphs:
old-notes
```

---

## Search Your Notes

Search for content in a specific graph:

```bash
logseq search -g my-notes "meeting notes"
```

**Options:**

- `-g, --graph <name>` - Graph to search
- Default: searches currently open graph if Logseq is running

---

## Query Your Graph

Run Datalog queries:

```bash
logseq query -g my-notes '[:find ?title :where [?p :block/title ?title]]'
```

**Common queries:**

```bash
# Get all page titles
logseq query -g my-notes '[:find ?title :where [?p :block/name] [?p :block/title ?title]]'

# Find all tasks
logseq query -g my-notes '[:find ?title :where [?b :block/tags ?t] [?t :db/ident :logseq.class/Task] [?b :block/title ?title]]'

# Count blocks
logseq query -g my-notes '[:find (count ?b) :where [?b :block/uuid]]'
```

---

## Export Your Graph

Export to various formats:

```bash
# Export to Markdown (default)
logseq export -g my-notes

# Export to EDN format
logseq export -g my-notes --format edn

# Export to specific directory
logseq export -g my-notes -o ~/backups/
```

**Output:** Creates a ZIP file with all pages.

---

## Import Data

Import EDN data into a graph:

```bash
logseq import-edn -g my-notes -f backup.edn
```

**Options:**

- `-g, --graph <name>` - Target graph
- `-f, --file <path>` - EDN file to import

---

## Show Graph Info

View graph metadata:

```bash
logseq show -g my-notes
```

**Output:**

```
| Name                | Value                                  |
|---------------------|----------------------------------------|
| Graph directory     | /Users/you/.logseq/graphs/my-notes     |
| Graph created at    | Jan 15th, 2025                         |
| Schema version      | {:major 65, :minor 7}                  |
| Page count          | 234                                    |
| Block count         | 5,678                                  |
```

---

## Validate Graph

Check for data integrity issues:

```bash
logseq validate -g my-notes
```

**Output (success):**

```
Valid!
```

**Output (errors):**

```
Found 3 validation errors:
- Block abc123: missing required property
- Page xyz789: invalid reference
...
```

---

## Append Content

Add blocks to your current page (requires Logseq running):

```bash
logseq append "- New task #task"
```

**Options:**

- Content is appended to the currently focused page
- Requires Logseq desktop app to be running

---

## Scripting Examples

### Daily Backup Script

```bash
#!/bin/bash
DATE=$(date +%Y-%m-%d)
logseq export -g my-notes -o ~/backups/logseq-$DATE.zip
```

### Query to JSON

```bash
logseq query -g my-notes '[:find ?title :where [?p :block/name] [?p :block/title ?title]]' | jq '.'
```

### Search Multiple Graphs

```bash
for graph in notes work personal; do
  echo "=== $graph ==="
  logseq search -g $graph "project"
done
```

---

## Working Modes

### Offline Mode (Local)

When Logseq is NOT running:

- Works with local database files
- Read-only queries and exports
- Specify graph with `-g` flag

### API Mode (Connected)

When Logseq IS running:

- Connects to running instance
- Real-time operations
- Can append content
- Uses HTTP API on port 12315

---

## Troubleshooting

### "Graph not found"

```bash
# List available graphs
logseq list

# Use exact name from list
logseq show -g "exact-graph-name"
```

### "Cannot connect to Logseq"

- Ensure Logseq desktop is running
- Check API server is enabled (Settings → API)
- Verify port 12315 is not blocked

### "Permission denied"

```bash
# Check Node.js version
node --version  # Must be 22.17.0+

# Reinstall with correct permissions
sudo npm install -g @logseq/cli
```

### "Invalid query syntax"

```bash
# Wrap query in single quotes
logseq query -g my-notes '[:find ...]'

# Escape special characters
logseq query -g my-notes '[:find ?t :where [?b :block/title ?t]]'
```

---

## Command Reference

| Command      | Description       | Example                             |
| ------------ | ----------------- | ----------------------------------- |
| `list`       | List all graphs   | `logseq list`                       |
| `show`       | Show graph info   | `logseq show -g name`               |
| `search`     | Search content    | `logseq search -g name "term"`      |
| `query`      | Run Datalog query | `logseq query -g name '[...]'`      |
| `export`     | Export graph      | `logseq export -g name`             |
| `import-edn` | Import EDN        | `logseq import-edn -g name -f file` |
| `validate`   | Validate graph    | `logseq validate -g name`           |
| `append`     | Add content       | `logseq append "text"`              |

---

## See Also

- **HTTP API**: [http-api.md](../../reference/api/http-api.md) for programmatic access
- **MCP Server**: [mcp-server.md](../../reference/api/mcp-server.md) for AI integration
- **Backup Guide**: [backup-restore.md](../backup/backup-restore.md)

---

_Last updated: 2026-01-29_
