---
title: 'CLI Basics'
description: 'How to use the Logseq command-line interface for automation and scripting'
status: stable
lastVerified: '2026-03-10'
logseqVersion: '0.11.x'
cliVersion: '0.4.3'
platforms: [desktop]
graphType: [db]
relatedDocs:
  reference:
    - '[HTTP API](../../reference/api/http-api.md)'
    - '[MCP Server](../../reference/api/mcp-server.md)'
---

# CLI Basics

> **Last revised:** 2026-03-10

> How to use the Logseq CLI to interact with your graphs from the terminal.

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

See all available local DB graphs:

```bash
logseq list
```

**Output:**

```
my-notes
work-journal
personal
```

---

## Search Your Notes

Search for content in a graph:

```bash
# Search a local graph
logseq search -g my-notes "meeting notes"

# Search the current in-app graph (requires HTTP API Server)
logseq search -a YOUR_TOKEN "meeting notes"
```

**Options:**

- `-g, --graph <name>` - Local graph to search
- `-a, --api-server-token <token>` - API server token to search current in-app graph
- `-r, --raw` - Print raw response
- `-l, --limit <n>` - Limit max number of results (default: 100)

When using `-g`, the search matches against `:block/title` of blocks. When using `-a`, a full-text search is performed via the app's API.

You can also set the `$LOGSEQ_API_SERVER_TOKEN` environment variable instead of passing `-a` each time.

---

## Query Your Graph

Run queries against a graph. Supports Datalog queries and entity queries:

```bash
# Datalog query on a local graph
logseq query -g my-notes '[:find ?title :where [?p :block/title ?title]]'

# Entity query by id, :db/ident, or UUID
logseq query -g my-notes 10 :logseq.class/Tag

# Query the current in-app graph via API (supports advanced and simple queries)
logseq query -a YOUR_TOKEN '(task DOING)'
```

**Options:**

- `-g, --graphs <name>` - Local graph(s) to query (can specify multiple)
- `-a, --api-server-token <token>` - API server token to query current in-app graph
- `-p, --properties-readable` - Show property values instead of ids (local entity queries)
- `-t, --title-query` - Invoke local query on `:block/title`

**Common Datalog queries:**

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

### Export as Markdown

Export a local graph to Markdown. Creates a ZIP file with all pages:

```bash
# Export to auto-named ZIP
logseq export -g my-notes

# Export to a specific file
logseq export -g my-notes -f ~/backups/my-notes.zip
```

**Options:**

- `-g, --graph <name>` - Local graph to export (required)
- `-f, --file <path>` - File to save export

### Export as EDN

Export a graph to EDN format:

```bash
# Export a local graph
logseq export-edn -g my-notes -f my-notes.edn

# Export the current in-app graph
logseq export-edn -a YOUR_TOKEN
```

**Options:**

- `-g, --graph <name>` - Local graph to export
- `-a, --api-server-token <token>` - API server token to export current in-app graph
- `-f, --file <path>` - File to save export
- `-T, --include-timestamps?` - Include timestamps in export
- `-b, --exclude-built-in-pages?` - Exclude built-in pages
- `-e, --exclude-namespaces <ns>` - Namespaces to exclude from properties and classes

---

## Import Data

Import EDN data into a graph:

```bash
# Import into a local graph
logseq import-edn -g my-notes -f backup.edn

# Import into the current in-app graph
logseq import-edn -a YOUR_TOKEN -f backup.edn
```

**Options:**

- `-g, --graph <name>` - Local graph to import into
- `-a, --api-server-token <token>` - API server token to import into current in-app graph
- `-f, --file <path>` - EDN file to import (required)

---

## Show Graph Info

View graph metadata (takes graph names as positional arguments):

```bash
logseq show my-notes
```

**Output:**

```
|                         Name |                                              Value |
|------------------------------+----------------------------------------------------|
|              Graph directory |                    /Users/you/.logseq/graphs/my-notes |
|             Graph created at |                                     Jan 15th, 2025 |
|         Graph schema version |                              {:major 65, :minor 7} |
| Graph initial schema version |                              {:major 65, :minor 7} |
|      Graph created by commit | https://github.com/logseq/logseq/commit/abcdef1234 |
|            Graph imported by |                                  :cli/create-graph |
```

Note: "Graph created by commit" and "Graph imported by" rows only appear if the graph has that metadata.

---

## Validate Graph

Check for data integrity issues:

```bash
logseq validate -g my-notes
```

The `-g` flag accepts multiple graphs:

```bash
logseq validate -g my-notes -g work-journal
```

**Options:**

- `-g, --graphs <name>` - Local graph(s) to validate (required)
- `-o, --open-schema` - Validate with open schema to allow extra keys/attributes

**Output (success):**

```
Valid!
```

**Output (errors):**

```
Found 3 entities with errors:
[... entity details ...]
```

---

## Append Content

Add text to the currently focused page in the running Logseq app:

```bash
logseq append "New task text" -a YOUR_TOKEN
```

**Options:**

- `-a, --api-server-token <token>` - API server token (required, or set `$LOGSEQ_API_SERVER_TOKEN`)

The text is appended as a new block on the currently focused page. Requires the Logseq desktop app to be running with the HTTP API Server enabled.

---

## MCP Server

Run a Model Context Protocol (MCP) server for AI integration:

```bash
# Start against a local graph (HTTP Streamable transport)
logseq mcp-server -g my-notes

# Start against the current in-app graph
logseq mcp-server -a YOUR_TOKEN

# Run as stdio server (for editor integrations)
logseq mcp-server -g my-notes --stdio
```

**Options:**

- `-g, --graph <name>` - Local graph to use
- `-a, --api-server-token <token>` - API server token to connect to current in-app graph
- `-s, --stdio` - Run as stdio server instead of HTTP Streamable
- `-p, --port <n>` - Port for HTTP Streamable server (default: 12315)
- `--host <addr>` - Host for HTTP Streamable server (default: 127.0.0.1)

---

## Scripting Examples

### Daily Backup Script

```bash
#!/bin/bash
DATE=$(date +%Y-%m-%d)
logseq export -g my-notes -f ~/backups/logseq-$DATE.zip
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

### Local Mode (Offline)

When using `-g` to specify a local graph:

- Works directly with local database files
- Queries, exports, search, and validation
- No running Logseq instance required

### API Mode (Connected)

When using `-a` or `$LOGSEQ_API_SERVER_TOKEN`:

- Connects to the running Logseq desktop app
- Communicates via HTTP API Server on port 12315
- Supports search, query, export-edn, import-edn, append, and mcp-server
- The HTTP API Server must be enabled in the desktop app

Note: `export` (Markdown) and `validate` work only in local mode. `append` works only in API mode.

---

## Troubleshooting

### "Graph not found"

```bash
# List available graphs
logseq list

# Use exact name from list
logseq show "exact-graph-name"
```

### "Cannot connect to Logseq"

- Ensure Logseq desktop is running
- Check the HTTP API Server is enabled in the desktop app
- Verify port 12315 is not blocked
- Make sure you are providing a valid API server token via `-a` or `$LOGSEQ_API_SERVER_TOKEN`

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

| Command      | Description                | Example                                |
| ------------ | -------------------------- | -------------------------------------- |
| `list`       | List local DB graphs       | `logseq list`                          |
| `show`       | Show graph info            | `logseq show my-notes`                 |
| `search`     | Search graph content       | `logseq search -g name "term"`         |
| `query`      | Query graph (Datalog/entity) | `logseq query -g name '[...]'`       |
| `export`     | Export graph as Markdown   | `logseq export -g name`               |
| `export-edn` | Export graph as EDN        | `logseq export-edn -g name -f out.edn`|
| `import-edn` | Import EDN into graph      | `logseq import-edn -g name -f file`   |
| `validate`   | Validate graph integrity   | `logseq validate -g name`             |
| `append`     | Append text to current page| `logseq append "text" -a token`        |
| `mcp-server` | Run MCP server             | `logseq mcp-server -g name`           |

---

## See Also

- **HTTP API**: [http-api.md](../../reference/api/http-api.md) for programmatic access
- **MCP Server**: [mcp-server.md](../../reference/api/mcp-server.md) for AI integration
- **Backup Guide**: [backup-restore.md](../backup/backup-restore.md)

---

_Last updated: 2026-03-10_
