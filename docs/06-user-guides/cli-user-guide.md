# Logseq CLI - Complete User Guide

> Command-line interface for Logseq database graphs

**Version**: 0.4.0
**Last Updated**: 2025-11-20
**For**: Normal Users & Developers

---

## Table of Contents

### For Users

1. [Introduction](#introduction)
2. [Installation](#installation)
3. [Quick Start](#quick-start)
4. [Basic Concepts](#basic-concepts)
5. [Command Reference](#command-reference)
6. [Common Use Cases](#common-use-cases)
7. [Troubleshooting](#troubleshooting)
8. [FAQ](#faq)

### For Developers

9. [Development Setup](#development-setup)
10. [Architecture Overview](#architecture-overview)
11. [Testing](#testing)
12. [Contributing](#contributing)
13. [Advanced Topics](#advanced-topics)

---

# For Users

## Introduction

The Logseq CLI is a command-line tool that lets you interact with your Logseq database graphs from the terminal. You can:

- 📊 **Query** your knowledge graph with powerful DataScript queries
- 🔍 **Search** across all your notes
- 📤 **Export** graphs to Markdown or EDN format
- 📥 **Import** EDN data into graphs
- 📋 **List** and inspect graph metadata
- ✅ **Validate** graph data for errors
- ✍️ **Append** content to your current page (while Logseq is running)
- 🤖 **Automate** workflows with scripts and CI/CD pipelines

### What You Can Do

**Without Logseq Running (Offline Mode)**

- List all your graphs
- Search and query local graphs
- Export graphs to various formats
- Inspect graph metadata

**With Logseq Running (API Mode)**

- Search your currently open graph
- Run advanced queries
- Append blocks to the current page
- Real-time graph interactions

## Installation

### Prerequisites

- **Node.js**: Version 22.17.0 or higher
  - Check version: `node --version`
  - Download: [nodejs.org](https://nodejs.org/)
- **Logseq Desktop App**: Database version must be installed
  - Download: [logseq.com](https://logseq.com/)

### Install from npm

```bash
npm install -g @logseq/cli
```

### Verify Installation

```bash
logseq --version
# Output: 0.4.0
```

### Update

```bash
npm update -g @logseq/cli
```

### Uninstall

```bash
npm uninstall -g @logseq/cli
```

## Quick Start

### 1. List Your Graphs

See all available graphs on your system:

```bash
logseq list
```

Output:

```
DB Graphs:
my-notes
work-journal
personal

File Graphs:
old-notes
```

### 2. Search Your Notes

Search for content in a graph:

```bash
logseq search my-notes "meeting notes"
```

Output:

```
Search found 15 results:
Project meeting notes from Q4
Team meeting notes - Sprint Planning
...
```

### 3. Query Your Graph

Get all pages with a specific tag:

```bash
logseq query my-notes '[:find ?title :where [?b :block/title ?title]]'
```

### 4. Export Your Graph

Export to Markdown:

```bash
logseq export my-notes
# Output: Exported 234 pages to my-notes_markdown_1704844800.zip
```

### 5. Show Graph Info

View metadata about a graph:

```bash
logseq show my-notes
```

Output:

```
| Name                      | Value                                  |
|---------------------------+----------------------------------------|
| Graph directory           | /Users/you/logseq/graphs/my-notes      |
| Graph created at          | Jan 15th, 2025                         |
| Graph schema version      | {:major 65, :minor 7}                  |
```

### 6. Import Graph Data

Import EDN data into a graph:

```bash
logseq import-edn -g my-notes -f backup.edn
# Output: Imported 45 properties, 12 classes and 234 pages!
```

### 7. Validate Graph

Check graph data integrity:

```bash
logseq validate my-notes
# Output: Read graph my-notes with counts: {...}
#         Valid!
```

## Basic Concepts

### Graph Names

Your graphs are stored in `~/logseq/graphs/` with prefixes:

- **DB Graphs**: `logseq_db__graph-name`
- **File Graphs**: `logseq_version_graph-name`

The CLI automatically handles these prefixes - just use the simple name:

```bash
# Use this:
logseq search my-graph "todo"

# NOT this:
logseq search logseq_db__my-graph "todo"
```

**Local Path Support (v0.4.0+):**

You can also use direct paths to graph database files:

```bash
# Using absolute path
logseq search /Users/you/Downloads/logseq_db_mydb_1234567890.sqlite "todo"

# Using relative path
logseq search ./my-backup/graph.sqlite "todo"

# Using home directory shorthand
logseq search ~/Downloads/exported-graph.sqlite "todo"
```

This is useful for:

- Querying exported or backed-up graphs
- Working with graphs outside the standard location
- Analyzing downloaded graph databases

### Two Operation Modes

#### Local Mode (Offline)

Access graphs directly from disk without running Logseq:

```bash
logseq search my-graph "todo"
```

**Benefits:**

- Works offline
- No setup required
- Perfect for scripts and automation

**Limitations:**

- Read-only access
- Search only searches titles (not full content)

#### API Mode (Online)

Connect to a running Logseq app via HTTP API:

```bash
logseq search "todo" -a YOUR_TOKEN
```

**Benefits:**

- Full-text search
- Can modify data (append blocks)
- Access currently open graph

**Requirements:**

1. Logseq app must be running
2. HTTP API Server must be enabled
3. Valid API token required

### Command Structure

All commands follow this pattern:

```bash
logseq <command> [arguments] [options]
```

Examples:

```bash
logseq list                                    # No arguments or options
logseq search my-graph "todo"                  # Arguments: graph, search terms
logseq search "todo" -a TOKEN                  # Arguments + options
logseq query my-graph 123 -p                   # Arguments + flag option
logseq export my-notes -f backup.zip           # Arguments + value option
```

## Command Reference

### `list` - List All Graphs

List all DB and file graphs on your system.

**Syntax:**

```bash
logseq list
```

**Example:**

```bash
$ logseq list
DB Graphs:
my-notes
work-journal

File Graphs:
old-graph
```

**Use When:**

- You forgot the exact name of your graph
- Checking if a graph exists
- Verifying graph creation

---

### `show` - Display Graph Metadata

Show detailed information about one or more graphs.

**Syntax:**

```bash
logseq show GRAPH [GRAPH2 ...]
```

**Examples:**

```bash
# Single graph
logseq show my-notes

# Multiple graphs
logseq show my-notes work-journal personal
```

**Output:**

```
| Name                         | Value                              |
|------------------------------+------------------------------------|
| Graph directory              | /Users/you/logseq/graphs/my-notes  |
| Graph created at             | Jan 15th, 2025                     |
| Graph schema version         | {:major 65, :minor 7}              |
| Graph initial schema version | {:major 65, :minor 7}              |
| Graph created by commit      | https://github.com/...             |
```

**Use When:**

- Debugging graph issues
- Checking schema version
- Finding graph location on disk

---

### `search` - Search Your Notes

Search for text across your graph.

**Syntax:**

```bash
# Local mode
logseq search GRAPH SEARCH_TERMS...

# API mode
logseq search SEARCH_TERMS... -a TOKEN
```

**Options:**

```
-a, --api-server-token TOKEN    Use API mode (searches currently open graph)
-r, --raw                       Print raw response without formatting
-l, --limit NUMBER              Max results (default: 100)
```

**Examples:**

```bash
# Basic search (local mode)
logseq search my-notes "meeting notes"

# Multi-word search
logseq search my-notes "project planning 2025"

# API mode - search current graph
logseq search "todo urgent" -a YOUR_TOKEN

# Limit results
logseq search my-notes "javascript" -l 20

# Raw output (for parsing)
logseq search my-notes "test" -r
```

**Environment Variable:**

```bash
# Set token once
export LOGSEQ_API_SERVER_TOKEN=YOUR_TOKEN

# Then use without -a flag
logseq search "meeting notes"
```

**Output:**

```
Search found 3 results:
Project meeting notes from Q4
Team meeting notes - Sprint Planning
Client meeting notes - Discovery
```

**Tips:**

- Local mode only searches block titles
- API mode searches full content (including block text)
- Search terms are case-sensitive in local mode
- Use quotes for multi-word phrases

---

### `query` - Query Your Graph

Execute powerful DataScript queries on your graph.

**Syntax:**

```bash
# Entity query (by ID, UUID, or keyword)
logseq query GRAPH ID|UUID|IDENT...

# DataLog query
logseq query GRAPH 'DATALOG_QUERY'

# API query
logseq query 'QUERY' -a TOKEN
```

**Options:**

```
-a, --api-server-token TOKEN    Use API mode
-g, --graphs GRAPH...           Additional graphs for multi-graph query
-p, --properties-readable       Show property values instead of IDs
-t, --title-query               Search block titles
```

**Examples:**

#### 1. Entity Query by ID

```bash
logseq query my-notes 123
```

Output:

```clojure
{:db/id 123
 :block/title "My Page"
 :block/created-at 1704844800000}
```

#### 2. Entity Query by Keyword

```bash
logseq query my-notes :logseq.class/Tag
```

Output:

```clojure
{:db/id 2
 :db/ident :logseq.class/Tag
 :block/title "Tag"
 :block/name "tag"}
```

#### 3. Entity Query by UUID

```bash
logseq query my-notes "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
```

#### 4. DataLog Query - Find All Pages

```bash
logseq query my-notes '[:find ?title :where [?b :block/title ?title]]'
```

Output:

```clojure
["Page 1"
 "Page 2"
 "Page 3"]
```

#### 5. DataLog Query - Find TODO Items

```bash
logseq query my-notes '[:find ?title :where [?b :block/marker "TODO"] [?b :block/title ?title]]'
```

#### 6. DataLog Query with Pull

```bash
logseq query my-notes '[:find (pull ?b [*]) :where [?b :block/marker "DOING"]]'
```

#### 7. Multi-Graph Query

```bash
logseq query main-graph '[:find ?b :where [?b :block/name]]' -g graph2 -g graph3
```

#### 8. Readable Properties

```bash
logseq query my-notes 10 -p
```

Output with `-p`:

```clojure
{:block/tags [:logseq.class/Tag :logseq.class/Project]}
```

Without `-p`:

```clojure
{:block/tags #{#datascript/Entity{:db/id 2} #datascript/Entity{:db/id 3}}}
```

#### 9. API Mode - Simple Query

```bash
logseq query '(task DOING)' -a TOKEN
```

#### 10. API Mode - DataLog Query

```bash
logseq query '[:find (pull ?b [*]) :where [?b :block/marker "TODO"]]' -a TOKEN
```

**Query Tips:**

- DataLog queries must start with `[:find ...]`
- Use single quotes to prevent shell interpretation
- Built-in DSL rules are automatically included
- Results are pretty-printed by default

**Common Queries:**

```bash
# All pages
logseq query my-notes '[:find ?title :where [?b :block/title ?title]]'

# All TODO blocks
logseq query my-notes '[:find ?title :where [?b :block/marker "TODO"] [?b :block/title ?title]]'

# Pages with specific tag
logseq query my-notes '[:find ?title :where [?b :block/tags ?tag] [?tag :db/ident :logseq.class/Project] [?b :block/title ?title]]'

# Recent pages (last 7 days)
logseq query my-notes '[:find ?title :where [?b :block/created-at ?t] [(> ?t 1704758400000)] [?b :block/title ?title]]'
```

---

### `export` - Export to Markdown

Export your entire graph to a Markdown zip file.

**Syntax:**

```bash
logseq export GRAPH [options]
```

**Options:**

```
-f, --file PATH    Output file path (default: GRAPH_markdown_TIMESTAMP.zip)
```

**Examples:**

```bash
# Basic export
logseq export my-notes
# Output: my-notes_markdown_1704844800.zip

# Custom filename
logseq export my-notes -f backup-2025-01.zip

# Custom path
logseq export my-notes -f ~/backups/notes.zip
```

**Output:**

```
Exported 234 pages to my-notes_markdown_1704844800.zip
```

**What's Included:**

- All pages as individual `.md` files
- Respects your graph's export settings
- Preserves indentation preferences
- Includes page properties

**Export Settings:**

The export respects your `logseq/config.edn` settings:

```clojure
{:export/bullet-indentation :tab}      ; Uses tabs
{:export/bullet-indentation :two-spaces}   ; Uses 2 spaces
{:export/bullet-indentation :four-spaces}  ; Uses 4 spaces
{:export/bullet-indentation :eight-spaces} ; Uses 8 spaces
```

**Use Cases:**

- Backup your entire graph
- Share notes with others
- Import into other tools
- Archive old graphs

---

### `export-edn` - Export to EDN

Export your graph structure to EDN (Extensible Data Notation) format.

**Syntax:**

```bash
# Local mode
logseq export-edn -g GRAPH [options]

# API mode
logseq export-edn -a TOKEN [options]
```

**Options:**

```
-g, --graph GRAPH                  Graph name (required for local mode)
-a, --api-server-token TOKEN       Use API mode (exports currently open graph)
-f, --file PATH                    Output file (default: GRAPH_TIMESTAMP.edn)
-T, --include-timestamps?          Include created-at and updated-at
-c, --catch-validation-errors?     Continue on errors (development mode)
-e, --exclude-namespaces NS...     Exclude property/class namespaces
-b, --exclude-built-in-pages?      Exclude system pages
-F, --exclude-files?               Exclude file path information
-t, --export-type TYPE             Export type (default: :graph)
```

**Examples:**

```bash
# Basic local export
logseq export-edn -g my-notes
# Output: my-notes_1704844800.edn

# API mode - export current graph
logseq export-edn -a YOUR_TOKEN -f backup.edn

# Custom filename
logseq export-edn -g my-notes -f backup.edn

# Include timestamps
logseq export-edn -g my-notes -T

# Exclude system pages
logseq export-edn -g my-notes -b

# Exclude namespaces
logseq export-edn -g my-notes -e logseq.class -e logseq.property

# Exclude files
logseq export-edn -g my-notes -F

# Full export with all options
logseq export-edn -g my-notes -f full-backup.edn -T -b -F
```

**Output:**

```
Exported 45 properties, 12 classes and 234 pages to my-notes_1704844800.edn
```

**EDN Structure:**

```clojure
{:properties [...]        ; Property definitions
 :classes [...]           ; Class definitions
 :pages-and-blocks [...]} ; All pages and blocks
```

**Use Cases:**

- Create importable graph backups
- Transfer graphs between systems
- Graph analysis and processing
- Development and debugging

---

### `import-edn` - Import from EDN

Import EDN data into a graph.

**Syntax:**

```bash
# Local mode
logseq import-edn -g GRAPH -f FILE

# API mode
logseq import-edn -f FILE -a TOKEN
```

**Options:**

```
-g, --graph GRAPH                Graph name (required for local mode)
-f, --file PATH                  Input EDN file (required)
-a, --api-server-token TOKEN     Use API mode (imports to currently open graph)
```

**Examples:**

```bash
# Import to local graph
logseq import-edn -g my-notes -f backup.edn

# Import via API to current graph
logseq import-edn -f backup.edn -a YOUR_TOKEN

# Import with environment variable
export LOGSEQ_API_SERVER_TOKEN=YOUR_TOKEN
logseq import-edn -f data.edn
```

**Output:**

```
Imported 45 properties, 12 classes and 234 pages!
```

**EDN Structure:**

The import expects EDN files with this structure (typically created by `export-edn`):

```clojure
{:properties [...]        ; Property definitions
 :classes [...]           ; Class definitions
 :pages-and-blocks [...]} ; All pages and blocks
```

**Use Cases:**

- Restore graph backups
- Transfer data between graphs
- Import curated datasets
- Migrate from other systems
- Development and testing

**Important Notes:**

- The import is additive - it doesn't delete existing data
- Conflicting data may cause validation errors
- Use `validate` command after import to check data integrity
- For large imports, local mode is faster than API mode

---

### `mcp-server` - Run MCP Server

Run a Model Context Protocol (MCP) server to integrate Logseq with MCP-compatible tools and AI assistants.

**Syntax:**

```bash
# Local graph mode
logseq mcp-server -g GRAPH [options]

# API mode (current graph)
logseq mcp-server -a TOKEN [options]
```

**Options:**

```
-g, --graph GRAPH                Graph name (for local mode)
-a, --api-server-token TOKEN     API token (for in-app graph)
-s, --stdio                      Run via stdio transport (default: HTTP)
-p, --port NUMBER                Port for HTTP server (default: 12315)
    --host ADDRESS               Host for HTTP server (default: 127.0.0.1)
-t, --debug-tool                 Debug MCP tool with direct invocation
```

**Examples:**

```bash
# Run MCP server for local graph via stdio
logseq mcp-server -g my-notes -s

# Run MCP server for current in-app graph
logseq mcp-server -a YOUR_TOKEN

# Run HTTP MCP server on custom port
logseq mcp-server -g my-notes -p 8080

# Run with custom host and port
logseq mcp-server -g my-notes --host 0.0.0.0 -p 9000

# Debug mode
logseq mcp-server -g my-notes -t
```

**Output:**

```
MCP server running on http://127.0.0.1:12315
```

**Transport Modes:**

1. **HTTP Streamable Server (Default):**

   - Runs as HTTP server
   - Can be accessed by multiple clients
   - Use `-p` to customize port
   - Use `--host` to customize bind address

2. **Stdio Transport (`-s`):**
   - Communicates via standard input/output
   - Typically used by MCP-compatible tools
   - Single-client mode

**Use Cases:**

- **AI Assistant Integration**: Connect Claude, ChatGPT, or other AI tools to your Logseq graph
- **Custom Tools**: Build MCP clients that interact with your knowledge base
- **Automation**: Create workflows that query and modify your graph via MCP
- **Multi-Tool Access**: Run HTTP server to allow multiple MCP clients simultaneously

**MCP Capabilities:**

The MCP server exposes your Logseq graph with the following capabilities:

- Query your graph using DataScript
- Search across pages and blocks
- Read page contents
- Access graph metadata
- Execute custom queries

**Requirements:**

- For API mode: Logseq app must be running with HTTP API enabled
- For local mode: Direct access to graph database files
- MCP-compatible client application

**Important Notes:**

- The server runs continuously until terminated (Ctrl+C)
- In API mode, changes made in Logseq app are immediately available
- In local mode, the server has read-only access to the graph
- For security, bind to localhost (127.0.0.1) unless you need network access

---

### `validate` - Validate Graph Data

Validate one or more graphs for data integrity errors.

**Syntax:**

```bash
logseq validate GRAPH [GRAPH2 ...]
```

**Examples:**

```bash
# Validate single graph
logseq validate my-notes

# Validate multiple graphs
logseq validate my-notes work-journal personal
```

**Output (when valid):**

```
Read graph my-notes with counts: {:properties 45, :classes 12, :pages 234, :blocks 1523, :datoms 15234}
Valid!
```

**Output (when invalid):**

```
Read graph my-notes with counts: {:properties 45, :classes 12, :pages 234, :blocks 1523, :datoms 15234}
Found 3 entities with errors:
[{:db/id 8763
  :block/order ["invalid dispatch value"]
  :errors {:dispatch-key nil, :errors {39651 ["invalid dispatch value"]}}}
 {:db/id 1404
  :block/uuid ["invalid dispatch value"]
  :errors {:dispatch-key nil, :errors {40799 ["invalid dispatch value"]}}}
 {:db/id 734
  :logseq.property.history/property ["invalid dispatch value"]
  :errors {:dispatch-key nil, :errors {44181 ["invalid dispatch value"]}}}]
```

**Use Cases:**

- Check data integrity after import
- Debug graph corruption issues
- Validate before backup
- Quality assurance for automated workflows
- Development and testing

**Technical Details:**

- Uses Malli schema validation
- Checks closed-value validation for higher data quality
- Reports entity IDs with specific error details
- Exits with code 1 if errors found
- Useful for CI/CD pipelines

---

### `append` - Add Content to Current Page

Append a new block to the currently open page in Logseq.

**Syntax:**

```bash
logseq append TEXT... -a TOKEN
```

**Options:**

```
-a, --api-server-token TOKEN    API token (required)
```

**Requirements:**

- Logseq app must be running
- HTTP API Server must be enabled
- A page must be open in the app

**Examples:**

```bash
# Append single block
logseq append "TODO Review pull request" -a YOUR_TOKEN

# Multi-word content (quotes optional)
logseq append Meeting notes from today -a YOUR_TOKEN

# With environment variable
export LOGSEQ_API_SERVER_TOKEN=YOUR_TOKEN
logseq append "New idea: Implement feature X"
```

**Output:**

```
Success!
```

**Use Cases:**

- Quick capture from terminal
- Script-based note-taking
- Automation workflows
- Voice-to-text pipelines

**Tips:**

- Block is added at the end of the current page
- Inherits page context
- Can include Markdown formatting
- Supports block references and tags

---

### `help` - Get Command Help

Display help information for any command.

**Syntax:**

```bash
logseq help COMMAND
```

**Examples:**

```bash
logseq help query
logseq help export-edn
logseq help search
logseq help mcp-server
```

**Also Available:**

```bash
logseq -h         # Show all commands
logseq --help     # Same as -h
```

## Common Use Cases

### Use Case 1: Daily Backup Script

Automatically backup your graph every day:

```bash
#!/bin/bash
# backup.sh

DATE=$(date +%Y-%m-%d)
BACKUP_DIR=~/backups/logseq

# Create backup directory
mkdir -p $BACKUP_DIR

# Export to Markdown
logseq export my-notes -f "$BACKUP_DIR/notes-$DATE.zip"

# Export to EDN (with timestamps)
logseq export-edn my-notes -f "$BACKUP_DIR/notes-$DATE.edn" -T

echo "Backup completed: $DATE"
```

**Set up daily cron job:**

```bash
crontab -e
```

Add:

```
0 2 * * * /path/to/backup.sh
```

### Use Case 2: Search and Process Results

Find all TODO items and process them:

```bash
#!/bin/bash
# find-todos.sh

# Query all TODO items
TODOS=$(logseq query my-notes '[:find ?title :where [?b :block/marker "TODO"] [?b :block/title ?title]]')

# Parse and display
echo "$TODOS" | grep -v "^\[" | while read -r line; do
  echo "- $line"
done
```

### Use Case 3: Quick Capture Workflow

Create a keyboard shortcut to capture thoughts:

**macOS (using Automator):**

1. Create Quick Action in Automator
2. Add "Run Shell Script"
3. Script:

```bash
export LOGSEQ_API_SERVER_TOKEN=YOUR_TOKEN
INPUT=$(osascript -e 'text returned of (display dialog "Quick capture:" default answer "")')
logseq append "$INPUT"
```

4. Assign keyboard shortcut in System Preferences

**Linux (using xdotool):**

```bash
#!/bin/bash
INPUT=$(zenity --entry --text="Quick capture:")
export LOGSEQ_API_SERVER_TOKEN=YOUR_TOKEN
logseq append "$INPUT"
```

### Use Case 4: Graph Statistics Script

Generate statistics about your graph:

```bash
#!/bin/bash
# stats.sh

GRAPH=$1

echo "=== Graph Statistics for $GRAPH ==="
echo

# Total pages
PAGES=$(logseq query $GRAPH '[:find (count ?b) :where [?b :block/name]]')
echo "Total pages: $PAGES"

# TODO count
TODOS=$(logseq query $GRAPH '[:find (count ?b) :where [?b :block/marker "TODO"]]')
echo "TODO items: $TODOS"

# DOING count
DOING=$(logseq query $GRAPH '[:find (count ?b) :where [?b :block/marker "DOING"]]')
echo "DOING items: $DOING"

# DONE count
DONE=$(logseq query $GRAPH '[:find (count ?b) :where [?b :block/marker "DONE"]]')
echo "DONE items: $DONE"
```

### Use Case 5: CI/CD Documentation Testing

Test documentation links in your graph:

```bash
#!/bin/bash
# check-links.sh

# Export graph
logseq export my-docs -f docs-temp.zip

# Extract
unzip -q docs-temp.zip -d docs-temp

# Check for broken links
find docs-temp -name "*.md" -exec grep -l "http" {} \; | while read -r file; do
  echo "Checking links in $file"
  # Add your link checking logic here
done

# Cleanup
rm -rf docs-temp docs-temp.zip
```

### Use Case 6: Multi-Graph Search

Search across multiple graphs:

```bash
#!/bin/bash
# multi-search.sh

TERM=$1
GRAPHS=("notes" "work" "personal")

echo "Searching for: $TERM"
echo

for graph in "${GRAPHS[@]}"; do
  echo "=== $graph ==="
  logseq search $graph "$TERM" | head -5
  echo
done
```

### Use Case 7: Weekly Review Report

Generate a weekly summary:

```bash
#!/bin/bash
# weekly-review.sh

WEEK_AGO=$(date -d '7 days ago' +%s)000
WEEK_AGO_MS=$WEEK_AGO

echo "=== Weekly Review ==="
echo "Period: Last 7 days"
echo

# Pages created this week
echo "New pages:"
logseq query my-notes "[:find ?title :where [?b :block/created-at ?t] [(> ?t $WEEK_AGO_MS)] [?b :block/name ?n] [?b :block/title ?title]]"
echo

# Completed tasks
echo "Completed tasks:"
logseq query my-notes '[:find ?title :where [?b :block/marker "DONE"] [?b :block/title ?title]]' | head -10
```

### Use Case 8: MCP Server for AI Integration

Run an MCP server to integrate your Logseq graph with AI assistants:

**Basic Setup (Local Graph):**

```bash
#!/bin/bash
# mcp-server.sh

# Start MCP server for local graph via stdio
logseq mcp-server -g my-notes -s
```

**HTTP Server (Multiple Clients):**

```bash
#!/bin/bash
# mcp-http-server.sh

# Start HTTP MCP server
logseq mcp-server -g my-notes -p 8080

echo "MCP server running on http://localhost:8080"
echo "Press Ctrl+C to stop"
```

**With API Mode (Current Graph):**

```bash
#!/bin/bash
# mcp-api-server.sh

export LOGSEQ_API_SERVER_TOKEN=YOUR_TOKEN

# Start MCP server for current in-app graph
logseq mcp-server -a $LOGSEQ_API_SERVER_TOKEN -p 9000
```

**Docker Integration:**

```dockerfile
FROM node:22-alpine

# Install Logseq CLI
RUN npm install -g @logseq/cli

# Copy graph data
COPY ./my-graph /data/graph

# Expose MCP port
EXPOSE 8080

# Run MCP server
CMD ["logseq", "mcp-server", "-g", "/data/graph", "-p", "8080", "--host", "0.0.0.0"]
```

**Systemd Service:**

```ini
[Unit]
Description=Logseq MCP Server
After=network.target

[Service]
Type=simple
User=youruser
ExecStart=/usr/bin/logseq mcp-server -g my-notes -p 8080
Restart=always

[Install]
WantedBy=multi-user.target
```

### Use Case 9: Export for Publishing

Export and prepare for publishing:

```bash
#!/bin/bash
# publish.sh

GRAPH=$1
OUTPUT_DIR=~/public/notes

# Export to Markdown
logseq export $GRAPH -f /tmp/export.zip

# Extract
unzip -q /tmp/export.zip -d /tmp/export

# Process files (remove private tags, etc.)
find /tmp/export -name "*.md" -exec sed -i '' 's/#private//g' {} \;

# Copy to public directory
cp -r /tmp/export/* $OUTPUT_DIR/

# Cleanup
rm -rf /tmp/export /tmp/export.zip

echo "Published to $OUTPUT_DIR"
```

## Troubleshooting

### Problem: "Graph does not exist"

**Symptoms:**

```
Error: Graph "my-graph" does not exist
```

**Solutions:**

1. **Check exact graph name:**

```bash
logseq list
```

2. **Verify graph directory:**

```bash
ls ~/logseq/graphs/
```

3. **Use the exact name from `logseq list`:**

```bash
# If list shows "my-notes", use:
logseq search my-notes "test"

# NOT:
logseq search "My Notes" "test"
```

---

### Problem: "Failed to connect to HTTP API Server"

**Symptoms:**

```
Error: Failed to connect to HTTP API Server with error "ECONNREFUSED"
Make sure the HTTP API Server is turned on.
```

**Solutions:**

1. **Enable HTTP API Server in Logseq:**

   - Open Logseq
   - Settings → Advanced → Features
   - Enable "HTTP APIs server"

2. **Verify Logseq is running:**

```bash
ps aux | grep -i logseq
```

3. **Check if port 12315 is listening:**

```bash
lsof -i :12315
```

4. **Test connection:**

```bash
curl http://127.0.0.1:12315/api
```

---

### Problem: "API Server responded with status 401"

**Symptoms:**

```
Error: API Server responded with status 401
```

**Solutions:**

1. **Generate new API token:**

   - Logseq → Settings → Advanced → HTTP APIs server
   - Copy the token

2. **Verify token is correct:**

```bash
echo $LOGSEQ_API_SERVER_TOKEN
```

3. **Use token explicitly:**

```bash
logseq search "test" -a YOUR_TOKEN
```

---

### Problem: Slow Query Performance

**Symptoms:**

- Queries take 5+ seconds
- Large result sets freeze terminal

**Solutions:**

1. **Limit results:**

```bash
logseq search my-notes "test" -l 20
```

2. **Use more specific queries:**

```bash
# Instead of:
logseq query my-notes '[:find ?b :where [?b :block/title]]'

# Use:
logseq query my-notes '[:find ?b :where [?b :block/name "specific-page"]]'
```

3. **Close Logseq before local queries:**
   - SQLite locks can slow down reads
   - Close the graph in Logseq desktop app

---

### Problem: Export Creates Empty Zip

**Symptoms:**

- Export completes but zip is empty or very small
- "Exported 0 pages" message

**Solutions:**

1. **Check graph has content:**

```bash
logseq query my-notes '[:find (count ?b) :where [?b :block/name]]'
```

2. **Verify graph is not corrupted:**

```bash
logseq show my-notes
```

3. **Try EDN export instead:**

```bash
logseq export-edn my-notes
```

---

### Problem: Command Not Found

**Symptoms:**

```
bash: logseq: command not found
```

**Solutions:**

1. **Verify installation:**

```bash
npm list -g @logseq/cli
```

2. **Reinstall:**

```bash
npm uninstall -g @logseq/cli
npm install -g @logseq/cli
```

3. **Check npm global path:**

```bash
npm config get prefix
```

4. **Add to PATH (if needed):**

```bash
export PATH="$PATH:$(npm config get prefix)/bin"
```

---

### Problem: Node Version Error

**Symptoms:**

```
Error: The engine "node" is incompatible with this module
```

**Solutions:**

1. **Check Node version:**

```bash
node --version
```

2. **Upgrade Node.js:**
   - Install Node.js 22.17.0 or higher
   - Use nvm: `nvm install 22`

---

### Problem: Permission Denied

**Symptoms:**

```
Error: EACCES: permission denied
```

**Solutions:**

1. **Fix npm permissions:**

```bash
sudo chown -R $USER:$USER ~/.npm
sudo chown -R $USER:$USER /usr/local/lib/node_modules
```

2. **Install without sudo:**

```bash
npm config set prefix ~/.npm-global
export PATH=~/.npm-global/bin:$PATH
npm install -g @logseq/cli
```

## FAQ

### General Questions

**Q: What's the difference between local and API mode?**

A: Local mode reads the SQLite database directly (offline), while API mode connects to a running Logseq app via HTTP (online). Local mode is read-only but works offline. API mode can modify data but requires setup.

**Q: Can I use this with file-based graphs?**

A: The CLI is designed for database (DB) graphs only. File-based graphs are not fully supported.

**Q: Is my data safe?**

A: Local mode is read-only - it cannot modify your graphs. API mode can modify data (append command) but only through the official Logseq API.

**Q: Can I run this on CI/CD?**

A: Yes! Local mode works perfectly on CI/CD platforms like GitHub Actions, GitLab CI, etc. No Logseq app installation needed.

**Q: Does this work on Windows/Mac/Linux?**

A: Yes, it works on all platforms that support Node.js 22+.

---

### Performance Questions

**Q: Why is the first command slow?**

A: The CLI lazy-loads command namespaces. First run takes ~500ms, subsequent runs are faster (~300ms).

**Q: How can I speed up queries?**

A: Use specific queries instead of broad ones, limit results with `-l`, and close Logseq app before local queries.

**Q: Can I query multiple graphs at once?**

A: Yes, use `-g` flag:

```bash
logseq query main '[:find ?b :where [?b :block/name]]' -g graph2 -g graph3
```

---

### Search Questions

**Q: Why doesn't local search find my content?**

A: Local mode only searches `:block/title` (block titles). For full-text search, use API mode.

**Q: How do I search for exact phrases?**

A: Use API mode and quotes:

```bash
logseq search "exact phrase here" -a TOKEN
```

**Q: Can I use regular expressions?**

A: Not directly, but you can pipe results to grep:

```bash
logseq search my-notes "test" -r | grep -E "pattern"
```

---

### Export Questions

**Q: What's the difference between Markdown and EDN export?**

A: Markdown export creates human-readable `.md` files (for sharing/publishing). EDN export preserves the full graph structure (for backup/import).

**Q: Can I export specific pages only?**

A: Not currently. Both export commands export the entire graph. Use queries to extract specific pages instead.

**Q: Does export include assets (images, PDFs)?**

A: Markdown export includes asset references but not the files themselves. EDN export includes file metadata.

---

### API Questions

**Q: Where do I find my API token?**

A: Logseq → Settings → Advanced → Features → HTTP APIs server → Auth token

**Q: Is the API token safe to use?**

A: The token allows full access to your graph. Keep it private. Use environment variables instead of command-line arguments to avoid exposing it in shell history.

**Q: Can I have multiple tokens?**

A: No, Logseq generates one token per instance. You can regenerate it if needed.

**Q: What if I accidentally expose my token?**

A: Regenerate it immediately in Logseq settings. Old token will be invalidated.

---

### Query Questions

**Q: Where can I learn DataLog?**

A: See these resources:

- [Logseq Query Documentation](https://docs.logseq.com/#/page/queries)
- [DataScript Query Reference](https://github.com/tonsky/datascript/wiki/Query)
- [Learn DataLog Today](http://www.learndatalogtoday.org/)

**Q: What built-in rules are available?**

A: The CLI includes all Logseq DSL rules. Common ones:

- `(task ?b ?marker)` - Find tasks by marker
- `(block-content ?b ?query)` - Search block content
- `(page-ref ?b ?page)` - Find page references

**Q: How do I query by date?**

A: Use timestamps in milliseconds:

```bash
# Pages from last 7 days
logseq query my-notes '[:find ?title :where [?b :block/created-at ?t] [(> ?t 1704758400000)] [?b :block/title ?title]]'
```

**Q: Can I update data with queries?**

A: No, queries are read-only. Use API mode with `append` command to add data.

---

### MCP Server Questions

**Q: What is MCP and why would I use it?**

A: MCP (Model Context Protocol) is a standard protocol for connecting AI assistants to external data sources. Use the `mcp-server` command to:

- Connect Claude, ChatGPT, or other AI tools directly to your Logseq graph
- Enable AI assistants to query and understand your knowledge base
- Build custom integrations with MCP-compatible tools
- Allow multiple tools to access your graph simultaneously

**Q: Which transport mode should I use - stdio or HTTP?**

A:

- **Stdio (`-s`)**: Use when integrating with a single MCP client tool (like Claude Desktop). This is the standard mode for MCP tools.
- **HTTP (default)**: Use when you want multiple clients to connect, or when building custom integrations. Allows concurrent access from multiple tools.

**Q: Can the MCP server modify my graph?**

A:

- In **local mode** (`-g`): Read-only access. The server can query but not modify your graph.
- In **API mode** (`-a`): The server has the same permissions as the API, which may include write access depending on MCP tool configuration.

**Q: How do I connect Claude Desktop to my Logseq graph?**

A:

1. Start the MCP server in stdio mode: `logseq mcp-server -g my-notes -s`
2. Configure Claude Desktop to use this MCP server (see MCP documentation)
3. Claude will be able to query your graph directly

**Q: Can I run multiple MCP servers simultaneously?**

A: Yes, you can run multiple MCP servers for different graphs or on different ports:

```bash
# Terminal 1: Graph A on port 8080
logseq mcp-server -g graph-a -p 8080

# Terminal 2: Graph B on port 9000
logseq mcp-server -g graph-b -p 9000
```

**Q: Is the MCP server secure?**

A:

- By default, it binds to localhost (127.0.0.1) for security
- Only expose to network (`--host 0.0.0.0`) if needed and behind a firewall
- In API mode, uses the same authentication as Logseq HTTP API
- Consider using a reverse proxy with authentication for production use

---

# For Developers

## Development Setup

### Prerequisites

1. **Node.js**: Version 22.17.0 or higher
2. **Yarn**: `npm install -g yarn`
3. **Babashka**: Install from [babashka.org](https://babashka.org/)
4. **Logseq Desktop App**: Database version

### Clone and Setup

```bash
# Clone repository
git clone https://github.com/logseq/logseq.git
cd logseq/deps/cli

# Install dependencies
yarn install

# Link for local development
yarn link
```

### Development Commands

```bash
# Run CLI locally (without install)
node cli.mjs list
node cli.mjs query my-graph 123

# Run tests
yarn test

# Run specific test
yarn test -i focus

# Load all namespaces (validate requires)
bb test:load-all-namespaces-with-nbb

# Lint
bb lint:carve              # Find unused code
bb lint:large-vars         # Find large functions
bb lint:ns-docstrings      # Check namespace docs
```

### Project Structure

```
deps/cli/
├── cli.mjs                    # Node.js entry point
├── package.json               # npm package
├── bb.edn                     # Babashka tasks
├── src/
│   └── logseq/cli/
│       ├── cli.cljs           # Main namespace
│       ├── spec.cljs          # Command specs
│       ├── util.cljs          # Utilities
│       ├── commands/          # Command implementations
│       └── common/            # Shared code
├── test/                      # Tests
└── vendor/                    # Vendored dependencies
```

### Key Files

| File                             | Purpose                                |
| -------------------------------- | -------------------------------------- |
| `cli.mjs`                        | Node.js bootstrap, loads ClojureScript |
| `src/logseq/cli.cljs`            | Command routing, error handling        |
| `src/logseq/cli/spec.cljs`       | babashka.cli option specs              |
| `src/logseq/cli/util.cljs`       | API fetch, graph utilities             |
| `src/logseq/cli/commands/*.cljs` | Individual command implementations     |

## Architecture Overview

### Bootstrap Process

1. **Node.js** executes `cli.mjs`
2. **nbb-logseq** loads ClojureScript runtime
3. **Classpaths** added: `src/` and `vendor/src/`
4. **Main namespace** loaded: `logseq.cli`
5. **-main function** invoked with args

### Command Dispatch

```
User command
    ↓
cli.mjs
    ↓
logseq.cli/-main
    ↓
babashka.cli/dispatch
    ↓
Lazy load command namespace
    ↓
Execute command function
    ↓
Print results / handle errors
```

### Lazy Loading

Commands are lazy-loaded to optimize startup time:

```clojure
(defn- lazy-load-fn
  [fn-sym]
  (fn [& args]
    (p/let [_ (require (symbol (namespace fn-sym)))]
      (apply (resolve fn-sym) args))))
```

**Impact**: Initial startup ~500ms, first command +200ms, subsequent commands fast.

### Mode Selection

Commands check for `--api-server-token` to determine mode:

```clojure
(defn search [{{:keys [api-server-token]} :opts :as m}]
  (if api-server-token
    (api-search m)    ; HTTP API mode
    (local-search m))) ; SQLite mode
```

### Database Access

Local mode uses `better-sqlite3` via `logseq.db.common.sqlite-cli`:

```clojure
(let [conn (sqlite-cli/open-db! graphs-dir graph-name)]
  (d/datoms @conn :aevt :block/title))
```

### API Communication

API mode sends JSON-RPC style requests:

```clojure
(defn api-fetch [token method args]
  (js/fetch "http://127.0.0.1:12315/api"
    {:method "POST"
     :headers {"Authorization" (str "Bearer " token)
               "Content-Type" "application/json"}
     :body (js/JSON.stringify {:method method :args args})}))
```

## Testing

### Test Framework

Uses **nbb-test-runner** (nbb port of cljs.test):

```bash
# Run all tests
yarn test

# Run with focus
yarn test -i focus

# Run specific namespace
yarn test logseq.cli-test
```

### Test Structure

```clojure
(ns logseq.cli-test
  (:require [cljs.test :refer [deftest is testing]]
            [logseq.cli :as cli]))

(deftest test-something
  (testing "description"
    (is (= expected actual))))
```

### Test Files

- `test/logseq/cli_test.cljs` - Main CLI tests
- `test/logseq/cli/text_util_test.cljs` - Text utility tests

### Writing Tests

```clojure
(deftest test-cut-by
  (testing "cuts string by markers"
    (is (= ["before" "middle" "after"]
           (text-util/cut-by "before$start$middle$end$after"
                            "$start$" "$end$")))))
```

### Testing Commands

```bash
# Test against real graph
node cli.mjs list
node cli.mjs show test-graph
node cli.mjs query test-graph 123
```

### Load All Namespaces Test

Validates all requires load without errors:

```bash
bb test:load-all-namespaces-with-nbb
```

## Contributing

### Contribution Workflow

1. **Fork** the repository
2. **Create branch**: `git checkout -b feature/my-feature`
3. **Make changes**
4. **Test**: `yarn test`
5. **Lint**: Run bb lint tasks
6. **Commit**: Follow commit conventions
7. **Push**: `git push origin feature/my-feature`
8. **Pull Request**: Open PR on GitHub

### Commit Message Convention

```
type(scope): description

Examples:
feat(query): add multi-graph query support
fix(search): handle empty search results
docs(readme): update installation instructions
refactor(util): simplify api-fetch function
test(query): add entity query tests
```

### Code Style

Follow Logseq frontend practices:

- **Namespace docstrings**: Required for all public namespaces
- **Function docstrings**: For public functions
- **Max function length**: 45 lines (configurable via `:large-vars` metadata)
- **Minimize public vars**: Use `^:private` or `defn-` for internals
- **ClojureScript standard**: Follow Clojure style guide

### Linting

```bash
# Check for unused code
bb lint:carve

# Check for large functions
bb lint:large-vars

# Check namespace docstrings
bb lint:ns-docstrings

# Check for private var opportunities
bb lint:minimize-public-vars
```

### Adding New Commands

1. **Create command namespace**:

```clojure
(ns logseq.cli.commands.my-command
  "My command description"
  (:require [logseq.cli.util :as cli-util]))

(defn my-command [{{:keys [graph]} :opts}]
  (println "Executing my-command on" graph))
```

2. **Add spec** in `src/logseq/cli/spec.cljs`:

```clojure
(def my-command
  {:option1 {:alias :o
             :desc "Description"}})
```

3. **Register in table** in `src/logseq/cli.cljs`:

```clojure
{:cmds ["my-command"]
 :desc "Short description"
 :fn (lazy-load-fn 'logseq.cli.commands.my-command/my-command)
 :args->opts [:graph]
 :spec cli-spec/my-command}
```

4. **Add tests**:

```clojure
(deftest test-my-command
  (testing "my command works"
    ;; test implementation
    ))
```

5. **Update documentation**

### Vendoring Dependencies

CLI vendors nbb dependencies to avoid nbb.edn conflicts:

```bash
# Update vendored deps
bb build:vendor-nbb-deps
```

This copies dependencies from `.nbb/.cache` to `vendor/src/`.

### Release Process

1. **Update version** in `package.json`
2. **Test thoroughly**: `yarn test`
3. **Build vendor deps**: `bb build:vendor-nbb-deps`
4. **Commit**: `git commit -m "chore: bump version to X.Y.Z"`
5. **Tag**: `git tag vX.Y.Z`
6. **Push**: `git push && git push --tags`
7. **Publish**: `npm publish`

### CI/CD

GitHub Actions workflow (`.github/workflows/cli.yml`):

- Runs on: Push, PR
- Tests: `yarn test`
- Lints: bb lint tasks
- Node versions: 22.x

## Advanced Topics

### Custom Graph Locations

Override default graph directory:

```clojure
;; Modify logseq.cli.common.graph/get-db-graphs-dir
(defn get-db-graphs-dir []
  (or (System/getenv "LOGSEQ_GRAPHS_DIR")
      (node-path/join (os/homedir) "logseq" "graphs")))
```

Usage:

```bash
LOGSEQ_GRAPHS_DIR=/custom/path logseq list
```

### Extending with Custom Queries

Create query templates:

```bash
# ~/.logseq-queries.sh

alias lq-todos="logseq query my-notes '[:find ?title :where [?b :block/marker \"TODO\"] [?b :block/title ?title]]'"
alias lq-recent="logseq query my-notes '[:find ?title :where [?b :block/created-at ?t] [(> ?t DATE)] [?b :block/title ?title]]'"
```

### Integration with Other Tools

#### jq Integration

Process JSON output:

```bash
# Get API response as JSON (requires custom formatter)
logseq query '(task DOING)' -a TOKEN --format json | jq '.[] | .title'
```

#### fzf Integration

Interactive graph selection:

```bash
# Select graph with fuzzy finder
GRAPH=$(logseq list | grep "^[a-z]" | fzf)
logseq search $GRAPH "todo"
```

#### Tmux Integration

Create tmux command panel:

```bash
# .tmux.conf
bind-key C-l split-window -h "logseq search $(tmux display-message -p '#{pane_current_path}') ''"
```

### Performance Optimization

#### Connection Pooling

Reuse connections for multiple queries:

```clojure
(let [conn (sqlite-cli/open-db! graphs-dir graph-name)]
  (doseq [query queries]
    (execute-query conn query)))
```

#### Streaming Exports

Stream large exports to avoid memory issues:

```clojure
(-> zip
    (.generateNodeStream #js {:streamFiles true})
    (.pipe (fs/createWriteStream output-file)))
```

### Debugging

#### Enable nbb debug output:

```bash
NODE_DEBUG=nbb-logseq logseq query my-notes 123
```

#### Add debug prints:

```clojure
(defn my-function [args]
  (prn "DEBUG:" args)  ; Add this
  ;; rest of function
  )
```

#### Inspect DataScript database:

```bash
sqlite3 ~/logseq/graphs/logseq_db__my-graph/db.sqlite
.schema
SELECT * FROM datoms LIMIT 10;
```

### Error Handling Patterns

Custom error handler:

```clojure
(defn my-command [opts]
  (try
    ;; command logic
    (catch :default e
      (js/console.error "Command failed:" (.-message e))
      (js/process.exit 1))))
```

### Creating Plugins

Extend CLI with custom commands:

```javascript
// my-plugin.mjs
import { loadFile } from '@logseq/nbb-logseq'

const { myCommand } = await loadFile('./my-command.cljs')
myCommand(process.argv.slice(2))
```

---

## Resources

### Documentation

- [Logseq Docs](https://docs.logseq.com/)
- [DataScript Reference](https://github.com/tonsky/datascript)
- [Babashka CLI](https://github.com/babashka/cli)
- [nbb Documentation](https://github.com/babashka/nbb)

### Community

- [Logseq Forum](https://discuss.logseq.com/)
- [GitHub Issues](https://github.com/logseq/logseq/issues)
- [Discord](https://discord.gg/logseq)

### Related Projects

- [Logseq](https://github.com/logseq/logseq) - Main repository
- [logseq-plugin-samples](https://github.com/logseq/logseq-plugin-samples)
- [awesome-logseq](https://github.com/logseq/awesome-logseq)

---

## Appendix

### Exit Codes

| Code | Meaning          |
| ---- | ---------------- |
| 0    | Success          |
| 1    | Error (any kind) |

### Environment Variables

| Variable                  | Purpose              | Default |
| ------------------------- | -------------------- | ------- |
| `LOGSEQ_API_SERVER_TOKEN` | API authentication   | none    |
| `NODE_DEBUG`              | Node.js debug output | none    |

### File Locations

| Item        | Path                                  |
| ----------- | ------------------------------------- |
| DB Graphs   | `~/logseq/graphs/logseq_db__*/`       |
| File Graphs | `~/logseq/graphs/logseq_version_*/`   |
| Config      | `~/logseq/graphs/*/logseq/config.edn` |
| Database    | `~/logseq/graphs/*/db.sqlite`         |

### Known Issues

See [GitHub Issues](https://github.com/logseq/logseq/issues?q=is%3Aissue+label%3Acli)

---

**Document Version**: 1.2
**Last Updated**: 2025-11-20
**CLI Version**: 0.4.0
**Feedback**: [GitHub Issues](https://github.com/logseq/logseq/issues)

**License**: MIT

**Changes in v1.2:**

- Updated for @logseq/cli version 0.4.0
- Added documentation for `mcp-server` command
- Added local path support for graph arguments
- Added MCP server use cases and FAQ entries
