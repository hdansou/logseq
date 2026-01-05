# Logseq CLI Commands Reference

**Logseq Version:** 0.4.0

This document provides detailed help for all Logseq CLI commands.

---

## **list**

```bash
Usage: logseq list
```

**Description:**

List all DB and file graphs on your system.

**Examples:**

```bash
# List all graphs
logseq list
```

**Sample Output:**

```text
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

## **show**

```bash
Usage: logseq show [graphs]
```

**Description:**

For each graph, prints information related to a graph's creation and anything that is helpful for debugging.

**Examples:**

```bash
# Show info for a single graph
logseq show my-notes

# Show info for multiple graphs
logseq show my-notes work-journal personal
```

**Sample Output:**

```text
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

## **search**

```bash
Usage: logseq search [search-terms] [options]
```

**Options:**

- `-a, --api-server-token` - API server token to search current graph
- `-g, --graph` - Local graph to search
- `-r, --raw` - Print raw response
- `-l, --limit` - Limit max number of results (default: 100)

**Description:**

Search a local graph or the current in-app graph if --api-server-token is given. For a local graph it searches the `:block/title` attribute of all blocks and pages.

**Examples:**

```bash
# Basic search in local graph
logseq search -g my-notes "meeting notes"

# Multi-word search
logseq search -g my-notes "project planning 2025"

# API mode - search current graph
logseq search "todo urgent" -a YOUR_TOKEN

# Limit results to 20
logseq search -g my-notes "javascript" -l 20

# Raw output (for parsing with jq or other tools)
logseq search -g my-notes "test" -r
```

**Using Environment Variable:**

```bash
# Set token once
export LOGSEQ_API_SERVER_TOKEN=YOUR_TOKEN

# Then search without -a flag
logseq search "meeting notes"
```

**Sample Output:**

```text
Search found 3 results:
Project meeting notes from Q4
Team meeting notes - Sprint Planning
Client meeting notes - Discovery
```

**Tips:**

- Local mode searches `:block/title` attribute (both pages and blocks)
- API mode searches full content (including block text)
- Use quotes for multi-word phrases
- Search is case-sensitive

---

## **query**

```bash
Usage: logseq query [args] [options]
```

**Options:**

- `-a, --api-server-token` - API server token to query current graph
- `-g, --graphs` - Local graph(s) to query
- `-p, --properties-readable` - Make properties on local, entity queries show property values instead of ids
- `-t, --title-query` - Invoke local query on :block/title

**Description:**

Query a local graph or the current in-app graph if --api-server-token is given. For a local graph, queries are a datalog query or an entity query. A datalog query can use built-in rules. An entity query consists of one or more integers, uuids or :db/ident keywords. For an in-app query, queries can be an advanced or simple query.

**Examples:**

```bash
# Entity query by ID
logseq query -g my-notes 123

# Entity query by keyword (built-in class)
logseq query -g my-notes :logseq.class/Tag

# Entity query by UUID
logseq query -g my-notes "a1b2c3d4-e5f6-7890-abcd-ef1234567890"

# DataLog query - Find all pages
logseq query -g my-notes '[:find ?title :where [?b :block/title ?title] [?b :block/name]]'

# DataLog query - Find blocks with TODO status
logseq query -g my-notes '[:find ?title :where [?b :logseq.property/status ?s] [?s :db/ident :logseq.property/status.todo] [?b :block/title ?title]]'

# DataLog query - Find blocks with DOING status
logseq query -g my-notes '[:find (pull ?b [*]) :where [?b :logseq.property/status ?s] [?s :db/ident :logseq.property/status.doing]]'

# Find pages with a specific tag/class
logseq query -g my-notes '[:find ?title :where [?b :block/tags ?tag] [?tag :db/ident :logseq.class/Project] [?b :block/title ?title]]'

# Find blocks with a specific property value
logseq query -g my-notes '[:find ?b :where [?b :logseq.property/status ?status] [?status :db/ident :logseq.property/status.doing]]'

# Multi-graph query
logseq query -g main-graph '[:find ?b :where [?b :block/name]]' -g graph2 -g graph3

# Entity query with readable properties (shows property values instead of entity IDs)
logseq query -g my-notes 10 -p

# API mode - simple query (finds tasks with DOING status)
logseq query '(task DOING)' -a YOUR_TOKEN

# API mode - DataLog query (finds blocks with TODO status)
logseq query '[:find (pull ?b [*]) :where [?b :logseq.property/status ?s] [?s :db/ident :logseq.property/status.todo]]' -a YOUR_TOKEN
```

**Sample Output (Entity Query):**

```clojure
{:db/id 123
 :block/title "My Page"
 :block/name "my-page"
 :block/created-at 1704844800000
 :block/updated-at 1704844800000}
```

**Sample Output (DataLog Query):**

```clojure
["Page 1"
 "Page 2"
 "Page 3"]
```

**Common Queries for DB Graphs:**

```bash
# All pages (excluding journal pages)
logseq query -g my-notes '[:find ?title :where [?b :block/title ?title] [?b :block/name] (not [?b :block/journal-day])]'

# All journal pages
logseq query -g my-notes '[:find ?title :where [?b :block/title ?title] [?b :block/journal-day]]'

# Blocks with TODO status
logseq query -g my-notes '[:find ?title :where [?b :logseq.property/status ?s] [?s :db/ident :logseq.property/status.todo] [?b :block/title ?title]]'

# Pages tagged with a specific class
logseq query -g my-notes '[:find ?title :where [?b :block/tags ?tag] [?tag :db/ident :logseq.class/Project] [?b :block/title ?title]]'

# Blocks with "Doing" status property
logseq query -g my-notes '[:find ?title :where [?b :logseq.property/status ?s] [?s :db/ident :logseq.property/status.doing] [?b :block/title ?title]]'

# Recent pages (created in last 7 days)
logseq query -g my-notes '[:find ?title :where [?b :block/created-at ?t] [(> ?t 1704758400000)] [?b :block/title ?title] [?b :block/name]]'
```

**DB Schema Key Attributes:**

- **`:block/title`** - Block or page content/title
- **`:block/name`** - Normalized page name (lowercase, hyphenated, only on pages)
- **`:block/tags`** - Classes/tags (entity references)
- **`:block/journal-day`** - Journal date in YYYYMMDD format (only on journal pages)
- **`:db/ident`** - Unique identifier for built-in entities (classes, properties, closed values)
- **`:logseq.property/status`** - Task status property (references closed values like `:logseq.property/status.todo`, `:logseq.property/status.doing`, `:logseq.property/status.done`)
- **`:logseq.property/priority`** - Task priority property (references `:logseq.property/priority.low`, `.medium`, `.high`, `.urgent`)
- **`:logseq.property/deadline`** - Task deadline datetime
- **`:logseq.property/scheduled`** - Task scheduled datetime

**Tips:**

- DataLog queries must start with `[:find ...]`
- Use single quotes to prevent shell interpretation
- Built-in DSL rules are automatically included (e.g., `task`, `priority`, `between`)
- Results are pretty-printed by default
- Pages have `:block/name` (normalized) and `:block/title` (display name)
- Task status uses `:logseq.property/status` with closed values (`.todo`, `.doing`, `.done`, `.canceled`, etc.)
- Classes/tags are stored in `:block/tags` as entity references
- Properties reference closed values via `:db/ident`
- Use `-p` flag to make property values readable (shows `:db/ident` instead of entity objects)

---

## **export**

```bash
Usage: logseq export [options]
```

**Options:**

- `-g, --graph` - Local graph to export
- `-f, --file` - File to save export

**Description:**

Export a local graph to Markdown like the in-app graph export.

**Examples:**

```bash
# Basic export (creates timestamped zip file)
logseq export -g my-notes

# Export with custom filename
logseq export -g my-notes -f backup-2025-01.zip

# Export to specific path
logseq export -g my-notes -f ~/backups/notes.zip
```

**Sample Output:**

```text
Exported 234 pages to my-notes_markdown_1704844800.zip
```

**What's Included:**

- All pages as individual `.md` files
- Respects your graph's export settings from `logseq/config.edn`
- Preserves indentation preferences (tab, 2/4/8 spaces)
- Includes page properties

**Use Cases:**

- Backup your entire graph
- Share notes with others
- Import into other tools
- Archive old graphs

---

## **export-edn**

```bash
Usage: logseq export-edn [options]
```

**Options:**

- `-t, --export-type` - Export type (default: :graph)
- `-e, --exclude-namespaces` - Namespaces to exclude from properties and classes
- `-f, --file` - File to save export
- `-a, --api-server-token` - API server token to export current graph
- `-g, --graph` - Local graph to export
- `-T, --include-timestamps?` - Include timestamps in export
- `-c, --catch-validation-errors?` - Catch validation errors for dev
- `-F, --exclude-files?` - Exclude :file/path files
- `-b, --exclude-built-in-pages?` - Exclude built-in pages

**Description:**

Export a local graph to EDN or the current in-app graph if --api-server-token is given. See <https://github.com/logseq/docs/blob/master/db-version.md#edn-data-export> for more about this export type.

**Export Types:**

- **`:graph`** (default) - Complete graph backup with all pages, blocks, properties, and classes
- **`:graph-ontology`** - Schema only (properties and classes, no content)

**Examples:**

```bash
# Basic local export (full graph)
logseq export-edn -g my-notes

# Export with custom filename
logseq export-edn -g my-notes -f backup.edn

# API mode - export current graph
logseq export-edn -a YOUR_TOKEN -f backup.edn

# Export schema only (properties and classes)
logseq export-edn -g my-notes -t :graph-ontology

# Include timestamps
logseq export-edn -g my-notes -T

# Exclude system/built-in pages
logseq export-edn -g my-notes -b

# Exclude specific namespaces
logseq export-edn -g my-notes -e logseq.class -e logseq.property

# Exclude file paths
logseq export-edn -g my-notes -F

# Full export with all options
logseq export-edn -g my-notes -f full-backup.edn -T -b -F
```

**Sample Output:**

```text
Exported 45 properties, 12 classes and 234 pages to my-notes_1704844800.edn
```

**EDN Structure:**

Full graph export:

```clojure
{:properties [...]        ; Property definitions
 :classes [...]           ; Class definitions
 :pages-and-blocks [...]  ; All pages and blocks
 :logseq.db.sqlite.export/export-type :graph
 :logseq.db.sqlite.export/schema-version [1 2]}
```

Schema-only export:

```clojure
{:properties [...]        ; User properties only
 :classes [...]           ; User classes/tags only
 :logseq.db.sqlite.export/export-type :graph-ontology}
```

**Use Cases:**

- Full graph backups (`:graph` type)
- Graph migration between systems
- Schema sharing without content (`:graph-ontology`)
- Development and testing with realistic structure
- Graph analysis and external processing

---

## **import-edn**

```bash
Usage: logseq import-edn [options]
```

**Options:**

- `-a, --api-server-token` - API server token to import into current graph
- `-g, --graph` - Local graph to import into
- `-f, --file` - EDN File to import

**Description:**

Import with EDN into a local graph or the current in-app graph if --api-server-token is given. See <https://github.com/logseq/docs/blob/master/db-version.md#edn-data-export> for more about this import type.

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

**Sample Output:**

```text
Imported 45 properties, 12 classes and 234 pages!
```

**Expected EDN Structure:**

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
- Use `validate` command after import to check data integrity
- For large imports, local mode (`-g`) is faster than API mode (`-a`)

---

## **append**

```bash
Usage: logseq append [args] [options]
```

**Options:**

- `-a, --api-server-token` - API server token to modify current graph

**Description:**

Append text to current page of current in-app graph.

**Requirements:**

- Logseq app must be running
- HTTP API Server must be enabled in settings
- A page must be open in the app

**Examples:**

```bash
# Append a single block
logseq append "TODO Review pull request" -a YOUR_TOKEN

# Multi-word content (quotes optional)
logseq append Meeting notes from today -a YOUR_TOKEN

# Using environment variable
export LOGSEQ_API_SERVER_TOKEN=YOUR_TOKEN
logseq append "New idea: Implement feature X"

# Append formatted content
logseq append "## Project Update" -a YOUR_TOKEN
logseq append "- Completed task A" -a YOUR_TOKEN
logseq append "- In progress: task B" -a YOUR_TOKEN
```

**Sample Output:**

```text
Success!
```

**Use Cases:**

- Quick capture from terminal
- Automation scripts adding content
- CLI-based workflows
- Integration with other command-line tools

**Tips:**

- The block is added to the currently active page in Logseq
- Content supports markdown formatting
- Can be used in scripts for automated note-taking

---

## **mcp-server**

```bash
Usage: logseq mcp-server [options]
```

**Options:**

- `-a, --api-server-token` - API server token to connect to current graph
- `-g, --graph` - Local graph to use with MCP server
- `-s, --stdio` - Run the MCP server via stdio transport
- `-p, --port` - Port for streamable HTTP server (default: 12315)
- `--host` - Host for streamable HTTP server (default: 127.0.0.1)
- `-t, --debug-tool` - Debug mcp tool with direct invocation

**Description:**

Run a MCP server against a local graph if --graph is given or against the current in-app graph. For the in-app graph, the API server must be on in the app. By default the MCP server runs as a HTTP Streamable server. Use --stdio to run it as a stdio server.

**Examples:**

```bash
# Run MCP server for local graph (HTTP mode, default)
logseq mcp-server -g my-notes

# Run MCP server via stdio (for Claude Desktop integration)
logseq mcp-server -g my-notes -s

# Run MCP server for current in-app graph
logseq mcp-server -a YOUR_TOKEN

# Run HTTP MCP server on custom port
logseq mcp-server -g my-notes -p 8080

# Run with custom host and port (network accessible)
logseq mcp-server -g my-notes --host 0.0.0.0 -p 9000

# Debug a specific MCP tool
logseq mcp-server -g my-notes -t listPages
```

**Sample Output (HTTP mode):**

```text
MCP Streamable HTTP Server started on 127.0.0.1:12315
```

**Transport Modes:**

1. **HTTP Streamable Server (Default):**

   - Runs as HTTP server
   - Can be accessed by multiple clients simultaneously
   - Use `-p` to customize port
   - Use `--host` to customize bind address
   - URL: `http://127.0.0.1:12315/mcp`

2. **Stdio Transport (`-s`):**
   - Communicates via standard input/output
   - Single-client mode
   - Typically used by MCP-compatible tools like Claude Desktop
   - Required for direct tool integration

**MCP Capabilities:**

The server exposes these tools:

- `getPage` - Retrieve page content
- `listPages` - List all pages
- `listProperties` - List all properties
- `listTags` - List all tags
- `upsertNodes` - Create or update nodes (local mode only)
- Execute DataScript queries
- Search across pages and blocks

**Use Cases:**

- **AI Assistant Integration**: Connect Claude, ChatGPT, or other AI tools
- **Custom Tools**: Build MCP clients that interact with your knowledge base
- **Automation**: Create workflows that query and modify your graph
- **Multi-Tool Access**: Run HTTP server for multiple MCP clients

**Integration Example (Claude Desktop):**

Edit `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "logseq": {
      "command": "logseq",
      "args": ["mcp-server", "-g", "my-notes", "-s"]
    }
  }
}
```

**Requirements:**

- For API mode (`-a`): Logseq app running with HTTP API enabled
- For local mode (`-g`): Direct access to graph database files
- MCP-compatible client application

**Important Notes:**

- Server runs continuously until terminated (Ctrl+C)
- In API mode, changes in Logseq app are immediately available
- In local mode, the server has read-only access to the graph
- For security, bind to localhost (127.0.0.1) unless network access needed

---

## **validate**

```bash
Usage: logseq validate [options]
```

**Options:**

- `-g, --graphs` - Local graph(s) to validate

**Description:**

Validate a local DB graph. Exit 1 if there are validation errors.

**Examples:**

```bash
# Validate a single graph
logseq validate -g my-notes

# Validate multiple graphs
logseq validate -g my-notes -g work-journal -g personal
```

**Sample Output (Valid Graph):**

```text
Read graph my-notes with counts: {:properties 45, :classes 12, :pages 234, :blocks 1523, :datoms 15234}
Valid!
```

**Sample Output (Invalid Graph):**

```text
Read graph my-notes with counts: {:properties 45, :classes 12, :pages 234, :blocks 1523, :datoms 15234}
Found 3 entities with errors:
[{:db/id 8763
  :block/order ["invalid dispatch value"]
  :errors {:dispatch-key nil, :errors {39651 ["invalid dispatch value"]}}}
 {:db/id 1404
  :block/uuid ["invalid dispatch value"]
  :errors {:dispatch-key nil, :errors {40799 ["invalid dispatch value"]}}}]
```

**Use Cases:**

- Check data integrity after importing EDN
- Debug graph corruption issues
- Validate before creating backups
- Quality assurance for automated workflows
- Development and testing
- CI/CD pipeline validation

**Technical Details:**

- Uses Malli schema validation
- Checks closed-value validation for data quality
- Reports entity IDs with specific error details
- Exit code 0 for valid graphs, 1 for invalid graphs
- Can be integrated into scripts and pipelines

**Best Practices:**

- Always validate after `import-edn` operations
- Run periodically on production graphs
- Use in CI/CD to catch data quality issues
- Check before major migrations

---

## **help**

```bash
Usage: logseq help [command]
```

**Description:**

Print a command's help.

**Examples:**

```bash
# Get general help
logseq --help

# Get help for a specific command
logseq help search
logseq help query
logseq help export-edn
logseq help mcp-server

# Get version
logseq --version
```

**Sample Output:**

```text
Usage: logseq search [search-terms] [options]

Options:
  -a, --api-server-token     API server token to search current graph
  -g, --graph                Local graph to search
  -r, --raw                  Print raw response
  -l, --limit            100 Limit max number of results

Description:
Search a local graph or the current in-app graph if --api-server-token is
given. For a local graph it only searches the :block/title of blocks.
```
