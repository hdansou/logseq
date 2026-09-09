---
title: 'Model Context Protocol (MCP) Integration'
status: 🟡 Active Development
version: 'Since v0.10.9+'
quadrant: 'Tutorial'
platform: 'Desktop + CLI'
related-docs:
  - '[CLI Development Guide](../../reference/tools/cli-reference.md)'
  - '[Database Properties](../development/db-properties.md)'
  - '[Plugin Development](./first-plugin.md)'
tags: ['mcp', 'ai', 'claude', 'integration', 'cli', 'api']
---

# Model Context Protocol (MCP) Integration

This tutorial explains how to integrate Logseq with AI assistants using the Model Context Protocol (MCP). MCP enables Claude Desktop, Cline, and other AI tools to read, search, and modify your Logseq graphs directly.

**What You'll Learn:**

- Understanding MCP and Logseq's MCP server
- Setting up MCP with Claude Desktop
- Available MCP tools and their capabilities
- Using MCP from the Logseq CLI
- Advanced MCP integration patterns

**Prerequisites:**

- Logseq v0.10.9+ installed
- Node.js 18+ for CLI usage
- Claude Desktop (for Claude integration)
- Basic command-line knowledge

**Time Required:** ~20 minutes

---

## Table of Contents

1. [Understanding MCP](#1-understanding-mcp)
2. [MCP Architecture in Logseq](#2-mcp-architecture-in-logseq)
3. [Setting Up MCP Server](#3-setting-up-mcp-server)
4. [Integrating with Claude Desktop](#4-integrating-with-claude-desktop)
5. [Available MCP Tools](#5-available-mcp-tools)
6. [Using MCP from CLI](#6-using-mcp-from-cli)
7. [Advanced Usage](#7-advanced-usage)
8. [Troubleshooting](#8-troubleshooting)

---

## 1. Understanding MCP

### What is MCP?

The **Model Context Protocol** (MCP) is an open standard for connecting AI assistants to external data sources and tools. It provides a structured way for LLMs to:

- **Read data** from applications (pages, blocks, properties)
- **Write data** to applications (create/update content)
- **Search** across datasets
- **Execute operations** with proper validation

**Key Benefits:**

- ✅ **Standardized Interface:** One protocol for multiple AI tools
- ✅ **Bidirectional:** AI can both read and write to Logseq
- ✅ **Structured Data:** JSON-based communication with schema validation
- ✅ **Safe Operations:** Validation and dry-run support

### MCP vs Other Integrations

| Feature                     | MCP                        | Plugin API | CLI        |
| --------------------------- | -------------------------- | ---------- | ---------- |
| **AI Assistant Access**     | ✅ Yes                     | ❌ No      | ⚠️ Manual  |
| **Bidirectional (R/W)**     | ✅ Yes                     | ✅ Yes     | ✅ Yes     |
| **Requires Logseq Running** | ⚠️ Desktop: Yes<br>CLI: No | ✅ Yes     | ❌ No      |
| **Real-time Updates**       | ✅ Yes (Desktop)           | ✅ Yes     | ❌ No      |
| **Remote Access**           | ✅ HTTP mode               | ❌ No      | ⚠️ Via API |

---

## 2. MCP Architecture in Logseq

### Architecture Overview

```
┌────────────────────────────────────────────────────────┐
│  AI Assistant (Claude Desktop, Cline, etc.)           │
│  ┌──────────────────────────────────────────────────┐ │
│  │  MCP Client                                      │ │
│  └──────────┬───────────────────────────────────────┘ │
└─────────────┼──────────────────────────────────────────┘
              │ MCP Protocol (JSON-RPC)
              │ Transport: STDIO or HTTP
              ▼
┌────────────────────────────────────────────────────────┐
│  Logseq MCP Server                                     │
│  ┌──────────────────────────────────────────────────┐ │
│  │  Tools:                                          │ │
│  │  - listPages()    - getPage()                    │ │
│  │  - searchBlocks() - upsertNodes()                │ │
│  │  - listTags()     - listProperties()             │ │
│  └──────────┬───────────────────────────────────────┘ │
└─────────────┼──────────────────────────────────────────┘
              │
              ▼
┌────────────────────────────────────────────────────────┐
│  Data Layer                                            │
│  ┌────────────┐        ┌────────────────────────────┐ │
│  │  CLI Mode  │───OR───│  API Mode (Desktop)        │ │
│  │  (Direct   │        │  (IPC to running Logseq)   │ │
│  │   SQLite)  │        │                            │ │
│  └────────────┘        └────────────────────────────┘ │
└────────────────────────────────────────────────────────┘
```

### Two Operating Modes

**1. CLI Mode (Standalone)**

- MCP server reads directly from SQLite database
- No running Logseq instance required
- Read-only for safety (unless `--write` flag used)
- Best for: Bulk operations, AI analysis, background processing

**2. API Mode (Desktop Integration)**

- MCP server communicates with running Logseq app via IPC
- Real-time updates reflected immediately in UI
- Full read/write capabilities
- Best for: Interactive AI assistance, live editing

---

## 3. Setting Up MCP Server

### Desktop: Enable MCP in Settings

1. **Open Settings** → **Advanced**
2. **Enable "MCP Server"** toggle
3. **Restart Logseq**

The MCP server will now listen for connections from AI assistants.

![MCP Settings](./assets/mcp-settings.png)

**Default Configuration:**

- **Host:** `localhost` (127.0.0.1)
- **Port:** `12315` (HTTP mode)
- **Transport:** HTTP with session management

---

### CLI: Install Logseq CLI

```bash
# Install globally via npm
npm install -g @logseq/cli

# Verify installation
logseq-cli --version
```

**Supported Platforms:**

- ✅ macOS (Intel & Apple Silicon)
- ✅ Linux (x64, arm64)
- ✅ Windows 10+ (x64)

---

### CLI: Start MCP Server

#### Option 1: STDIO Mode (for Claude Desktop)

```bash
# Start MCP server with stdio transport
logseq-cli mcp-server \
  --graph /path/to/your/graph \
  --stdio
```

**Use Case:** Claude Desktop, Cline, other MCP clients expecting stdio

#### Option 2: HTTP Mode (for remote/web access)

```bash
# Start HTTP server
logseq-cli mcp-server \
  --graph /path/to/your/graph \
  --host 127.0.0.1 \
  --port 12315
```

**Use Case:** Testing, web clients, remote AI services

#### Option 3: API Mode (connect to running Logseq)

```bash
# Connect to running Logseq Desktop instance
logseq-cli mcp-server \
  --api-server-token YOUR_TOKEN \
  --stdio
```

**How to get API token:**

1. Open Logseq → Settings → Advanced
2. Enable "HTTP API Server"
3. Copy the displayed token

---

## 4. Integrating with Claude Desktop

### Step 1: Configure Claude Desktop

Edit Claude's MCP configuration file:

**macOS:**

```bash
nano ~/Library/Application\ Support/Claude/claude_desktop_config.json
```

**Windows:**

```bash
notepad %APPDATA%\Claude\claude_desktop_config.json
```

**Linux:**

```bash
nano ~/.config/Claude/claude_desktop_config.json
```

### Step 2: Add Logseq MCP Server

```json
{
  "mcpServers": {
    "logseq": {
      "command": "logseq-cli",
      "args": [
        "mcp-server",
        "--graph",
        "/Users/yourname/Documents/logseq-graph",
        "--stdio"
      ]
    }
  }
}
```

**⚠️ Important:** Replace `/Users/yourname/Documents/logseq-graph` with your actual graph path.

**To find your graph path:**

```bash
# macOS/Linux
ls ~/Documents/logseq-*

# Or open Logseq → Graph → Show graph folder
```

### Step 3: Restart Claude Desktop

Close and reopen Claude Desktop. You should see a small 🔨 hammer icon indicating MCP tools are available.

![Claude with MCP](./assets/claude-mcp-enabled.png)

### Step 4: Test the Integration

In Claude, type:

```
List all pages in my Logseq graph
```

Claude should call the `listPages` tool and display your pages.

---

## 5. Available MCP Tools

### Tool 1: `listPages`

List all pages in the graph.

**Input Schema:**

```typescript
{
  expand?: boolean  // Include additional page details (default: false)
}
```

**Example Usage (Claude):**

```
Show me all my pages in Logseq
```

**Example Response:**

```json
{
  "pages": [
    { "name": "Project Alpha", "uuid": "..." },
    { "name": "Daily Notes", "uuid": "..." },
    { "name": "Reading List", "uuid": "..." }
  ]
}
```

**With `expand: true`:**

```json
{
  "pages": [
    {
      "name": "Project Alpha",
      "uuid": "65f2...",
      "createdAt": 1704067200000,
      "updatedAt": 1735689600000,
      "properties": { "status": "active" }
    }
  ]
}
```

---

### Tool 2: `getPage`

Get a page's content including all its blocks.

**Input Schema:**

```typescript
{
  pageName: string // Page name or UUID
}
```

**Example Usage (Claude):**

```
Show me the content of my "Project Alpha" page
```

**Example Response:**

```json
{
  "page": {
    "name": "Project Alpha",
    "uuid": "65f2...",
    "blocks": [
      {
        "uuid": "abc1...",
        "title": "Project goals",
        "children": [
          { "uuid": "def2...", "title": "Goal 1: Launch MVP" },
          { "uuid": "ghi3...", "title": "Goal 2: Reach 1000 users" }
        ]
      }
    ]
  }
}
```

**⚠️ Note:** Properties and tags are also pages, so you can fetch them with `getPage`.

---

### Tool 3: `searchBlocks`

Search for blocks containing specific text.

**Input Schema:**

```typescript
{
  searchTerm: string // Text to search for
}
```

**Example Usage (Claude):**

```
Search my Logseq for "machine learning"
```

**Example Response:**

```json
{
  "blocks": [
    {
      "uuid": "abc1...",
      "content": "Learning about machine learning models",
      "page": 123
    },
    {
      "uuid": "def2...",
      "content": "TODO Read machine learning paper",
      "page": 456
    }
  ]
}
```

---

### Tool 4: `listTags`

List all tags (classes) in the graph.

**Input Schema:**

```typescript
{
  expand?: boolean  // Include tag details (parents, properties)
}
```

**Example Usage (Claude):**

```
What tags do I have in my graph?
```

**Example Response (expanded):**

```json
{
  "tags": [
    {
      "name": "Project",
      "uuid": "tag1...",
      "extends": [], // Parent tags
      "properties": ["status", "priority"]
    },
    {
      "name": "Task",
      "uuid": "tag2...",
      "extends": ["00000002-1282-1814-5700-000000000000"], // Built-in Task class
      "properties": ["due-date", "assigned-to"]
    }
  ]
}
```

---

### Tool 5: `listProperties`

List all properties in the graph.

**Input Schema:**

```typescript
{
  expand?: boolean  // Include property schema (type, cardinality)
}
```

**Example Usage (Claude):**

```
Show me all properties in my graph
```

**Example Response (expanded):**

```json
{
  "properties": [
    {
      "name": "status",
      "uuid": "prop1...",
      "type": "default",
      "cardinality": "one"
    },
    {
      "name": "tags",
      "uuid": "prop2...",
      "type": "node",
      "cardinality": "many"
    }
  ]
}
```

---

### Tool 6: `upsertNodes`

Create or edit pages, blocks, tags, and properties.

**⚠️ CRITICAL:** This tool is powerful and can modify your graph. Always use `dry-run: true` first!

**Input Schema:**

```typescript
{
  operations: Array<{
    operation: "add" | "edit"
    entityType: "block" | "page" | "tag" | "property"
    id: string | null  // UUID for edit, temp ID for add
    data: {
      title?: string           // Block content or page name
      "page-id"?: string       // Parent page UUID (for blocks)
      tags?: string[]          // Tag UUIDs to apply
      "property-type"?: string
      "property-cardinality"?: "one" | "many"
      ...
    }
  }>
  "dry-run"?: boolean  // Test without committing (default: false)
}
```

**Example Usage (Claude):**

```
Create a new page called "Meeting Notes" with a block "Discuss Q1 goals"
```

**MCP Tool Call:**

```json
{
  "operations": [
    {
      "operation": "add",
      "entityType": "page",
      "id": "temp-meeting-notes",
      "data": { "title": "Meeting Notes" }
    },
    {
      "operation": "add",
      "entityType": "block",
      "id": null,
      "data": {
        "page-id": "temp-meeting-notes",
        "title": "Discuss Q1 goals"
      }
    }
  ],
  "dry-run": false
}
```

**⚠️ Best Practices:**

1. Always use `"dry-run": true` first to validate
2. Use `getPage` before `upsertNodes` to check existence
3. Use temporary IDs (e.g., `"temp-inbox"`) for references
4. Only call once per user request (not in loops)

---

## 6. Using MCP from CLI

### Debug Individual Tools

The `--debug-tool` flag lets you test MCP tools without starting the server:

```bash
# List all pages
logseq-cli mcp-server \
  --graph /path/to/graph \
  --debug-tool listPages

# Get specific page
logseq-cli mcp-server \
  --graph /path/to/graph \
  --debug-tool getPage \
  --pageName "Project Alpha"

# Search blocks
logseq-cli mcp-server \
  --graph /path/to/graph \
  --debug-tool searchBlocks \
  --searchTerm "TODO"
```

**Output Format:** JSON (can be piped to `jq` for processing)

```bash
# Pretty-print with jq
logseq-cli mcp-server \
  --graph ~/logseq \
  --debug-tool listPages | jq .
```

---

### Dry-Run Node Upserts

Test `upsertNodes` operations without modifying the database:

```bash
# Test creating a page (JSON input)
echo '{"operations":[{"operation":"add","entityType":"page","id":"temp-test","data":{"title":"Test Page"}}],"dry-run":true}' | \
logseq-cli mcp-server \
  --graph ~/logseq \
  --debug-tool upsertNodes
```

**Expected Response:**

```json
{
  "result": "Success (dry-run, no changes committed)",
  "operations": [...]
}
```

---

### HTTP Mode Testing

Start HTTP server and test with `curl`:

```bash
# Terminal 1: Start server
logseq-cli mcp-server \
  --graph ~/logseq \
  --host 127.0.0.1 \
  --port 12315

# Terminal 2: Test with curl
curl -X POST http://127.0.0.1:12315/mcp \
  -H "Content-Type: application/json" \
  -d '{
    "jsonrpc": "2.0",
    "method": "initialize",
    "params": {
      "protocolVersion": "2025-03-26",
      "capabilities": {},
      "clientInfo": { "name": "test-client", "version": "1.0.0" }
    },
    "id": 1
  }'
```

---

## 7. Advanced Usage

### Use Case 1: AI-Powered Page Summaries

**Prompt to Claude:**

```
Summarize all pages with the "Project" tag in my Logseq graph
```

**What Claude Does:**

1. Calls `listPages` to get all pages
2. Filters pages with `tag: Project`
3. Calls `getPage` for each project page
4. Generates summaries and presents them

---

### Use Case 2: Bulk Task Creation

**Prompt to Claude:**

```
Create a page called "Q1 Goals" with these tasks:
- Launch MVP
- Reach 1000 users
- Improve onboarding
```

**What Claude Does:**

1. Calls `getPage("Q1 Goals")` to check if it exists
2. If not found, calls `upsertNodes` with operations:
   - Create page "Q1 Goals"
   - Add 3 blocks with TODO markers
   - Apply Task tag to each block

---

### Use Case 3: Graph Analysis

**Prompt to Claude:**

```
Analyze my Logseq graph and tell me:
- How many pages I have
- My most common tags
- How many TODO items are incomplete
```

**What Claude Does:**

1. Calls `listPages` and counts
2. Calls `listTags(expand: true)` for tag statistics
3. Calls `searchBlocks("TODO")` to find incomplete tasks
4. Presents analysis with insights

---

## 8. Troubleshooting

### Issue: MCP Tools Not Appearing in Claude

**Symptoms:** No 🔨 hammer icon in Claude Desktop

**Solutions:**

1. **Check CLI installation:**

```bash
which logseq-cli
logseq-cli --version
```

2. **Verify config path:**

```bash
# macOS
cat ~/Library/Application\ Support/Claude/claude_desktop_config.json
```

3. **Test MCP server manually:**

```bash
logseq-cli mcp-server --graph ~/logseq --stdio
# Should wait for input (don't see errors)
```

4. **Check Claude logs:**

```bash
# macOS
tail -f ~/Library/Logs/Claude/mcp*.log
```

---

### Issue: "Graph not found" Error

**Symptoms:** MCP server fails to start with "Graph does not exist"

**Solutions:**

1. **Verify graph path:**

```bash
ls /path/to/your/graph
# Should show: logseq/ pages/ etc.
```

2. **Use absolute paths (not ~):**

```json
{
  "args": ["--graph", "/Users/yourname/Documents/logseq", "--stdio"]
}
```

3. **Check graph is database-based:**

```bash
ls /path/to/graph/logseq/graphs-txid
# Should exist for DB graphs
```

---

### Issue: "Permission Denied" on SQLite

**Symptoms:** `SQLITE_CANTOPEN` error in CLI mode

**Solutions:**

```bash
# Check file permissions
ls -la /path/to/graph/logseq/graphs-txid/*.db

# Fix permissions
chmod 644 /path/to/graph/logseq/graphs-txid/*.db
```

---

### Issue: Stale Data in CLI Mode

**Symptoms:** CLI mode shows outdated data while Logseq is running

**Explanation:** CLI reads directly from SQLite, which may not reflect in-memory changes.

**Solution:** Use API mode instead:

```bash
logseq-cli mcp-server \
  --api-server-token YOUR_TOKEN \
  --stdio
```

---

### Issue: HTTP Mode Session Errors

**Symptoms:** "Invalid or missing session ID" in HTTP mode

**Solutions:**

1. **Initialize properly:**

```bash
# Send initialize request first
curl -X POST http://localhost:12315/mcp \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"initialize",...}'
```

2. **Store and reuse session ID:**

```bash
# Extract mcp-session-id from response headers
# Include in subsequent requests
```

3. **Check DNS rebinding protection:**

```javascript
// Server config includes:
allowedHosts: ['localhost:12315']
```

---

## Next Steps

**Congratulations!** You've integrated Logseq with AI assistants via MCP. 🎉

### Continue Learning:

1. **[CLI Development Guide](../../reference/tools/cli-reference.md)** - Full CLI reference
2. **[Database Properties](../development/db-properties.md)** - DB schema for upsertNodes
3. **[Plugin Development](./first-plugin.md)** - Build custom integrations
4. **[MCP Specification](https://modelcontextprotocol.io)** - Official MCP docs

### Explore Advanced Topics:

- **Custom MCP Tools:** Extend Logseq's MCP server with custom tools
- **Multi-Graph Support:** Connect multiple graphs to Claude
- **Webhook Integration:** Trigger MCP operations from external events
- **AI Workflows:** Automate complex graph operations with AI

---

## Additional Resources

- **MCP Specification:** https://modelcontextprotocol.io
- **Logseq CLI Source:** https://github.com/logseq/logseq/tree/master/deps/cli
- **Claude Desktop:** https://claude.ai/download
- **Cline (MCP Client):** https://github.com/clinebot/cline

---

**Questions or Feedback?** Join the [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
