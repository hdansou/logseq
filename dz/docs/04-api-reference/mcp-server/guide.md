# Logseq MCP Server Integration Guide

> **Last revised:** 2026-03-10

## Table of Contents

- [Overview](#overview)
- [What is MCP?](#what-is-mcp)
- [Features](#features)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Available Tools](#available-tools)
- [Usage Modes](#usage-modes)
- [Integration Examples](#integration-examples)
- [Client Setup](#client-setup)
- [Tool Reference](#tool-reference)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Developer Guide](#developer-guide)
- [Additional Resources](#additional-resources)

## Overview

The **Logseq MCP (Model Context Protocol) Server** enables AI assistants and LLM-powered applications to interact directly with your Logseq knowledge graph. By implementing the Model Context Protocol specification, Logseq becomes a **context provider** that AI tools can query, read from, and write to - effectively giving your AI assistant direct access to your personal knowledge base.

### Why MCP?

The Model Context Protocol (MCP) is an open standard created by Anthropic for connecting AI applications to external data sources. By implementing MCP, Logseq can:

- **Expose your knowledge graph** to AI assistants like Claude Desktop, Cline, and other MCP-compatible clients
- **Enable bidirectional communication** - AI can both read and write to your notes
- **Provide semantic context** - AI assistants can search, query, and understand your knowledge structure
- **Standardize integrations** - One protocol works with multiple AI tools

### Use Cases

- 🤖 **AI Assistant Integration**: Connect Claude Desktop, Cline, or custom AI agents to your Logseq graph
- 📝 **Automated Note-Taking**: Have AI assistants automatically add notes, links, and summaries to your graph
- 🔍 **Intelligent Search**: Enable AI to search through your notes and provide context-aware answers
- 🔗 **Knowledge Enhancement**: AI can suggest connections, create cross-references, and enrich your notes
- 📊 **Data Analysis**: Query and analyze patterns across your entire knowledge base
- 🎯 **Task Management**: AI can help manage tasks, update statuses, and track progress

## What is MCP?

The **Model Context Protocol (MCP)** is an open protocol that standardizes how AI applications connect to external data sources and tools. Think of it as a universal adapter that lets AI assistants plug into different data sources.

### Core Concepts

1. **MCP Server** (Logseq): Provides tools and data to clients
2. **MCP Client** (AI Assistant): Consumes tools and requests data
3. **Tools**: Specific operations the server exposes (e.g., "search blocks", "add to page")
4. **Resources**: Data exposed by the server (in Logseq: pages, blocks, properties)
5. **Transport**: Communication method (HTTP Streamable or stdio)

### MCP Architecture

```
┌─────────────────────┐         MCP Protocol         ┌──────────────────┐
│   AI Assistant      │◄────────────────────────────►│  Logseq MCP      │
│  (Claude, Cline)    │   HTTP or stdio transport    │     Server       │
└─────────────────────┘                              └──────────────────┘
                                                              │
                                                              │
                                                              ▼
                                                      ┌──────────────────┐
                                                      │  Your Logseq     │
                                                      │  Knowledge Graph │
                                                      └──────────────────┘
```

## Features

### ✅ Supported Operations

**Read Operations:**

- 📄 List all pages in your graph
- 📖 Get full page content with all blocks
- 🏷️ List all tags and their metadata
- 🔧 List all properties and their configurations
- 🔎 Search blocks by content

**Write Operations (via `upsertNodes` tool):**

- Batch create or edit pages, blocks, tags, and properties in a single call
- Supports dry-run mode for validation without committing changes

### 🔧 Advanced Features

- **Auto HTTP Server**: Automatically enables HTTP API server when MCP is enabled
- **Session Management**: Maintains persistent sessions for connected clients
- **Error Handling**: Comprehensive error messages for debugging
- **Authentication**: Leverages existing HTTP server token authentication
- **Local & Remote**: Works with both local graphs and remote API server

## Getting Started

### Prerequisites

- **Logseq Desktop** (Database version or newer with MCP support)
- **An MCP-compatible client** (e.g., Claude Desktop, Cline, custom client)
- **Node.js 22+** (for CLI mode only)

### Quick Start: Enabling MCP in Desktop App

1. **Open Logseq Desktop**

2. **Navigate to Settings**:

   - Click the gear icon (⚙️) in the toolbar
   - Or press `t s` keyboard shortcut

3. **Go to AI Settings**:

   - Scroll to the "AI" section in the left sidebar
   - Or search for "MCP" in settings

4. **Enable MCP Server**:

   - Find the "Enable MCP Server" toggle
   - Switch it ON
   - This automatically enables the HTTP API server if not already running

5. **Configure Authentication**:

   - Click the API icon (📡) in the toolbar
   - Select "Authorization tokens"
   - Add a new token with a descriptive name
   - Save a secure random token (see [Authentication](#authentication) for token generation)

6. **Verify Server is Running**:
   - The API icon should show green/active status
   - Visit `http://127.0.0.1:12315/` in your browser to see API docs
   - MCP endpoint is available at `http://127.0.0.1:12315/mcp`

### Quick Start: CLI Mode

For advanced users or automation, you can run MCP server from the command line:

```bash
# Navigate to Logseq CLI directory
cd /path/to/logseq/deps/cli

# Install dependencies
yarn install

# Run MCP server with local graph
yarn logseq mcp-server -g /path/to/your/graph

# Run with HTTP transport (default)
yarn logseq mcp-server -g /path/to/your/graph --port 3000 --host 127.0.0.1

# Run with stdio transport (for Claude Desktop integration)
yarn logseq mcp-server -g /path/to/your/graph --stdio

# Debug a specific tool
yarn logseq mcp-server -g /path/to/your/graph --debug-tool listPages
```

## Configuration

### Desktop App Configuration

#### MCP Settings

Access via: **Settings → AI → MCP Server**

| Setting           | Description              | Default |
| ----------------- | ------------------------ | ------- |
| Enable MCP Server | Toggle MCP server on/off | `false` |

**Note**: Enabling MCP automatically enables the HTTP API server if needed.

#### HTTP Server Settings

Access via: **API Icon (📡) → Server configurations**

| Setting    | Options                  | Default     | Notes                                  |
| ---------- | ------------------------ | ----------- | -------------------------------------- |
| Host       | `127.0.0.1` or `0.0.0.0` | `127.0.0.1` | Use `0.0.0.0` only on trusted networks |
| Port       | 1-65535                  | `12315`     | MCP uses same port as HTTP API         |
| Auto-start | On/Off                   | `false`     | Start server with Logseq launch        |

#### Authentication Tokens

Access via: **API Icon (📡) → Authorization tokens**

1. Click "+ Add new token"
2. Set name (e.g., "Claude Desktop MCP")
3. Generate secure token:

   ```bash
   # macOS/Linux
   openssl rand -hex 32

   # Any platform with Node.js
   node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"
   ```

4. Save the token configuration

### CLI Configuration

Command-line options for `yarn logseq mcp-server`:

| Option               | Type   | Description                              | Required                |
| -------------------- | ------ | ---------------------------------------- | ----------------------- |
| `-g, --graph`        | path   | Path to local graph directory            | For local mode          |
| `--stdio`            | flag   | Use stdio transport (for Claude Desktop) | No                      |
| `--port`             | number | HTTP server port                         | No (default: 3000)      |
| `--host`             | string | HTTP server host                         | No (default: 127.0.0.1) |
| `--api-server-token` | string | Token for remote API server              | For API mode            |
| `--debug-tool`       | string | Run single tool for debugging            | No                      |

### Environment Variables

For CLI mode, you can use environment variables:

```bash
# Set API server token
export LOGSEQ_API_TOKEN="your-token-here"

# Run MCP server
yarn logseq mcp-server --api-server-token "$LOGSEQ_API_TOKEN"
```

## Available Tools

The MCP server exposes the following tools to connected clients:

### Read Tools

| Tool             | Description            | Input                          | Output                                      |
| ---------------- | ---------------------- | ------------------------------ | ------------------------------------------- |
| `listPages`      | List all pages         | `expand` (optional bool)       | Array of page objects with minimal metadata |
| `getPage`        | Get full page content  | `pageName` (string)            | Page entity + array of blocks               |
| `listTags`       | List all tags          | `expand` (optional bool)       | Array of tag entities with metadata         |
| `listProperties` | List all properties    | `expand` (optional bool)       | Array of property entities with schemas     |
| `searchBlocks`   | Search for blocks      | `searchTerm` (string)          | Array of matching blocks                    |

### Write Tools

| Tool          | Description                                        | Input                                    | Output                  |
| ------------- | -------------------------------------------------- | ---------------------------------------- | ----------------------- |
| `upsertNodes` | Batch create/edit pages, blocks, tags, properties  | `operations` (array), `dry-run` (optional bool) | Summary of changes |

## Usage Modes

### Mode 1: Desktop App with API Server (Recommended)

This is the simplest mode - enable MCP in Logseq Desktop settings.

**How it works:**

1. Logseq Desktop runs HTTP API server on `http://127.0.0.1:12315`
2. MCP endpoint exposed at `/mcp`
3. Client connects via HTTP Streamable transport
4. Authentication via Bearer tokens

**Best for:**

- Claude Desktop integration
- Web-based MCP clients
- Remote access (with proper security)

**Example client configuration** (Claude Desktop):

```json
{
  "mcpServers": {
    "logseq": {
      "command": "curl",
      "args": [
        "-X",
        "POST",
        "-H",
        "Content-Type: application/json",
        "-H",
        "Authorization: Bearer YOUR_TOKEN",
        "-H",
        "mcp-session-id: SESSION_ID",
        "http://127.0.0.1:12315/mcp"
      ],
      "transport": "http"
    }
  }
}
```

### Mode 2: CLI with Local Graph

Run MCP server directly against a local graph using the CLI.

**How it works:**

1. CLI opens SQLite database directly
2. No Logseq Desktop required
3. Can use stdio or HTTP transport

**Best for:**

- Automation scripts
- Headless servers
- Direct database access
- Development and testing

**Example (stdio transport):**

```bash
yarn logseq mcp-server -g ~/Logseq/graphs/my-graph --stdio
```

**Example (HTTP transport):**

```bash
yarn logseq mcp-server -g ~/Logseq/graphs/my-graph --port 3000
```

### Mode 3: CLI with Remote API Server

Connect CLI MCP server to a running Logseq Desktop instance via HTTP API.

**How it works:**

1. Logseq Desktop runs with HTTP server enabled
2. CLI MCP server acts as a bridge
3. Forwards all operations to Desktop API

**Best for:**

- Custom MCP clients with existing API setup
- Testing and development
- Complex deployment scenarios

**Example:**

```bash
# Get token from Logseq Desktop
export API_TOKEN="your-token-here"

# Run MCP server connected to Desktop
yarn logseq mcp-server --api-server-token "$API_TOKEN"
```

## Integration Examples

### Claude Desktop Integration

Claude Desktop is an AI assistant that supports MCP servers. Here's how to connect it to Logseq:

#### Method 1: Direct HTTP Connection (Recommended)

**Step 1: Enable MCP in Logseq**

- Follow [Quick Start](#quick-start-enabling-mcp-in-desktop-app)
- Note your authentication token

**Step 2: Configure Claude Desktop**

Edit Claude configuration file:

- **macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows**: `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "logseq": {
      "url": "http://127.0.0.1:12315/mcp",
      "headers": {
        "Authorization": "Bearer YOUR_TOKEN_HERE"
      },
      "transport": "http-streamable"
    }
  }
}
```

**Step 3: Restart Claude Desktop**

**Step 4: Test the Connection**

Ask Claude:

- "Can you list my Logseq pages?"
- "Show me the content of my 'Projects' page"
- "Add a note to my daily journal"

#### Method 2: CLI with stdio (Alternative)

If you prefer the CLI approach with stdio transport:

**Claude Desktop config:**

```json
{
  "mcpServers": {
    "logseq": {
      "command": "node",
      "args": [
        "/path/to/logseq/deps/cli/cli.mjs",
        "mcp-server",
        "-g",
        "/path/to/your/graph",
        "--stdio"
      ]
    }
  }
}
```

### Cline (VS Code Extension) Integration

Cline is an AI coding assistant for VS Code that supports MCP.

**Step 1: Install Cline**

- Open VS Code
- Install "Cline" extension from marketplace

**Step 2: Configure MCP Server**

In Cline settings (`settings.json`):

```json
{
  "cline.mcpServers": [
    {
      "name": "logseq",
      "url": "http://127.0.0.1:12315/mcp",
      "headers": {
        "Authorization": "Bearer YOUR_TOKEN_HERE"
      }
    }
  ]
}
```

**Step 3: Use in Cline**

In Cline chat:

- "Search my Logseq for information about X"
- "Add this code snippet to my Logseq notes"
- "Create a page in Logseq with meeting notes"

### Custom Python Client

Build a custom MCP client in Python:

```python
import requests
import json
import uuid

class LogseqMCPClient:
    def __init__(self, base_url, token):
        self.base_url = base_url.rstrip('/') + '/mcp'
        self.token = token
        self.session_id = None

    def _headers(self):
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.token}'
        }
        if self.session_id:
            headers['mcp-session-id'] = self.session_id
        return headers

    def initialize(self):
        """Initialize MCP session"""
        payload = {
            'jsonrpc': '2.0',
            'id': 1,
            'method': 'initialize',
            'params': {
                'protocolVersion': '2024-11-05',
                'capabilities': {},
                'clientInfo': {
                    'name': 'custom-python-client',
                    'version': '1.0.0'
                }
            }
        }

        response = requests.post(
            self.base_url,
            headers=self._headers(),
            json=payload
        )
        response.raise_for_status()

        # Extract session ID from response headers
        self.session_id = response.headers.get('mcp-session-id')
        return response.json()

    def call_tool(self, tool_name, arguments=None):
        """Call an MCP tool"""
        if not self.session_id:
            self.initialize()

        payload = {
            'jsonrpc': '2.0',
            'id': str(uuid.uuid4()),
            'method': 'tools/call',
            'params': {
                'name': tool_name,
                'arguments': arguments or {}
            }
        }

        response = requests.post(
            self.base_url,
            headers=self._headers(),
            json=payload
        )
        response.raise_for_status()
        return response.json()

    def list_pages(self):
        """Get all pages"""
        result = self.call_tool('listPages')
        content = result['result']['content'][0]['text']
        return json.loads(content)

    def get_page(self, page_name):
        """Get page content"""
        result = self.call_tool('getPage', {'pageName': page_name})
        content = result['result']['content'][0]['text']
        return json.loads(content)

    def search_blocks(self, search_term):
        """Search for blocks"""
        result = self.call_tool('searchBlocks', {'searchTerm': search_term})
        content = result['result']['content'][0]['text']
        return json.loads(content)

    def upsert_nodes(self, operations, dry_run=False):
        """Batch create/edit pages, blocks, tags, and properties"""
        result = self.call_tool('upsertNodes', {
            'operations': operations,
            'dry-run': dry_run
        })
        return result

# Example usage
if __name__ == '__main__':
    client = LogseqMCPClient(
        'http://127.0.0.1:12315',
        'your-token-here'
    )

    # Initialize session
    client.initialize()

    # List all pages
    pages = client.list_pages()
    print(f"Found {len(pages)} pages")

    # Get specific page
    page = client.get_page('My Page')
    print(f"Page has {len(page['blocks'])} blocks")

    # Search
    results = client.search_blocks('TODO')
    print(f"Found {len(results)} blocks with TODO")

    # Add content via upsert
    client.upsert_nodes([
        {'operation': 'add', 'entityType': 'page', 'id': 'temp-daily', 'data': {'title': 'Daily Notes'}},
        {'operation': 'add', 'entityType': 'block', 'data': {'page-id': 'temp-daily', 'title': 'New item from Python'}}
    ])
```

### Custom Node.js Client

```javascript
import fetch from 'node-fetch'

class LogseqMCPClient {
  constructor(baseUrl, token) {
    this.baseUrl = baseUrl.replace(/\/$/, '') + '/mcp'
    this.token = token
    this.sessionId = null
  }

  async initialize() {
    const response = await fetch(this.baseUrl, {
      method: 'POST',
      headers: this._headers(),
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'initialize',
        params: {
          protocolVersion: '2024-11-05',
          capabilities: {},
          clientInfo: {
            name: 'custom-node-client',
            version: '1.0.0',
          },
        },
      }),
    })

    if (!response.ok) {
      throw new Error(`Initialize failed: ${response.statusText}`)
    }

    this.sessionId = response.headers.get('mcp-session-id')
    return await response.json()
  }

  async callTool(toolName, args = {}) {
    if (!this.sessionId) {
      await this.initialize()
    }

    const response = await fetch(this.baseUrl, {
      method: 'POST',
      headers: this._headers(),
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: crypto.randomUUID(),
        method: 'tools/call',
        params: {
          name: toolName,
          arguments: args,
        },
      }),
    })

    if (!response.ok) {
      throw new Error(`Tool call failed: ${response.statusText}`)
    }

    const result = await response.json()
    const content = result.result.content[0].text
    return JSON.parse(content)
  }

  async listPages() {
    return await this.callTool('listPages')
  }

  async getPage(pageName) {
    return await this.callTool('getPage', { pageName })
  }

  async searchBlocks(searchTerm) {
    return await this.callTool('searchBlocks', { searchTerm })
  }

  async upsertNodes(operations, dryRun = false) {
    return await this.callTool('upsertNodes', { operations, 'dry-run': dryRun })
  }

  _headers() {
    const headers = {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${this.token}`,
    }
    if (this.sessionId) {
      headers['mcp-session-id'] = this.sessionId
    }
    return headers
  }
}

// Example usage
const client = new LogseqMCPClient('http://127.0.0.1:12315', 'your-token-here')

// List pages
const pages = await client.listPages()
console.log(`Found ${pages.length} pages`)

// Get page content
const page = await client.getPage('Projects')
console.log(`Page has ${page.blocks.length} blocks`)

// Add content via upsert
await client.upsertNodes([
  { operation: 'add', entityType: 'page', id: 'temp-daily', data: { title: 'Daily Notes' } },
  { operation: 'add', entityType: 'block', data: { 'page-id': 'temp-daily', title: 'Meeting notes from client' } },
])
```

## Client Setup

### MCP Protocol Basics

When implementing an MCP client, follow these steps:

#### 1. Initialize Session

Send an initialize request to establish a session:

```json
POST /mcp
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "initialize",
  "params": {
    "protocolVersion": "2024-11-05",
    "capabilities": {},
    "clientInfo": {
      "name": "your-client-name",
      "version": "1.0.0"
    }
  }
}
```

Response includes `mcp-session-id` header - save this for subsequent requests.

#### 2. Call Tools

Once initialized, call tools using the session ID:

```json
POST /mcp
Headers:
  Content-Type: application/json
  Authorization: Bearer YOUR_TOKEN
  mcp-session-id: SESSION_ID

Body:
{
  "jsonrpc": "2.0",
  "id": "unique-request-id",
  "method": "tools/call",
  "params": {
    "name": "listPages",
    "arguments": {}
  }
}
```

#### 3. Handle Responses

Successful tool responses return data in this format:

```json
{
  "jsonrpc": "2.0",
  "id": "request-id",
  "result": {
    "content": [
      {
        "type": "text",
        "text": "{\"data\": \"...\"}"
      }
    ]
  }
}
```

Parse the `text` field as JSON to get actual data.

#### 4. Error Handling

Errors are returned with error objects:

```json
{
  "jsonrpc": "2.0",
  "id": "request-id",
  "error": {
    "code": -32000,
    "message": "Error description"
  }
}
```

#### 5. Close Session

When done, send DELETE request:

```http
DELETE /mcp
Headers:
  mcp-session-id: SESSION_ID
```

### Transport Types

#### HTTP Streamable (Recommended)

- Uses standard HTTP POST/GET/DELETE
- Stateful sessions via headers
- Best for web clients and desktop apps
- Endpoint: `http://127.0.0.1:12315/mcp`

#### stdio

- Communication via standard input/output streams
- Stateless, direct process communication
- Best for CLI tools and local integrations
- Usage: `yarn logseq mcp-server --stdio`

## Tool Reference

### listPages

List all pages in the graph with minimal metadata.

**Input:** None

**Output:**

```json
[
  {
    "block/uuid": "page-uuid-string",
    "block/title": "Page Title",
    "block/created-at": 1234567890,
    "block/updated-at": 1234567891
  }
]
```

**Example:**

```python
pages = client.call_tool('listPages')
```

**Notes:**

- Returns minimal data to avoid exceeding payload limits
- Hidden pages are excluded
- UUIDs are returned as strings

---

### getPage

Get complete page content including all blocks in a hierarchical structure.

**Input:**

```json
{
  "pageName": "string" // Page name or UUID
}
```

**Output:**

```json
{
  "entity": {
    "block/uuid": "page-uuid",
    "block/title": "Page Title",
    "block/created-at": 1234567890,
    "block/updated-at": 1234567891
    // ... other page properties
  },
  "blocks": [
    {
      "block/uuid": "block-uuid",
      "block/content": "Block content",
      "block/level": 1,
      "block/children": [
        {
          "block/uuid": "child-uuid",
          "block/content": "Child block",
          "block/level": 2
        }
      ]
    }
  ]
}
```

**Example:**

```python
page = client.call_tool('getPage', {'pageName': 'My Project'})
```

**Notes:**

- Returns full block tree with nesting
- Hidden properties are filtered out
- Returns error if page not found

---

### listTags

Get all tags in the graph with their metadata.

**Input:** None

**Output:**

```json
[
  {
    "block/uuid": "tag-uuid",
    "block/original-name": "tag-name",
    "db/ident": ":tag-name",
    "logseq.property.class/extends": [":parent-tag"],
    "logseq.property.class/properties": [":prop1", ":prop2"],
    "logseq.property/description": "Tag description"
  }
]
```

**Example:**

```python
tags = client.call_tool('listTags')
```

**Notes:**

- Includes tag hierarchy (extends)
- Includes associated properties
- Excludes internal/hidden tags

---

### listProperties

Get all properties with their schemas and configurations.

**Input:** None

**Output:**

```json
[
  {
    "block/uuid": "property-uuid",
    "block/original-name": "property-name",
    "db/ident": ":property-name",
    "logseq.property/classes": [":class1", ":class2"],
    "logseq.property/description": "Property description",
    "logseq.property/schema": {
      "type": "string",
      "required": false
    }
  }
]
```

**Example:**

```python
properties = client.call_tool('listProperties')
```

**Notes:**

- Includes property type information
- Shows which classes use the property
- Contains validation schemas

---

### searchBlocks

Search for blocks containing a search term.

**Input:**

```json
{
  "searchTerm": "string" // Text to search for
}
```

**Output:**

```json
[
  {
    "block/uuid": "block-uuid",
    "block/content": "Block content with search term",
    "block/page": "page-name",
    "block/created-at": 1234567890
    // ... other block properties
  }
]
```

**Example:**

```python
results = client.call_tool('searchBlocks', {'searchTerm': 'TODO urgent'})
```

**Notes:**

- Case-insensitive search
- Returns full block data
- No snippet highlighting (full content returned)

---

### upsertNodes

Batch create or edit pages, blocks, tags, and properties. This tool should be called at most once per user request.

**Input:**

```json
{
  "operations": [
    {
      "operation": "add" | "edit",
      "entityType": "block" | "page" | "tag" | "property",
      "id": "string (optional, required for edit, temp ID for add)",
      "data": {
        "title": "string",
        "page-id": "string (page UUID, required when adding blocks)",
        "tags": ["uuid-string"],
        "property-type": "string",
        "property-cardinality": "one" | "many",
        "property-classes": ["string"],
        "class-extends": ["string"],
        "class-properties": ["string"]
      }
    }
  ],
  "dry-run": false  // Optional: validate without committing
}
```

**Output:** A summary string describing what was added or edited.

**Example:**

```python
# Add a block to an existing page
client.call_tool('upsertNodes', {
    'operations': [
        {
            'operation': 'add',
            'entityType': 'block',
            'data': {
                'page-id': '119268a6-704f-4e9e-8c34-36dfc6133729',
                'title': 'New block text'
            }
        }
    ]
})

# Create a new page with a block
client.call_tool('upsertNodes', {
    'operations': [
        {
            'operation': 'add',
            'entityType': 'page',
            'id': 'temp-inbox',
            'data': {'title': 'Inbox'}
        },
        {
            'operation': 'add',
            'entityType': 'block',
            'data': {'page-id': 'temp-inbox', 'title': 'First item'}
        }
    ]
})
```

**Notes:**

- Supports batch operations in a single call
- Use temporary string IDs to reference newly created pages in subsequent block operations
- Use `dry-run: true` to validate without committing changes
- Before creating a page, tag, or property, check if it already exists with `getPage`
- Editing pages, tags, or properties is not yet supported (only blocks support `edit`)

## Best Practices

### Security

1. **Token Management**

   - Generate strong, random tokens (32+ characters)
   - Store tokens securely (use environment variables, not hardcoded)
   - Use different tokens for different clients
   - Rotate tokens regularly
   - Revoke unused tokens immediately

2. **Network Security**

   - Keep server on `127.0.0.1` for local-only access
   - Only use `0.0.0.0` on fully trusted networks
   - Consider reverse proxy with HTTPS for remote access
   - Use firewall rules to restrict access

3. **Authentication**

   ```javascript
   // ❌ Bad - hardcoded token
   const token = 'abc123'

   // ✅ Good - environment variable
   const token = process.env.LOGSEQ_MCP_TOKEN

   if (!token) {
     throw new Error('LOGSEQ_MCP_TOKEN not set')
   }
   ```

### Performance

1. **Batch Operations**

   ```python
   # ❌ Slow - many individual calls
   for page_name in pages:
       page = client.get_page(page_name)

   # ✅ Better - get all pages once
   all_pages = client.list_pages()
   for page in all_pages:
       if page['block/title'] in targets:
           full_page = client.get_page(page['block/uuid'])
   ```

2. **Cache Results**

   - Cache page lists and tag hierarchies
   - Invalidate cache after write operations
   - Use stale data when acceptable

3. **Limit Data Returned**
   - Use `searchBlocks` with specific terms
   - Filter results on client side when possible
   - Request only needed pages

### Error Handling

1. **Robust Client Implementation**

   ```python
   def safe_get_page(client, page_name, retries=3):
       for attempt in range(retries):
           try:
               return client.get_page(page_name)
           except ConnectionError:
               if attempt == retries - 1:
                   raise
               time.sleep(2 ** attempt)  # Exponential backoff
           except PageNotFoundError:
               # Create page if allowed
               client.add_to_page(page_name, "# New Page", force=True)
               return client.get_page(page_name)
   ```

2. **Graceful Degradation**

   ```javascript
   try {
     const result = await client.callTool('getPage', { pageName: 'Test' })
     return result
   } catch (error) {
     console.error('Failed to get page:', error)
     // Return cached data or default value
     return getCachedPageData('Test')
   }
   ```

3. **Validate Input**
   ```python
   def upsert_nodes_safe(client, operations):
       if not operations or not isinstance(operations, list):
           raise ValueError("Invalid operations")

       for op in operations:
           if 'operation' not in op or 'entityType' not in op:
               raise ValueError("Each operation must have 'operation' and 'entityType'")

       return client.upsert_nodes(operations)
   ```

### Integration Patterns

1. **Session Management**

   ```python
   class ManagedMCPClient:
       def __init__(self, url, token):
           self.client = LogseqMCPClient(url, token)
           self.initialized = False

       def __enter__(self):
           self.client.initialize()
           self.initialized = True
           return self.client

       def __exit__(self, exc_type, exc_val, exc_tb):
           if self.initialized:
               self.client.close()

   # Usage
   with ManagedMCPClient(url, token) as client:
       pages = client.list_pages()
   ```

2. **Asynchronous Operations**

   ```javascript
   async function processPages(client) {
     const pages = await client.listPages()

     // Process in parallel with concurrency limit
     const limit = 5
     for (let i = 0; i < pages.length; i += limit) {
       const batch = pages.slice(i, i + limit)
       await Promise.all(batch.map((page) => processPage(client, page)))
     }
   }
   ```

3. **Monitoring and Logging**

   ```python
   import logging

   logging.basicConfig(level=logging.INFO)
   logger = logging.getLogger(__name__)

   def call_tool_logged(client, tool_name, args):
       logger.info(f"Calling tool: {tool_name} with args: {args}")
       start = time.time()

       try:
           result = client.call_tool(tool_name, args)
           duration = time.time() - start
           logger.info(f"Tool {tool_name} succeeded in {duration:.2f}s")
           return result
       except Exception as e:
           logger.error(f"Tool {tool_name} failed: {e}")
           raise
   ```

## Troubleshooting

### MCP Server Won't Start

**Symptom**: Toggle enabled but server doesn't start

**Solutions**:

1. Check HTTP server status:

   - Look at API icon - should be green/active
   - Try manually starting HTTP server first
   - Check console logs for error messages

2. Port conflicts:

   ```bash
   # Check if port is in use (macOS/Linux)
   lsof -i :12315

   # Windows
   netstat -ano | findstr :12315
   ```

3. Configuration issues:

   - Verify `server/mcp-enabled?` is set
   - Check Logseq storage: Settings → Advanced → Show technical details
   - Reset server config if needed

4. Restart Logseq:
   - Save all work
   - Quit Logseq completely
   - Relaunch and re-enable MCP

### Authentication Failures

**Symptom**: 401 Unauthorized errors

**Solutions**:

1. Verify token:

   ```bash
   # Test API directly
   curl -X POST http://127.0.0.1:12315/api \
     -H "Content-Type: application/json" \
     -H "Authorization: Bearer YOUR_TOKEN" \
     -d '{"method": "logseq.App.getCurrentGraph", "args": []}'
   ```

2. Check token configuration:

   - Token must be in Logseq's authorization tokens list
   - Token must match exactly (no extra spaces)
   - Token must not be expired or revoked

3. Header format:

   ```javascript
   // ✅ Correct
   headers: {
     'Authorization': 'Bearer your-token-here'
   }

   // ❌ Wrong
   headers: {
     'Authorization': 'your-token-here'  // Missing 'Bearer '
   }
   ```

### Session Issues

**Symptom**: "Invalid or missing session ID" errors

**Solutions**:

1. Initialize properly:

   ```python
   # Must initialize before calling tools
   client.initialize()

   # Then call tools
   client.call_tool('listPages')
   ```

2. Check session ID:

   - Session ID should be returned in response headers
   - Save and include in all subsequent requests
   - Don't reuse expired session IDs

3. Session timeout:
   ```python
   # Re-initialize if session expired
   try:
       result = client.call_tool('getPage', args)
   except SessionExpiredError:
       client.initialize()
       result = client.call_tool('getPage', args)
   ```

### Connection Refused

**Symptom**: Cannot connect to MCP endpoint

**Solutions**:

1. Verify server is running:

   ```bash
   curl http://127.0.0.1:12315/
   # Should return HTML documentation
   ```

2. Check host/port:

   - Confirm Logseq is using correct port (default 12315)
   - Try accessing from browser first
   - Check firewall isn't blocking connection

3. Network issues:

   ```bash
   # Ping localhost
   ping 127.0.0.1

   # Check routing
   netstat -rn
   ```

### Tool Execution Errors

**Symptom**: Tool calls fail with errors

**Solutions**:

1. **Page not found**:

   ```python
   # ❌ This will fail if page doesn't exist
   client.get_page('Nonexistent Page')

   # ✅ Create the page first via upsertNodes
   client.upsert_nodes([
       {'operation': 'add', 'entityType': 'page', 'id': 'temp', 'data': {'title': 'New Page'}},
       {'operation': 'add', 'entityType': 'block', 'data': {'page-id': 'temp', 'title': 'Content'}}
   ])
   ```

2. **Invalid UUID**:

   ```python
   # Validate UUID format
   import re
   uuid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'

   if not re.match(uuid_pattern, block_uuid):
       raise ValueError(f"Invalid UUID: {block_uuid}")
   ```

3. **Empty results**:
   - Check search terms are correct
   - Verify page names match exactly (case-sensitive)
   - Ensure graph has data

### CLI Mode Issues

**Symptom**: CLI commands fail or hang

**Solutions**:

1. Check Node version:

   ```bash
   node --version  # Should be 22+
   ```

2. Verify graph path:

   ```bash
   # Graph directory should contain logseq/ folder
   ls -la /path/to/graph/logseq/

   # Should see graphs.db or .transit files
   ```

3. Permissions:

   ```bash
   # Ensure read access to graph
   chmod -R u+r /path/to/graph
   ```

4. Debug mode:
   ```bash
   # Test individual tools
   yarn logseq mcp-server -g /path/to/graph --debug-tool listPages
   ```

### Debug Logging

Enable detailed logging:

**Desktop App:**

1. Open DevTools: View → Toggle Developer Tools
2. Check Console tab for MCP logs
3. Look for "[server] MCP routes initialized"

**CLI:**

```bash
# Set debug environment
DEBUG=* yarn logseq mcp-server -g /path/to/graph

# Or specific namespaces
DEBUG=logseq:* yarn logseq mcp-server -g /path/to/graph
```

**Client-side:**

```javascript
// Add logging to client
const originalFetch = fetch
global.fetch = (...args) => {
  console.log('MCP Request:', args)
  return originalFetch(...args).then((res) => {
    console.log('MCP Response:', res)
    return res
  })
}
```

## Developer Guide

### Architecture Overview

```
┌──────────────────────────────────────────┐
│         MCP Client (Claude, etc)         │
└──────────────┬───────────────────────────┘
               │ MCP Protocol (JSON-RPC)
               │
┌──────────────▼───────────────────────────┐
│     Logseq MCP Server Implementation     │
│  (logseq.cli.common.mcp.server)          │
│                                          │
│  ┌─────────────────────────────────────┐ │
│  │  Tool Registry                      │ │
│  │  - listPages, getPage, etc.         │ │
│  └─────────────────────────────────────┘ │
│                                          │
│  ┌─────────────────────────────────────┐ │
│  │  Session Management                 │ │
│  │  - HTTP Streamable Transport        │ │
│  └─────────────────────────────────────┘ │
└──────────────┬───────────────────────────┘
               │
        ┌──────┴──────┐
        │             │
┌───────▼────┐  ┌─────▼─────┐
│Local Graph │  │  API      │
│(SQLite DB) │  │  Server   │
└────────────┘  └───────────┘
```

### Key Files

| File                                               | Purpose                                     |
| -------------------------------------------------- | ------------------------------------------- |
| `deps/cli/src/logseq/cli/commands/mcp_server.cljs` | CLI entry point and HTTP server setup       |
| `deps/cli/src/logseq/cli/common/mcp/server.cljs`   | Core MCP server implementation, shared code |
| `deps/cli/src/logseq/cli/common/mcp/tools.cljs`    | Tool implementations (data access logic)    |
| `src/electron/electron/server.cljs`                | Desktop app integration, API routing        |
| `src/main/frontend/components/settings.cljs`       | UI settings for MCP toggle                  |

### Adding New Tools

To add a new MCP tool:

**Step 1: Define Tool in `tools.cljs`**

```clojure
(defn my-new-tool
  "Main fn for MyNewTool"
  [db args]
  ;; Access database and return data
  (let [data (d/q '[:find ?e :where [?e :my/attribute]] db)]
    (map process-entity data)))
```

**Step 2: Add API Version in `server.cljs`**

```clojure
(defn- api-my-new-tool
  [call-api-fn args]
  (call-api-fn "logseq.api.myNewMethod"
               [(aget args "param1")
                (aget args "param2")]))

;; Add to api-tools map
(def api-tools
  {:myNewTool
   {:fn api-my-new-tool
    :config #js {:title "My New Tool"
                 :description "Description of what it does"
                 :inputSchema #js {:param1 (z/string)
                                   :param2 (-> (z/number) (z/optional))}}}
   ;; ... existing tools
   })
```

**Step 3: Add Local Version in `mcp_server.cljs`**

```clojure
(defn- local-my-new-tool [conn args]
  (cli-common-mcp-server/mcp-success-response
    (cli-common-mcp-tools/my-new-tool @conn args)))

(def ^:private local-tools
  {:myNewTool {:fn local-my-new-tool}
   ;; ... existing tools
   })
```

**Step 4: Implement API Method (if needed)**

In `src/main/logseq/api.cljs`:

```clojure
(defn ^:export my_new_method
  [param1 param2]
  (p/let [result (fetch-data param1 param2)]
    (bean/->js result)))
```

### Testing Tools

**Test with CLI:**

```bash
# Test tool directly
yarn logseq mcp-server -g /path/to/graph --debug-tool myNewTool \
  --param1 "value1" --param2 42
```

**Test with API:**

```bash
# Start MCP server
yarn logseq mcp-server --api-server-token YOUR_TOKEN

# In another terminal, test with curl
curl -X POST http://localhost:3000/mcp \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_TOKEN" \
  -d '{
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
      "protocolVersion": "2024-11-05",
      "clientInfo": {"name": "test", "version": "1.0"}
    }
  }'
```

### MCP Protocol Implementation

Logseq implements MCP 2024-11-05 specification:

**Supported Methods:**

| Method             | Description          | Implementation                   |
| ------------------ | -------------------- | -------------------------------- |
| `initialize`       | Start session        | Creates transport and session ID |
| `tools/list`       | List available tools | Returns registered tools         |
| `tools/call`       | Execute a tool       | Routes to tool function          |
| Session management | GET/DELETE /mcp      | HTTP Streamable transport        |

**Response Format:**

All tool responses follow MCP content format:

```javascript
{
  content: [
    {
      type: 'text',
      text: JSON.stringify(data), // Actual data as JSON string
    },
  ]
}
```

### Error Codes

| Code   | Meaning          | Cause                     |
| ------ | ---------------- | ------------------------- |
| -32000 | Server Error     | General server-side error |
| -32600 | Invalid Request  | Malformed JSON-RPC        |
| -32601 | Method Not Found | Unknown method            |
| -32602 | Invalid Params   | Wrong parameters          |
| -32603 | Internal Error   | Server internal error     |

### Contributing

To contribute improvements to Logseq MCP:

1. **Setup Development Environment**:

   ```bash
   git clone https://github.com/logseq/logseq.git
   cd logseq
   git checkout feat/mcp-server
   ```

2. **Make Changes**:

   - Follow existing code patterns
   - Add tests for new tools
   - Update documentation

3. **Test Thoroughly**:

   - Test with local graph
   - Test with API server
   - Test with stdio transport
   - Test multiple concurrent sessions

4. **Submit Pull Request**:
   - Clear description of changes
   - Include test cases
   - Update relevant docs

## Additional Resources

### Official Documentation

- **MCP Specification**: https://modelcontextprotocol.io/
- **MCP TypeScript SDK**: https://github.com/modelcontextprotocol/typescript-sdk
- **Logseq Plugin API**: https://plugins-doc.logseq.com
- **HTTP API Guide**: [http-server-api-guide.md](./http-server-api-guide.md)

### Community Resources

- **Logseq Discord**: https://discord.com/invite/KpN4eHY
- **Logseq Forum**: https://discuss.logseq.com
- **GitHub Repository**: https://github.com/logseq/logseq

### MCP Client Implementations

- **Claude Desktop**: https://claude.ai/download
- **Cline (VS Code)**: https://marketplace.visualstudio.com/items?itemName=saoudrizwan.claude-dev
- **MCP Clients List**: https://github.com/modelcontextprotocol/servers

### Example Projects

- **MCP Python Client**: https://github.com/modelcontextprotocol/python-sdk
- **MCP Server Examples**: https://github.com/modelcontextprotocol/servers

### Video Tutorials

(To be added as community creates them)

### Related Features

- **Logseq Plugin System**: For building in-app extensions
- **HTTP API Server**: For programmatic access without AI
- **Logseq Sync**: For cloud synchronization
- **Logseq Publish**: For public knowledge sharing

---

## Appendix

### Protocol Version

Logseq MCP Server implements:

- **MCP Protocol Version**: 2024-11-05
- **Transport**: HTTP Streamable, stdio
- **SDK Version**: @modelcontextprotocol/sdk ^1.17.5

### Changelog

**Version 0.1.0** (2025-09-25):

- Initial MCP server implementation
- 6 tools: listPages, getPage, listTags, listProperties, searchBlocks, upsertNodes
- HTTP Streamable and stdio transports
- Desktop app integration
- CLI support for local graphs

### License

Logseq MCP Server is released under the same license as Logseq.

### Support

For issues, questions, or feature requests:

- **GitHub Issues**: https://github.com/logseq/logseq/issues
- **Discord**: #api-integration channel
- **Forum**: https://discuss.logseq.com/c/plugins-extensions

---

_Document Version: 1.1_
_Last Updated: 2026-03-10_
_Logseq Version: feat/mcp-server branch_
_MCP Protocol: 2024-11-05_
