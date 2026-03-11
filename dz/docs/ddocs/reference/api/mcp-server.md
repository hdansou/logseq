---
title: 'MCP Server Reference'
description: "Reference for Logseq's Model Context Protocol (MCP) server for AI assistant integration"
status: stable
lastVerified: '2026-03-10'
logseqVersion: '0.11.x'
platforms: [desktop]
graphType: [db]
sinceVersion: '0.10.5'
relatedDocs:
  tutorials:
    - '[MCP Integration Tutorial](../../tutorials/plugins/mcp-integration.md)'
  reference:
    - '[HTTP API Reference](http-api.md)'
---

# MCP Server Reference

> **Last revised:** 2026-03-10

> Reference for Logseq's Model Context Protocol (MCP) server, enabling AI assistants to interact with your knowledge graph.

## Overview

The **MCP Server** implements the [Model Context Protocol](https://modelcontextprotocol.io/) specification, allowing AI assistants like Claude Desktop, Cline, and other MCP-compatible clients to:

- **Read** pages, blocks, tags, and properties from your graph
- **Write** new pages, blocks, tags, and properties (via `upsertNodes`)
- **Search** your knowledge base for matching blocks
- **Query** structured data (tags, properties)

**Endpoint**: `http://127.0.0.1:12315/mcp`

---

## Quick Reference

| Tool             | Description                       | Key Input                          |
| ---------------- | --------------------------------- | ---------------------------------- |
| `listPages`      | List all pages                    | `expand?`                          |
| `getPage`        | Get page content and blocks       | `pageName`                         |
| `searchBlocks`   | Search blocks by text             | `searchTerm`                       |
| `upsertNodes`    | Create or edit pages/blocks/tags/properties | `operations`, `dry-run?` |
| `listTags`       | List all tags                     | `expand?`                          |
| `listProperties` | List all properties               | `expand?`                          |

---

## Getting Started

### Enable in Desktop App

1. **Open Settings**: Click gear icon or press `t s`
2. **Go to AI Settings**: Find "AI" section
3. **Enable MCP Server**: Toggle "MCP Server" ON (this also enables the HTTP API server if not already running)
4. **Configure Token**: Click API icon -> "Authorization tokens" -> Add token
5. **Verify**: Visit `http://127.0.0.1:12315/mcp` in browser

### Enable via CLI

```bash
# Navigate to CLI directory
cd /path/to/logseq/deps/cli

# Run with local graph (HTTP Streamable transport, default port 12315)
logseq mcp-server -g /path/to/your/graph

# Run with custom port
logseq mcp-server -g /path/to/graph --port 3000

# Run with stdio transport (for Claude Desktop)
logseq mcp-server -g /path/to/graph --stdio

# Run against the in-app graph via API server token
logseq mcp-server -a YOUR_API_TOKEN
```

---

## Configuration

### Desktop Settings

The MCP Server shares the HTTP API server's host and port settings.

| Setting    | Default     | Description                                                |
| ---------- | ----------- | ---------------------------------------------------------- |
| MCP Server | `false`     | Toggle MCP on/off (under Settings -> AI)                   |
| Host       | `127.0.0.1` | HTTP API server host (shared with MCP)                    |
| Port       | `12315`     | HTTP API server port (shared with MCP)                    |

### CLI Options

| Option                    | Description                                          |
| ------------------------- | ---------------------------------------------------- |
| `-g, --graph <path>`      | Path to local graph directory                        |
| `-a, --api-server-token`  | API server token to connect to current in-app graph  |
| `-p, --port <number>`     | HTTP port (default: 12315)                           |
| `--host <address>`        | Host address (default: 127.0.0.1)                    |
| `-s, --stdio`             | Use stdio transport instead of HTTP Streamable       |
| `-t, --debug-tool <name>` | Debug a specific MCP tool with direct invocation     |

---

## MCP Tools Reference

### listPages

List all pages in the graph.

**Input**:
| Parameter | Type    | Required | Description                             |
|-----------|---------|----------|-----------------------------------------|
| `expand`  | boolean | No       | Provide additional detail on each page  |

**Output** (default, without `expand`):

```json
[
  {
    "block/title": "Page Name",
    "block/uuid": "page-uuid"
  }
]
```

**Output** (with `expand: true`):

```json
[
  {
    "block/title": "Page Name",
    "block/uuid": "page-uuid",
    "block/created-at": 1640000000000,
    "block/updated-at": 1640000000000
  }
]
```

---

### getPage

Get full content of a specific page including its blocks.

**Input**:
| Parameter  | Type   | Required | Description          |
|------------|--------|----------|----------------------|
| `pageName` | string | Yes      | Page name or UUID    |

**Output**:

```json
{
  "entity": {
    "block/uuid": "page-uuid",
    "block/title": "Page Name"
  },
  "blocks": [
    {
      "block/uuid": "block-uuid",
      "block/title": "Block content"
    }
  ]
}
```

Note: Properties and tags are also pages. You can use `getPage` to retrieve their details.

---

### searchBlocks

Search for blocks containing specific text. This tool is only available when connecting to the in-app graph via the API server (not available in local CLI mode).

**Input**:
| Parameter    | Type   | Required | Description            |
|--------------|--------|----------|------------------------|
| `searchTerm` | string | Yes      | Text to search for     |

**Output**: Returns search results from the app's search index.

---

### upsertNodes

Create or edit pages, blocks, tags, and properties in a single batch operation. This tool replaces separate add/update tools with a unified operations-based approach.

**Input**:
| Parameter    | Type    | Required | Description                                                  |
|--------------|---------|----------|--------------------------------------------------------------|
| `operations` | array   | Yes      | Array of operation objects (see below)                       |
| `dry-run`    | boolean | No       | Validate without committing changes to the database          |

Each operation object has:
| Field        | Type   | Description                                                        |
|--------------|--------|--------------------------------------------------------------------|
| `operation`  | string | `"add"` or `"edit"`                                                |
| `entityType` | string | `"block"`, `"page"`, `"tag"`, or `"property"`                     |
| `id`         | string | For `edit`: must be a UUID string. For `add`: a temporary unique ID if referenced by later operations |
| `data`       | object | Fields to set (see below)                                          |

The `data` object can contain:
| Field                    | Description                                           |
|--------------------------|-------------------------------------------------------|
| `title`                  | Page/tag/property name or block content               |
| `page-id`               | Page UUID string (required when adding a block)        |
| `tags`                   | Array of tag UUID strings                             |
| `property-type`          | Property type (e.g., `"string"`, `"node"`)            |
| `property-cardinality`   | `"one"` or `"many"`                                   |
| `property-classes`       | Array of allowed tag UUIDs or names                   |
| `class-extends`          | Array of parent tag UUIDs or names                    |
| `class-properties`       | Array of property UUIDs or names                      |

**Example** -- Add a block to an existing page:

```json
{
  "operations": [
    {
      "operation": "add",
      "entityType": "block",
      "id": null,
      "data": {
        "page-id": "119268a6-704f-4e9e-8c34-36dfc6133729",
        "title": "New block text"
      }
    }
  ]
}
```

**Example** -- Create a new page with a block:

```json
{
  "operations": [
    {
      "operation": "add",
      "entityType": "page",
      "id": "temp-Inbox",
      "data": { "title": "Inbox" }
    },
    {
      "operation": "add",
      "entityType": "block",
      "data": {
        "page-id": "temp-Inbox",
        "title": "My first block"
      }
    }
  ]
}
```

**Limitations**: Editing pages, tags, and properties is not yet supported; only blocks can be edited.

---

### listTags

List all tags/classes in the graph.

**Input**:
| Parameter | Type    | Required | Description                                              |
|-----------|---------|----------|----------------------------------------------------------|
| `expand`  | boolean | No       | Include additional detail (parents, tag properties, etc) |

**Output** (default, without `expand`):

```json
[
  {
    "block/title": "Tag Name",
    "block/uuid": "tag-uuid"
  }
]
```

---

### listProperties

List all properties defined in the graph.

**Input**:
| Parameter | Type    | Required | Description                                                |
|-----------|---------|----------|------------------------------------------------------------|
| `expand`  | boolean | No       | Include additional detail (type, cardinality, etc)         |

**Output** (default, without `expand`):

```json
[
  {
    "block/title": "property-name",
    "block/uuid": "property-uuid"
  }
]
```

---

## Client Configuration

### Claude Desktop

Add to `claude_desktop_config.json`:

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

### Cline (VS Code)

Add to Cline MCP settings:

```json
{
  "servers": [
    {
      "name": "logseq",
      "url": "http://127.0.0.1:12315/mcp",
      "headers": {
        "Authorization": "Bearer YOUR_TOKEN"
      }
    }
  ]
}
```

### Custom Client

```javascript
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { StreamableHTTPClientTransport } from '@modelcontextprotocol/sdk/client/streamableHttp.js'

const transport = new StreamableHTTPClientTransport(
  new URL('http://127.0.0.1:12315/mcp')
)

const client = new Client({
  name: 'my-client',
  version: '1.0.0',
})

await client.connect(transport)

// List pages
const pages = await client.callTool({
  name: 'listPages',
  arguments: {},
})
```

Note: When connecting to the desktop app's MCP endpoint, requests must include an `Authorization: Bearer YOUR_TOKEN` header (handled by the HTTP API server's auth layer).

---

## Troubleshooting

### MCP Not Available

1. Ensure MCP Server is enabled in Settings -> AI
2. Check HTTP API server is running (API icon should be green)
3. Verify token authentication

### Connection Refused

1. Check host/port settings
2. Ensure Logseq desktop is running
3. Try `http://127.0.0.1:12315/` in browser

### Tool Errors

- Verify page name is correct and page exists in graph
- Use `dry-run` option with `upsertNodes` to validate operations before committing
- Use `--debug-tool` CLI option to test individual tools directly

---

## See Also

- **HTTP API**: [http-api.md](http-api.md) for general API access
- **MCP Specification**: [modelcontextprotocol.io](https://modelcontextprotocol.io/)
- **Tutorial**: [MCP Integration Tutorial](../../tutorials/plugins/mcp-integration.md)

---

_Last updated: 2026-03-10 | [Report an issue](https://github.com/logseq/logseq/issues)_
