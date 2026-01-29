---
title: 'MCP Server Reference'
description: "Reference for Logseq's Model Context Protocol (MCP) server for AI assistant integration"
status: stable
lastVerified: '2026-01-29'
logseqVersion: '0.11.x'
platforms: [desktop]
graphType: [file, db]
sinceVersion: '0.10.5'
relatedDocs:
  tutorials:
    - '[MCP Integration Tutorial](../../tutorials/plugins/mcp-integration.md)'
  reference:
    - '[HTTP API Reference](http-api.md)'
---

# MCP Server Reference

> Reference for Logseq's Model Context Protocol (MCP) server, enabling AI assistants to interact with your knowledge graph.

## Overview

The **MCP Server** implements the [Model Context Protocol](https://modelcontextprotocol.io/) specification, allowing AI assistants like Claude Desktop, Cline, and other MCP-compatible clients to:

- **Read** pages, blocks, tags, and properties from your graph
- **Write** new blocks and update existing content
- **Search** your knowledge base semantically
- **Query** structured data

**Endpoint**: `http://127.0.0.1:12315/mcp`

---

## Quick Reference

| Tool             | Description        | Input                           |
| ---------------- | ------------------ | ------------------------------- |
| `listPages`      | Get all pages      | -                               |
| `getPage`        | Get page content   | `pageName`                      |
| `searchBlocks`   | Search blocks      | `searchTerm`                    |
| `addToPage`      | Add block to page  | `pageName`, `content`, `force?` |
| `updateBlock`    | Update block       | `blockUUID`, `content`          |
| `listTags`       | Get all tags       | -                               |
| `listProperties` | Get all properties | -                               |

---

## Getting Started

### Enable in Desktop App

1. **Open Settings**: Click gear icon or press `t s`
2. **Go to AI Settings**: Find "AI" section
3. **Enable MCP Server**: Toggle ON
4. **Configure Token**: Click API icon → "Authorization tokens" → Add token
5. **Verify**: Visit `http://127.0.0.1:12315/mcp` in browser

### Enable via CLI

```bash
# Navigate to CLI directory
cd /path/to/logseq/deps/cli

# Run with local graph
yarn logseq mcp-server -g /path/to/your/graph

# Run with HTTP transport
yarn logseq mcp-server -g /path/to/graph --port 3000

# Run with stdio (for Claude Desktop)
yarn logseq mcp-server -g /path/to/graph --stdio
```

---

## Configuration

### Desktop Settings

| Setting    | Default     | Description       |
| ---------- | ----------- | ----------------- |
| MCP Server | `false`     | Toggle MCP on/off |
| Host       | `127.0.0.1` | Server host       |
| Port       | `12315`     | Server port       |

### CLI Options

| Option                | Description               |
| --------------------- | ------------------------- |
| `-g, --graph <path>`  | Path to graph directory   |
| `--port <number>`     | HTTP port (default: 3000) |
| `--host <address>`    | Host address              |
| `--stdio`             | Use stdio transport       |
| `--debug-tool <name>` | Debug specific tool       |

---

## MCP Tools Reference

### listPages

Get all pages in the graph.

**Input**: None

**Output**:

```json
{
  "pages": [
    {
      "name": "Page Name",
      "uuid": "page-uuid",
      "createdAt": 1640000000000,
      "updatedAt": 1640000000000
    }
  ]
}
```

**Example (Claude)**:

```
Use the listPages tool to show me all pages in my knowledge base.
```

---

### getPage

Get full content of a specific page.

**Input**:
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `pageName` | string | Yes | Page name or UUID |

**Output**:

```json
{
  "page": {
    "name": "Page Name",
    "uuid": "page-uuid",
    "blocks": [
      {
        "uuid": "block-uuid",
        "content": "Block content",
        "children": [...]
      }
    ]
  }
}
```

---

### searchBlocks

Search for blocks containing specific text.

**Input**:
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `searchTerm` | string | Yes | Text to search for |

**Output**:

```json
{
  "blocks": [
    {
      "uuid": "block-uuid",
      "content": "Matching content",
      "page": "Page Name"
    }
  ]
}
```

---

### addToPage

Add a new block to a page.

**Input**:
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `pageName` | string | Yes | Target page name |
| `content` | string | Yes | Block content |
| `force` | boolean | No | Create page if not exists |

**Output**:

```json
{
  "success": true,
  "blockUUID": "new-block-uuid"
}
```

---

### updateBlock

Update an existing block's content.

**Input**:
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `blockUUID` | string | Yes | Block UUID |
| `content` | string | Yes | New content |

**Output**:

```json
{
  "success": true
}
```

---

### listTags

Get all tags/classes in the graph.

**Input**: None

**Output**:

```json
{
  "tags": [
    {
      "name": "Tag Name",
      "uuid": "tag-uuid",
      "count": 42
    }
  ]
}
```

---

### listProperties

Get all properties defined in the graph.

**Input**: None

**Output**:

```json
{
  "properties": [
    {
      "name": "property-name",
      "type": "string",
      "cardinality": "one"
    }
  ]
}
```

---

## MCP Resources

Resources provide direct URI-based access to content:

| URI Pattern                  | Description           |
| ---------------------------- | --------------------- |
| `logseq://page/{pageName}`   | Access specific page  |
| `logseq://block/{blockUUID}` | Access specific block |
| `logseq://graph`             | Access graph metadata |

---

## MCP Prompts

Pre-built AI workflows:

### search-and-summarize

Search and summarize a topic from your knowledge base.

**Parameters**:
| Parameter | Type | Description |
|-----------|------|-------------|
| `topic` | string | Subject to search for |
| `depth` | string | How detailed (optional) |

**Example**:

```
Use the search-and-summarize prompt to summarize everything about machine learning.
```

### create-page-structure

Generate structured template for a new page.

**Parameters**:
| Parameter | Type | Description |
|-----------|------|-------------|
| `pageType` | string | Type (e.g., "meeting notes") |
| `pageName` | string | Name for new page |
| `sections` | array | Custom sections (optional) |

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
        "/path/to/logseq/deps/cli/dist/logseq.js",
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
import { StreamableHTTPClientTransport } from '@anthropic-ai/sdk/mcp'

const transport = new StreamableHTTPClientTransport({
  url: 'http://127.0.0.1:12315/mcp',
  headers: {
    Authorization: 'Bearer YOUR_TOKEN',
  },
})

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

---

## Troubleshooting

### MCP Not Available

1. Ensure MCP Server is enabled in Settings → AI
2. Check HTTP server is running (API icon should be green)
3. Verify token authentication

### Connection Refused

1. Check host/port settings
2. Ensure Logseq desktop is running
3. Try `http://127.0.0.1:12315/` in browser

### Tool Errors

```json
{ "error": "Page not found" }
```

- Verify page name is correct
- Check page exists in graph

---

## See Also

- **HTTP API**: [http-api.md](http-api.md) for general API access
- **MCP Specification**: [modelcontextprotocol.io](https://modelcontextprotocol.io/)
- **Tutorial**: [MCP Integration Tutorial](../../tutorials/plugins/mcp-integration.md)

---

_Last updated: 2026-01-29 | [Report an issue](https://github.com/logseq/logseq/issues)_
