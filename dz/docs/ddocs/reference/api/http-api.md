---
title: 'HTTP API Reference'
description: "Reference for Logseq's HTTP Server API for external application integration"
status: stable
lastVerified: '2026-03-10'
logseqVersion: '0.11.x'
platforms: [desktop]
graphType: [file, db]
sinceVersion: '0.10.0'
relatedDocs:
  reference:
    - '[MCP Server Reference](mcp-server.md)'
    - '[Plugin API Reference](plugin-api.md)'
  howTo:
    - '[CLI Integration](../../how-to/integrations/cli-basic.md)'
---

# HTTP API Reference

> **Last revised:** 2026-03-10

> Complete reference for Logseq's HTTP Server API, enabling external applications to interact with your knowledge graph programmatically.

## Overview

The HTTP Server API provides REST-like access to Logseq's plugin API methods. It enables:

- **Automation**: Create, read, update, delete blocks and pages
- **Integration**: Connect Logseq with external tools and services
- **Remote control**: Trigger Logseq actions from scripts or web applications
- **AI integration**: Access via MCP endpoint for AI assistants

**Base URL**: `http://127.0.0.1:12315`

---

## Quick Reference

| Endpoint      | Purpose                          | Auth Required |
| ------------- | -------------------------------- | ------------- |
| `GET /`       | HTML documentation               | No            |
| `POST /api`   | Main API endpoint                | Yes           |
| `POST /mcp`   | MCP Streamable HTTP endpoint     | Yes           |
| `GET /mcp`    | MCP SSE transport endpoint       | Yes           |
| `DELETE /mcp` | MCP session termination endpoint | Yes           |

> **Note:** The `/mcp` endpoints are only available when the MCP Server is enabled in Settings.

---

## Getting Started

### Enable the Server

1. Click the API icon (📡) in the Logseq toolbar
2. Click "Start server"
3. Server runs at `http://127.0.0.1:12315`

**Alternative**: Enable MCP Server (Settings → AI → MCP Server) to auto-start HTTP server.

### Configuration

Access via: API icon → "Server configurations"

| Setting    | Default     | Description                      |
| ---------- | ----------- | -------------------------------- |
| Host       | `127.0.0.1` | Use `0.0.0.0` for network access |
| Port       | `12315`     | Custom port if needed            |
| Auto-start | `false`     | Start with Logseq launch         |

---

## Authentication

All `/api` requests require token authentication.

### Managing Tokens

1. Click API icon → "Authorization tokens"
2. Click "+ Add new token"
3. Set name and token value
4. Save

### Generate Secure Token

```bash
# macOS/Linux
openssl rand -hex 32

# Node.js
node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"

# Python
python -c "import secrets; print(secrets.token_hex(32))"
```

### Include in Requests

```bash
curl -X POST http://127.0.0.1:12315/api \
  -H "Authorization: Bearer YOUR_TOKEN_HERE" \
  -H "Content-Type: application/json" \
  -d '{"method": "logseq.Editor.getCurrentBlock"}'
```

---

## Request Format

All API calls use `POST /api` with JSON body:

```json
{
  "method": "logseq.Namespace.methodName",
  "args": [arg1, arg2, ...]
}
```

### Example

```bash
curl -X POST http://127.0.0.1:12315/api \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"method": "logseq.Editor.insertBlock", "args": ["page-uuid", "Hello World"]}'
```

---

## Response Format

### Success

```json
{
  "result": <method_return_value>
}
```

### Error

```json
{
  "error": "Error description"
}
```

---

## Available API Methods

The HTTP API exposes all Logseq Plugin SDK methods. Organized by namespace:

### logseq.App

| Method                   | Description            | Args                             |
| ------------------------ | ---------------------- | -------------------------------- |
| `getCurrentGraph`        | Get current graph info | -                                |
| `getUserConfigs`         | Get user configuration | -                                |
| `getInfo`                | Get app info           | `key?`                           |
| `pushState`              | Navigate to route      | `type`, `opts`                   |
| `replaceState`           | Replace current route  | `type`, `opts`                   |
| `setLeftSidebarVisible`  | Toggle left sidebar    | `visible`                        |
| `setRightSidebarVisible` | Toggle right sidebar   | `visible`                        |
| `registerCommand`        | Register command       | `type`, `opts`, `action`         |
| `registerCommandPalette` | Add to command palette | `opts`, `action`                 |

### logseq.Editor

| Method                | Description              | Args                               |
| --------------------- | ------------------------ | ---------------------------------- |
| `getCurrentBlock`     | Get block being edited   | -                                  |
| `getCurrentPage`      | Get current page         | -                                  |
| `getBlock`            | Get block by ID          | `srcBlock`, `opts?`                |
| `getPage`             | Get page by name/ID      | `srcPage`, `opts?`                 |
| `getAllPages`         | Get all pages            | `repo?`                            |
| `createPage`          | Create new page          | `pageName`, `properties?`, `opts?` |
| `deletePage`          | Delete page              | `pageName`                         |
| `renamePage`          | Rename page              | `oldName`, `newName`               |
| `insertBlock`         | Insert block             | `srcBlock`, `content`, `opts?`     |
| `updateBlock`         | Update block content     | `srcBlock`, `content`, `opts?`     |
| `removeBlock`         | Delete block             | `srcBlock`                         |
| `moveBlock`           | Move block               | `srcBlock`, `targetBlock`, `opts?` |
| `insertBatchBlock`    | Insert multiple blocks   | `srcBlock`, `blocks`, `opts?`      |
| `getBlockProperty`    | Get block property       | `block`, `key`                     |
| `upsertBlockProperty` | Set block property       | `block`, `key`, `value`            |
| `removeBlockProperty` | Remove block property    | `block`, `key`                     |
| `getBlockProperties`  | Get all block properties | `block`                            |
| `scrollToBlockInPage` | Scroll to block          | `pageName`, `blockId`, `opts?`     |
| `editBlock`           | Enter edit mode          | `srcBlock`, `opts?`                |
| `exitEditingMode`     | Exit edit mode           | `selectBlock?`                     |

### logseq.DB

| Method            | Description         | Args               |
| ----------------- | ------------------- | ------------------ |
| `datascriptQuery` | Run Datalog query   | `query`, `inputs?` |
| `q`               | Shorthand for query | `query`, `inputs?` |
| `customQuery`     | Run custom query    | `query`, `inputs?` |

### logseq.UI

| Method             | Description            | Args                          |
| ------------------ | ---------------------- | ----------------------------- |
| `showMsg`          | Show notification      | `content`, `status?`, `opts?` |
| `closeMsg`         | Close notification     | `key`                         |
| `queryElementRect` | Get element dimensions | `selector`                    |

### logseq.Assets

| Method                    | Description      | Args            |
| ------------------------- | ---------------- | --------------- |
| `listFilesOfCurrentGraph` | List asset files | `exts?`         |
| `builtInOpen`             | Open asset       | `path`          |

---

## Code Examples

### Python

```python
import requests

BASE_URL = "http://127.0.0.1:12315/api"
TOKEN = "your-token-here"

def logseq_api(method, args=None):
    response = requests.post(
        BASE_URL,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/json"
        },
        json={"method": method, "args": args or []}
    )
    return response.json()

# Get all pages
pages = logseq_api("logseq.Editor.getAllPages")
print(pages)

# Insert a block
result = logseq_api("logseq.Editor.insertBlock", [
    "page-uuid",  # target block/page
    "New block content",
    {"sibling": True}
])
```

### JavaScript

```javascript
const BASE_URL = 'http://127.0.0.1:12315/api'
const TOKEN = 'your-token-here'

async function logseqApi(method, args = []) {
  const response = await fetch(BASE_URL, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${TOKEN}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ method, args }),
  })
  return response.json()
}

// Get current page
const page = await logseqApi('logseq.Editor.getCurrentPage')
console.log(page)

// Run a query
const results = await logseqApi('logseq.DB.q', [
  `[:find (pull ?b [*])
    :where [?b :block/marker ?m]
           [(contains? #{"TODO" "DOING"} ?m)]]`,
])
```

### curl

```bash
# Get all pages
curl -X POST http://127.0.0.1:12315/api \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"method": "logseq.Editor.getAllPages"}'

# Insert block to page
curl -X POST http://127.0.0.1:12315/api \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "method": "logseq.Editor.insertBlock",
    "args": ["page-uuid", "Hello from API", {"sibling": true}]
  }'

# Run Datalog query
curl -X POST http://127.0.0.1:12315/api \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "method": "logseq.DB.q",
    "args": ["[:find (pull ?p [*]) :where [?p :block/name]]"]
  }'
```

---

## Error Handling

| HTTP Status | Meaning                  |
| ----------- | ------------------------ |
| 200         | Success                  |
| 401         | Missing or invalid token |
| 400         | Invalid request format   |
| 500         | Server error             |

### Common Errors

```json
{"error": "Invalid authorization token"}
{"error": "Method not found: logseq.Invalid.method"}
{"error": "Block not found"}
```

---

## Best Practices

1. **Use unique tokens** per integration
2. **Handle errors** gracefully
3. **Rate limit** your requests
4. **Keep tokens secure** - never commit to version control
5. **Use batch operations** when possible (`insertBatchBlock`)

---

## Troubleshooting

### Server Not Starting

- Check if port 12315 is already in use
- Try a different port in settings
- Restart Logseq

### 401 Unauthorized

- Verify token is correct
- Check `Authorization: Bearer TOKEN` format
- Ensure token is saved in Logseq

### Method Not Found

- Check method name spelling
- Verify namespace is correct (e.g., `logseq.Editor.` not `logseq.editor.`)

---

## See Also

- **MCP Server**: [mcp-server.md](mcp-server.md) for AI assistant integration
- **Plugin API**: [plugin-api.md](plugin-api.md) for in-app plugin development
- **Source Code**: [src/main/frontend/handler/plugin.cljs](https://github.com/logseq/logseq)

---

_Last updated: 2026-01-29 | [Report an issue](https://github.com/logseq/logseq/issues)_
