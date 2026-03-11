# Logseq HTTP Server API Guide

> **Last revised:** 2026-03-10

## Table of Contents

- [Overview](#overview)
- [Getting Started](#getting-started)
- [Configuration](#configuration)
- [Authentication](#authentication)
- [API Endpoints](#api-endpoints)
- [Available Methods](#available-methods)
- [Request/Response Format](#requestresponse-format)
- [Code Examples](#code-examples)
- [Error Handling](#error-handling)
- [Best Practices](#best-practices)
- [Troubleshooting](#troubleshooting)
- [Use Cases](#use-cases)

## Overview

The Logseq HTTP Server API enables external applications and scripts to interact with Logseq programmatically via HTTP requests. This powerful integration mechanism allows you to:

- **Automate workflows**: Create, read, update, and delete blocks and pages
- **Build integrations**: Connect Logseq with other tools and services
- **Develop custom applications**: Build external interfaces that interact with your Logseq graph
- **Remote control**: Trigger Logseq actions from scripts, web applications, or other devices on your network

The HTTP server runs locally on your machine (by default on `http://127.0.0.1:12315`) and exposes the same API methods available to Logseq plugins, making it a robust and feature-rich integration point.

### Key Features

- ✅ Access to the full Logseq Plugin SDK API
- 🔐 Token-based authentication for security
- 🌐 CORS enabled for web-based integrations
- 🔄 Bidirectional communication with your Logseq graph
- 📝 Support for both file-based and database graphs
- 🚀 Easy to use REST API
- 🤖 [MCP (Model Context Protocol) endpoint](/docs/mcp-server-guide.md) for AI assistant integration

> **Note:** The HTTP server also hosts an MCP endpoint at `/mcp` for connecting AI assistants like Claude Desktop and Cline to your knowledge base. When you enable the MCP server (Settings → AI → MCP Server), it automatically starts the HTTP server with both the `/api` and `/mcp` endpoints available. See the [MCP Server Guide](/docs/mcp-server-guide.md) for details.

## Getting Started

### Enabling the HTTP Server

The HTTP server is built into Logseq Desktop and can be controlled from the toolbar:

1. **Locate the Server Icon**: Look for the API icon (📡) in the Logseq toolbar (top-right area)
2. **Click the Icon**: This opens a dropdown menu with server options
3. **Start the Server**: Click "Start server" to activate the HTTP server
4. **Check Status**: The icon will change to indicate the server is running

**Alternative:** Enabling the MCP server (Settings → AI → MCP Server) will automatically start the HTTP server as well, making both the `/api` and `/mcp` endpoints available.

### Accessing the Server

Once started, the server is accessible at:

```bash
http://127.0.0.1:12315
```

Visit this URL in your browser to see the built-in API documentation.

**Endpoints:**

- `/` - Interactive API documentation (HTML)
- `/api` - Main HTTP API endpoint (this guide)
- `/mcp` - Model Context Protocol endpoint ([see MCP Server Guide](/docs/mcp-server-guide.md))

## Configuration

### Server Settings

Access server configuration through the API icon menu → "Server configurations":

#### Host

- **Default**: `127.0.0.1` (localhost only)
- **Custom**: Set to `0.0.0.0` to allow access from other devices on your network
- **Security Note**: Only use `0.0.0.0` on trusted networks

#### Port

- **Default**: `12315`
- **Range**: 1-65535
- **Note**: Choose an unused port if the default is occupied

#### Auto-start

- **Enable**: Server starts automatically when Logseq launches
- **Disable**: Manual start required each time

### Saving Configuration

After changing settings:

1. Click "Save & Apply"
2. The server will restart automatically if host/port changed
3. Settings persist across Logseq sessions

> **Shared Configuration:** The [MCP Server](/docs/mcp-server-guide.md) uses the same HTTP server infrastructure and inherits these same host, port, and auto-start settings. When MCP is enabled, both `/api` and `/mcp` endpoints are available on the same server instance.

## Authentication

### Token-Based Security

All API requests require a valid authorization token for security. Tokens are configured within Logseq and must be included in every request.

### Managing Tokens

1. **Access Token Manager**: Click the API icon → "Authorization tokens"
2. **Add Token**: Click "+ Add new token"
3. **Configure Token**:
   - **Name**: Descriptive label (e.g., "Python Script", "Home Automation")
   - **Value**: The secret token string (create a secure random string)
4. **Save**: Click "Save" to store tokens

### Token Best Practices

- ✅ Use unique, randomly generated tokens
- ✅ Use different tokens for different integrations
- ✅ Keep tokens secure and never commit to version control
- ✅ Rotate tokens periodically
- ✅ Delete unused tokens

### Generating Secure Tokens

```bash
# Generate a secure random token (Linux/macOS)
openssl rand -hex 32

# Alternative (any platform with Node.js)
node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"

# Alternative (Python)
python -c "import secrets; print(secrets.token_hex(32))"
```

## API Endpoints

### GET `/`

**Purpose**: Returns the HTML documentation page

**Usage**:

```bash
curl http://127.0.0.1:12315/
```

**Response**: HTML page with API documentation

---

### POST `/mcp`

**Purpose**: Model Context Protocol (MCP) endpoint for AI assistant integration

This endpoint implements the MCP protocol, enabling AI assistants like Claude Desktop and Cline to interact with your Logseq knowledge base. It provides optimized read and write operations designed for AI use cases.

**When Available**:

- Requires MCP Server to be enabled in Settings → AI → MCP Server
- Uses the same authentication mechanism as the `/api` endpoint
- Automatically starts when you enable the MCP server

**MCP Capabilities**:

The MCP endpoint exposes three types of capabilities to AI assistants:

**1. Tools (6 available)** - Execute operations on your graph:

| Tool             | Description                         | Input                    | Output                     |
| ---------------- | ----------------------------------- | ------------------------ | -------------------------- |
| `listPages`      | List all pages in graph             | `expand` (optional bool) | Array of page objects      |
| `getPage`        | Get page content including blocks   | `pageName`               | Page entity + blocks       |
| `searchBlocks`   | Search for blocks by term           | `searchTerm`             | Array of matching blocks   |
| `upsertNodes`    | Batch create/edit pages, blocks, tags, properties | `operations`, `dry-run` (optional) | Summary of changes |
| `listTags`       | List all tags in graph              | `expand` (optional bool) | Array of tag entities      |
| `listProperties` | List all properties in graph        | `expand` (optional bool) | Array of property entities |

**What AI Assistants Can Do**:

With these capabilities, AI assistants can:

- 📖 **Read**: List pages, search content, access blocks/pages via URIs
- ✍️ **Write**: Create pages, add blocks, update existing content
- 🔍 **Analyze**: Generate summaries, find patterns, answer questions about your notes
- 🏗️ **Organize**: Create structured content using prompts, manage tags and properties
- 🔗 **Navigate**: Browse your knowledge graph using resource URIs

**Documentation**:

For complete MCP integration details, see the [**MCP Server Guide**](/docs/mcp-server-guide.md), which includes:

- Quick start guide for enabling MCP
- Detailed tool, resource, and prompt reference with examples
- Client setup for Claude Desktop, Cline, and custom implementations
- Protocol details and request/response formats
- Integration examples in Python, JavaScript, and more
- Best practices for AI assistant development

**Quick Example** (Claude Desktop config):

```json
{
  "mcpServers": {
    "logseq": {
      "url": "http://127.0.0.1:12315/mcp",
      "headers": {
        "Authorization": "Bearer YOUR_TOKEN"
      },
      "transport": "http-streamable"
    }
  }
}
```

---

### POST `/api`

**Purpose**: Invoke any Logseq Plugin SDK API method

**Headers Required**:

- `Content-Type: application/json`
- `Authorization: Bearer YOUR_TOKEN_HERE`

**Request Body**:

```json
{
  "method": "logseq.NameSpace.methodName",
  "args": [
    /* array of arguments */
  ]
}
```

## Available Methods

The HTTP Server API provides access to the complete Logseq Plugin SDK. Methods are organized into namespaces:

### Editor Namespace (`logseq.Editor.*`)

Interact with blocks, pages, and editor content:

| Method                     | Description                | Example Args                                    |
| -------------------------- | -------------------------- | ----------------------------------------------- |
| `getCurrentPage`           | Get current page info      | `[]`                                            |
| `getCurrentBlock`          | Get current block          | `[]`                                            |
| `getBlock`                 | Get block by UUID          | `["block-uuid"]`                                |
| `getPage`                  | Get page by name/UUID      | `["page-name"]`                                 |
| `createPage`               | Create a new page          | `["Page Name", {"journal": false}]`             |
| `insertBlock`              | Insert a new block         | `["parent-uuid", "content", {"before": false}]` |
| `updateBlock`              | Update block content       | `["block-uuid", "new content"]`                 |
| `removeBlock`              | Delete a block             | `["block-uuid"]`                                |
| `moveBlock`                | Move block to new location | `["block-uuid", "target-uuid"]`                 |
| `getPageBlocksTree`        | Get page's block tree      | `["page-name"]`                                 |
| `getCurrentPageBlocksTree` | Get current page blocks    | `[]`                                            |
| `selectBlock`              | Select a block in UI       | `["block-uuid"]`                                |
| `editBlock`                | Open block for editing     | `["block-uuid"]`                                |
| `exitEditingMode`          | Exit edit mode             | `[]`                                            |
| `checkEditing`             | Check if editing           | `[]`                                            |

### App Namespace (`logseq.App.*`)

Application-level operations:

| Method                   | Description             | Example Args                     |
| ------------------------ | ----------------------- | -------------------------------- |
| `getCurrentGraph`        | Get current graph info  | `[]`                             |
| `getStateFromStore`      | Get app state value     | `["ui/sidebar-open?"]`           |
| `setStateFromStore`      | Set app state value     | `["path", value]`                |
| `showMsg`                | Show notification       | `["Message", "success"]`         |
| `queryRecentPages`       | Get recent pages        | `[]`                             |
| `getUserConfigs`         | Get user configuration  | `[]`                             |
| `setLeftSidebarVisible`  | Show/hide left sidebar  | `[true]`                         |
| `setRightSidebarVisible` | Show/hide right sidebar | `["toggle"]`                     |
| `openExternalLink`       | Open URL externally     | `["https://example.com"]`        |
| `pushState`              | Navigate to route       | `["page", {"name": "PageName"}]` |
| `replaceState`           | Replace route state     | `["page", {"name": "PageName"}]` |
| `quit`                   | Exit Logseq             | `[]`                             |

### DB Namespace (`logseq.DB.*`)

Database query operations:

| Method            | Description       | Example Args                                |
| ----------------- | ----------------- | ------------------------------------------- |
| `datascriptQuery` | Run Datalog query | `["[:find ?b :where [?b :block/content]]"]` |
| `q`               | Query database    | `["(and [[page]] [[tag]])"]`                |

### UI Namespace (`logseq.UI.*`)

User interface operations:

| Method    | Description          | Example Args            |
| --------- | -------------------- | ----------------------- |
| `showMsg` | Display notification | `["Hello!", "success"]` |

### Git Namespace (`logseq.Git.*`)

Git operations (when available):

| Method        | Description         | Example Args |
| ------------- | ------------------- | ------------ |
| `execCommand` | Execute Git command | `["status"]` |

## Request/Response Format

### Request Structure

```http
POST /api HTTP/1.1
Host: 127.0.0.1:12315
Content-Type: application/json
Authorization: Bearer your-token-here

{
  "method": "logseq.Editor.getBlock",
  "args": ["block-uuid-here"]
}
```

### Response Structure

#### Success Response

```json
{
  "uuid": "block-uuid",
  "content": "Block content here",
  "page": {"id": 123, "name": "page-name"},
  "parent": {"id": 456},
  ...
}
```

#### Error Response (400 - Bad Request)

```json
{
  "message": ":method of body is missing!"
}
```

#### Error Response (401 - Unauthorized)

```json
{
  "message": "Access Denied!"
}
```

#### Error Response (500 - Internal Error)

```json
{
  "error": "Error message from API call"
}
```

## Code Examples

### JavaScript/Node.js

```javascript
const axios = require('axios')

const LOGSEQ_API = 'http://127.0.0.1:12315/api'
const TOKEN = 'your-token-here'

async function callLogseqAPI(method, args = []) {
  try {
    const response = await axios.post(
      LOGSEQ_API,
      {
        method: method,
        args: args,
      },
      {
        headers: {
          Authorization: `Bearer ${TOKEN}`,
          'Content-Type': 'application/json',
        },
      }
    )
    return response.data
  } catch (error) {
    console.error('API Error:', error.response?.data || error.message)
    throw error
  }
}

// Example usage
async function main() {
  // Get current page
  const page = await callLogseqAPI('logseq.Editor.getCurrentPage')
  console.log('Current page:', page)

  // Show a message
  await callLogseqAPI('logseq.UI.showMsg', ['Hello from Node.js!', 'success'])

  // Create a new page
  const newPage = await callLogseqAPI('logseq.Editor.createPage', [
    'API Test Page',
    { journal: false },
  ])
  console.log('Created page:', newPage)

  // Insert a block
  const block = await callLogseqAPI('logseq.Editor.insertBlock', [
    newPage.uuid,
    'This block was created via API!',
    { sibling: false },
  ])
  console.log('Created block:', block)
}

main().catch(console.error)
```

### Python

```python
import requests
import json

LOGSEQ_API = 'http://127.0.0.1:12315/api'
TOKEN = 'your-token-here'

def call_logseq_api(method, args=None):
    """Call Logseq API with method and arguments"""
    if args is None:
        args = []

    headers = {
        'Authorization': f'Bearer {TOKEN}',
        'Content-Type': 'application/json'
    }

    payload = {
        'method': method,
        'args': args
    }

    response = requests.post(LOGSEQ_API, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()

# Example usage
def main():
    # Get current page
    page = call_logseq_api('logseq.Editor.getCurrentPage')
    print(f'Current page: {page}')

    # Show notification
    call_logseq_api('logseq.UI.showMsg', ['Hello from Python!', 'success'])

    # Get a specific block
    block = call_logseq_api('logseq.Editor.getBlock', ['block-uuid-here'])
    print(f'Block content: {block.get("content")}')

    # Query database
    query = '[:find ?b ?content :where [?b :block/content ?content]]'
    results = call_logseq_api('logseq.DB.datascriptQuery', [query])
    print(f'Query returned {len(results)} results')

if __name__ == '__main__':
    main()
```

### Bash/cURL

```bash
#!/bin/bash

LOGSEQ_API="http://127.0.0.1:12315/api"
TOKEN="your-token-here"

# Function to call Logseq API
logseq_api() {
    local method=$1
    local args=$2

    curl -s -X POST "$LOGSEQ_API" \
        -H "Content-Type: application/json" \
        -H "Authorization: Bearer $TOKEN" \
        -d "{\"method\": \"$method\", \"args\": $args}"
}

# Get current page
echo "Getting current page..."
logseq_api "logseq.Editor.getCurrentPage" "[]"

# Show a message
echo "Showing message..."
logseq_api "logseq.UI.showMsg" '["Hello from Bash!", "success"]'

# Get page by name
echo "Getting page..."
logseq_api "logseq.Editor.getPage" '["My Page"]'
```

### PowerShell

```powershell
# Configuration
$LOGSEQ_API = "http://127.0.0.1:12315/api"
$TOKEN = "your-token-here"

function Invoke-LogseqAPI {
    param(
        [string]$Method,
        [array]$Args = @()
    )

    $headers = @{
        "Authorization" = "Bearer $TOKEN"
        "Content-Type" = "application/json"
    }

    $body = @{
        method = $Method
        args = $Args
    } | ConvertTo-Json -Depth 10

    try {
        $response = Invoke-RestMethod -Uri $LOGSEQ_API -Method Post -Headers $headers -Body $body
        return $response
    }
    catch {
        Write-Error "API Error: $_"
        throw
    }
}

# Example usage
# Get current page
$page = Invoke-LogseqAPI -Method "logseq.Editor.getCurrentPage"
Write-Host "Current page: $($page | ConvertTo-Json)"

# Show notification
Invoke-LogseqAPI -Method "logseq.UI.showMsg" -Args @("Hello from PowerShell!", "success")

# Create page and block
$newPage = Invoke-LogseqAPI -Method "logseq.Editor.createPage" -Args @("Test Page", @{journal=$false})
Invoke-LogseqAPI -Method "logseq.Editor.insertBlock" -Args @($newPage.uuid, "Content from PowerShell", @{sibling=$false})
```

### Go

```go
package main

import (
    "bytes"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
)

const (
    LogseqAPI = "http://127.0.0.1:12315/api"
    Token     = "your-token-here"
)

type APIRequest struct {
    Method string        `json:"method"`
    Args   []interface{} `json:"args"`
}

func callLogseqAPI(method string, args []interface{}) (interface{}, error) {
    if args == nil {
        args = []interface{}{}
    }

    reqBody := APIRequest{
        Method: method,
        Args:   args,
    }

    jsonData, err := json.Marshal(reqBody)
    if err != nil {
        return nil, err
    }

    req, err := http.NewRequest("POST", LogseqAPI, bytes.NewBuffer(jsonData))
    if err != nil {
        return nil, err
    }

    req.Header.Set("Content-Type", "application/json")
    req.Header.Set("Authorization", "Bearer "+Token)

    client := &http.Client{}
    resp, err := client.Do(req)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    body, err := io.ReadAll(resp.Body)
    if err != nil {
        return nil, err
    }

    var result interface{}
    if err := json.Unmarshal(body, &result); err != nil {
        return nil, err
    }

    return result, nil
}

func main() {
    // Get current page
    page, err := callLogseqAPI("logseq.Editor.getCurrentPage", nil)
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }
    fmt.Printf("Current page: %v\n", page)

    // Show message
    _, err = callLogseqAPI("logseq.UI.showMsg", []interface{}{"Hello from Go!", "success"})
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }
}
```

## Error Handling

### Common HTTP Status Codes

| Code | Meaning               | Cause                              |
| ---- | --------------------- | ---------------------------------- |
| 200  | OK                    | Request successful                 |
| 400  | Bad Request           | Missing or invalid method          |
| 401  | Unauthorized          | Missing, invalid, or expired token |
| 500  | Internal Server Error | API method threw an error          |

### Error Response Format

Errors return a JSON object with an `error` key:

```json
{
  "error": "MethodNotExist: invalid_method_name"
}
```

### Handling Errors in Code

```javascript
async function safeAPICall(method, args) {
  try {
    const result = await callLogseqAPI(method, args)

    // Check for API-level errors
    if (result && result.error) {
      console.error('API returned error:', result.error)
      return null
    }

    return result
  } catch (error) {
    // HTTP-level errors
    if (error.response) {
      console.error('HTTP Error:', error.response.status, error.response.data)
    } else {
      console.error('Network Error:', error.message)
    }
    return null
  }
}
```

### Common Error Scenarios

1. **Token Authentication Failure**

   - **Error**: "Access Denied!"
   - **Solution**: Check token is correct and matches one in Logseq

2. **Method Not Found**

   - **Error**: "MethodNotExist: method_name"
   - **Solution**: Verify method name spelling and namespace

3. **Server Not Running**

   - **Error**: Connection refused
   - **Solution**: Start the HTTP server in Logseq

4. **Invalid Arguments**
   - **Error**: Various JavaScript errors
   - **Solution**: Check API documentation for correct argument types

## Best Practices

### Security

1. **Network Access**

   - Keep server on `127.0.0.1` when possible
   - Only use `0.0.0.0` on trusted networks
   - Consider using a reverse proxy with HTTPS for remote access

2. **Token Management**

   - Store tokens in environment variables, not in code
   - Use different tokens for different applications
   - Regularly rotate tokens
   - Never commit tokens to version control

3. **Rate Limiting**
   - Implement delays between requests to avoid overwhelming Logseq
   - Consider queuing operations for bulk updates

### Performance

1. **Batch Operations**

   ```javascript
   // Instead of many individual updates
   for (const uuid of blockUUIDs) {
     await updateBlock(uuid, content) // Slow!
   }

   // Use Promise.all for parallel operations
   await Promise.all(blockUUIDs.map((uuid) => updateBlock(uuid, content)))
   ```

2. **Cache Results**

   - Cache graph structure information
   - Avoid redundant queries for same data
   - Invalidate cache after modifications

3. **Efficient Queries**
   - Use specific queries instead of fetching all data
   - Limit query results when possible

### Code Organization

1. **Create a wrapper library**

   ```javascript
   class LogseqClient {
     constructor(baseURL, token) {
       this.baseURL = baseURL
       this.token = token
     }

     async call(method, args) {
       // Implementation
     }

     // Convenience methods
     async getCurrentPage() {
       return this.call('logseq.Editor.getCurrentPage')
     }

     async showMessage(text, type = 'info') {
       return this.call('logseq.UI.showMsg', [text, type])
     }
   }
   ```

2. **Environment Configuration**
3. ```javascript
   // config.js
   module.exports = {
     logseqAPI: process.env.LOGSEQ_API_URL || 'http://127.0.0.1:12315/api',
     token: process.env.LOGSEQ_TOKEN,
     timeout: parseInt(process.env.LOGSEQ_TIMEOUT || '5000'),
   }
   ```

## Troubleshooting

### Server Won't Start

**Symptoms**: Server status shows "error" or won't start

**Solutions**:

1. Check if port is already in use:

   ```bash
   # Linux/macOS
   lsof -i :12315

   # Windows
   netstat -ano | findstr :12315
   ```

2. Try a different port in configuration
3. Check Logseq logs for specific error messages
4. Restart Logseq

### Authentication Failures

**Symptoms**: 401 Unauthorized responses

**Solutions**:

1. Verify token exists in Logseq authorization tokens list
2. Check token is correctly copied (no extra spaces)
3. Ensure `Bearer` prefix in Authorization header
4. Regenerate token if necessary

### Connection Refused

**Symptoms**: Cannot connect to server

**Solutions**:

1. Verify server is running (check API icon)
2. Confirm URL and port are correct
3. Check firewall settings
4. Try accessing from browser first: `http://127.0.0.1:12315/`

### Method Not Found

**Symptoms**: "MethodNotExist" error

**Solutions**:

1. Verify method name format: `logseq.Namespace.methodName`
2. Check [plugin documentation](https://plugins-doc.logseq.com) for correct method names
3. Ensure method is available in your Logseq version

### Timeout Issues

**Symptoms**: Requests take too long or timeout

**Solutions**:

1. Increase HTTP client timeout
2. Check Logseq isn't busy with other operations
3. Reduce complexity of queries
4. Check system resources (CPU, memory)

## Use Cases

### 1. AI Assistant Integration

**Connect AI assistants to your Logseq knowledge base**

The MCP endpoint enables AI assistants like Claude Desktop and Cline to read from and write to your knowledge base, creating a powerful AI-augmented workflow.

**What you can do:**

- Ask AI to search your notes for specific information
- Have AI create pages and add structured content
- Get AI-powered summaries of your pages
- AI-assisted task management and project tracking
- Natural language queries against your knowledge base

**Example: Claude Desktop searching your notes**

1. **Enable MCP** in Logseq (Settings → AI → MCP Server)
2. **Configure Claude Desktop** with your Logseq MCP endpoint:

```json
{
  "mcpServers": {
    "logseq": {
      "url": "http://127.0.0.1:12315/mcp",
      "headers": {
        "Authorization": "Bearer YOUR_TOKEN"
      },
      "transport": "http-streamable"
    }
  }
}
```

3. **Use natural language** with Claude:
   - "Search my Logseq for all TODO items related to the XYZ project"
   - "Add meeting notes from today's discussion to my Work page"
   - "Summarize the content of my 'Project Ideas' page"
   - "Create a new page called 'Q1 Goals' and add bullet points"

**How it works:**

When you ask Claude to interact with Logseq, it automatically:

- Calls the appropriate MCP tools (`searchBlocks`, `addToPage`, `getPage`, etc.)
- Processes the JSON-RPC protocol communication
- Returns natural language responses based on your knowledge base

**Benefits:**

- **No code required** - Just talk to your AI assistant naturally
- **Bidirectional** - Read existing content and create new content
- **Context-aware** - AI has access to your entire knowledge graph
- **Automated workflows** - Combine AI reasoning with your notes

**Setup Resources:**

See the complete [MCP Server Guide](/docs/mcp-server-guide.md) for:

- Detailed client configuration steps
- Integration examples for different AI assistants
- Custom client development in Python/JavaScript
- Best practices and troubleshooting

---

### 2. Automated Daily Journal Creation

Create a daily journal entry with a template:

```python
import datetime

def create_daily_journal():
    today = datetime.date.today()
    page_name = today.strftime('%Y-%m-%d')

    # Create journal page
    page = call_logseq_api('logseq.Editor.createPage', [
        page_name,
        {'journal': True}
    ])

    # Add template blocks
    template = [
        '## Goals',
        '- ',
        '## Notes',
        '- ',
        '## Review',
        '- What went well:',
        '- What to improve:'
    ]

    for line in template:
        call_logseq_api('logseq.Editor.insertBlock', [
            page['uuid'],
            line,
            {'sibling': True}
        ])
```

### 2. Import Data from External Sources

Import tasks from a project management tool:

```javascript
async function importTasks(tasks) {
  // Get or create project page
  let page = await callLogseqAPI('logseq.Editor.getPage', ['Projects'])
  if (!page) {
    page = await callLogseqAPI('logseq.Editor.createPage', [
      'Projects',
      { journal: false },
    ])
  }

  // Import each task
  for (const task of tasks) {
    const content = `TODO ${task.title}
  - Due: [[${task.dueDate}]]
  - Priority: ${task.priority}
  - ${task.description}`

    await callLogseqAPI('logseq.Editor.insertBlock', [
      page.uuid,
      content,
      { sibling: true },
    ])
  }

  await callLogseqAPI('logseq.UI.showMsg', [
    `Imported ${tasks.length} tasks`,
    'success',
  ])
}
```

### 3. Export Data for Reports

Generate weekly report:

```javascript
async function generateWeeklyReport() {
  const query = `
    [:find ?b ?content ?page
     :where
     [?b :block/content ?content]
     [?b :block/page ?p]
     [?p :block/name ?page]
     [(clojure.string/includes? ?content "TODO")]]
  `

  const results = await callLogseqAPI('logseq.DB.datascriptQuery', [query])

  // Process results
  const report = results.map(([blockId, content, page]) => ({
    page,
    task: content,
  }))

  // Create report page
  const reportPage = await callLogseqAPI('logseq.Editor.createPage', [
    `Weekly Report ${new Date().toISOString().split('T')[0]}`,
    { journal: false },
  ])

  for (const item of report) {
    await callLogseqAPI('logseq.Editor.insertBlock', [
      reportPage.uuid,
      `${item.task} ([[${item.page}]])`,
      { sibling: true },
    ])
  }
}
```

### 4. Smart Home Integration

Control Logseq from Home Assistant or other smart home systems:

```python
# Add note via voice command or automation
def log_note(note_text):
    """Log a quick note to today's journal"""
    # Get current journal page
    page = call_logseq_api('logseq.Editor.getCurrentPage')

    # Add timestamped note
    timestamp = datetime.datetime.now().strftime('%H:%M')
    content = f"{timestamp} - {note_text}"

    call_logseq_api('logseq.Editor.insertBlock', [
        page['uuid'],
        content,
        {'sibling': True}
    ])

    return "Note added to Logseq"
```

### 5. Backup and Sync Script

Create backups with custom metadata:

```bash
#!/bin/bash
# Backup script using Logseq API

# Get all pages
pages=$(logseq_api "logseq.DB.datascriptQuery" '[
  "[:find ?name :where [?p :block/name ?name]]"
]')

# Create backup page
backup_page=$(logseq_api "logseq.Editor.createPage" '[
  "Backup '$(date +%Y-%m-%d)'",
  {"journal": false}
]')

# Add backup metadata
logseq_api "logseq.Editor.insertBlock" "[
  \"${backup_page}\",
  \"Backup created at $(date)\",
  {\"sibling\": false}
]"

echo "Backup complete: ${backup_page}"
```

### 6. Cross-Reference Generator

Find and link related content:

```javascript
async function findAndLinkRelated(searchTerm) {
  // Query for blocks containing search term
  const query = `
    [:find ?b ?content
     :where
     [?b :block/content ?content]
     [(clojure.string/includes? ?content "${searchTerm}")]]
  `

  const results = await callLogseqAPI('logseq.DB.datascriptQuery', [query])

  // Create a summary page
  const summaryPage = await callLogseqAPI('logseq.Editor.createPage', [
    `Links to: ${searchTerm}`,
    { journal: false },
  ])

  // Add all references
  for (const [blockId, content] of results) {
    await callLogseqAPI('logseq.Editor.insertBlock', [
      summaryPage.uuid,
      `((${blockId}))`, // Block reference
      { sibling: true },
    ])
  }

  return summaryPage
}
```

## Additional Resources

### Official Documentation

- **MCP Server Guide**: [/docs/mcp-server-guide.md](/docs/mcp-server-guide.md) - AI assistant integration with the `/mcp` endpoint
- **Plugin API Documentation**: https://plugins-doc.logseq.com
- **Plugin SDK on npm**: https://www.npmjs.com/package/@logseq/libs
- **Logseq GitHub**: https://github.com/logseq/logseq
- **Logseq Forum**: https://discuss.logseq.com

### Community Resources

- **Discord Community**: https://discord.com/invite/KpN4eHY
- **Plugin Examples**: https://github.com/logseq/logseq-plugin-samples
- **Community Plugins**: https://github.com/topics/logseq-plugin

### TypeScript Type Definitions

For TypeScript projects, install the SDK for full type support:

```bash
npm install --save-dev @logseq/libs
```

Then reference types:

```typescript
import type { BlockEntity, PageEntity } from '@logseq/libs/dist/LSPlugin'
```

## Conclusion

The Logseq HTTP Server API provides a powerful way to extend and integrate Logseq with external tools and workflows. Whether you're automating repetitive tasks, building custom integrations, or creating entirely new interfaces, the API gives you programmatic access to your knowledge graph.

Remember to:

- Keep your tokens secure
- Test thoroughly before deploying automation
- Monitor API performance and errors
- Contribute back to the community with your integrations

Happy integrating! 🚀

---

_Document Version: 1.1_
_Last Updated: 2026-03-10_
_Logseq Version: DB Version Compatible_
