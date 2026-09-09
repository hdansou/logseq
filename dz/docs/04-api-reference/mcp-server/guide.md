# Logseq MCP Server Integration Guide

> **Last revised:** 2026-05-29

## Overview

This guide documents the MCP server behavior verified from current source in:

- src/electron/electron/mcp_server.cljs
- src/electron/electron/server.cljs
- src/main/frontend/components/settings.cljs
- src/main/logseq/api/db_based/tools.cljs

The implementation in this workspace is the Desktop HTTP API server MCP integration at /mcp.

## What Is Implemented

### Endpoint and Transport

- MCP endpoint is mounted on the Desktop HTTP API server at /mcp.
- Routes are:
  - POST /mcp for initialize and tool calls
  - GET /mcp for session transport requests
  - DELETE /mcp for session close
- Session handling uses mcp-session-id headers.
- New session initialization requires a valid MCP initialize request without an existing session header.

### Authentication

- MCP routes use the same pre-handler auth as the HTTP API server.
- Requests must include Authorization: Bearer <token> unless hitting /.
- Token validation is shared with server token configuration.

### Desktop Settings Behavior

From settings UI behavior:

- The MCP toggle persists :mcp-enabled? in server config.
- Enabling MCP also sets HTTP server enabled in storage to simplify MCP usage.
- If server status is already running, toggling MCP triggers a server restart to apply route changes.

### Available Tools (Verified)

The MCP server registers exactly these six tools:

1. listPages
2. getPage
3. upsertNodes
4. searchBlocks
5. listTags
6. listProperties

#### Tool Inputs

- listPages
  - expand optional boolean
- getPage
  - pageName string (page name or uuid)
- searchBlocks
  - searchTerm string
- listTags
  - expand optional boolean
- listProperties
  - expand optional boolean
- upsertNodes
  - operations array of operation objects
  - dry-run optional boolean

#### Data Shapes and Notes

- listPages (minimal mode) returns page :block/title and string :block/uuid.
- listPages with expand: true returns minimal payload fields to reduce response size (:block/uuid, :block/title, :block/created-at, :block/updated-at).
- getPage returns:
  - entity (page metadata with hidden/internal fields removed)
  - blocks (tree-derived blocks with filtered fields)
- listTags / listProperties:
  - minimal mode: title + uuid
  - expand mode: richer metadata with selected field normalization
- searchBlocks delegates to app search API with snippets disabled.
- upsertNodes supports batch add/edit operations with schema validation.

## Findings: New Capabilities

Compared with older guide revisions, current source confirms these capabilities clearly:

1. Streamable HTTP session lifecycle is explicitly implemented with POST, GET, and DELETE handlers for /mcp.
2. DNS rebinding protections are enabled in MCP transport initialization (enableDnsRebindingProtection) with host allowlist based on configured host and port.
3. Expand controls on listPages, listTags, and listProperties are first-class input schema options.
4. upsertNodes includes explicit dry-run validation mode in MCP input schema.

## Removed or Corrected Claims

> **Removed:** CLI mcp-server operational instructions (flags, transports, and examples) are removed from this guide because no current MCP CLI implementation source was found under src/, deps/, or packages/ in this workspace snapshot. This guide now documents only the verified Desktop MCP implementation.

> **Removed:** Tool references to addToPage and updateBlock. Current MCP tool registry exposes only six tools listed above, with writes done through upsertNodes.

> **Removed:** Architecture/file path claims pointing to deps/cli/src/logseq/cli/common/mcp/server.cljs and related MCP CLI source paths. These paths are not present in the current workspace source tree.

> **Removed:** Branch/version claims such as feat/mcp-server branch, fixed MCP SDK pin assertions, and protocol version pin text that are not validated by current in-tree source docs.

## Usage Notes for Clients

### Minimal Session Flow

1. Send initialize JSON-RPC request to POST /mcp without mcp-session-id.
2. Store returned session id header.
3. Call tools/list and tools/call on POST /mcp with mcp-session-id header.
4. Optionally use GET /mcp with session header if your MCP client transport requires it.
5. Close with DELETE /mcp and session header.

### Error Behavior

- Invalid or missing session id for active-session routes returns HTTP 400.
- Auth failures return HTTP 401 from shared API pre-handler.
- Tool execution errors are returned in MCP content payload text responses.

## Source Map

Primary source references used for this validation:

- src/electron/electron/mcp_server.cljs
- src/electron/electron/server.cljs
- src/main/frontend/components/settings.cljs
- src/main/logseq/api/db_based/tools.cljs
- deps/cli/docs/building_logseq_cli_from_source.md (docs-only mention of MCP CLI source path, no corresponding in-tree implementation files in this workspace)

## Additional Resources

- MCP specification: https://modelcontextprotocol.io/
- MCP TypeScript SDK: https://github.com/modelcontextprotocol/typescript-sdk
- HTTP API guide: ./http-server-api-guide.md
