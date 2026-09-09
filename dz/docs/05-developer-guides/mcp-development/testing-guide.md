# Logseq MCP Server Testing Guide

> **Last revised:** 2026-05-29

## Overview

This guide documents how to test the currently implemented MCP server in Logseq based on source under `src/`, `deps/`, and `packages/`.

Current implementation is the Desktop API server MCP route set in Electron:

- `POST /mcp` (initialize + requests)
- `GET /mcp` (session transport)
- `DELETE /mcp` (session close)

## Verified Architecture

### Transport and Session Model

- MCP uses streamable HTTP transport (`StreamableHTTPServerTransport`), not CLI stdio.
- Sessions are keyed by `mcp-session-id` and tracked in-memory.
- Invalid or missing session IDs return HTTP 400 errors.
- DNS rebinding protection is enabled with allowed host list derived from configured `host:port`.

### Server Wiring

- MCP routes are attached only when `:mcp-enabled?` is true in API server state.
- Routes are served by the Desktop API server host/port (default `127.0.0.1:12315`).
- MCP requests pass through the same API pre-handler path as other API routes.

### Available Tools

The MCP server registers six tools:

| Tool             | Description                                    |
| ---------------- | ---------------------------------------------- |
| `listPages`      | List pages                                     |
| `getPage`        | Get page content/blocks by page name or uuid   |
| `upsertNodes`    | Batch add/edit pages, blocks, tags, properties |
| `searchBlocks`   | Search graph blocks                            |
| `listTags`       | List tags/classes                              |
| `listProperties` | List properties                                |

## Removed or Corrected Claims

> **Removed:** "CLI `mcp-server` command validation" procedures (`./logseq mcp-server ...`) are not validated by current source in `src/`, `deps/`, and `packages/`.

> **Removed:** "Local Graph Mode" for MCP (`-g/--graph`) as a tested current implementation.

> **Removed:** "Debug tool mode" (`-t/--debug-tool`) as part of current source-verified MCP path.

> **Removed:** Claim that `listTags` and `listProperties` are unavailable in API mode. Current registered MCP tools include both.

## Testing Procedures (Current Implementation)

### 1. Preconditions

1. Start Logseq Desktop.
2. Enable Local HTTP API server.
3. Enable MCP server in settings.
4. Confirm API server is running at configured host/port.

### 2. Route Availability

Validate MCP routes on the API server:

- `POST /mcp` should accept initialize requests.
- `GET /mcp` without valid `mcp-session-id` should return 400.
- `DELETE /mcp` without valid `mcp-session-id` should return 400.

### 3. Session Lifecycle

Validate initialize and follow-up behavior:

1. Send initialize request to `POST /mcp` with no session header.
2. Capture returned `mcp-session-id`.
3. Reuse that session ID for subsequent MCP requests.
4. Call `DELETE /mcp` with the session ID.
5. Verify further requests on that session fail as invalid/missing session.

### 4. Tool Discovery and Execution

Using an MCP-capable client against the API endpoint:

1. Verify exactly six tools are discoverable: `listPages`, `getPage`, `upsertNodes`, `searchBlocks`, `listTags`, `listProperties`.
2. Execute read tools on an existing graph and validate response payloads are MCP `content` blocks with text JSON payloads.
3. Execute `upsertNodes` in dry-run mode to validate schema/operation handling without committing writes.

### 5. Error Path Validation

Validate these negative paths:

- Missing/invalid `mcp-session-id` on `GET` and `DELETE` => 400.
- Bad session on `POST` non-initialize request => JSON-RPC error response (`-32000`).
- Tool/API failures return MCP text content with error prefix (`API Error:` or `Unexpected API error:`).

### 6. Security Checks

Verify:

- MCP endpoint is not active when MCP setting is disabled.
- Allowed hosts for transport session initialization are constrained to configured `host:port`.
- Requests require valid API server authorization behavior consistent with API routes.

## Findings: New Capabilities

- Streamable HTTP MCP transport with explicit `POST`/`GET`/`DELETE` handling is now first-class in Desktop API server integration.
- Sessioned MCP lifecycle is explicitly implemented with server-side session tracking and cleanup on close.
- `listTags` and `listProperties` are part of the active registered tool surface (not API-excluded).
- `upsertNodes` supports optional `dry-run` in its input schema.
- `listPages`, `listTags`, and `listProperties` expose optional `expand` input for richer results.

## Test Checklist

- [ ] MCP enabled setting controls route availability.
- [ ] `POST /mcp` initialize succeeds.
- [ ] `GET /mcp` requires valid session ID.
- [ ] `DELETE /mcp` closes valid session and rejects invalid session.
- [ ] Tool discovery returns six registered tools.
- [ ] `listTags` and `listProperties` succeed through MCP.
- [ ] `upsertNodes` dry-run path validates without commit.
- [ ] Error responses follow expected MCP content/error behavior.

## Reporting Template

When reporting MCP issues, include:

- Desktop version/commit under test
- Host/port configuration
- Whether MCP setting was enabled
- Exact MCP request and response payloads
- Session ID lifecycle steps (initialize/use/delete)
- Reproduction steps
