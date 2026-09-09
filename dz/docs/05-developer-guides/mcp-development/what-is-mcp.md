# What is Model Context Protocol (MCP)?

> **Last revised:** 2026-05-29

Model Context Protocol is an open standard that lets host applications expose tools and context to LLMs through a common protocol.

In this repository, the currently implemented MCP integration is the Desktop HTTP API server MCP endpoint, mounted at `/mcp` in Electron server code.

## Core Concepts

### Architecture Components

- **MCP Hosts**: Applications that initiate MCP connections (for example IDE agents or assistant apps)
- **MCP Clients**: Protocol clients embedded in those hosts
- **MCP Servers**: Programs/endpoints that expose tools and context over MCP
- **Tool Calls**: Structured invocations from the model to a server capability
- **Session Transport**: Per-session request lifecycle between host and server

### Key Features

- **Tools**: Functions an LLM can call with validated inputs
- **Structured Inputs**: Server-side schema validation for tool arguments
- **Sessioned Transport**: Request/response lifecycle tied to `mcp-session-id`

## Logseq Implementation (Verified)

Current source under `src/` shows a Desktop MCP integration with these characteristics:

- MCP endpoint routes are registered on the Electron API server at `/mcp`.
- `POST /mcp` handles initialization and tool calls.
- `GET /mcp` handles session transport requests.
- `DELETE /mcp` closes a session transport.
- Session IDs are tracked server-side in an atom and passed through `mcp-session-id`.
- MCP route access uses the same auth pre-handler as other API endpoints (Bearer token required except `/`).

### Verified Tool Surface

The MCP server currently registers exactly six tools:

1. `listPages`
2. `getPage`
3. `upsertNodes`
4. `searchBlocks`
5. `listTags`
6. `listProperties`

The write path is `upsertNodes` (with optional `dry-run`), not separate `addToPage`/`updateBlock` tools.

### Settings Integration

From the frontend settings source:

- MCP is toggled by `:mcp-enabled?`.
- Enabling MCP also enables HTTP server storage config if it is off.
- If the API server is running, toggling MCP triggers restart so route changes apply.

## Findings: New Capabilities

Compared with older generic MCP guidance, current in-repo implementation confirms:

1. Streamable HTTP MCP transport lifecycle via `POST`/`GET`/`DELETE` on `/mcp`.
2. Session-based MCP request handling with explicit `mcp-session-id` validation paths.
3. DNS rebinding protection enabled for transport initialization with allowed host list.
4. Expanded list options for `listPages`, `listTags`, and `listProperties`.
5. `upsertNodes` supports schema-validated batch operations and `dry-run` mode.

## Removed or Corrected Claims

> **Removed:** This guide no longer claims a currently implemented CLI MCP server under `deps/` or `packages/`. In this workspace snapshot, MCP server implementation is verified in `src/electron/electron/*`.
> **Removed:** Transport wording that implied SSE as the active Logseq implementation path. Verified implementation uses streamable HTTP transport handling in Electron server routes.
> **Removed:** References to `addToPage` and `updateBlock` as current MCP tool names. Current tool surface exposes `upsertNodes` for writes.

## How to Create an MCP Server

### Basic Server Structure

- MCP servers are protocol endpoints that expose tools and optionally resources/prompts.
- Input schemas should be explicit and validated at the boundary.
- Session lifecycle and auth behavior should be defined up front.

### Implementation Steps

1. **Choose Your Transport Layer**
   - Define whether your host/server integration uses stdio or HTTP-based transport.
   - For HTTP transport, define initialize, follow-up request, and session close behavior.

2. **Define Your Tools**
   - Each tool needs a stable name, clear description, and input schema.
   - Keep tools narrowly scoped and deterministic where possible.

3. **Implement Tool Handlers**
   - Validate inputs before execution.
   - Return structured output payloads.
   - Return actionable errors without exposing sensitive internals.

4. **Register Resources (Optional)**
   - Expose readable context sources when needed.
   - Keep payload sizes bounded.

5. **Add Prompts (Optional)**
   - Provide reusable, parameterized prompt templates for recurring flows.

### Example Setup Checklist

```text
- Define transport and session lifecycle
- Register tools with schemas
- Implement tool handlers
- Add auth and input validation
- Add logging and error handling
```

## Managing Context with Tool Calling

### Context Flow

1. **User Query** → Host application receives user input
2. **Context Building** → Host adds available tools/resources to context
3. **LLM Processing** → Model decides which tools (if any) to call
4. **Tool Execution** → Server executes requested tools
5. **Result Integration** → Results are added back to context
6. **Response Generation** → LLM generates final response with tool results

### Best Practices for Context Management

#### Tool Design

- Keep tool purposes focused and single-responsibility
- Provide clear, detailed descriptions for each tool
- Use descriptive parameter names with validation

#### Result Handling

- Return structured data when possible
- Include relevant context in tool responses
- Keep responses concise but informative

#### Error Management

- Return meaningful error messages
- Don't expose sensitive system information
- Allow graceful degradation

#### Performance

- Keep payloads minimal by default and offer explicit expansion flags
- Paginate or constrain heavy responses
- Set reasonable timeouts and retry boundaries

## How to Use an MCP Server

### Installation & Configuration

1. **Install MCP Host Application**
   - Claude Desktop, Cline, or other MCP-compatible clients

2. **Configure Server Connection**
   - Add server details to host's configuration file
   - Specify endpoint or command to launch server
   - Set environment variables if needed

3. **Verify Connection**
   - Check that host recognizes the server
   - Confirm available tools appear in interface

### Using Tools in Practice

#### Discovery

- Host applications automatically discover available tools
- Tools appear in UI or can be invoked by LLM
- LLM sees tool descriptions and schemas in its context

#### Invocation

- User makes request that requires tool functionality
- LLM analyzes request and selects appropriate tool(s)
- Tool executes and returns results
- LLM incorporates results into response

#### Chaining

- LLM can call multiple tools in sequence
- Results from one tool can inform next tool call
- Complex workflows emerge from simple tool combinations

## Practical Tips for Better MCP Usage

### For Server Developers

#### Security

- Never expose sensitive credentials in tool responses
- Validate all inputs thoroughly
- Implement proper authentication for remote servers
- Use least-privilege principles

#### Documentation

- Write clear tool descriptions explaining purpose and usage
- Provide examples in tool documentation
- Document expected input/output formats
- Keep schema definitions up to date

#### Debugging

- Log tool calls and results for troubleshooting
- Implement verbose mode for development
- Test tools independently before integration
- Use MCP Inspector tool for testing

#### Optimization

- Profile tool execution times
- Implement caching where appropriate
- Batch operations when possible
- Consider async operations for long-running tasks

### For Users

#### Effective Prompting

- Be specific about what data or actions you need
- Understand what tools your server provides
- Reference specific files or resources when relevant
- Break complex tasks into steps

#### Troubleshooting

- Check server logs if tools aren't working
- Verify configuration file syntax
- Ensure server process is running
- Restart host application after config changes

#### Organization

- Use multiple specialized servers rather than one monolithic server
- Name servers clearly by their purpose
- Group related functionality together
- Document your server setup

### Common Use Cases

#### Development Workflows

- Database querying and management
- File system operations
- Git repository interactions
- API testing and integration

#### Data Analysis

- Reading and processing local files
- Querying databases
- Web scraping and data collection
- Report generation

#### Productivity

- Calendar and email integration
- Note-taking and knowledge management
- Task tracking and project management
- Document processing

## Resources for Learning More

### Official Documentation

- Anthropic MCP Documentation: <https://modelcontextprotocol.io>
- GitHub Repository: <https://github.com/modelcontextprotocol>
- Example Servers: Check official examples repository

## Source Validation Scope

Validated against current implementation and references in:

- `src/electron/electron/mcp_server.cljs`
- `src/electron/electron/server.cljs`
- `src/main/frontend/components/settings.cljs`
- `src/main/logseq/api/db_based/tools.cljs`
- `deps/cli/docs/building_logseq_cli_from_source.md` (docs mention only; no active MCP server source found under `deps/` or `packages/`)

### Community

- MCP Discord community
- GitHub discussions
- Example implementations from community

### Tools

- MCP Inspector: Test and debug MCP servers
- TypeScript/Python SDKs: Official implementation libraries
- Example servers: Learn from working implementations
