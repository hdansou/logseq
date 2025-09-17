# Logseq MCP Server Testing Guide

## Overview

This guide provides comprehensive testing procedures for the Logseq MCP (Model Context Protocol) Server implementation. The MCP server exposes Logseq graph data to AI assistants and other MCP-compatible tools through a standardized protocol.

## MCP Server Architecture

The server operates in two modes:
- **API Server Mode**: Connects to a running Logseq instance via HTTP API
- **Local Graph Mode**: Direct access to local graph database files

### Available Tools

| Tool | Description | API Mode | Local Mode |
|------|-------------|----------|------------|
| `getAllPages` | List all pages in the graph | ✅ | ✅ |
| `getPage` | Get specific page content and block structure | ✅ | ✅ |
| `listProperties` | List all properties in the graph | ❌ | ✅ |
| `listTags` | List all tags/classes in the graph | ❌ | ✅ |

## Prerequisites

1. **Node.js Dependencies**: Ensure MCP SDK is installed
   ```bash
   cd deps/cli
   yarn install
   ```

2. **Logseq CLI**: Build the CLI if needed
   ```bash
   cd deps/cli
   yarn build
   ```

3. **Test Graph**: Create or identify a test graph with:
   - Multiple pages with content
   - Pages with properties
   - Pages with tags
   - Block hierarchies
   - Various content types

## Testing Procedures

### 1. Command Line Interface Testing

#### 1.1 Basic Command Validation

Test that the MCP server command is properly registered:

```bash
# From project root
./logseq --help
# Should show mcp-server as an available command

./logseq mcp-server --help
# Should display MCP server options:
# -a, --api-server-token  API server token to connect to current graph
# -g, --graph            Local graph to use with MCP server
# -t, --debug-tool       Debug mcp tool with direct invocation
```

#### 1.2 Parameter Validation

Test invalid parameter combinations:

```bash
# No parameters (should fail gracefully)
./logseq mcp-server

# Invalid graph path (should show error)
./logseq mcp-server -g "nonexistent-graph"

# Invalid debug tool (should show error)
./logseq mcp-server -g "test-graph" -t invalidTool
```

### 2. Local Graph Mode Testing

#### 2.1 Setup Local Graph Test

```bash
# Test with a real graph directory
./logseq mcp-server -g "your-test-graph-name"
```

Expected behavior:
- Server starts without errors
- Connects via stdio transport
- Registers 4 tools: getAllPages, getPage, listProperties, listTags

#### 2.2 Debug Tool Testing

Test each tool individually using debug mode:

```bash
# Test getAllPages
./logseq mcp-server -g "test-graph" -t getAllPages

# Test getPage with existing page
./logseq mcp-server -g "test-graph" -t getPage --pageName "Your Page Title"

# Test getPage with non-existent page
./logseq mcp-server -g "test-graph" -t getPage --pageName "Non-Existent Page"

# Test listProperties
./logseq mcp-server -g "test-graph" -t listProperties

# Test listTags
./logseq mcp-server -g "test-graph" -t listTags
```

#### 2.3 Expected Debug Output Validation

For each debug test, verify:

**getAllPages**:
```json
{
  "content": [{
    "type": "text",
    "text": "[{\"id\":\"...\",\"title\":\"...\",\"createdAt\":...,\"updatedAt\":...}]"
  }]
}
```

**getPage** (existing page):
```json
{
  "content": [{
    "type": "text", 
    "text": "[{\"block/uuid\":\"...\",\"block/content\":\"...\",\"block/children\":[...]}]"
  }]
}
```

**getPage** (non-existent page):
```json
{
  "content": [{
    "type": "text",
    "text": "Error: Page \"Non-Existent Page\" not found"
  }]
}
```

**listProperties**:
```json
{
  "content": [{
    "type": "text",
    "text": "[{\"block/uuid\":\"...\",\"block/title\":\"...\",\"logseq.property/classes\":[...]}]"
  }]
}
```

**listTags**:
```json
{
  "content": [{
    "type": "text", 
    "text": "[{\"block/uuid\":\"...\",\"block/title\":\"...\",\"logseq.property.class/extends\":[...]}]"
  }]
}
```

### 3. API Server Mode Testing

#### 3.1 Setup API Server Test

1. Start Logseq desktop application
2. Enable API server in Logseq settings
3. Get API token from Logseq settings

```bash
# Test with API token
./logseq mcp-server -a "your-api-token"

# Or set environment variable
export LOGSEQ_API_SERVER_TOKEN="your-api-token"
./logseq mcp-server
```

#### 3.2 API Mode Debug Testing

```bash
# Test getAllPages via API
./logseq mcp-server -a "your-token" -t getAllPages

# Test getPage via API
./logseq mcp-server -a "your-token" -t getPage --pageName "Page Title"

# Verify listProperties and listTags are not available in API mode
./logseq mcp-server -a "your-token" -t listProperties  # Should fail
./logseq mcp-server -a "your-token" -t listTags        # Should fail
```

#### 3.3 API Connection Error Testing

Test various API failure scenarios:

```bash
# Invalid token
./logseq mcp-server -a "invalid-token" -t getAllPages

# Logseq not running (API server down)
# Stop Logseq, then:
./logseq mcp-server -a "valid-token" -t getAllPages

# Network connectivity issues
# Block port 12315, then test
```

### 4. MCP Protocol Integration Testing

#### 4.1 MCP Client Testing

Test with an actual MCP client like Claude Desktop or other MCP-compatible tools:

1. **Configure MCP Client**:
   ```json
   {
     "mcpServers": {
       "logseq": {
         "command": "/path/to/logseq",
         "args": ["mcp-server", "-g", "test-graph"]
       }
     }
   }
   ```

2. **Test Tool Discovery**: Verify client can discover all available tools

3. **Test Tool Execution**: Execute each tool through the client

#### 4.2 Protocol Compliance Testing

Verify MCP protocol compliance:
- Server responds to initialization requests
- Tools are properly registered with correct schemas
- Responses follow MCP format specification
- Error handling follows MCP error format

### 5. Performance and Load Testing

#### 5.1 Large Graph Testing

Test with graphs containing:
- 1000+ pages
- Complex block hierarchies (10+ levels deep)
- Large pages (1000+ blocks)
- Many properties and tags (100+)

#### 5.2 Concurrent Access Testing

Test server behavior with:
- Multiple simultaneous tool calls
- Rapid successive requests
- Long-running operations

### 6. Error Handling and Edge Cases

#### 6.1 Graph Database Issues

Test scenarios:
- Corrupted database files
- Database locked by another process
- Insufficient permissions
- Disk space issues

#### 6.2 API Server Issues

Test scenarios:
- API server timeout
- Malformed API responses
- API authentication failures
- Network interruptions

#### 6.3 Input Validation

Test with various inputs:
- Special characters in page names
- Unicode characters
- Very long page names
- Empty strings
- SQL injection attempts (should be prevented by DataScript)

### 7. Security Testing

#### 7.1 Access Control

Verify that:
- Server only accesses specified graph directories
- API tokens are handled securely
- No unauthorized file system access occurs

#### 7.2 Input Sanitization

Test that:
- Page name inputs are properly sanitized
- No code injection is possible
- Error messages don't leak sensitive information

### 8. Cross-Platform Testing

Test on different platforms:
- macOS
- Linux
- Windows (if supported)

Verify:
- File path handling works correctly
- Database access works across platforms
- Dependencies are properly resolved

### 9. Regression Testing

#### 9.1 Automated Test Suite Creation

Create automated tests for:
- Each tool function
- Error conditions
- Protocol compliance
- Performance benchmarks

#### 9.2 Version Compatibility

Test compatibility with:
- Different Logseq graph formats
- Different MCP SDK versions
- Different Node.js versions

### 10. Documentation and Examples

#### 10.1 Integration Examples

Create example integrations with:
- Claude Desktop
- Other MCP clients
- Custom MCP tools

#### 10.2 Troubleshooting Guide

Document common issues:
- Connection problems
- Performance issues
- Error messages and solutions

## Test Checklist

- [ ] CLI command registration
- [ ] Parameter validation
- [ ] Local graph mode functionality
- [ ] All four tools in debug mode
- [ ] API server mode functionality
- [ ] Error handling for invalid inputs
- [ ] MCP protocol compliance
- [ ] Performance with large graphs
- [ ] Cross-platform compatibility
- [ ] Security validation
- [ ] Integration with MCP clients

## Test Data Requirements

Create test graphs with:
- [ ] Basic pages with text content
- [ ] Pages with block hierarchies
- [ ] Pages with properties (various types)
- [ ] Pages with tags/classes
- [ ] Pages with special characters in names
- [ ] Empty pages
- [ ] Pages with large amounts of content
- [ ] Interconnected pages (references)

## Reporting Issues

When reporting issues, include:
- Logseq version
- CLI version
- Operating system
- Graph size and complexity
- Exact command used
- Complete error output
- Steps to reproduce

## Success Criteria

The MCP server implementation passes testing when:
1. All tools work correctly in both modes
2. Error handling is graceful and informative
3. Performance is acceptable for typical graph sizes
4. MCP protocol compliance is verified
5. Security requirements are met
6. Cross-platform compatibility is confirmed
7. Integration with MCP clients works seamlessly