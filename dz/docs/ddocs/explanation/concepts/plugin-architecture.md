---
title: 'Plugin Architecture'
status: 🟡 Active Development
version: 'Logseq v0.10.9 | SDK @logseq/libs v0.0.17'
quadrant: 'Explanation'
platform: 'Desktop + Mobile (Limited)'
related-docs:
  - '[Plugin Development Guide](../developer/plugin-development.md)'
  - '[Plugin API Reference](../developer/plugin-api.md)'
  - '[Plugin Security](../developer/plugin-security.md)'
tags: ['plugins', 'architecture', 'extensibility', 'SDK']
---

# Plugin Architecture

Understand how Logseq's plugin system works. This explanation covers the architecture, lifecycle, APIs, and design principles that make Logseq extensible while maintaining security and performance.

**What You'll Understand:**

- Plugin system architecture
- Lifecycle and events
- API surface and capabilities
- Security model and sandboxing
- Communication patterns
- Extension points

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Plugin Lifecycle](#plugin-lifecycle)
3. [API Layers](#api-layers)
4. [Extension Points](#extension-points)
5. [Security Model](#security-model)
6. [Communication Patterns](#communication-patterns)
7. [Performance Considerations](#performance-considerations)
8. [Design Principles](#design-principles)

---

## Architecture Overview

### System Components

**High-Level Architecture:**

```markdown
┌─────────────────────────────────────────┐
│ Logseq Application │
│ │
│ ┌───────────────────────────────┐ │
│ │ Core Application │ │
│ │ - Graph Management │ │
│ │ - Block Editor │ │
│ │ - Query Engine │ │
│ │ - File System │ │
│ └───────────┬───────────────────┘ │
│ │ │
│ ┌───────────▼───────────────────┐ │
│ │ Plugin System (Host) │ │
│ │ - Plugin Manager │ │
│ │ - SDK API (@logseq/libs) │ │
│ │ - Event Dispatcher │ │
│ │ - Security Sandbox │ │
│ └───────────┬───────────────────┘ │
│ │ │
└──────────────┼──────────────────────────┘
│
┌──────────┼──────────┐
│ │ │
┌───▼───┐ ┌──▼────┐ ┌──▼────┐
│Plugin │ │Plugin │ │Plugin │
│ A │ │ B │ │ C │
└───────┘ └───────┘ └───────┘
```

### Plugin as Extension

**What is a Plugin?**

```markdown
A Logseq plugin is:

- JavaScript code bundle
- Runs in isolated context
- Interacts via SDK API
- Extends Logseq functionality
- Cannot access core directly

Not a plugin:

- Custom CSS (theme)
- Query templates
- Markdown templates
```

### Isolation Model

**Sandboxed Execution:**

```javascript
// Plugin code runs in isolated context
// Cannot access:
- window.* directly
- Logseq internals
- File system directly
- Other plugins

// Can only access:
- logseq.* SDK API
- Standard JavaScript
- Whitelisted browser APIs
```

**Why Sandbox?**

```markdown
Security:

- Prevent malicious code
- Protect user data
- Limit damage scope

Stability:

- Plugin crashes don't crash app
- Errors contained
- Independent lifecycles

Maintainability:

- Clear API boundaries
- Versioned SDK
- Backward compatibility
```

---

## Plugin Lifecycle

### Lifecycle States

**Plugin States:**

```markdown
1. Unloaded
   ↓ (User installs)
2. Installed
   ↓ (App starts / User enables)
3. Loading
   ↓ (Registration)
4. Loaded
   ↓ (Initialization)
5. Active
   ↓ (User disables / App closes)
6. Unloading
   ↓ (Cleanup)
7. Unloaded
```

### Lifecycle Hooks

**Main Function:**

```javascript
// index.js - Plugin entry point
function main() {
  console.log('Plugin loaded!')

  // Register features
  logseq.App.registerCommand(...)
  logseq.Editor.registerSlashCommand(...)

  // Listen to events
  logseq.App.onMacroRendererSlotted(...)
  logseq.DB.onChanged(...)
}

// Logseq calls main() when plugin loads
logseq.ready(main).catch(console.error)
```

**Lifecycle Events:**

```javascript
// On plugin load
logseq.ready(() => {
  console.log('Plugin ready')

  // Setup
  initializePlugin()
  registerCommands()
  attachEventListeners()
})

// On app navigation
logseq.App.onRouteChanged(({ path, template }) => {
  console.log('Route changed:', path)
  // Update UI, refresh state
})

// On graph change
logseq.App.onCurrentGraphChanged(() => {
  console.log('Graph switched')
  // Reload data, reset state
})

// Before unload (cleanup)
logseq.beforeunload(() => {
  console.log('Plugin unloading')
  // Save state, cleanup resources
  return true // Allow unload
})
```

### Load Sequence

**Startup Flow:**

```markdown
1. Logseq Application Starts
   - Load user graph
   - Initialize core systems
2. Plugin Manager Initializes
   - Scan plugins directory
   - Read manifest files (package.json)
   - Check dependencies
3. For Each Enabled Plugin:
   a. Load plugin bundle (index.js)
   b. Create isolated context
   c. Inject SDK API
   d. Execute main function
   e. Plugin registers features
   f. Plugin marks ready
4. All Plugins Loaded
   - Dispatch onReady event
   - Enable plugin features
   - Ready for user interaction
```

---

## API Layers

### SDK Structure

**@logseq/libs API:**

```javascript
logseq.
├── App              // Application-level
│   ├── getCurrentGraph()
│   ├── showMsg()
│   ├── getUserConfigs()
│   └── registerCommand()
│
├── Editor           // Block editing
│   ├── getBlock()
│   ├── updateBlock()
│   ├── insertBlock()
│   └── registerSlashCommand()
│
├── DB               // Database queries
│   ├── q()
│   ├── datascriptQuery()
│   └── onChanged()
│
├── UI               // User interface
│   ├── showMsg()
│   ├── setMainUIInlineStyle()
│   └── queryElementById()
│
├── Assets           // File handling
│   ├── listFilesOfCurrentGraph()
│   └── makeAssetUrl()
│
└── FileStorage      // File I/O (experimental)
    ├── readFile()
    └── writeFile()
```

### Core APIs

**Application API:**

```javascript
// Get current graph info
const graph = await logseq.App.getCurrentGraph()
// { name: "my-graph", path: "/path/to/graph" }

// Show notification
logseq.App.showMsg('Hello from plugin!', 'success')

// Register command (cmd+k)
logseq.App.registerCommand(
  'my-command',
  {
    key: 'my-plugin-action',
    label: 'Do something cool',
    keybinding: { binding: 'mod+shift+c' },
  },
  async () => {
    // Command handler
    logseq.App.showMsg('Command executed!')
  }
)

// Listen to events
logseq.App.onRouteChanged(({ path }) => {
  console.log('Navigated to:', path)
})
```

**Editor API:**

```javascript
// Get current block
const block = await logseq.Editor.getCurrentBlock()
// { uuid: "...", content: "...", parent: {...} }

// Update block content
await logseq.Editor.updateBlock(block.uuid, 'Updated content')

// Insert new block
await logseq.Editor.insertBlock(
  block.uuid, // sibling UUID
  'New block content',
  { before: false } // Insert after
)

// Register slash command
logseq.Editor.registerSlashCommand('Insert Timestamp', async () => {
  const block = await logseq.Editor.getCurrentBlock()
  const timestamp = new Date().toISOString()
  await logseq.Editor.insertAtCursor(`⏰ ${timestamp}`)
})
```

**Database API:**

```javascript
// Simple query
const todos = await logseq.DB.q(`
  [:find (pull ?b [*])
   :where
   [?b :block/marker "TODO"]]
`)

// Advanced Datalog query
const results = await logseq.DB.datascriptQuery(`
  [:find ?content ?deadline
   :where
   [?b :block/marker "TODO"]
   [?b :block/content ?content]
   [?b :block/deadline ?deadline]
   [(< ?deadline [:today])]]
`)

// Listen to database changes
logseq.DB.onChanged(({ blocks, txData, txMeta }) => {
  console.log('Database changed:', blocks.length, 'blocks')
  // Refresh plugin UI, update state
})
```

**UI API:**

```javascript
// Show message
logseq.UI.showMsg('Success!', 'success')
// Types: success, warning, error, info

// Inject custom CSS
logseq.UI.setMainUIInlineStyle(`
  .my-plugin-class {
    color: blue;
  }
`)

// Query DOM element
const el = parent.document.querySelector('#app')

// Close modal/popup
logseq.UI.closeMsg()
```

### Advanced APIs

**Macro API:**

```javascript
// Register macro renderer
logseq.App.onMacroRendererSlotted(({ slot, payload }) => {
  const { name, args } = payload

  if (name === 'my-chart') {
    // Render custom chart
    logseq.provideUI({
      key: `chart-${slot}`,
      slot,
      template: `
        <div class="chart-container">
          <canvas id="chart-${slot}"></canvas>
        </div>
      `,
      style: { width: '400px', height: '300px' },
    })

    // Draw chart with args
    drawChart(`chart-${slot}`, args)
  }
})

// Usage in Logseq:
// {{renderer my-chart, data1, data2, data3}}
```

**Settings API:**

```javascript
// Define plugin settings
logseq.useSettingsSchema([
  {
    key: 'apiKey',
    type: 'string',
    default: '',
    title: 'API Key',
    description: 'Your service API key',
  },
  {
    key: 'enableFeature',
    type: 'boolean',
    default: true,
    title: 'Enable Feature X',
    description: 'Turn on advanced feature',
  },
  {
    key: 'theme',
    type: 'enum',
    default: 'light',
    enumChoices: ['light', 'dark', 'auto'],
    title: 'Theme',
    description: 'UI theme preference',
  },
])

// Access settings
const settings = logseq.settings
console.log(settings.apiKey)
console.log(settings.enableFeature)

// Listen to settings changes
logseq.onSettingsChanged((newSettings, oldSettings) => {
  console.log('Settings updated:', newSettings)
  // Reinitialize with new settings
})
```

---

## Extension Points

### What Can Plugins Extend?

**1. Commands:**

```javascript
// Palette commands (Cmd+K)
logseq.App.registerCommand(
  'search-github',
  {
    key: 'github-search',
    label: 'Search GitHub Issues',
    keybinding: { binding: 'mod+shift+g' },
  },
  async () => {
    const query = await logseq.App.getUserInput('Search query:')
    // Open GitHub search
    window.open(`https://github.com/search?q=${query}`)
  }
)
```

**2. Slash Commands:**

```javascript
// Editor slash commands
logseq.Editor.registerSlashCommand('Generate UUID', async () => {
  const uuid = crypto.randomUUID()
  await logseq.Editor.insertAtCursor(uuid)
})

// Usage: Type /Generate UUID in editor
```

**3. Block Context Menu:**

```javascript
// Right-click block menu
logseq.Editor.registerBlockContextMenuItem(
  'Export as JSON',
  async ({ uuid }) => {
    const block = await logseq.Editor.getBlock(uuid, {
      includeChildren: true,
    })
    const json = JSON.stringify(block, null, 2)
    await navigator.clipboard.writeText(json)
    logseq.App.showMsg('Copied to clipboard!')
  }
)
```

**4. Macro Renderers:**

```javascript
// Custom block renderers
// {{renderer video, youtube-id}}
logseq.App.onMacroRendererSlotted(({ slot, payload }) => {
  if (payload.name === 'video') {
    const [videoId] = payload.args
    logseq.provideUI({
      key: `video-${slot}`,
      slot,
      template: `
        <iframe 
          width="560" 
          height="315" 
          src="https://youtube.com/embed/${videoId}"
          frameborder="0"
          allowfullscreen>
        </iframe>
      `,
    })
  }
})
```

**5. UI Slots:**

```javascript
// Toolbar buttons
logseq.App.registerUIItem('toolbar', {
  key: 'my-plugin-button',
  template: `
    <a class="button" data-on-click="handleClick">
      <i class="ti ti-star"></i>
    </a>
  `,
})

logseq.provideModel({
  handleClick() {
    logseq.App.showMsg('Toolbar button clicked!')
  },
})
```

**6. Themes (via provideStyle):**

```javascript
// Inject custom styles
logseq.provideStyle(`
  .block-content {
    font-family: 'Custom Font', sans-serif;
  }
  
  .page-title {
    color: var(--ls-primary-text-color);
  }
`)
```

**7. Background Tasks:**

```javascript
// Periodic sync
setInterval(async () => {
  const todos = await logseq.DB.q(`
    [:find (pull ?b [*])
     :where [?b :block/marker "TODO"]]
  `)

  // Sync to external service
  await syncToService(todos)
}, 5 * 60 * 1000) // Every 5 minutes
```

---

## Security Model

### Sandboxing

**Isolated Context:**

```markdown
Plugin runs in separate context:

- Cannot access Logseq internals
- Cannot access other plugins
- Cannot directly modify DOM outside slots
- Cannot access file system directly (except via API)

Prevents:

- Malicious code execution
- Data theft
- System compromise
- Plugin conflicts
```

**Allowed APIs:**

```javascript
// ✅ Allowed
- logseq.* (SDK API)
- fetch() (network requests)
- localStorage (plugin-scoped)
- Standard JavaScript
- Web APIs (limited)

// ❌ Not allowed
- Direct DOM access (outside slots)
- Node.js APIs (in browser plugins)
- File system (except via SDK)
- Logseq internals
- Other plugins' data
```

### Permissions Model

**Implicit Permissions:**

```markdown
Current model (v0.10.9):

- All plugins have same permissions
- No granular permission system (yet)
- Trust-based model

User must trust plugin to:

- Read all graph data
- Modify blocks
- Make network requests
- Store data locally

Future: Explicit permissions requested
```

### Best Practices for Security

**Plugin Developers:**

```javascript
// ✅ Good: Ask before external requests
const consent = await logseq.App.getUserConfigs()
if (!consent.allowExternalRequests) {
  const allow = confirm('Allow plugin to access external API?')
  if (!allow) return
}

// ✅ Good: Validate user input
const input = await logseq.App.getUserInput('Enter URL:')
if (!input.match(/^https?:\/\//)) {
  logseq.App.showMsg('Invalid URL', 'error')
  return
}

// ✅ Good: Store sensitive data securely
// Don't log API keys
console.log('Authenticating...') // ✅
console.log('API Key:', apiKey) // ❌

// ✅ Good: Handle errors gracefully
try {
  await riskyOperation()
} catch (error) {
  logseq.App.showMsg(`Error: ${error.message}`, 'error')
  // Don't crash entire plugin
}
```

**Users:**

```markdown
Before installing plugin:

1. Check source (GitHub)
2. Read permissions (future)
3. Check author reputation
4. Review code if possible
5. Start with popular plugins

Red flags:

- Requests unusual permissions
- Closed source
- Unknown author
- No documentation
- Few users
```

---

## Communication Patterns

### Plugin → Logseq

**SDK API Calls:**

```javascript
// Async API calls
const block = await logseq.Editor.getBlock(uuid)
const graph = await logseq.App.getCurrentGraph()

// All SDK methods return Promises
// Use async/await or .then()
```

### Logseq → Plugin

**Events:**

```javascript
// Event-driven communication
logseq.DB.onChanged(({ blocks }) => {
  // React to database changes
})

logseq.App.onRouteChanged(({ path }) => {
  // React to navigation
})

logseq.App.onThemeModeChanged(({ mode }) => {
  // React to theme changes
})
```

### Plugin → Plugin

**No Direct Communication:**

```markdown
Plugins cannot directly communicate (by design)

Workarounds:

1. Use graph as message bus
   - Plugin A writes block
   - Plugin B reads block
2. Use localStorage
   - Plugin A sets value
   - Plugin B reads value
   - Poll for changes
3. External service
   - Both plugins connect to service
   - Service mediates communication

Not recommended due to coupling
Better: Keep plugins independent
```

### User → Plugin

**User Interactions:**

```javascript
// 1. Commands (Cmd+K)
logseq.App.registerCommand(...)

// 2. Slash commands
logseq.Editor.registerSlashCommand(...)

// 3. Context menus
logseq.Editor.registerBlockContextMenuItem(...)

// 4. UI buttons
logseq.provideUI({
  template: `<button data-on-click="handler">Click</button>`
})

// 5. Settings
logseq.useSettingsSchema([...])
```

---

## Performance Considerations

### Plugin Load Time

**Minimize Startup Work:**

```javascript
// ❌ Bad: Heavy initialization
function main() {
  loadEntireDatabase() // Slow!
  processAllBlocks() // Slow!
  initializeHugeLibrary() // Slow!
}

// ✅ Good: Lazy loading
function main() {
  registerCommands() // Fast
  attachEventListeners() // Fast

  // Load data only when needed
  logseq.App.registerCommand(
    'my-command',
    {
      label: 'Heavy Operation',
    },
    async () => {
      // Now load data
      const data = await loadData()
      process(data)
    }
  )
}
```

### Memory Usage

**Avoid Memory Leaks:**

```javascript
// ❌ Bad: Growing cache
const cache = {}
logseq.DB.onChanged(({ blocks }) => {
  blocks.forEach((b) => {
    cache[b.uuid] = b // Never cleared!
  })
})

// ✅ Good: Bounded cache
const cache = new Map()
const MAX_SIZE = 1000

logseq.DB.onChanged(({ blocks }) => {
  blocks.forEach((b) => {
    cache.set(b.uuid, b)
    if (cache.size > MAX_SIZE) {
      const firstKey = cache.keys().next().value
      cache.delete(firstKey)
    }
  })
})
```

### Debouncing Events

**Avoid Excessive Processing:**

```javascript
// ❌ Bad: Process every change
logseq.DB.onChanged(async ({ blocks }) => {
  await heavyProcessing(blocks) // Called too often!
})

// ✅ Good: Debounce
let debounceTimer
logseq.DB.onChanged(({ blocks }) => {
  clearTimeout(debounceTimer)
  debounceTimer = setTimeout(() => {
    heavyProcessing(blocks)
  }, 1000) // Wait 1s after last change
})
```

### Bundle Size

**Keep Plugins Small:**

```markdown
Tips:

- Use tree-shaking
- Avoid large dependencies
- Load external resources lazily
- Minify production builds

Target: <100KB total
Good: <50KB
Excellent: <20KB
```

---

## Design Principles

### 1. Minimal API Surface

**Why:**

```markdown
Smaller API = Easier to:

- Learn
- Maintain
- Version
- Keep stable

Logseq exposes only essentials
Complex operations via simpler primitives
```

### 2. Async by Default

**Why:**

```javascript
// All API calls are async
await logseq.Editor.getBlock(uuid)
await logseq.DB.q(query)

Reason:
- Non-blocking
- Better performance
- Future-proof (some calls may be slow)
```

### 3. Event-Driven

**Why:**

```javascript
// React to changes vs polling
logseq.DB.onChanged(() => {
  // Update when something changes
})

// Not: setInterval to check changes
// More efficient, responsive
```

### 4. Declarative UI

**Why:**

```javascript
// Describe what to render
logseq.provideUI({
  template: `<div>Hello</div>`,
  style: { color: 'blue' },
})

// Not: Direct DOM manipulation
// Safer, easier to maintain
```

### 5. Capability-Based

**Why:**

```markdown
Plugin can only do what API allows

- Can't break core
- Can't access internals
- Limited blast radius

Trade-off: Less flexibility for more safety
```

---

## Future Directions

### Planned Improvements

**Permission System:**

```markdown
Future: Explicit permissions

- Read graph data
- Modify blocks
- Network access
- File system access

User grants permissions on install
Granular control
```

**Enhanced APIs:**

```markdown
Roadmap includes:

- More UI extension points
- Better file system access
- Plugin-to-plugin communication
- Advanced theming APIs
- Mobile plugin support
```

**Performance:**

```markdown
Improvements:

- Faster plugin loading
- Better isolation
- Reduced overhead
- Optimized SDK
```

---

## Summary

**Plugin Architecture Key Points:**

1. **Sandboxed Execution**

   - Plugins run isolated
   - Access via SDK only
   - Security and stability

2. **Event-Driven**

   - React to changes
   - Async communication
   - Non-blocking

3. **Extension Points**

   - Commands, slash commands
   - Block menus, macros
   - UI slots, themes

4. **SDK API**

   - App, Editor, DB, UI
   - Async by default
   - Versioned and stable

5. **Security Model**

   - Trust-based (current)
   - Permissions (future)
   - Best practices important

6. **Performance**
   - Minimize startup work
   - Debounce events
   - Keep bundles small

**Plugins make Logseq infinitely extensible while maintaining core stability and security.**

---

## Next Steps

**Understand Plugin System:**

1. Read plugin documentation
2. Study existing plugins
3. Experiment with SDK
4. Build simple plugin
5. Share with community

**For Developers:**

- [Plugin Development Guide](../developer/plugin-development.md)
- [Plugin API Reference](../developer/plugin-api.md)
- [Plugin Security](../developer/plugin-security.md)

**For Users:**

- Explore plugin marketplace
- Install popular plugins
- Provide feedback
- Support developers

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
