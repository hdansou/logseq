# Logseq Plugin Developer Guide

> **Last revised:** 2026-03-10

**Welcome to Logseq Plugin Development!**

This is your starting point for building plugins for Logseq. Choose the guide that matches your target graph type.

---

## Quick Start: Which Guide Do I Need?

Logseq supports two types of graphs, each with different capabilities and APIs:

### 📁 File-Based Graphs (Markdown/Org Mode)

**Use this if your users will:**

- Store notes in `.md` or `.org` files
- Use Git for version control
- Want portable, plain-text notes
- Need traditional file system access

**→ [File-Based Graph Plugin Guide](./file-graphs.md)**

### 🗄️ Database (DB) Graphs

**Use this if your users will:**

- Use Logseq's database storage
- Need structured properties with types
- Want real-time collaboration features
- Need advanced querying capabilities

**→ [DB Graph Plugin Guide](./db-graphs.md)**

---

## Understanding Graph Types

### Comparison Table

| Feature             | File-Based Graphs             | DB Graphs                                 |
| ------------------- | ----------------------------- | ----------------------------------------- |
| **Storage**         | Markdown/Org files on disk    | SQLite database                           |
| **Properties**      | Inline text (`key:: value`)   | Structured schema with types              |
| **Property Types**  | All strings (untyped)         | Typed (number, date, url, checkbox, etc.) |
| **Tags/Classes**    | Simple page references        | First-class entities with metadata        |
| **Queries**         | Basic DSL only                | DSL + Full Datascript/Datalog             |
| **File Access**     | ✅ Direct file system access  | ❌ No file system access                  |
| **Git Integration** | ✅ Native Git support         | ⚠️ Requires export                        |
| **Collaboration**   | Git-based                     | Real-time (upcoming)                      |
| **Portability**     | ✅ Plain text, works anywhere | ⚠️ Requires Logseq                        |
| **Performance**     | File I/O dependent            | Database optimized                        |
| **Maturity**        | Stable, well-established      | Newer, actively developed                 |

### When to Support Both

If you want your plugin to work with both graph types:

1. **Detect graph type** at runtime
2. **Use conditional code paths** for type-specific features
3. **Abstract common operations** into helper functions
4. **Test thoroughly** with both graph types

Example:

```javascript
const isDbGraph = await logseq.App.checkCurrentIsDbGraph()

if (isDbGraph) {
  // Use DB-specific APIs
  await logseq.Editor.upsertProperty('priority', {
    type: 'number',
    cardinality: 'one',
  })
} else {
  // Use file-based approach (manual property formatting)
  await updateFileProperty(blockUuid, 'priority', '5')
}
```

---

## Available Guides

### Core Guides

1. **[Plugin Developer Guide: File-Based Graphs](./file-graphs.md)**

   - Working with Markdown/Org files
   - Manual property parsing and formatting
   - Basic DSL queries
   - File system operations
   - Git integration

2. **[Plugin Developer Guide: DB Graphs](./db-graphs.md)**
   - Structured property system with types
   - Property schemas and validation
   - Advanced Datascript queries
   - Class/tag system
   - Real-time subscriptions

### Language-Specific Guides

3. **[ClojureScript Plugin SDK Guide](./clojurescript-guide.md)**

   - Complete API reference for ClojureScript developers
   - Idiomatic ClojureScript patterns
   - Works with both graph types

4. **[ClojureScript Plugin SDK: Detective Guide](./clojurescript-detective.md)**
   - Deep dive into SDK internals
   - Code generation system
   - Advanced topics

### Additional Resources

5. **Library and Namespace User Guide** (see architecture docs)

   - Namespace organization in DB graphs
   - Page hierarchies

6. **Graph Validation Developer Guide** (see architecture docs)
   - Validation patterns
   - Error handling

---

## Choosing Your Development Stack

### JavaScript/TypeScript

**Best for:**

- Quick prototyping
- Familiar web development workflows
- Access to npm ecosystem

**Get Started:**

```bash
npm install @logseq/libs
```

**Example:**

```javascript
import '@logseq/libs'

async function main() {
  const isDb = await logseq.App.checkCurrentIsDbGraph()
  console.log(`Graph type: ${isDb ? 'DB' : 'File-based'}`)
}

logseq.ready(main).catch(console.error)
```

### ClojureScript

**Best for:**

- Functional programming enthusiasts
- Complex data transformations
- Leveraging Clojure ecosystem

**Get Started:**

```bash
yarn add @logseq/cljs-libs
```

**Example:**

```clojure
(ns my-plugin.core
  (:require [com.logseq.core :as logseq]
            [com.logseq.app :as app]))

(defn main []
  (-> (logseq/ready)
      (.then (fn []
               (-> (app/check-current-is-db-graph)
                   (.then (fn [is-db?]
                            (println "Graph type:" (if is-db? "DB" "File-based")))))))))

(main)
```

> **Note:** The `@logseq/cljs-libs` package wraps the JS SDK; there is no
> separate `@logseq/cljs-libs` npm install step beyond adding it to your
> project dependencies. See the [ClojureScript Guide](./clojurescript-guide.md)
> for full setup instructions.

---

## Common Plugin Patterns

### Pattern 1: Universal Plugin (Supports Both Graph Types)

```javascript
class GraphAdapter {
  constructor() {
    this._isDb = null
  }

  async isDbGraph() {
    if (this._isDb === null) {
      this._isDb = await logseq.App.checkCurrentIsDbGraph()
    }
    return this._isDb
  }

  async getProperty(blockUuid, key) {
    if (await this.isDbGraph()) {
      return await logseq.Editor.getBlockProperty(blockUuid, key)
    } else {
      const block = await logseq.Editor.getBlock(blockUuid)
      return this.parseFileProperty(block.content, key)
    }
  }

  async setProperty(blockUuid, key, value) {
    if (await this.isDbGraph()) {
      await logseq.Editor.upsertBlockProperty(blockUuid, key, value)
    } else {
      const block = await logseq.Editor.getBlock(blockUuid)
      const newContent = this.formatFileProperty(block.content, key, value)
      await logseq.Editor.updateBlock(blockUuid, newContent)
    }
  }

  parseFileProperty(content, key) {
    const match = content.match(new RegExp(`${key}::\\s*(.+)`))
    return match ? match[1].trim() : null
  }

  formatFileProperty(content, key, value) {
    const regex = new RegExp(`${key}::\\s*.+`)
    if (regex.test(content)) {
      return content.replace(regex, `${key}:: ${value}`)
    }
    return content + `\n${key}:: ${value}`
  }
}

// Usage
const adapter = new GraphAdapter()
await adapter.setProperty(blockUuid, 'priority', 'high')
```

### Pattern 2: DB-Only Plugin with Graceful Degradation

```javascript
async function initPlugin() {
  const isDb = await logseq.App.checkCurrentIsDbGraph()

  if (!isDb) {
    logseq.UI.showMsg(
      'This plugin requires a DB graph. Some features are unavailable.',
      'warning'
    )
    // Optionally: provide limited functionality
    return
  }

  // Initialize DB-specific features
  await setupDbFeatures()
}
```

### Pattern 3: File-Only Plugin

```javascript
async function initPlugin() {
  const isDb = await logseq.App.checkCurrentIsDbGraph()

  if (isDb) {
    logseq.UI.showMsg(
      'This plugin is designed for file-based graphs.',
      'warning'
    )
    return
  }

  // Initialize file-based features
  await setupFileFeatures()
}
```

---

## API Compatibility Matrix

### ✅ Available in Both Graph Types

- ✅ Block CRUD (create, read, update, delete)
- ✅ Page CRUD
- ✅ Basic queries (`DB.q` with DSL)
- ✅ Database change subscriptions
- ✅ UI operations (messages, commands, etc.)
- ✅ Application info
- ✅ User configuration
- ✅ Theme/styling

### 🗄️ DB Graphs Only

- 🗄️ `Editor.upsertProperty()` - Create property schemas with types
- 🗄️ `Editor.getProperty()` - Get property definitions
- 🗄️ `Editor.removeProperty()` - Remove property schemas
- 🗄️ `Editor.getAllProperties()` - List all property definitions
- 🗄️ `Editor.getAllTags()` - List all tags/classes
- 🗄️ `Editor.createTag()` - Create new tags/classes (Added November 2025)
- 🗄️ `Editor.getTag()` - Get tag by name or identifier (Added November 2025)
- 🗄️ `Editor.getTagObjects()` - Get all blocks tagged with a specific class
- 🗄️ `Editor.addBlockTag()` - Add tag to block programmatically (Added November 2025)
- 🗄️ `Editor.removeBlockTag()` - Remove tag from block (Added November 2025)
- 🗄️ `Editor.addTagProperty()` - Add property to tag/class (Added November 2025)
- 🗄️ `Editor.removeTagProperty()` - Remove property from tag/class (Added November 2025)
- 🗄️ `Editor.upsertBlockProperty()` - Set typed property values (with optional schema)
- 🗄️ `Editor.getBlockProperty()` - Get typed property values
- 🗄️ `Editor.getPageProperties()` - Get all properties of a page
- 🗄️ `DB.datascriptQuery()` - Advanced Datalog queries

### 📁 File-Based Graphs Only

- 📁 `Assets.listFilesOfCurrentGraph()` - List files
- 📁 `Git.execCommand()` - Git operations
- 📁 `App.getCurrentGraphTemplates()` - Get all templates
- 📁 `App.getTemplate()` - Get template by name
- 📁 `App.insertTemplate()` - Insert template at location
- 📁 `App.existTemplate()` - Check if template exists
- 📁 `App.createTemplate()` - Create new template
- 📁 `App.removeTemplate()` - Delete template
- 📁 `App.getPagesFromNamespace()` - Get pages in namespace
- 📁 `App.getPagesTreeFromNamespace()` - Get namespace hierarchy
- 📁 Direct file system access

---

## Testing Your Plugin

### Test with Both Graph Types

1. **Create test graphs:**

   ```bash
   # Create file-based graph (Markdown)
   mkdir test-graph-file

   # Create DB graph (in Logseq UI)
   # File > New DB graph
   ```

2. **Test graph detection:**

   ```javascript
   async function testGraphType() {
     const isDb = await logseq.App.checkCurrentIsDbGraph()
     console.log('Graph type:', isDb ? 'DB' : 'File-based')

     // Test your plugin features
     await testFeatures(isDb)
   }
   ```

3. **Test property operations:**

   ```javascript
   // Test file-based
   // - Properties should be inline (property:: value)
   // - Should persist across Logseq restarts

   // Test DB-based
   // - Properties should have types
   // - Should validate against schema
   ```

### Automated Testing

```javascript
async function runTests() {
  const isDb = await logseq.App.checkCurrentIsDbGraph()

  console.log(`Running tests for ${isDb ? 'DB' : 'File-based'} graph...`)

  // Test block operations
  await testBlockOperations()

  // Test property handling
  await testPropertyOperations(isDb)

  // Test queries
  await testQueries(isDb)

  console.log('All tests passed!')
}
```

---

## Migration Guide

### Migrating from File-Based to DB Graph Plugin

If you have a file-based plugin and want to support DB graphs:

1. **Add graph type detection**
2. **Abstract property operations**
3. **Update query logic**
4. **Test with both types**

See detailed examples in:

- [DB Graph Migration Section](./db-graphs.md#migration-guide)

### Migrating from DB to File-Based Support

If you have a DB-only plugin and want to support file-based:

1. **Implement manual property parsing**
2. **Handle untyped property values**
3. **Use basic DSL queries only**
4. **Add file system operations**

See detailed examples in:

- [File Graph Guide](./file-graphs.md#properties-in-file-based-graphs)

---

## Community Resources

### Official Resources

- 📖 [Plugin API Documentation](https://logseq.github.io/plugins/)
- 💬 [Discord Community](https://discord.gg/KpN4eHY) - #plugin-dev channel
- 🔧 [Plugin Samples Repository](https://github.com/logseq/logseq-plugin-samples)
- 🎓 [Plugin Marketplace](https://github.com/logseq/marketplace)

### Templates

- [TypeScript Plugin Template](https://github.com/pengx17/logseq-plugin-template-react)
- [React Plugin Template](https://github.com/pengx17/logseq-plugin-template-react)
- [Svelte Plugin Template](https://github.com/pengx17/logseq-plugin-template-svelte)
- [ClojureScript Playground](https://github.com/tiensonqin/logseq-cljs-playground)

### Example Plugins

**File-Based Focus:**

- Most existing plugins in the marketplace

**DB Graph Focus:**

- Newer plugins leveraging property types
- Query-heavy applications

**Universal (Both Types):**

- UI-focused plugins
- Content manipulation tools

---

## Getting Help

### Stuck? Here's What to Do:

1. **Check the relevant guide:**

   - [File-Based Guide](./file-graphs.md)
   - [DB Graph Guide](./db-graphs.md)

2. **Search existing issues:**

   - [Logseq Issues](https://github.com/logseq/logseq/issues)
   - Filter by `plugin` label

3. **Ask the community:**

   - Discord #plugin-dev channel
   - Include: graph type, plugin code snippet, error message

4. **File a bug report:**
   - [GitHub Issues](https://github.com/logseq/logseq/issues/new)
   - Provide: Logseq version, graph type, steps to reproduce

---

## Quick Reference

### Graph Type Detection

```javascript
const isDb = await logseq.App.checkCurrentIsDbGraph()
```

### Property Operations

```javascript
// DB Graph
await logseq.Editor.upsertProperty('priority', { type: 'number' })
await logseq.Editor.upsertBlockProperty(uuid, 'priority', 5)
const value = await logseq.Editor.getBlockProperty(uuid, 'priority')

// File-Based Graph (manual)
const block = await logseq.Editor.getBlock(uuid)
const content = block.content
// Parse: "priority:: 5"
const match = content.match(/priority::\s*(.+)/)
const value = match ? match[1] : null
```

### Queries

```javascript
// Basic DSL (both types)
const results = await logseq.DB.q('(property priority)')

// Advanced Datalog (DB only)
const advanced = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :where
   [?b :block/properties ?props]
   [(get ?props :priority) ?p]
   [(> ?p 3)]]
`)
```

---

## License

This guide is part of the Logseq project documentation.
MIT License - see the Logseq repository for details.

---

**Happy Plugin Development! 🚀**

Choose your guide above and start building amazing Logseq plugins!
