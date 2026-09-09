# Logseq Plugin Developer Guide: File-Based Graphs

> **Last revised:** 2026-05-29

**Last Updated:** May 29, 2026
**Logseq Version:** Current plugin API surface validated from source
**Target Audience:** Plugin developers building for Logseq file-based graphs (Markdown/Org mode)

---

## Table of Contents

1. [Introduction](#introduction)
2. [File-Based vs DB Graphs](#file-based-vs-db-graphs)
3. [Getting Started](#getting-started)
4. [Working with Blocks and Pages](#working-with-blocks-and-pages)
5. [Properties in File-Based Graphs](#properties-in-file-based-graphs)
6. [Querying File-Based Graphs](#querying-file-based-graphs)
7. [Content Manipulation](#content-manipulation)
8. [File System Operations](#file-system-operations)
9. [Best Practices](#best-practices)
10. [Findings: New Capabilities](#findings-new-capabilities)
11. [API Reference](#api-reference)
12. [Examples](#examples)

---

## Introduction

Logseq file-based graphs store content in **Markdown** (`.md`) or **Org mode** (`.org`) files. This traditional approach offers:

- **Human-readable files** - Text files you can edit in any editor
- **Git-friendly** - Easy version control and collaboration via Git
- **Portable** - Files work without Logseq
- **Simple properties** - Inline property syntax (`property:: value`)
- **Mature ecosystem** - Established workflows and tools
- **File system access** - Direct file manipulation

This guide focuses exclusively on plugin development for file-based graphs.

---

## File-Based vs DB Graphs

### Key Differences

| Feature             | File-Based Graphs               | DB Graphs                    |
| ------------------- | ------------------------------- | ---------------------------- |
| **Storage**         | Markdown/Org files              | SQLite database              |
| **Properties**      | Inline text (`key:: value`)     | Structured schema with types |
| **Tags**            | Simple page references (`#tag`) | Classes with inheritance     |
| **Queries**         | Basic DSL                       | Full Datascript queries      |
| **File Access**     | Yes (via File API)              | No                           |
| **Version Control** | Git-native                      | Requires export              |
| **Portability**     | High (plain text)               | Low (requires Logseq)        |
| **Property Types**  | Untyped (strings)               | Typed (number, date, etc.)   |
| **Performance**     | File I/O dependent              | Database optimized           |

### Detecting Graph Type

Always check the graph type if your plugin needs to support both:

```javascript
// JavaScript
const isDbGraph = await logseq.App.checkCurrentIsDbGraph()

if (isDbGraph) {
  console.log('DB graph - some features unavailable')
} else {
  console.log('File-based graph - full file access available')
}
```

```clojure
;; ClojureScript
(-> (app/check-current-is-db-graph)
    (.then (fn [is-db?]
             (if is-db?
               (println "DB graph")
               (println "File-based graph")))))
```

---

## Getting Started

### Prerequisites

- Logseq desktop application (file-based graph)
- Node.js and npm/yarn
- Basic understanding of Markdown or Org mode

### Installation

**JavaScript/TypeScript:**

```bash
npm install @logseq/libs
```

**ClojureScript:**

```bash
yarn add @logseq/cljs-libs
```

### Basic Setup

**JavaScript:**

```javascript
import '@logseq/libs'

async function main() {
  console.log('File-based graph plugin loaded!')

  // Verify it's a file-based graph
  const isDb = await logseq.App.checkCurrentIsDbGraph()
  if (isDb) {
    logseq.UI.showMsg(
      'This plugin is designed for file-based graphs',
      'warning'
    )
    return
  }

  // Initialize plugin features
  await setupFileBasedFeatures()
}

logseq.ready(main).catch(console.error)
```

**ClojureScript:**

```clojure
(ns my-plugin.core
  (:require [com.logseq.core :as logseq]
            [com.logseq.app :as app]
            [com.logseq.editor :as editor]
            [com.logseq.ui :as ui]))

(defn setup-features! []
  (println "Setting up file-based features"))

(defn main []
  (-> (logseq/ready)
      (.then (fn []
               (-> (app/check-current-is-db-graph)
                   (.then (fn [is-db?]
                            (if is-db?
                              (ui/show-msg "This plugin is for file-based graphs" "warning")
                              (setup-features!)))))))))

(main)
```

---

## Working with Blocks and Pages

### Block Structure

In file-based graphs, blocks are stored as text with indentation:

```markdown
- Parent block
  - Child block 1
  - Child block 2
    - Nested child
```

### Creating Blocks

```javascript
// Insert a simple block
const block = await logseq.Editor.insertBlock(
  parentBlock.uuid,
  'This is a new block'
)

// Insert as sibling (after parent)
const sibling = await logseq.Editor.insertBlock(
  referenceBlock.uuid,
  'Sibling block',
  { sibling: true }
)

// Insert before
const before = await logseq.Editor.insertBlock(
  referenceBlock.uuid,
  'Before block',
  { before: true }
)

// Insert at specific position
const positioned = await logseq.Editor.insertBlock(
  parentBlock.uuid,
  'Positioned block',
  { sibling: false } // As child
)
```

```clojure
;; ClojureScript
(editor/insert-block
  parent-uuid
  "This is a new block")

(editor/insert-block
  reference-uuid
  "Sibling block"
  {:sibling true})
```

### Batch Block Creation

Create multiple blocks efficiently:

```javascript
const blocks = [
  {
    content: 'First block',
  },
  {
    content: 'Second block',
    children: [{ content: 'Nested block 1' }, { content: 'Nested block 2' }],
  },
  {
    content: 'Third block',
  },
]

const created = await logseq.Editor.insertBatchBlock(parentBlock.uuid, blocks, {
  sibling: false,
})

console.log(`Created ${created.length} blocks`)
```

### Reading Blocks

```javascript
// Get a specific block
const block = await logseq.Editor.getBlock(blockUuid)
console.log('Content:', block.content)
console.log('Format:', block.format) // 'markdown' or 'org'

// Get block with children
const blockWithChildren = await logseq.Editor.getBlock(blockUuid, {
  includeChildren: true,
})

// Get current block
const current = await logseq.Editor.getCurrentBlock()

// Get selected blocks
const selected = await logseq.Editor.getSelectedBlocks()
```

```clojure
;; ClojureScript
(-> (editor/get-block block-uuid)
    (.then (fn [block]
             (println "Content:" (:content block))
             (println "Format:" (:format block)))))
```

### Updating Blocks

```javascript
// Update block content
await logseq.Editor.updateBlock(block.uuid, 'Updated content')

// Preserve properties when updating
const existingContent = block.content
const properties = extractProperties(existingContent)
const mainContent = removeProperties(existingContent)

await logseq.Editor.updateBlock(
  block.uuid,
  mainContent + '\n' + formatProperties(properties)
)
```

### Deleting Blocks

```javascript
// Remove a block
await logseq.Editor.removeBlock(block.uuid)

// Remove with confirmation
if (confirm('Delete this block?')) {
  await logseq.Editor.removeBlock(block.uuid)
}
```

### Working with Pages

```javascript
// Create a page
const page = await logseq.Editor.createPage('My New Page')

// Create with properties
const pageWithProps = await logseq.Editor.createPage(
  'Project Page',
  { status: 'active', priority: 'high' },
  { createFirstBlock: true }
)

// Get a page
const page = await logseq.Editor.getPage('Page Name')
// Or by UUID
const pageByUuid = await logseq.Editor.getPage(pageUuid)

// Get all pages
const allPages = await logseq.Editor.getAllPages()

// Get page blocks
const blocks = await logseq.Editor.getPageBlocksTree('Page Name')

// Delete a page
await logseq.Editor.deletePage('Page Name')

// Rename a page
await logseq.Editor.renamePage('Old Name', 'New Name')
```

```clojure
;; ClojureScript
(editor/create-page "My New Page")

(-> (editor/get-page "Page Name")
    (.then (fn [page]
             (println "Page:" (:original-name page)))))

(editor/delete-page "Page Name")
```

### Journal Pages

```javascript
// Create today's journal
const today = await logseq.Editor.createJournalPage(new Date())

// Create specific date journal
const specificDate = await logseq.Editor.createJournalPage(
  new Date('2025-12-31')
)

// Get current page (could be journal)
const current = await logseq.Editor.getCurrentPage()
if (current?.journalDay) {
  console.log('This is a journal page for:', current.journalDay)
}
```

---

## Properties in File-Based Graphs

### Property Syntax

Properties in file-based graphs are stored as inline text:

```markdown
- Block with properties
  property1:: value1
  property2:: value2
  tags:: [[tag1]], [[tag2]]
  date:: [[2025-10-21]]
```

Properties still serialize to file content, but plugins now have structured property APIs for block/page property reads and updates.

> **Removed:** "File graphs don't have structured property APIs" is no longer accurate.

### Structured Property APIs (Current)

Use structured APIs when you need robust behavior across value types and plugin-scoped property identifiers:

```javascript
// Set or update a property on a block
await logseq.Editor.upsertBlockProperty(block.uuid, 'priority', 'high')

// Get one property
const priority = await logseq.Editor.getBlockProperty(block.uuid, 'priority')

// Get all properties (normalized map)
const allProps = await logseq.Editor.getBlockProperties(block.uuid)

// Remove one property
await logseq.Editor.removeBlockProperty(block.uuid, 'priority')

// Page-level helper
const pageProps = await logseq.Editor.getPageProperties('Project Page')
```

### Reading Properties (Manual Parsing)

Manual parsing is still useful when you specifically need raw line-level control of Markdown/Org content:

```javascript
// Helper function to extract properties
function parseProperties(content) {
  const properties = {}
  const propertyRegex = /^(\w+)::\s*(.+)$/gm
  let match

  while ((match = propertyRegex.exec(content)) !== null) {
    const [, key, value] = match
    properties[key] = value.trim()
  }

  return properties
}

// Usage
const block = await logseq.Editor.getBlock(uuid)
const properties = parseProperties(block.content)
console.log('Priority:', properties.priority)
```

```clojure
;; ClojureScript
(defn parse-properties [content]
  (let [property-regex #"(?m)^(\w+)::\s*(.+)$"
        matches (re-seq property-regex content)]
    (into {} (map (fn [[_ k v]] [(keyword k) (clojure.string/trim v)]) matches))))

(-> (editor/get-block uuid)
    (.then (fn [block]
             (let [props (parse-properties (:content block))]
               (println "Properties:" props)))))
```

### Writing Properties (Manual Formatting)

```javascript
// Helper to add/update property
function setProperty(content, key, value) {
  const propertyLine = `${key}:: ${value}`
  const regex = new RegExp(`^${key}::\\s*.+$`, 'm')

  if (regex.test(content)) {
    // Update existing property
    return content.replace(regex, propertyLine)
  } else {
    // Add new property
    return content + '\n' + propertyLine
  }
}

// Helper to remove property
function removeProperty(content, key) {
  const regex = new RegExp(`^${key}::\\s*.+$\\n?`, 'gm')
  return content.replace(regex, '')
}

// Usage
const block = await logseq.Editor.getBlock(uuid)
let newContent = setProperty(block.content, 'priority', 'high')
newContent = setProperty(newContent, 'status', 'active')

await logseq.Editor.updateBlock(uuid, newContent)
```

```clojure
;; ClojureScript
(defn set-property [content key value]
  (let [property-line (str key ":: " value)
        regex (js/RegExp. (str "^" key "::\\s*.+$") "m")]
    (if (.test regex content)
      (.replace content regex property-line)
      (str content "\n" property-line))))

(-> (editor/get-block uuid)
    (.then (fn [block]
             (let [new-content (-> (:content block)
                                   (set-property "priority" "high")
                                   (set-property "status" "active"))]
               (editor/update-block uuid new-content)))))
```

### Property Utilities

Here's a complete property utility class:

```javascript
class FileGraphProperties {
  // Extract properties from content
  static parse(content) {
    const properties = {}
    const lines = content.split('\n')

    for (const line of lines) {
      const match = line.match(/^(\w+)::\s*(.+)$/)
      if (match) {
        const [, key, value] = match
        properties[key] = value.trim()
      }
    }

    return properties
  }

  // Get content without properties
  static getMainContent(content) {
    return content
      .split('\n')
      .filter((line) => !line.match(/^\w+::/))
      .join('\n')
      .trim()
  }

  // Format properties back to text
  static format(properties) {
    return Object.entries(properties)
      .map(([key, value]) => `${key}:: ${value}`)
      .join('\n')
  }

  // Set a property value
  static set(content, key, value) {
    const properties = this.parse(content)
    const mainContent = this.getMainContent(content)

    properties[key] = value

    return mainContent + '\n' + this.format(properties)
  }

  // Remove a property
  static remove(content, key) {
    const properties = this.parse(content)
    const mainContent = this.getMainContent(content)

    delete properties[key]

    const propsText = this.format(properties)
    return propsText ? mainContent + '\n' + propsText : mainContent
  }

  // Get property value
  static get(content, key) {
    const properties = this.parse(content)
    return properties[key] || null
  }
}

// Usage
const block = await logseq.Editor.getBlock(uuid)

// Get property value
const priority = FileGraphProperties.get(block.content, 'priority')

// Set property
const updated = FileGraphProperties.set(block.content, 'status', 'done')
await logseq.Editor.updateBlock(uuid, updated)

// Get main content
const mainContent = FileGraphProperties.getMainContent(block.content)
```

### Page References in Properties

```javascript
// Properties can reference pages
const content = `Task description
assigned-to:: [[John Doe]]
project:: [[Project Alpha]]
tags:: [[urgent]], [[review]]
`

// Parse page references
function parsePageReferences(propertyValue) {
  const matches = propertyValue.match(/\[\[([^\]]+)\]\]/g)
  return matches ? matches.map((m) => m.slice(2, -2)) : []
}

const block = await logseq.Editor.getBlock(uuid)
const tags = FileGraphProperties.get(block.content, 'tags')
if (tags) {
  const tagList = parsePageReferences(tags)
  console.log('Tags:', tagList) // ['urgent', 'review']
}
```

### Date Properties

```javascript
// Date properties reference journal pages
const today = new Date()
const dateStr = today.toISOString().split('T')[0] // YYYY-MM-DD

const content = `Meeting notes
date:: [[${dateStr}]]
`

// Or using Logseq's date format
function formatLogseqDate(date) {
  const months = [
    'Jan',
    'Feb',
    'Mar',
    'Apr',
    'May',
    'Jun',
    'Jul',
    'Aug',
    'Sep',
    'Oct',
    'Nov',
    'Dec',
  ]
  const day = date.getDate()
  const month = months[date.getMonth()]
  const year = date.getFullYear()

  const suffix =
    day === 1 || day === 21 || day === 31
      ? 'st'
      : day === 2 || day === 22
        ? 'nd'
        : day === 3 || day === 23
          ? 'rd'
          : 'th'

  return `${month} ${day}${suffix}, ${year}`
}

const logseqDate = formatLogseqDate(new Date())
console.log(logseqDate) // "Oct 21st, 2025"
```

---

## Querying File-Based Graphs

### Simple Queries

File-based graphs support basic DSL queries:

```javascript
// Find blocks with a property
const results = await logseq.DB.q('(property priority)')

// Find blocks with specific property value
const highPriority = await logseq.DB.q('(property priority high)')

// Find blocks with page reference
const projectBlocks = await logseq.DB.q('(page-ref "Project Alpha")')

// Find blocks with tag
const tagged = await logseq.DB.q('(page-tags "urgent")')

// Combine conditions with AND
const combined = await logseq.DB.q(`
  (and
    (property status active)
    (page-tags urgent))
`)

// OR conditions
const either = await logseq.DB.q(`
  (or
    (property priority high)
    (property priority critical))
`)
```

```clojure
;; ClojureScript
(db/q "(property priority)")
(db/q "(property priority high)")
(db/q "(page-ref \"Project Alpha\")")
```

### Advanced Queries

```javascript
// Query with multiple conditions
const query = `
(and
  (property status)
  (not (property status done))
  (page-tags project))
`

const results = await logseq.DB.q(query)

// Query for TODO blocks
const todos = await logseq.DB.q('(task TODO DOING)')

// Query for scheduled/deadline items
const scheduled = await logseq.DB.q('(between -7d +7d)')
```

### Datascript and Custom Queries

```javascript
// Datascript query with inputs
const datalogResult = await logseq.DB.datascriptQuery(
  '[:find ?b :where [?b :block/marker "TODO"]]'
)

// Custom query pipeline
const custom = await logseq.DB.customQuery(`
{:query [:find (pull ?b [*])
         :where [?b :block/marker ?m]
                [(contains? #{"TODO" "DOING"} ?m)]]}
`)
```

### Query Limitations

File-based graphs still differ from DB graphs in storage/modeling semantics, but plugin query support is broader than older docs indicated.

> **Removed:** "No Datascript queries" for file graphs is no longer accurate.

### Reactive Queries

Subscribe to database changes:

```javascript
const unsubscribe = logseq.DB.onChanged(({ blocks, txData }) => {
  console.log(`${blocks.length} blocks changed`)

  // Re-run queries to update UI
  refreshResults()
})

// Clean up
logseq.beforeunload(() => {
  unsubscribe()
})
```

### Block-Specific Subscriptions

```javascript
const unsubscribe = logseq.DB.onBlockChanged(blockUuid, (block, txData) => {
  console.log('Block updated:', block.content)

  // Parse properties
  const props = FileGraphProperties.parse(block.content)
  console.log('Properties:', props)
})
```

---

## Content Manipulation

### Working with Block Content

```javascript
// Get editing block content
const content = await logseq.Editor.getEditingBlockContent()

// Insert at cursor
await logseq.Editor.insertAtEditingCursor('inserted text')

// Get cursor position
const pos = await logseq.Editor.getEditingCursorPosition()
console.log('Cursor at:', pos)

// Exit editing mode
await logseq.Editor.exitEditingMode()

// Restore editing cursor
await logseq.Editor.restoreEditingCursor()
```

### Text Formatting

```javascript
// Bold
await logseq.Editor.insertAtEditingCursor('**bold text**')

// Italic
await logseq.Editor.insertAtEditingCursor('*italic text*')

// Code
await logseq.Editor.insertAtEditingCursor('`code`')

// Link
await logseq.Editor.insertAtEditingCursor('[Link text](https://example.com)')

// Page reference
await logseq.Editor.insertAtEditingCursor('[[Page Name]]')

// Block reference
await logseq.Editor.insertAtEditingCursor(`((${blockUuid}))`)

// Tag
await logseq.Editor.insertAtEditingCursor('#tag')
```

### Block References

```javascript
// Create a block reference
const targetBlock = await logseq.Editor.getBlock(targetUuid)
await logseq.Editor.insertAtEditingCursor(`((${targetBlock.uuid}))`)

// Embed a block
await logseq.Editor.insertAtEditingCursor(`{{embed ((${targetBlock.uuid}))  }}`)
```

### Working with Code Blocks

```javascript
// Insert code block
const code = `\`\`\`javascript
function hello() {
  console.log('Hello, World!');
}
\`\`\``

await logseq.Editor.insertBlock(parentUuid, code)

// Save focused code editor
await logseq.Editor.saveFocusedCodeEditorContent()
```

### Markdown vs Org Mode

```javascript
// Check block format
const block = await logseq.Editor.getBlock(uuid)

if (block.format === 'markdown') {
  // Use Markdown syntax
  await logseq.Editor.updateBlock(uuid, '- [ ] Task item')
} else if (block.format === 'org') {
  // Use Org mode syntax
  await logseq.Editor.updateBlock(uuid, '- [ ] Task item')
}

// Get graph's default format
const graphInfo = await logseq.App.getCurrentGraph()
console.log('Graph format:', graphInfo.format)
```

---

## File System Operations

### Reading Files

File-based graphs allow direct file access:

```javascript
// List files in graph
const files = await logseq.Assets.listFilesOfCurrentGraph()

console.log('All files:', files)
// Filter by extension
const mdFiles = await logseq.Assets.listFilesOfCurrentGraph(['md'])
const images = await logseq.Assets.listFilesOfCurrentGraph(['png', 'jpg'])

files.forEach((file) => {
  console.log(`${file.path} - ${file.size} bytes`)
})
```

### Working with Assets

```javascript
// Get asset URL
const assetUrl = await logseq.Assets.makeUrl('assets/image.png')

// Open asset in Logseq
await logseq.Assets.builtInOpen('assets/document.pdf')

// Create sandboxed storage for plugin data
const storage = await logseq.Assets.makeSandboxStorage()
await storage.setItem('plugin-data', JSON.stringify(data))
const saved = await storage.getItem('plugin-data')
```

### Git Operations API

File-based graphs support Git natively:

```javascript
// Execute git command
const result = await logseq.Git.execCommand(['status'])
console.log('Git status:', result.stdout)

// Add files
await logseq.Git.execCommand(['add', '.'])

// Commit
await logseq.Git.execCommand(['commit', '-m', 'Plugin update'])

// Get branch
const branch = await logseq.Git.execCommand(['branch', '--show-current'])
console.log('Current branch:', branch.stdout.trim())

// Read .gitignore
const gitignore = await logseq.Git.loadIgnoreFile()
console.log('Gitignore content:', gitignore)

// Update .gitignore
await logseq.Git.saveIgnoreFile(gitignore + '\nnode_modules/\n')
```

```clojure
;; ClojureScript
(-> (git/exec-command ["status"])
    (.then (fn [result]
             (println "Git status:" (.-stdout result)))))

(git/exec-command ["add" "."])
(git/exec-command ["commit" "-m" "Update"])
```

---

## Best Practices

### 1. Always Parse Properties Carefully

```javascript
// Good: Robust property parsing
function parseProperties(content) {
  const properties = {}
  const lines = content.split('\n')

  for (const line of lines) {
    const match = line.trim().match(/^(\w+)::\s*(.+)$/)
    if (match) {
      const [, key, value] = match
      properties[key] = value.trim()
    }
  }

  return properties
}

// Avoid: Fragile regex on entire content
// Can miss properties or incorrectly match
```

### 2. Preserve User Content

```javascript
// Good: Preserve existing content when updating properties
async function updateProperty(blockUuid, key, value) {
  const block = await logseq.Editor.getBlock(blockUuid)
  const mainContent = FileGraphProperties.getMainContent(block.content)
  const newContent = FileGraphProperties.set(block.content, key, value)

  await logseq.Editor.updateBlock(blockUuid, newContent)
}

// Avoid: Replacing entire content
// await logseq.Editor.updateBlock(uuid, `${key}:: ${value}`);
```

### 3. Handle Different Formats

```javascript
async function insertTask(parentUuid, content) {
  const parent = await logseq.Editor.getBlock(parentUuid)

  if (parent.format === 'markdown') {
    await logseq.Editor.insertBlock(parentUuid, `- [ ] ${content}`)
  } else {
    // Org mode uses same syntax for tasks
    await logseq.Editor.insertBlock(parentUuid, `- [ ] ${content}`)
  }
}
```

### 4. Use Batch Operations

```javascript
// Good: Batch insert
const blocks = items.map((item) => ({
  content: `${item.title}\nstatus:: ${item.status}`,
}))

await logseq.Editor.insertBatchBlock(parentUuid, blocks)

// Avoid: Multiple individual inserts
// for (const item of items) {
//   await logseq.Editor.insertBlock(parentUuid, ...);
// }
```

### 5. Validate File Operations

```javascript
// Good: Check before file operations
try {
  const files = await logseq.Assets.listFilesOfCurrentGraph(['md'])

  if (files.length === 0) {
    logseq.UI.showMsg('No markdown files found', 'warning')
    return
  }

  // Process files
} catch (error) {
  console.error('File operation failed:', error)
  logseq.UI.showMsg('Failed to access files', 'error')
}
```

### 6. Clean Up Resources

```javascript
let dbSubscription = null

async function setupPlugin() {
  dbSubscription = logseq.DB.onChanged(handleChange)
}

logseq.beforeunload(() => {
  if (dbSubscription) {
    dbSubscription()
    dbSubscription = null
  }
})
```

### 7. Provide User Feedback

```javascript
async function processAllBlocks() {
  const msgKey = await logseq.UI.showMsg('Processing blocks...', 'info', {
    timeout: 0,
  })

  try {
    // Process blocks
    await doWork()

    logseq.UI.closeMsg(msgKey)
    logseq.UI.showMsg('Processing complete!', 'success')
  } catch (error) {
    logseq.UI.closeMsg(msgKey)
    logseq.UI.showMsg(`Error: ${error.message}`, 'error')
  }
}
```

### 8. Handle Page References Correctly

```javascript
// Good: Create valid page references
function createPageRef(pageName) {
  // Escape special characters if needed
  return `[[${pageName}]]`
}

// Parse page references from property values
function extractPageRefs(value) {
  const matches = value.match(/\[\[([^\]]+)\]\]/g)
  return matches ? matches.map((m) => m.slice(2, -2)) : []
}
```

### 9. Test with Both Markdown and Org

```javascript
// Test your plugin with both formats
async function testPlugin() {
  const graph = await logseq.App.getCurrentGraph()
  console.log(`Testing with ${graph.format} graph`)

  // Run format-specific tests
  if (graph.format === 'markdown') {
    await testMarkdownFeatures()
  } else {
    await testOrgFeatures()
  }
}
```

### 10. Document Property Conventions

```javascript
/**
 * Plugin Property Conventions:
 *
 * - priority: 'low' | 'medium' | 'high' | 'critical'
 * - status: 'pending' | 'active' | 'completed' | 'blocked'
 * - assigned-to: [[Person Name]] - page reference
 * - due-date: [[YYYY-MM-DD]] - journal page reference
 * - tags: [[tag1]], [[tag2]] - comma-separated page refs
 *
 * Properties serialize to file content, and structured property APIs are available.
 */
```

---

## Findings: New Capabilities

Validated against current source as of 2026-05-29:

- Structured property APIs are available for file graphs: `upsertBlockProperty`, `getBlockProperty`, `getBlockProperties`, `getPageProperties`, and `removeBlockProperty`.
- Query APIs include `DB.q`, `DB.datascriptQuery`, and `DB.customQuery`.
- DB-change hooks still power reactive plugin updates (`onChanged`/`onBlockChanged` behavior).
- Internal related-guide filenames changed in this docs folder; links were updated accordingly.

---

## API Reference

### Block Operations

#### `insertBlock(srcBlock, content, opts?)`

Insert a new block.

**Parameters:**

- `srcBlock` (BlockIdentity) - Reference block UUID
- `content` (string) - Block content
- `opts` (object, optional)
  - `before` (boolean) - Insert before reference
  - `sibling` (boolean) - Insert as sibling
  - `start` (boolean) - Insert at start position when supported
  - `end` (boolean) - Insert at end position when supported
  - `customUUID` (string) - Provide custom UUID
  - `properties` (object) - Property payload handled by the runtime property layer

> **Removed:** `isPageBlock` is no longer part of the `insertBlock` option surface.

**Returns:** Promise\<BlockEntity | null>

#### `insertBatchBlock(srcBlock, batch, opts?)`

Insert multiple blocks.

**Parameters:**

- `srcBlock` (BlockIdentity) - Reference block
- `batch` (Array\<IBatchBlock>) - Blocks to insert
- `opts` (object, optional)
  - `before` (boolean)
  - `sibling` (boolean)

**Returns:** Promise\<Array\<BlockEntity>>

#### `updateBlock(srcBlock, content)`

Update block content.

**Parameters:**

- `srcBlock` (BlockIdentity) - Block UUID
- `content` (string) - New content

**Returns:** Promise\<void>

#### `removeBlock(srcBlock)`

Delete a block.

**Parameters:**

- `srcBlock` (BlockIdentity) - Block UUID

**Returns:** Promise\<void>

#### `getBlock(srcBlock, opts?)`

Get a block.

**Parameters:**

- `srcBlock` (BlockIdentity) - Block UUID
- `opts` (object, optional)
  - `includeChildren` (boolean)

**Returns:** Promise\<BlockEntity | null>

### Page Operations

#### `createPage(pageName, properties?, opts?)`

Create a page.

**Parameters:**

- `pageName` (string) - Page name
- `properties` (object, optional) - Properties (formatted inline)
- `opts` (object, optional)
  - `redirect` (boolean) - Navigate to page
  - `createFirstBlock` (boolean)
  - `format` ('markdown' | 'org')

**Returns:** Promise\<PageEntity | null>

#### `getPage(srcPage, opts?)`

Get a page.

**Parameters:**

- `srcPage` (PageIdentity) - Page name or UUID
- `opts` (object, optional)
  - `includeChildren` (boolean)

**Returns:** Promise\<PageEntity | null>

#### `deletePage(pageName)`

Delete a page.

**Parameters:**

- `pageName` (string) - Page name

**Returns:** Promise\<void>

#### `getAllPages(repo?)`

Get all pages.

**Parameters:**

- `repo` (string, optional) - Repository path

**Returns:** Promise\<Array\<PageEntity>>

### Query Operations

#### `DB.q(dsl)`

Run a DSL query.

**Parameters:**

- `dsl` (string) - Query string

**Returns:** Promise\<Array\<any>>

#### `DB.datascriptQuery(query, ...inputs)`

Run a Datascript query with optional inputs.

#### `DB.customQuery(query)`

Run a custom query through the query-react pipeline.

#### `DB.onChanged(callback)`

Subscribe to database changes.

**Parameters:**

- `callback` (function) - Called with `{ blocks, txData }`

**Returns:** Function (unsubscribe)

### File Operations

#### `Assets.listFilesOfCurrentGraph(exts?)`

List files in graph.

**Parameters:**

- `exts` (Array\<string>, optional) - File extensions to filter

**Returns:** Promise\<Array\<FileInfo>>

#### `Assets.makeUrl(path)`

Convert path to asset URL.

**Parameters:**

- `path` (string) - Relative path

**Returns:** Promise\<string>

#### `Assets.builtInOpen(path)`

Open file with Logseq viewer.

**Parameters:**

- `path` (string) - File path

**Returns:** Promise\<void>

### Git Operations

#### `Git.execCommand(args)`

Execute git command.

**Parameters:**

- `args` (Array\<string>) - Git command arguments

**Returns:** Promise\<{ stdout, stderr, exitCode }>

---

## Examples

### Example 1: Property Manager Plugin

```javascript
class PropertyManager {
  // Parse all properties from content
  static parse(content) {
    const properties = {}
    const lines = content.split('\n')

    for (const line of lines) {
      const match = line.match(/^(\w+)::\s*(.+)$/)
      if (match) {
        properties[match[1]] = match[2].trim()
      }
    }

    return properties
  }

  // Set property value
  static async set(blockUuid, key, value) {
    const block = await logseq.Editor.getBlock(blockUuid)
    const props = this.parse(block.content)
    const mainContent = this.getMainContent(block.content)

    props[key] = value

    const propsText = Object.entries(props)
      .map(([k, v]) => `${k}:: ${v}`)
      .join('\n')

    await logseq.Editor.updateBlock(blockUuid, mainContent + '\n' + propsText)
  }

  // Get main content without properties
  static getMainContent(content) {
    return content
      .split('\n')
      .filter((line) => !line.match(/^\w+::/))
      .join('\n')
      .trim()
  }

  // Remove property
  static async remove(blockUuid, key) {
    const block = await logseq.Editor.getBlock(blockUuid)
    const props = this.parse(block.content)
    const mainContent = this.getMainContent(block.content)

    delete props[key]

    const propsText = Object.entries(props)
      .map(([k, v]) => `${k}:: ${v}`)
      .join('\n')

    const newContent = propsText ? mainContent + '\n' + propsText : mainContent

    await logseq.Editor.updateBlock(blockUuid, newContent)
  }
}

// Register slash commands
logseq.Editor.registerSlashCommand('Set Priority', async () => {
  const block = await logseq.Editor.getCurrentBlock()
  if (block) {
    await PropertyManager.set(block.uuid, 'priority', 'high')
    logseq.UI.showMsg('Priority set!', 'success')
  }
})
```

### Example 2: Task Dashboard

```javascript
async function showTaskDashboard() {
  // Query all TODO/DOING tasks
  const tasks = await logseq.DB.q('(task TODO DOING)')

  // Group by priority
  const byPriority = {
    high: [],
    medium: [],
    low: [],
    none: [],
  }

  for (const task of tasks) {
    const props = PropertyManager.parse(task.content)
    const priority = props.priority || 'none'
    const group = byPriority[priority] || byPriority.none
    group.push(task)
  }

  // Build HTML
  let html = '<div style="padding: 20px;"><h2>Task Dashboard</h2>'

  for (const [priority, tasks] of Object.entries(byPriority)) {
    if (tasks.length > 0) {
      html += `<h3>${priority.toUpperCase()} Priority (${
        tasks.length
      })</h3><ul>`
      tasks.forEach((task) => {
        const content = PropertyManager.getMainContent(task.content)
        html += `<li>${content.substring(0, 50)}...</li>`
      })
      html += '</ul>'
    }
  }

  html += '</div>'

  // Show in main UI
  logseq.provideUI({
    key: 'task-dashboard',
    slot: 'main',
    template: html,
    reset: true,
  })

  logseq.showMainUI()
}

// Register command
logseq.App.registerCommandPalette(
  {
    key: 'show-task-dashboard',
    label: 'Show Task Dashboard',
  },
  showTaskDashboard
)
```

### Example 3: Auto-Property Updater

```javascript
// Automatically update properties on block changes
let subscription = null

async function setupAutoUpdater() {
  subscription = logseq.DB.onChanged(async ({ blocks }) => {
    for (const block of blocks) {
      const props = PropertyManager.parse(block.content)

      // Auto-set modified timestamp
      if (!props['last-modified']) {
        const now = new Date().toISOString()
        await PropertyManager.set(block.uuid, 'last-modified', now)
      }

      // Auto-calculate task age
      if (block.content.includes('TODO') && props['created-at']) {
        const created = new Date(props['created-at'])
        const age = Math.floor(
          (Date.now() - created.getTime()) / (1000 * 60 * 60 * 24)
        )
        await PropertyManager.set(block.uuid, 'age-days', age.toString())
      }
    }
  })
}

logseq.ready(setupAutoUpdater)

logseq.beforeunload(() => {
  if (subscription) subscription()
})
```

### Example 4: ClojureScript File Manager

```clojure
(ns file-manager.core
  (:require [com.logseq.core :as logseq]
            [com.logseq.editor :as editor]
            [com.logseq.assets :as assets]
            [com.logseq.ui :as ui]
            [clojure.string :as string]))

(defn parse-properties [content]
  (let [lines (string/split content #"\n")
        property-lines (filter #(re-matches #"^\w+::\s*.+$" %) lines)]
    (into {}
          (map (fn [line]
                 (let [[_ k v] (re-find #"^(\w+)::\s*(.+)$" line)]
                   [(keyword k) (string/trim v)]))
               property-lines))))

(defn get-main-content [content]
  (let [lines (string/split content #"\n")
        non-prop-lines (remove #(re-matches #"^\w+::" %) lines)]
    (string/join "\n" non-prop-lines)))

(defn set-property! [block-uuid key value]
  (-> (editor/get-block block-uuid)
      (.then (fn [block]
               (let [props (parse-properties (:content block))
                     main-content (get-main-content (:content block))
                     new-props (assoc props (keyword key) value)
                     props-text (string/join "\n"
                                           (map (fn [[k v]]
                                                  (str (name k) ":: " v))
                                                new-props))
                     new-content (str main-content "\n" props-text)]
                 (editor/update-block block-uuid new-content))))))

(defn list-markdown-files! []
  (-> (assets/list-files-of-current-graph ["md"])
      (.then (fn [files]
               (ui/show-msg (str "Found " (count files) " markdown files") "success")
               (doseq [file files]
                 (println "File:" (:path file)))))))

(defn init-plugin! []
  (editor/register-slash-command
    "Set Status"
    (fn []
      (-> (editor/get-current-block)
          (.then (fn [block]
                   (when block
                     (set-property! (:uuid block) "status" "active"))))))))

(-> (logseq/ready)
    (.then init-plugin!))
```

### Example 5: Git Integration

```javascript
class GitHelper {
  static async getStatus() {
    const result = await logseq.Git.execCommand(['status', '--short'])
    return result.stdout.split('\n').filter((line) => line.trim())
  }

  static async commit(message) {
    try {
      // Add all changes
      await logseq.Git.execCommand(['add', '.'])

      // Commit
      const result = await logseq.Git.execCommand(['commit', '-m', message])

      logseq.UI.showMsg('Changes committed', 'success')
      return result
    } catch (error) {
      logseq.UI.showMsg(`Commit failed: ${error.message}`, 'error')
      throw error
    }
  }

  static async getCurrentBranch() {
    const result = await logseq.Git.execCommand(['branch', '--show-current'])
    return result.stdout.trim()
  }

  static async createBranch(name) {
    await logseq.Git.execCommand(['checkout', '-b', name])
    logseq.UI.showMsg(`Created branch: ${name}`, 'success')
  }
}

// Register commands
logseq.App.registerCommandPalette(
  {
    key: 'git-status',
    label: 'Git: Show Status',
  },
  async () => {
    const status = await GitHelper.getStatus()
    console.log('Git status:', status)
    logseq.UI.showMsg(`${status.length} changes`, 'info')
  }
)

logseq.App.registerCommandPalette(
  {
    key: 'git-commit',
    label: 'Git: Quick Commit',
  },
  async () => {
    const message = prompt('Commit message:')
    if (message) {
      await GitHelper.commit(message)
    }
  }
)
```

### Example 6: Content Export

```javascript
async function exportToMarkdown() {
  // Get all pages
  const pages = await logseq.Editor.getAllPages()

  let markdown = '# Graph Export\n\n'

  for (const page of pages) {
    if (page.journalDay) continue // Skip journals

    markdown += `## ${page.originalName}\n\n`

    // Get page blocks
    const blocks = await logseq.Editor.getPageBlocksTree(page.uuid)

    for (const block of blocks) {
      const content = PropertyManager.getMainContent(block.content)
      markdown += `- ${content}\n`

      // Include children
      if (block.children) {
        for (const child of block.children) {
          const childContent = PropertyManager.getMainContent(child.content)
          markdown += `  - ${childContent}\n`
        }
      }
    }

    markdown += '\n'
  }

  // Save to file (user would copy this)
  console.log(markdown)

  // Or write to graph as a new page
  await logseq.Editor.createPage('Export', { content: markdown })

  logseq.UI.showMsg('Export complete!', 'success')
}
```

---

## Troubleshooting

### Common Issues

#### 1. Properties Not Being Set

**Problem:** Properties don't appear in blocks.

**Solution:**

```javascript
// Make sure properties are on separate lines
const content = `Main content
property1:: value1
property2:: value2`

// Not:
// const content = `Main content property1:: value1`;
```

#### 2. Page References Not Working

**Problem:** Page references don't link properly.

**Solution:**

```javascript
// Good: Proper page reference syntax
const ref = '[[Page Name]]'

// Avoid: Missing brackets
// const ref = 'Page Name';
```

#### 3. Content Encoding Issues

**Problem:** Special characters break content.

**Solution:**

```javascript
function escapeContent(text) {
  // Don't escape Logseq syntax
  return text
  // Logseq handles escaping internally
}
```

### Debug Helpers

```javascript
// Log block structure
async function debugBlock(uuid) {
  const block = await logseq.Editor.getBlock(uuid, {
    includeChildren: true,
  })
  console.log('Block:', JSON.stringify(block, null, 2))
}

// Log properties
async function debugProperties(uuid) {
  const block = await logseq.Editor.getBlock(uuid)
  const props = PropertyManager.parse(block.content)
  console.table(props)
}

// Test query
async function testQuery(query) {
  console.time('query')
  const results = await logseq.DB.q(query)
  console.timeEnd('query')
  console.log(`Found ${results.length} results`)
  return results
}
```

---

## Resources

### Documentation

- [Logseq Plugin API Documentation](https://logseq.github.io/plugins/)
- [Plugin Samples Repository](https://github.com/logseq/logseq-plugin-samples)
- [Logseq Discord Community](https://discord.gg/KpN4eHY)

### Related Guides

- [Plugin Developer Guide: DB Graphs](./db-graphs.md)
- [ClojureScript Plugin SDK Guide](./clojurescript-guide.md)

> **Removed:** Links to `plugin-developer-guide-db-graphs.md` and `clojurescript-plugin-sdk-guide.md` (files no longer exist in this folder).

---

## License

This guide is part of the Logseq project documentation.
MIT License - see the Logseq repository for details.

---

**Contributing:** Found an error or want to add examples? Submit issues or PRs to the Logseq repository.

**Questions:** Join the [Logseq Discord](https://discord.gg/KpN4eHY) #plugin-dev channel.
