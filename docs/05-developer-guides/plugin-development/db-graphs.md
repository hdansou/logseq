# Logseq Plugin Developer Guide: Database Graphs

**Last Updated:** November 2025
**Logseq Version:** 0.2.3+
**Target Audience:** Plugin developers building for Logseq DB-based graphs

---

## Table of Contents

1. [Introduction](#introduction)
2. [DB vs File-Based Graphs](#db-vs-file-based-graphs)
3. [Getting Started](#getting-started)
4. [The Property System](#the-property-system)
5. [Working with Tags/Classes](#working-with-tagsclasses)
6. [Querying DB Graphs](#querying-db-graphs)
7. [Block and Page Operations](#block-and-page-operations)
8. [Advanced Topics](#advanced-topics)
9. [Best Practices](#best-practices)
10. [Migration Guide](#migration-guide)
11. [API Reference](#api-reference)
12. [Examples](#examples)

---

## Introduction

Logseq Database (DB) graphs represent a fundamental shift in how Logseq stores and manages data. Unlike file-based graphs that store content in Markdown/Org files, DB graphs use SQLite with a Datascript-like query interface, providing:

- **Structured data model** - First-class properties with types and schemas
- **Improved performance** - Optimized queries and indexing
- **Rich metadata** - Type-safe properties with cardinality control
- **Class/tag system** - Hierarchical organization beyond simple tags
- **Real-time collaboration** - Foundation for multi-user editing (RTC)

This guide focuses on plugin development specifically for DB graphs, highlighting the differences and new capabilities compared to file-based graphs.

---

## DB vs File-Based Graphs

### Key Differences

| Feature            | File-Based Graphs      | DB Graphs                       |
| ------------------ | ---------------------- | ------------------------------- |
| **Storage**        | Markdown/Org files     | SQLite database                 |
| **Properties**     | Inline text parsing    | Structured schema with types    |
| **Tags**           | Simple text references | Classes with inheritance        |
| **Queries**        | Limited DSL            | Full Datascript queries         |
| **Performance**    | File system I/O        | Database queries                |
| **Collaboration**  | Git-based              | Real-time sync ready            |
| **Property Types** | Untyped strings        | Typed (number, date, url, etc.) |
| **Cardinality**    | Implicit               | Explicit (one/many)             |

### Detecting Graph Type

Always check if the current graph is DB-based before using DB-specific features:

```javascript
// JavaScript
const isDbGraph = await logseq.App.checkCurrentIsDbGraph()

if (isDbGraph) {
  // Use DB-specific APIs
  await logseq.Editor.upsertProperty('priority', {
    type: 'number',
    cardinality: 'one',
  })
} else {
  // Fall back to file-based approach
  console.log('File-based graphs use inline properties')
}
```

```clojure
;; ClojureScript
(-> (app/check-current-is-db-graph)
    (.then (fn [is-db?]
             (if is-db?
               ;; Use DB-specific APIs
               (editor/upsert-property "priority"
                                      {:type "number"
                                       :cardinality "one"})
               ;; Fall back
               (println "File-based graph")))))
```

---

## Getting Started

### Prerequisites

- Logseq 0.2.0 or later with DB graph support
- Basic understanding of the Logseq Plugin API
- For ClojureScript plugins: `@logseq/cljs-libs` 0.0.10+

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
  console.log('Plugin loaded!')

  // Check graph type
  const isDb = await logseq.App.checkCurrentIsDbGraph()
  console.log(`Graph type: ${isDb ? 'DB' : 'File-based'}`)

  if (isDb) {
    // Initialize DB-specific features
    await setupDbFeatures()
  }
}

logseq.ready(main).catch(console.error)
```

**ClojureScript:**

```clojure
(ns my-plugin.core
  (:require [com.logseq.core :as logseq]
            [com.logseq.app :as app]
            [com.logseq.editor :as editor]))

(defn setup-db-features! []
  (println "Setting up DB-specific features"))

(defn main []
  (-> (logseq/ready)
      (.then (fn []
               (println "Plugin loaded!")
               (-> (app/check-current-is-db-graph)
                   (.then (fn [is-db?]
                            (when is-db?
                              (setup-db-features!)))))))))

(main)
```

---

## The Property System

The property system is the cornerstone of DB graphs. Properties are first-class entities with schemas, types, and validation.

### Property Types

DB graphs support the following property types:

| Type       | Description                | Example Values         | JavaScript Type      |
| ---------- | -------------------------- | ---------------------- | -------------------- |
| `default`  | Text/string                | `"hello"`, `"world"`   | `string`             |
| `number`   | Numeric values             | `42`, `3.14`, `-10`    | `number`             |
| `date`     | Date (no time)             | `"2025-10-21"`         | Date reference       |
| `datetime` | Date with time             | `1729513200000`        | `number` (timestamp) |
| `checkbox` | Boolean                    | `true`, `false`        | `boolean`            |
| `url`      | URL strings                | `"https://logseq.com"` | `string`             |
| `node`     | References to blocks/pages | `"block-uuid"`         | Block UUID           |
| `json`     | Arbitrary JSON             | `{"key": "value"}`     | `object`             |
| `string`   | Explicitly typed string    | `"text"`               | `string`             |

### Cardinality

Properties can have one of two cardinality settings:

- **`one`** (default) - Single value
- **`many`** - Multiple values (set semantics)

### Creating Property Schemas

Properties must be defined before use in DB graphs. Plugins can only create properties within their namespace.

**Property Namespacing:**

- Plugin properties are automatically namespaced: `plugin.property.<plugin-id>/<property-name>`
- This prevents conflicts between plugins
- Users see the property name without the namespace prefix

#### Example: Basic Property

```javascript
// Create a simple text property
await logseq.Editor.upsertProperty('status', {
  type: 'default',
})

// Create a numeric property
await logseq.Editor.upsertProperty('priority', {
  type: 'number',
  cardinality: 'one',
})

// Create a multi-value tag property
await logseq.Editor.upsertProperty('tags', {
  type: 'default',
  cardinality: 'many',
})
```

```clojure
;; ClojureScript
(editor/upsert-property "status" {:type "default"})

(editor/upsert-property "priority"
                       {:type "number"
                        :cardinality "one"})

(editor/upsert-property "tags"
                       {:type "default"
                        :cardinality "many"})
```

#### Example: Advanced Property with Closed Values

Closed values restrict property values to a predefined set:

```javascript
await logseq.Editor.upsertProperty('status', {
  type: 'default',
  cardinality: 'one',
  // Note: Closed values are typically set through the UI
  // Plugin API doesn't directly support setting closed values yet
})
```

#### Example: Node Property with Classes

Node properties can reference specific classes/tags:

```javascript
await logseq.Editor.upsertProperty(
  'assignee',
  {
    type: 'node',
    cardinality: 'one',
    // Classes are specified through opts
  },
  {
    name: 'Assignee',
  }
)
```

### Setting Property Values on Blocks

Once a property schema exists, you can set values on blocks:

```javascript
// Get current block
const block = await logseq.Editor.getCurrentBlock()

if (block) {
  // Set a single-value property
  await logseq.Editor.upsertBlockProperty(block.uuid, 'priority', 5)

  // Set a multi-value property
  await logseq.Editor.upsertBlockProperty(block.uuid, 'tags', [
    'urgent',
    'review',
  ])

  // Set a date property (reference to journal page)
  const today = new Date()
  await logseq.Editor.upsertBlockProperty(block.uuid, 'due-date', today)

  // Set a checkbox
  await logseq.Editor.upsertBlockProperty(block.uuid, 'completed', true)
}
```

```clojure
;; ClojureScript
(-> (editor/get-current-block)
    (.then (fn [block]
             (when block
               (editor/upsert-block-property (:uuid block) "priority" 5)
               (editor/upsert-block-property (:uuid block) "tags" ["urgent" "review"])
               (editor/upsert-block-property (:uuid block) "completed" true)))))
```

### Reading Property Values

```javascript
// Get a specific property value
const priority = await logseq.Editor.getBlockProperty(block.uuid, 'priority')
console.log('Priority:', priority) // 5

// Get all properties
const props = await logseq.Editor.getBlockProperties(block.uuid)
console.log('All properties:', props)
// { priority: 5, tags: ['urgent', 'review'], completed: true }

// Get page properties
const page = await logseq.Editor.getPage('My Page')
const pageProps = await logseq.Editor.getPageProperties(page.uuid)
```

```clojure
;; ClojureScript
(-> (editor/get-block-property block-uuid "priority")
    (.then (fn [priority]
             (println "Priority:" priority))))

(-> (editor/get-block-properties block-uuid)
    (.then (fn [props]
             (println "All properties:" props))))
```

### Removing Properties

```javascript
// Remove a property value from a block
await logseq.Editor.removeBlockProperty(block.uuid, 'priority')

// Remove the property schema entirely (plugin properties only!)
await logseq.Editor.removeProperty('priority')
```

### Property Schema Metadata

Properties can have additional metadata:

```javascript
await logseq.Editor.upsertProperty('internal-data', {
  type: 'json',
  hide: true, // Hide from UI
  public: false, // Not visible in published graphs
})
```

### JSON Properties

JSON properties allow storing complex data structures:

```javascript
// Create JSON property
await logseq.Editor.upsertProperty('metadata', {
  type: 'json',
})

// Set JSON value
await logseq.Editor.upsertBlockProperty(block.uuid, 'metadata', {
  version: '1.0',
  author: 'Plugin',
  data: [1, 2, 3],
  nested: {
    key: 'value',
  },
})

// Retrieved value is automatically parsed
const metadata = await logseq.Editor.getBlockProperty(block.uuid, 'metadata')
console.log(metadata.version) // '1.0'
```

---

## Working with Tags/Classes

In DB graphs, tags are elevated to "classes" - first-class entities that can have properties and hierarchies.

### Understanding Classes vs Tags

- **File-based:** Tags are simple page references (`#project`)
- **DB graphs:** Classes are entities with:
  - Their own properties
  - Objects (pages/blocks) that belong to them
  - Hierarchical relationships
  - Type information

### Getting All Tags/Classes

```javascript
const tags = await logseq.Editor.getAllTags()
console.log('All tags:', tags)
// Array of page entities representing classes

tags.forEach((tag) => {
  console.log(`Tag: ${tag.originalName}`)
})
```

```clojure
;; ClojureScript
(-> (editor/get-all-tags)
    (.then (fn [tags]
             (doseq [tag tags]
               (println "Tag:" (:original-name tag))))))
```

### Creating Tags/Classes

**Added:** November 2025

Create new tags/classes programmatically:

```javascript
// Create a new tag/class
const newTag = await logseq.Editor.createTag('ProjectType')
console.log('Created tag:', newTag)

// Create with custom UUID
const tagWithUuid = await logseq.Editor.createTag('CustomTag', {
  uuid: 'custom-uuid-here',
})
```

```clojure
;; ClojureScript
(-> (editor/create-tag "ProjectType")
    (.then (fn [tag]
             (println "Created tag:" (:original-name tag)))))
```

### Getting Tag Information

**Added:** November 2025

Get tag/class entity by name or identifier:

```javascript
// Get tag by name
const projectTag = await logseq.Editor.getTag('Project')

// Get tag by ident
const taskTag = await logseq.Editor.getTag('logseq.class/Task')

// Check if tag exists
if (projectTag) {
  console.log('Tag UUID:', projectTag.uuid)
  console.log('Tag name:', projectTag.originalName)
}
```

```clojure
;; ClojureScript
(-> (editor/get-tag "Project")
    (.then (fn [tag]
             (when tag
               (println "Tag UUID:" (:uuid tag))
               (println "Tag name:" (:original-name tag))))))
```

### Getting Objects by Class

Retrieve all blocks/pages that belong to a class:

```javascript
// By class UUID
const projectBlocks = await logseq.Editor.getTagObjects('class-uuid')

// By class name/ident
const issueBlocks = await logseq.Editor.getTagObjects('issue')

// By page title
const taskBlocks = await logseq.Editor.getTagObjects('Task')

console.log(`Found ${projectBlocks.length} project blocks`)
```

```clojure
;; ClojureScript
(-> (editor/get-tag-objects "project")
    (.then (fn [blocks]
             (println (str "Found " (count blocks) " project blocks")))))
```

### Adding and Removing Tags from Blocks

**Added:** November 2025

Programmatically tag and untag blocks:

```javascript
// Add a tag to a block
const block = await logseq.Editor.getCurrentBlock()
if (block) {
  // Get or create the tag
  let projectTag = await logseq.Editor.getTag('Project')
  if (!projectTag) {
    projectTag = await logseq.Editor.createTag('Project')
  }

  // Add the tag to the block
  await logseq.Editor.addBlockTag(block.uuid, projectTag.uuid)
  logseq.UI.showMsg('Block tagged as Project', 'success')
}

// Remove a tag from a block
await logseq.Editor.removeBlockTag(block.uuid, projectTag.uuid)
```

```clojure
;; ClojureScript
(-> (editor/get-current-block)
    (.then (fn [block]
             (when block
               (-> (editor/get-tag "Project")
                   (.then (fn [tag]
                            (when tag
                              (editor/add-block-tag (:uuid block) (:uuid tag))))))))))
```

### Managing Tag Properties

**Added:** November 2025

Add or remove properties that are associated with specific tags/classes:

```javascript
// Get the tag
const taskTag = await logseq.Editor.getTag('Task')

// Add a property to the tag
// This property will be available for all blocks tagged with 'Task'
await logseq.Editor.addTagProperty(taskTag.uuid, 'priority')
await logseq.Editor.addTagProperty(taskTag.uuid, 'status')

// Remove a property from the tag
await logseq.Editor.removeTagProperty(taskTag.uuid, 'priority')
```

```clojure
;; ClojureScript
(-> (editor/get-tag "Task")
    (.then (fn [tag]
             (when tag
               (-> (editor/add-tag-property (:uuid tag) "priority")
                   (.then (fn []
                            (editor/add-tag-property (:uuid tag) "status"))))))))
```

### Creating Tagged Blocks

```javascript
// Create a block with a class/tag
await logseq.Editor.insertBlock(parentBlock.uuid, 'New project task', {
  properties: {
    class: ['Project', 'Task'], // Multiple classes
  },
})

// Alternative: Create block then add tags
const newBlock = await logseq.Editor.insertBlock(
  parentBlock.uuid,
  'New project task'
)
if (newBlock) {
  const projectTag = await logseq.Editor.getTag('Project')
  const taskTag = await logseq.Editor.getTag('Task')

  if (projectTag) await logseq.Editor.addBlockTag(newBlock.uuid, projectTag.uuid)
  if (taskTag) await logseq.Editor.addBlockTag(newBlock.uuid, taskTag.uuid)
}
```

---

## Querying DB Graphs

DB graphs support powerful Datascript queries that leverage the structured data model.

### Query Types

1. **DSL Queries** - Simple query language (limited in DB graphs)
2. **Datascript Queries** - Full Datalog queries (recommended)

### Simple DSL Queries

```javascript
// Query blocks with a specific property value
const results = await logseq.DB.q(`
  (property priority 5)
`)

// Query blocks with any value for a property
const hasDeadline = await logseq.DB.q(`
  (property deadline)
`)

// Combine conditions
const urgentTasks = await logseq.DB.q(`
  (and
    (property priority 5)
    (property status "active"))
`)
```

### Datascript Queries

More powerful and flexible:

```javascript
// Find all blocks with priority > 3
const highPriority = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :where
   [?b :block/properties ?props]
   [(get ?props :priority) ?p]
   [(> ?p 3)]]
`)

// Find blocks by property and class
const projectTasks = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :where
   [?b :block/tags ?t]
   [?t :block/original-name "Project"]
   [?b :block/properties ?props]
   [(get ?props :status) ?status]
   [(= ?status "active")]]
`)

// Query with inputs
const blocksWithTag = await logseq.DB.datascriptQuery(
  `
  [:find (pull ?b [*])
   :in $ ?tag-name
   :where
   [?b :block/tags ?t]
   [?t :block/original-name ?tag-name]]
`,
  'Project'
)
```

```clojure
;; ClojureScript
(db/datascript-query
  '[:find (pull ?b [*])
    :where
    [?b :block/properties ?props]
    [(get ?props :priority) ?p]
    [(> ?p 3)]]
  )
```

### Common Query Patterns

#### Find Blocks by Multiple Properties

```javascript
const results = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :where
   [?b :block/properties ?props]
   [(get ?props :priority) ?priority]
   [(get ?props :status) ?status]
   [(= ?priority 5)]
   [(= ?status "active")]]
`)
```

#### Find Blocks with Property Relationships

```javascript
// Find all blocks assigned to a specific person
const assigned = await logseq.DB.datascriptQuery(
  `
  [:find (pull ?b [*])
   :in $ ?person-uuid
   :where
   [?b :block/properties ?props]
   [(get ?props :assignee) ?assignee]
   [?assignee :block/uuid ?person-uuid]]
`,
  personUuid
)
```

#### Find Blocks by Date Range

```javascript
const today = Date.now()
const weekAgo = today - 7 * 24 * 60 * 60 * 1000

const recentBlocks = await logseq.DB.datascriptQuery(
  `
  [:find (pull ?b [*])
   :in $ ?start ?end
   :where
   [?b :block/properties ?props]
   [(get ?props :created-at) ?created]
   [(>= ?created ?start)]
   [(<= ?created ?end)]]
`,
  weekAgo,
  today
)
```

### Subscribing to Query Results

Monitor query results for changes:

```javascript
// Subscribe to all database changes
const unsubscribe = logseq.DB.onChanged(({ blocks, txData, txMeta }) => {
  console.log(`Database changed: ${blocks.length} blocks affected`)
  console.log('Transaction data:', txData)
  console.log('Operation:', txMeta?.outlinerOp)

  // Re-run queries, update UI, etc.
})

// Unsubscribe when done
// unsubscribe();
```

```clojure
;; ClojureScript
(db/on-changed
  (fn [tx-data]
    (println "Database changed:" (count (:blocks tx-data)) "blocks affected")))
```

### Subscribe to Specific Block Changes

```javascript
const unsubscribe = logseq.DB.onBlockChanged(
  blockUuid,
  (block, txData, txMeta) => {
    console.log('Block updated:', block)
    console.log('Content:', block.content)
    console.log('Properties:', block.properties)
  }
)
```

---

## Block and Page Operations

### Creating Blocks with Properties

In DB graphs, you can set properties at block creation time. Properties will be created automatically with inferred types if they don't exist:

```javascript
const newBlock = await logseq.Editor.insertBlock(
  parentBlock.uuid,
  'Task description',
  {
    properties: {
      priority: 5, // Auto-inferred as number
      status: 'active', // Auto-inferred as default/string
      'due-date': new Date('2025-12-31'), // Auto-inferred as date
      tags: ['urgent', 'client'], // Auto-inferred as cardinality:many
      completed: false, // Auto-inferred as checkbox
    },
    sibling: false, // Create as child
  }
)

console.log('Created block:', newBlock.uuid)
```

**With Explicit Schema (Advanced):**

You can optionally provide schema definitions to control property types:

```javascript
const newBlock = await logseq.Editor.insertBlock(
  parentBlock.uuid,
  'Task with custom types',
  {
    properties: {
      priority: 5,
      url: 'https://example.com',
    },
    schema: {
      priority: { type: 'number', cardinality: 'one' },
      url: { type: 'url', cardinality: 'one' },
    },
    sibling: false,
  }
)
```

```clojure
;; ClojureScript
(editor/insert-block
  parent-uuid
  "Task description"
  {:properties {:priority 5
                :status "active"
                :tags ["urgent" "client"]}
   :sibling false})
```

### Batch Block Creation with Properties

```javascript
const batchBlocks = [
  {
    content: 'First task',
    properties: { priority: 3 },
  },
  {
    content: 'Second task',
    properties: { priority: 5 },
    children: [
      {
        content: 'Subtask',
        properties: { status: 'pending' },
      },
    ],
  },
]

const created = await logseq.Editor.insertBatchBlock(
  parentBlock.uuid,
  batchBlocks,
  {
    sibling: false,
    schema: {
      // Optional: Define schemas for batch properties
      priority: { type: 'number', cardinality: 'one' },
      status: { type: 'default', cardinality: 'one' },
    },
  }
)

console.log(`Created ${created.length} blocks`)
```

### Updating Blocks with Properties

```javascript
// Update content and properties together
await logseq.Editor.updateBlock(block.uuid, 'Updated task description', {
  properties: {
    priority: 4,
    status: 'in-progress',
  },
  schema: {
    // Optional: Define schemas if properties don't exist
    priority: { type: 'number' },
    status: { type: 'default' },
  },
})
```

### Creating Pages with Properties

```javascript
const newPage = await logseq.Editor.createPage(
  'Project Alpha',
  {
    status: 'active',
    'start-date': new Date(),
    budget: 50000,
    team: ['Alice', 'Bob'],
  },
  {
    redirect: false,
    createFirstBlock: true,
  }
)
```

```clojure
;; ClojureScript
(editor/create-page
  "Project Alpha"
  {:status "active"
   :budget 50000
   :team ["Alice" "Bob"]}
  {:redirect false
   :create-first-block true})
```

### Property Type Coercion

DB graphs automatically handle type coercion based on property schema:

```javascript
// If 'priority' is defined as type 'number'
await logseq.Editor.upsertBlockProperty(block.uuid, 'priority', '5')
// Automatically converted to number 5

// If 'tags' has cardinality 'many'
await logseq.Editor.upsertBlockProperty(block.uuid, 'tags', 'urgent')
// Automatically wrapped in array ['urgent']
```

---

## Advanced Topics

### Property Validation

DB graphs enforce schema validation:

```javascript
try {
  // This will fail if property doesn't exist
  await logseq.Editor.upsertBlockProperty(
    block.uuid,
    'undefined-property',
    'value'
  )
} catch (error) {
  console.error('Property not defined:', error)
}

try {
  // This will fail if type doesn't match
  await logseq.Editor.upsertBlockProperty(
    block.uuid,
    'priority', // type: number
    'not a number'
  )
} catch (error) {
  console.error('Type mismatch:', error)
}
```

### Working with Hidden Properties

```javascript
// Create a hidden property for internal plugin state
await logseq.Editor.upsertProperty('plugin-state', {
  type: 'json',
  hide: true,
})

// Use it to store plugin-specific data
await logseq.Editor.upsertBlockProperty(block.uuid, 'plugin-state', {
  lastProcessed: Date.now(),
  version: '1.0.0',
  metadata: {
    /* ... */
  },
})
```

### Property Namespacing Deep Dive

Plugin properties are automatically namespaced to prevent conflicts:

```javascript
// When you create a property named 'priority'
await logseq.Editor.upsertProperty('priority', { type: 'number' })

// Internally stored as: :plugin.property.my-plugin-id/priority
// But accessed simply as 'priority' in your plugin

// Other plugins can create their own 'priority' without conflict
// :plugin.property.other-plugin/priority
```

### Migrating Data

If migrating from file-based to DB graphs:

```javascript
async function migrateProperties(block) {
  // File-based: properties are in content as "property:: value"
  const content = block.content
  const propertyRegex = /(\w+)::\s*(.+)/g

  let match
  while ((match = propertyRegex.exec(content)) !== null) {
    const [, key, value] = match

    // Ensure property exists
    try {
      await logseq.Editor.upsertProperty(key, { type: 'default' })
    } catch (e) {
      // Property might already exist
    }

    // Set property value
    await logseq.Editor.upsertBlockProperty(block.uuid, key, value)
  }

  // Clean content (remove inline properties)
  const cleanContent = content.replace(propertyRegex, '').trim()
  await logseq.Editor.updateBlock(block.uuid, cleanContent)
}
```

### Performance Optimization

#### Batch Property Updates

```javascript
// Instead of multiple calls:
// await logseq.Editor.upsertBlockProperty(uuid, 'a', 1);
// await logseq.Editor.upsertBlockProperty(uuid, 'b', 2);
// await logseq.Editor.upsertBlockProperty(uuid, 'c', 3);

// Use update with properties:
await logseq.Editor.updateBlock(uuid, content, {
  properties: {
    a: 1,
    b: 2,
    c: 3,
  },
})
```

#### Query Optimization

```javascript
// Use pull patterns to get only what you need
const results = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [:block/uuid :block/content :block/properties])
   :where
   [?b :block/properties ?props]]
`)

// Instead of pulling everything with [*]
```

### Real-Time Collaboration Considerations

DB graphs are designed for real-time collaboration:

```javascript
// Subscribe to changes to keep UI in sync
logseq.DB.onChanged(({ blocks, txMeta }) => {
  // Check if change was from another user
  if (txMeta?.source !== 'plugin') {
    // Update plugin UI/state
    refreshUI()
  }
})

// Use optimistic updates but be ready to handle conflicts
async function optimisticUpdate(blockUuid, newValue) {
  // Update UI immediately
  updateLocalState(blockUuid, newValue)

  try {
    // Perform actual update
    await logseq.Editor.upsertBlockProperty(blockUuid, 'value', newValue)
  } catch (error) {
    // Revert on conflict
    revertLocalState(blockUuid)
    console.error('Update failed:', error)
  }
}
```

---

## Best Practices

### 1. Always Check Graph Type

```javascript
async function initPlugin() {
  const isDb = await logseq.App.checkCurrentIsDbGraph()

  if (!isDb) {
    logseq.UI.showMsg('This plugin requires a DB graph', 'warning')
    return
  }

  // Continue with DB-specific initialization
}
```

### 2. Define Properties Early

```javascript
async function ensureProperties() {
  const requiredProperties = [
    { name: 'priority', schema: { type: 'number' } },
    { name: 'status', schema: { type: 'default' } },
    { name: 'tags', schema: { type: 'default', cardinality: 'many' } },
  ]

  for (const { name, schema } of requiredProperties) {
    try {
      await logseq.Editor.upsertProperty(name, schema)
    } catch (error) {
      console.error(`Failed to create property ${name}:`, error)
    }
  }
}

logseq.ready(ensureProperties)
```

### 3. Use Type-Safe Property Values

```javascript
// Good: Use correct types
await logseq.Editor.upsertBlockProperty(uuid, 'priority', 5)
await logseq.Editor.upsertBlockProperty(uuid, 'completed', true)
await logseq.Editor.upsertBlockProperty(uuid, 'due-date', new Date())

// Avoid: Relying on coercion
await logseq.Editor.upsertBlockProperty(uuid, 'priority', '5') // Works but not ideal
```

### 4. Handle Property Errors Gracefully

```javascript
async function setPropertySafely(blockUuid, key, value) {
  try {
    // First ensure property exists
    const property = await logseq.Editor.getProperty(key)
    if (!property) {
      console.warn(`Property ${key} doesn't exist, creating...`)
      await logseq.Editor.upsertProperty(key, { type: 'default' })
    }

    // Then set value
    await logseq.Editor.upsertBlockProperty(blockUuid, key, value)
    return true
  } catch (error) {
    console.error(`Failed to set ${key}:`, error)
    logseq.UI.showMsg(`Failed to set ${key}: ${error.message}`, 'error')
    return false
  }
}
```

### 5. Clean Up Resources

```javascript
let dbSubscription = null

async function setupPlugin() {
  // Subscribe to changes
  dbSubscription = logseq.DB.onChanged(handleDatabaseChange)
}

// Clean up when plugin unloads
logseq.beforeunload(() => {
  if (dbSubscription) {
    dbSubscription() // Unsubscribe
    dbSubscription = null
  }
})
```

### 6. Use Queries Efficiently

```javascript
// Good: Specific pull pattern
const results = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [:block/uuid :block/content :block/properties])
   :where
   [?b :block/properties ?props]
   [(get ?props :priority) ?p]
   [(= ?p 5)]]
`)

// Avoid: Pulling all data
const allBlocks = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :where
   [?b :block/uuid]]  // Too broad!
`)
```

### 7. Namespace Your Properties

```javascript
// Property names are auto-namespaced, but be descriptive
await logseq.Editor.upsertProperty('task-priority', { type: 'number' })
await logseq.Editor.upsertProperty('task-status', { type: 'default' })

// Better than generic names that might conflict semantically
// 'priority', 'status'
```

### 8. Document Your Property Schema

```javascript
/**
 * Plugin Property Schema:
 *
 * - task-priority (number, cardinality: one)
 *   Priority level from 1-5, where 5 is highest
 *
 * - task-status (default, cardinality: one)
 *   Current status: 'pending', 'active', 'completed', 'blocked'
 *
 * - task-tags (default, cardinality: many)
 *   Arbitrary tags for categorization
 *
 * - task-metadata (json, cardinality: one, hidden)
 *   Internal plugin state
 */
```

### 9. Validate User Input

```javascript
async function setPriority(blockUuid, priority) {
  // Validate before setting
  if (typeof priority !== 'number' || priority < 1 || priority > 5) {
    logseq.UI.showMsg('Priority must be a number between 1 and 5', 'warning')
    return false
  }

  return await setPropertySafely(blockUuid, 'priority', priority)
}
```

### 10. Provide User Feedback

```javascript
async function processBlock(blockUuid) {
  logseq.UI.showMsg('Processing block...', 'info', {
    timeout: 0,
    key: 'process',
  })

  try {
    await performComplexOperation(blockUuid)
    logseq.UI.closeMsg('process')
    logseq.UI.showMsg('Block processed successfully!', 'success')
  } catch (error) {
    logseq.UI.closeMsg('process')
    logseq.UI.showMsg(`Error: ${error.message}`, 'error')
  }
}
```

---

## Migration Guide

### From File-Based Plugins to DB Plugins

#### 1. Conditional Code Paths

```javascript
async function getValue(block, key) {
  const isDb = await logseq.App.checkCurrentIsDbGraph()

  if (isDb) {
    // DB graph: use property API
    return await logseq.Editor.getBlockProperty(block.uuid, key)
  } else {
    // File-based: parse from content
    const match = block.content.match(new RegExp(`${key}::\\s*(.+)`))
    return match ? match[1] : null
  }
}

async function setValue(block, key, value) {
  const isDb = await logseq.App.checkCurrentIsDbGraph()

  if (isDb) {
    return await logseq.Editor.upsertBlockProperty(block.uuid, key, value)
  } else {
    // File-based: update content
    const content = block.content.replace(
      new RegExp(`${key}::\\s*.+`),
      `${key}:: ${value}`
    )
    return await logseq.Editor.updateBlock(block.uuid, content)
  }
}
```

#### 2. Query Migration

```javascript
// File-based query (limited)
const fileResults = await logseq.DB.q('(property priority)')

// DB graph equivalent (more powerful)
const dbResults = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :where
   [?b :block/properties ?props]
   [(get ?props :priority) ?p]]
`)
```

#### 3. Feature Detection

```javascript
const features = {
  hasDbSupport: false,
  hasPropertyApi: false,
  hasClassSupport: false,
}

async function detectFeatures() {
  try {
    features.hasDbSupport = await logseq.App.checkCurrentIsDbGraph()

    if (features.hasDbSupport) {
      // Try DB-specific APIs
      features.hasPropertyApi =
        typeof logseq.Editor.upsertProperty === 'function'
      features.hasClassSupport =
        typeof logseq.Editor.getTagObjects === 'function'
    }
  } catch (error) {
    console.warn('Feature detection failed:', error)
  }

  return features
}
```

### Supporting Both Graph Types

```javascript
class PropertyAdapter {
  async getValue(block, key) {
    if (await this.isDbGraph()) {
      return await logseq.Editor.getBlockProperty(block.uuid, key)
    }
    return this.parseInlineProperty(block.content, key)
  }

  async setValue(block, key, value) {
    if (await this.isDbGraph()) {
      await logseq.Editor.upsertBlockProperty(block.uuid, key, value)
    } else {
      await this.setInlineProperty(block, key, value)
    }
  }

  async isDbGraph() {
    if (this._isDb === undefined) {
      this._isDb = await logseq.App.checkCurrentIsDbGraph()
    }
    return this._isDb
  }

  parseInlineProperty(content, key) {
    const match = content.match(new RegExp(`${key}::\\s*(.+)`))
    return match ? match[1].trim() : null
  }

  async setInlineProperty(block, key, value) {
    let content = block.content
    const regex = new RegExp(`${key}::\\s*.+`)

    if (regex.test(content)) {
      content = content.replace(regex, `${key}:: ${value}`)
    } else {
      content += `\n${key}:: ${value}`
    }

    await logseq.Editor.updateBlock(block.uuid, content)
  }
}

const properties = new PropertyAdapter()
```

---

## API Reference

### Property Management

#### `upsertProperty(key, schema?, opts?)`

Create or update a property definition.

**Parameters:**

- `key` (string) - Property name (will be namespaced)
- `schema` (object, optional) - Property schema
  - `type` (string) - One of: `'default'`, `'number'`, `'date'`, `'datetime'`, `'checkbox'`, `'url'`, `'node'`, `'json'`, `'string'`
  - `cardinality` (string) - `'one'` or `'many'`
  - `hide` (boolean) - Hide from UI
  - `public` (boolean) - Visibility in published graphs
- `opts` (object, optional)
  - `name` (string) - Display name

**Returns:** Promise<Entity>

**Example:**

```javascript
await logseq.Editor.upsertProperty(
  'priority',
  {
    type: 'number',
    cardinality: 'one',
  },
  {
    name: 'Task Priority',
  }
)
```

#### `getProperty(key)`

Get a property definition.

**Parameters:**

- `key` (string) - Property name

**Returns:** Promise<Entity | null>

#### `removeProperty(key)`

Remove a property definition (plugin properties only).

**Parameters:**

- `key` (string) - Property name

**Returns:** Promise<void>

#### `getAllProperties()`

Get all properties in the graph.

**Returns:** Promise<Array<Entity>>

### Block Properties

#### `upsertBlockProperty(block, key, value, options?)`

Set a property value on a block. If the property doesn't exist, it will be created with auto-inferred types.

**Parameters:**

- `block` (BlockIdentity) - Block UUID or entity
- `key` (string) - Property name
- `value` (any) - Value (type-checked against schema if exists)
- `options` (object, optional)
  - `schema` (object) - Inline schema definition for creating the property if it doesn't exist

**Returns:** Promise<void>

**Auto Type Inference:**
When a property doesn't exist, the type is automatically inferred from the value:

- `boolean` → `checkbox` type
- `number` → `number` type
- URL string or array of URLs → `url` type
- `object` → `json` type
- `string` → `default` type
- Array → `cardinality: 'many'`

**Example with schema:**

```javascript
await logseq.Editor.upsertBlockProperty(blockUuid, 'priority', 5, {
  schema: { type: 'number', cardinality: 'one' },
})
```

#### `getBlockProperty(block, key)`

Get a specific property value from a block.

**Parameters:**

- `block` (BlockIdentity) - Block UUID or entity
- `key` (string) - Property name

**Returns:** Promise<any>

#### `getBlockProperties(block)`

Get all properties of a block.

**Parameters:**

- `block` (BlockIdentity) - Block UUID or entity

**Returns:** Promise<Object>

#### `removeBlockProperty(block, key)`

Remove a property from a block.

**Parameters:**

- `block` (BlockIdentity) - Block UUID or entity
- `key` (string) - Property name

**Returns:** Promise<void>

### Page Properties

#### `getPageProperties(page)`

Get all properties of a page.

**Parameters:**

- `page` (PageIdentity) - Page name, UUID, or entity

**Returns:** Promise<Object>

### Tags/Classes

#### `getAllTags()`

Get all tags/classes in the graph.

**Returns:** Promise<Array<PageEntity>>

#### `createTag(tagName, opts?)`

**Added:** November 2025

Create a new tag/class.

**Parameters:**

- `tagName` (string) - Name for the new tag/class
- `opts` (object, optional) - Creation options
  - `uuid` (string) - Custom UUID for the tag

**Returns:** Promise<PageEntity | null>

**Example:**

```javascript
const tag = await logseq.Editor.createTag('ProjectType')
```

#### `getTag(nameOrIdent)`

**Added:** November 2025

Get a tag/class by name or identifier.

**Parameters:**

- `nameOrIdent` (string) - Tag name, db/ident (e.g., "logseq.class/Task"), or page title

**Returns:** Promise<PageEntity | null>

**Example:**

```javascript
const tag = await logseq.Editor.getTag('Project')
```

#### `getTagObjects(identifier)`

Get all blocks/pages that belong to a class.

**Parameters:**

- `identifier` (string) - Class UUID, db/ident, or title

**Returns:** Promise<Array<BlockEntity>>

#### `addBlockTag(blockId, tagId)`

**Added:** November 2025

Add a tag/class to a block.

**Parameters:**

- `blockId` (BlockIdentity) - Block UUID or entity
- `tagId` (BlockIdentity) - Tag UUID or entity

**Returns:** Promise<void>

**Example:**

```javascript
await logseq.Editor.addBlockTag(blockUuid, tagUuid)
```

#### `removeBlockTag(blockId, tagId)`

**Added:** November 2025

Remove a tag/class from a block.

**Parameters:**

- `blockId` (BlockIdentity) - Block UUID or entity
- `tagId` (BlockIdentity) - Tag UUID or entity

**Returns:** Promise<void>

**Example:**

```javascript
await logseq.Editor.removeBlockTag(blockUuid, tagUuid)
```

#### `addTagProperty(tagId, propertyIdOrName)`

**Added:** November 2025

Add a property to a tag/class. The property will be available for all blocks with that tag.

**Parameters:**

- `tagId` (BlockIdentity) - Tag UUID or entity
- `propertyIdOrName` (BlockIdentity) - Property UUID or name

**Returns:** Promise<void>

**Example:**

```javascript
await logseq.Editor.addTagProperty(tagUuid, 'priority')
```

#### `removeTagProperty(tagId, propertyIdOrName)`

**Added:** November 2025

Remove a property from a tag/class.

**Parameters:**

- `tagId` (BlockIdentity) - Tag UUID or entity
- `propertyIdOrName` (BlockIdentity) - Property UUID or name

**Returns:** Promise<void>

**Example:**

```javascript
await logseq.Editor.removeTagProperty(tagUuid, 'priority')
```

### Database Queries

#### `DB.q(dsl)`

Run a DSL query.

**Parameters:**

- `dsl` (string) - Query string

**Returns:** Promise<Array<any>>

#### `DB.datascriptQuery(query, ...inputs)`

Run a Datalog query.

**Parameters:**

- `query` (string) - Datalog query
- `inputs` (any[]) - Query inputs

**Returns:** Promise<Array<any>>

#### `DB.onChanged(callback)`

Subscribe to all database changes.

**Parameters:**

- `callback` (function) - Called with `{ blocks, txData, txMeta }`

**Returns:** Function (unsubscribe)

#### `DB.onBlockChanged(uuid, callback)`

Subscribe to specific block changes.

**Parameters:**

- `uuid` (string) - Block UUID
- `callback` (function) - Called with `(block, txData, txMeta)`

**Returns:** Function (unsubscribe)

### Graph Type Detection

#### `App.checkCurrentIsDbGraph()`

Check if current graph is DB-based.

**Returns:** Promise<boolean>

---

## Examples

### Example 1: Task Management Plugin

```javascript
// Initialize properties
async function initTaskManager() {
  const isDb = await logseq.App.checkCurrentIsDbGraph()
  if (!isDb) {
    logseq.UI.showMsg('Task manager requires DB graph', 'warning')
    return
  }

  // Define properties
  await logseq.Editor.upsertProperty('task-priority', {
    type: 'number',
    cardinality: 'one',
  })

  await logseq.Editor.upsertProperty('task-status', {
    type: 'default',
    cardinality: 'one',
  })

  await logseq.Editor.upsertProperty('task-due', {
    type: 'date',
    cardinality: 'one',
  })

  // Register command
  logseq.Editor.registerSlashCommand('Create Task', async () => {
    const block = await logseq.Editor.getCurrentBlock()
    if (!block) return

    await logseq.Editor.upsertBlockProperty(
      block.uuid,
      'task-status',
      'pending'
    )
    await logseq.Editor.upsertBlockProperty(block.uuid, 'task-priority', 3)

    logseq.UI.showMsg('Task created!', 'success')
  })
}

logseq.ready(initTaskManager)
```

### Example 2: Property Dashboard

```javascript
async function showPropertyDashboard() {
  // Query all blocks with priorities
  const blocks = await logseq.DB.datascriptQuery(`
    [:find (pull ?b [:block/uuid :block/content :block/properties])
     :where
     [?b :block/properties ?props]
     [(get ?props :task-priority) ?p]]
  `)

  // Group by priority
  const byPriority = blocks.reduce((acc, [block]) => {
    const priority = block.properties['task-priority'] || 0
    if (!acc[priority]) acc[priority] = []
    acc[priority].push(block)
    return acc
  }, {})

  // Display
  let html = '<div><h2>Tasks by Priority</h2>'
  for (let i = 5; i >= 1; i--) {
    const tasks = byPriority[i] || []
    html += `<h3>Priority ${i} (${tasks.length} tasks)</h3><ul>`
    tasks.forEach((task) => {
      html += `<li>${task.content.substring(0, 50)}...</li>`
    })
    html += '</ul>'
  }
  html += '</div>'

  logseq.provideUI({
    key: 'task-dashboard',
    slot: 'main',
    template: html,
    reset: true,
  })

  logseq.showMainUI()
}
```

### Example 3: Property Sync

```javascript
// Keep properties in sync with external system
async function syncProperties() {
  const subscription = logseq.DB.onChanged(async ({ blocks }) => {
    for (const block of blocks) {
      const props = await logseq.Editor.getBlockProperties(block.uuid)

      if (props && props['sync-enabled']) {
        // Sync to external system
        await syncToExternalSystem(block.uuid, props)
      }
    }
  })

  // Clean up
  logseq.beforeunload(() => {
    subscription()
  })
}
```

### Example 4: ClojureScript Task Plugin

```clojure
(ns task-manager.core
  (:require [com.logseq.core :as logseq]
            [com.logseq.app :as app]
            [com.logseq.editor :as editor]
            [com.logseq.ui :as ui]))

(defn init-properties! []
  (-> (editor/upsert-property "task-priority"
                             {:type "number"
                              :cardinality "one"})
      (.then (fn []
               (editor/upsert-property "task-status"
                                      {:type "default"
                                       :cardinality "one"})))))

(defn create-task! []
  (-> (editor/get-current-block)
      (.then (fn [block]
               (when block
                 (-> (editor/upsert-block-property (:uuid block) "task-status" "pending")
                     (.then (fn []
                              (editor/upsert-block-property (:uuid block) "task-priority" 3)))
                     (.then (fn []
                              (ui/show-msg "Task created!" "success")))))))))

(defn init-plugin! []
  (-> (app/check-current-is-db-graph)
      (.then (fn [is-db?]
               (if is-db?
                 (do
                   (init-properties!)
                   (editor/register-slash-command "Create Task" create-task!))
                 (ui/show-msg "Task manager requires DB graph" "warning"))))))

(-> (logseq/ready)
    (.then init-plugin!))
```

### Example 5: Advanced Query Builder

```javascript
class QueryBuilder {
  constructor() {
    this.conditions = []
  }

  property(key, value) {
    if (value !== undefined) {
      this.conditions.push(`[(get ?props :${key}) ?${key}]`)
      this.conditions.push(`[(= ?${key} ${JSON.stringify(value)})]`)
    } else {
      this.conditions.push(`[(get ?props :${key})]`)
    }
    return this
  }

  propertyRange(key, min, max) {
    this.conditions.push(`[(get ?props :${key}) ?${key}]`)
    if (min !== undefined) {
      this.conditions.push(`[(>= ?${key} ${min})]`)
    }
    if (max !== undefined) {
      this.conditions.push(`[(<= ?${key} ${max})]`)
    }
    return this
  }

  tag(tagName) {
    this.conditions.push(`[?b :block/tags ?t]`)
    this.conditions.push(`[?t :block/original-name "${tagName}"]`)
    return this
  }

  async execute() {
    const query = `
      [:find (pull ?b [*])
       :where
       [?b :block/properties ?props]
       ${this.conditions.join('\n       ')}]
    `

    return await logseq.DB.datascriptQuery(query)
  }
}

// Usage
const results = await new QueryBuilder()
  .property('task-status', 'active')
  .propertyRange('task-priority', 3, 5)
  .tag('Project')
  .execute()
```

### Example 6: Property Migration Tool

```javascript
async function migrateFilePropertiesToDb() {
  const allBlocks = await logseq.Editor.getAllPages()

  let migrated = 0
  let failed = 0

  for (const page of allBlocks) {
    const blocks = await logseq.Editor.getPageBlocksTree(page.uuid)

    for (const block of blocks) {
      try {
        await migrateBlockProperties(block)
        migrated++
      } catch (error) {
        console.error(`Failed to migrate block ${block.uuid}:`, error)
        failed++
      }
    }
  }

  logseq.UI.showMsg(
    `Migration complete: ${migrated} blocks migrated, ${failed} failed`,
    'success'
  )
}

async function migrateBlockProperties(block) {
  const propertyRegex = /(\w+)::\s*(.+)/g
  let match
  const properties = {}

  while ((match = propertyRegex.exec(block.content)) !== null) {
    const [, key, value] = match
    properties[key] = value

    // Ensure property exists
    try {
      await logseq.Editor.upsertProperty(key, { type: 'default' })
    } catch (e) {
      // Property exists
    }
  }

  // Set all properties
  if (Object.keys(properties).length > 0) {
    for (const [key, value] of Object.entries(properties)) {
      await logseq.Editor.upsertBlockProperty(block.uuid, key, value)
    }

    // Clean content
    const cleanContent = block.content.replace(propertyRegex, '').trim()
    if (cleanContent !== block.content) {
      await logseq.Editor.updateBlock(block.uuid, cleanContent)
    }
  }
}
```

---

## Troubleshooting

### Common Issues

#### 1. "Property doesn't exist" Error

**Problem:** Trying to set a property that hasn't been defined.

**Solution:**

```javascript
// Always ensure property exists first
try {
  const prop = await logseq.Editor.getProperty('my-property')
  if (!prop) {
    await logseq.Editor.upsertProperty('my-property', { type: 'default' })
  }
  await logseq.Editor.upsertBlockProperty(uuid, 'my-property', 'value')
} catch (error) {
  console.error('Property error:', error)
}
```

#### 2. Type Mismatch Errors

**Problem:** Setting a value that doesn't match the property type.

**Solution:**

```javascript
// Check property type before setting
const prop = await logseq.Editor.getProperty('priority')
const propType = prop?.type || 'default'

let value = userInput
if (propType === 'number') {
  value = Number(value)
} else if (propType === 'checkbox') {
  value = Boolean(value)
}

await logseq.Editor.upsertBlockProperty(uuid, 'priority', value)
```

#### 3. Query Returns No Results

**Problem:** Datascript query doesn't return expected blocks.

**Solution:**

```javascript
// Debug query step by step
const debug = await logseq.DB.datascriptQuery(`
  [:find ?b ?props
   :where
   [?b :block/properties ?props]]
`)
console.log('All blocks with properties:', debug)

// Check property names (case-sensitive!)
const props = await logseq.Editor.getBlockProperties(blockUuid)
console.log('Property keys:', Object.keys(props))
```

#### 4. Plugin Properties Not Visible

**Problem:** Properties created by plugin don't appear in UI.

**Solution:**

```javascript
// Check if property was created with hide: true
const prop = await logseq.Editor.getProperty('my-property')
console.log('Property hidden?', prop?.hide)

// Recreate without hide flag
await logseq.Editor.upsertProperty('my-property', {
  type: 'default',
  hide: false,
})
```

### Debug Helpers

```javascript
// Log all properties
async function debugProperties() {
  const props = await logseq.Editor.getAllProperties()
  console.table(
    props.map((p) => ({
      name: p.originalName,
      type: p.type,
      cardinality: p.cardinality,
      hidden: p.hide,
    }))
  )
}

// Log block properties
async function debugBlock(uuid) {
  const block = await logseq.Editor.getBlock(uuid)
  const props = await logseq.Editor.getBlockProperties(uuid)
  console.log('Block:', block)
  console.log('Properties:', props)
}

// Test query
async function testQuery(query) {
  console.time('query')
  const results = await logseq.DB.datascriptQuery(query)
  console.timeEnd('query')
  console.log('Results:', results)
  return results
}
```

---

## Resources

### Documentation

- [Logseq Plugin API Documentation](https://logseq.github.io/plugins/)
- [Datascript Query Reference](https://github.com/tonsky/datascript)
- [Logseq Discord Community](https://discord.gg/KpN4eHY)

### Example Plugins

- [Plugin Samples Repository](https://github.com/logseq/logseq-plugin-samples)
- [ClojureScript Playground](https://github.com/tiensonqin/logseq-cljs-playground)

### Related Guides

- [ClojureScript Plugin SDK Guide](./clojurescript-plugin-sdk-guide.md)
- [ClojureScript Plugin SDK Detective Guide](./clojurescript-plugin-sdk-detective-guide.md)

---

## License

This guide is part of the Logseq project documentation.
MIT License - see the Logseq repository for details.

---

**Contributing:** Found an error or want to add examples? Please submit issues or PRs to the Logseq repository.

**Questions:** Join the [Logseq Discord](https://discord.gg/KpN4eHY) #plugin-dev channel.
