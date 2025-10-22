Logseq Plugin Developer Guide: Database Graphs
Last Updated: October 2025 Logseq Version: 0.2.3+ Target Audience: Plugin developers building for Logseq DB-based graphs

Table of Contents
Introduction
DB vs File-Based Graphs
Getting Started
The Property System
Working with Tags/Classes
Querying DB Graphs
Block and Page Operations
Advanced Topics
Best Practices
Migration Guide
API Reference
Examples
Introduction
Logseq Database (DB) graphs represent a fundamental shift in how Logseq stores and manages data. Unlike file-based graphs that store content in Markdown/Org files, DB graphs use SQLite with a Datascript-like query interface, providing:

Structured data model - First-class properties with types and schemas
Improved performance - Optimized queries and indexing
Rich metadata - Type-safe properties with cardinality control
Class/tag system - Hierarchical organization beyond simple tags
Real-time collaboration - Foundation for multi-user editing (RTC)
This guide focuses on plugin development specifically for DB graphs, highlighting the differences and new capabilities compared to file-based graphs.

DB vs File-Based Graphs
Key Differences
Feature	File-Based Graphs	DB Graphs
Storage	Markdown/Org files	SQLite database
Properties	Inline text parsing	Structured schema with types
Tags	Simple text references	Classes with inheritance
Queries	Limited DSL	Full Datascript queries
Performance	File system I/O	Database queries
Collaboration	Git-based	Real-time sync ready
Property Types	Untyped strings	Typed (number, date, url, etc.)
Cardinality	Implicit	Explicit (one/many)
Detecting Graph Type
Always check if the current graph is DB-based before using DB-specific features:

// JavaScript
const isDbGraph = await logseq.App.checkCurrentIsDbGraph();

if (isDbGraph) {
  // Use DB-specific APIs
  await logseq.Editor.upsertProperty('priority', {
    type: 'number',
    cardinality: 'one'
  });
} else {
  // Fall back to file-based approach
  console.log('File-based graphs use inline properties');
}
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
Getting Started
Prerequisites
Logseq 0.2.0 or later with DB graph support
Basic understanding of the Logseq Plugin API
For ClojureScript plugins: @logseq/cljs-libs 0.0.10+
Installation
JavaScript/TypeScript:

npm install @logseq/libs
ClojureScript:

yarn add @logseq/cljs-libs
Basic Setup
JavaScript:

import '@logseq/libs';

async function main() {
  console.log('Plugin loaded!');

  // Check graph type
  const isDb = await logseq.App.checkCurrentIsDbGraph();
  console.log(`Graph type: ${isDb ? 'DB' : 'File-based'}`);

  if (isDb) {
    // Initialize DB-specific features
    await setupDbFeatures();
  }
}

logseq.ready(main).catch(console.error);
ClojureScript:

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
The Property System
The property system is the cornerstone of DB graphs. Properties are first-class entities with schemas, types, and validation.

Property Types
DB graphs support the following property types:

Type	Description	Example Values	JavaScript Type
default	Text/string	"hello", "world"	string
number	Numeric values	42, 3.14, -10	number
date	Date (no time)	"2025-10-21"	Date reference
datetime	Date with time	1729513200000	number (timestamp)
checkbox	Boolean	true, false	boolean
url	URL strings	"https://logseq.com"	string
node	References to blocks/pages	"block-uuid"	Block UUID
json	Arbitrary JSON	{"key": "value"}	object
string	Explicitly typed string	"text"	string
Cardinality
Properties can have one of two cardinality settings:

one (default) - Single value
many - Multiple values (set semantics)
Creating Property Schemas
Properties must be defined before use in DB graphs. Plugins can only create properties within their namespace.

Property Namespacing:

Plugin properties are automatically namespaced: plugin.property.<plugin-id>/<property-name>
This prevents conflicts between plugins
Users see the property name without the namespace prefix
Example: Basic Property
// Create a simple text property
await logseq.Editor.upsertProperty('status', {
  type: 'default'
});

// Create a numeric property
await logseq.Editor.upsertProperty('priority', {
  type: 'number',
  cardinality: 'one'
});

// Create a multi-value tag property
await logseq.Editor.upsertProperty('tags', {
  type: 'default',
  cardinality: 'many'
});
;; ClojureScript
(editor/upsert-property "status" {:type "default"})

(editor/upsert-property "priority"
                       {:type "number"
                        :cardinality "one"})

(editor/upsert-property "tags"
                       {:type "default"
                        :cardinality "many"})
Example: Advanced Property with Closed Values
Closed values restrict property values to a predefined set:

await logseq.Editor.upsertProperty('status', {
  type: 'default',
  cardinality: 'one',
  // Note: Closed values are typically set through the UI
  // Plugin API doesn't directly support setting closed values yet
});
Example: Node Property with Classes
Node properties can reference specific classes/tags:

await logseq.Editor.upsertProperty('assignee', {
  type: 'node',
  cardinality: 'one',
  // Classes are specified through opts
}, {
  name: 'Assignee'
});
Setting Property Values on Blocks
Once a property schema exists, you can set values on blocks:

// Get current block
const block = await logseq.Editor.getCurrentBlock();

if (block) {
  // Set a single-value property
  await logseq.Editor.upsertBlockProperty(block.uuid, 'priority', 5);

  // Set a multi-value property
  await logseq.Editor.upsertBlockProperty(block.uuid, 'tags', ['urgent', 'review']);

  // Set a date property (reference to journal page)
  const today = new Date();
  await logseq.Editor.upsertBlockProperty(block.uuid, 'due-date', today);

  // Set a checkbox
  await logseq.Editor.upsertBlockProperty(block.uuid, 'completed', true);
}
;; ClojureScript
(-> (editor/get-current-block)
    (.then (fn [block]
             (when block
               (editor/upsert-block-property (:uuid block) "priority" 5)
               (editor/upsert-block-property (:uuid block) "tags" ["urgent" "review"])
               (editor/upsert-block-property (:uuid block) "completed" true)))))
Reading Property Values
// Get a specific property value
const priority = await logseq.Editor.getBlockProperty(block.uuid, 'priority');
console.log('Priority:', priority); // 5

// Get all properties
const props = await logseq.Editor.getBlockProperties(block.uuid);
console.log('All properties:', props);
// { priority: 5, tags: ['urgent', 'review'], completed: true }

// Get page properties
const page = await logseq.Editor.getPage('My Page');
const pageProps = await logseq.Editor.getPageProperties(page.uuid);
;; ClojureScript
(-> (editor/get-block-property block-uuid "priority")
    (.then (fn [priority]
             (println "Priority:" priority))))

(-> (editor/get-block-properties block-uuid)
    (.then (fn [props]
             (println "All properties:" props))))
Removing Properties
// Remove a property value from a block
await logseq.Editor.removeBlockProperty(block.uuid, 'priority');

// Remove the property schema entirely (plugin properties only!)
await logseq.Editor.removeProperty('priority');
Property Schema Metadata
Properties can have additional metadata:

await logseq.Editor.upsertProperty('internal-data', {
  type: 'json',
  hide: true,      // Hide from UI
  public: false    // Not visible in published graphs
});
JSON Properties
JSON properties allow storing complex data structures:

// Create JSON property
await logseq.Editor.upsertProperty('metadata', {
  type: 'json'
});

// Set JSON value
await logseq.Editor.upsertBlockProperty(block.uuid, 'metadata', {
  version: '1.0',
  author: 'Plugin',
  data: [1, 2, 3],
  nested: {
    key: 'value'
  }
});

// Retrieved value is automatically parsed
const metadata = await logseq.Editor.getBlockProperty(block.uuid, 'metadata');
console.log(metadata.version); // '1.0'
Working with Tags/Classes
In DB graphs, tags are elevated to "classes" - first-class entities that can have properties and hierarchies.

Understanding Classes vs Tags
File-based: Tags are simple page references (#project)
DB graphs: Classes are entities with:
Their own properties
Objects (pages/blocks) that belong to them
Hierarchical relationships
Type information
Getting All Tags/Classes
const tags = await logseq.Editor.getAllTags();
console.log('All tags:', tags);
// Array of page entities representing classes

tags.forEach(tag => {
  console.log(`Tag: ${tag.originalName}`);
});
;; ClojureScript
(-> (editor/get-all-tags)
    (.then (fn [tags]
             (doseq [tag tags]
               (println "Tag:" (:original-name tag))))))
Getting Objects by Class
Retrieve all blocks/pages that belong to a class:

// By class UUID
const projectBlocks = await logseq.Editor.getTagObjects('class-uuid');

// By class name/ident
const issueBlocks = await logseq.Editor.getTagObjects('issue');

// By page title
const taskBlocks = await logseq.Editor.getTagObjects('Task');

console.log(`Found ${projectBlocks.length} project blocks`);
;; ClojureScript
(-> (editor/get-tag-objects "project")
    (.then (fn [blocks]
             (println (str "Found " (count blocks) " project blocks")))))
Creating Tagged Blocks
// Create a block with a class/tag
await logseq.Editor.insertBlock(parentBlock.uuid, 'New project task', {
  properties: {
    class: ['Project', 'Task']  // Multiple classes
  }
});
Class-Specific Properties
You can create properties that only apply to specific classes:

// This is typically done through UI or advanced setup
// Property definitions can specify which classes they apply to
Querying DB Graphs
DB graphs support powerful Datascript queries that leverage the structured data model.

Query Types
DSL Queries - Simple query language (limited in DB graphs)
Datascript Queries - Full Datalog queries (recommended)
Simple DSL Queries
// Query blocks with a specific property value
const results = await logseq.DB.q(`
  (property priority 5)
`);

// Query blocks with any value for a property
const hasDeadline = await logseq.DB.q(`
  (property deadline)
`);

// Combine conditions
const urgentTasks = await logseq.DB.q(`
  (and
    (property priority 5)
    (property status "active"))
`);
Datascript Queries
More powerful and flexible:

// Find all blocks with priority > 3
const highPriority = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :where
   [?b :block/properties ?props]
   [(get ?props :priority) ?p]
   [(> ?p 3)]]
`);

// Find blocks by property and class
const projectTasks = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :where
   [?b :block/tags ?t]
   [?t :block/original-name "Project"]
   [?b :block/properties ?props]
   [(get ?props :status) ?status]
   [(= ?status "active")]]
`);

// Query with inputs
const blocksWithTag = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :in $ ?tag-name
   :where
   [?b :block/tags ?t]
   [?t :block/original-name ?tag-name]]
`, 'Project');
;; ClojureScript
(db/datascript-query
  '[:find (pull ?b [*])
    :where
    [?b :block/properties ?props]
    [(get ?props :priority) ?p]
    [(> ?p 3)]]
  )
Common Query Patterns
Find Blocks by Multiple Properties
const results = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :where
   [?b :block/properties ?props]
   [(get ?props :priority) ?priority]
   [(get ?props :status) ?status]
   [(= ?priority 5)]
   [(= ?status "active")]]
`);
Find Blocks with Property Relationships
// Find all blocks assigned to a specific person
const assigned = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :in $ ?person-uuid
   :where
   [?b :block/properties ?props]
   [(get ?props :assignee) ?assignee]
   [?assignee :block/uuid ?person-uuid]]
`, personUuid);
Find Blocks by Date Range
const today = Date.now();
const weekAgo = today - (7 * 24 * 60 * 60 * 1000);

const recentBlocks = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :in $ ?start ?end
   :where
   [?b :block/properties ?props]
   [(get ?props :created-at) ?created]
   [(>= ?created ?start)]
   [(<= ?created ?end)]]
`, weekAgo, today);
Subscribing to Query Results
Monitor query results for changes:

// Subscribe to all database changes
const unsubscribe = logseq.DB.onChanged(({ blocks, txData, txMeta }) => {
  console.log(`Database changed: ${blocks.length} blocks affected`);
  console.log('Transaction data:', txData);
  console.log('Operation:', txMeta?.outlinerOp);

  // Re-run queries, update UI, etc.
});

// Unsubscribe when done
// unsubscribe();
;; ClojureScript
(db/on-changed
  (fn [tx-data]
    (println "Database changed:" (count (:blocks tx-data)) "blocks affected")))
Subscribe to Specific Block Changes
const unsubscribe = logseq.DB.onBlockChanged(
  blockUuid,
  (block, txData, txMeta) => {
    console.log('Block updated:', block);
    console.log('Content:', block.content);
    console.log('Properties:', block.properties);
  }
);
Block and Page Operations
Creating Blocks with Properties
In DB graphs, you can set properties at block creation time:

const newBlock = await logseq.Editor.insertBlock(
  parentBlock.uuid,
  'Task description',
  {
    properties: {
      priority: 5,
      status: 'active',
      'due-date': new Date('2025-12-31'),
      tags: ['urgent', 'client']
    },
    sibling: false  // Create as child
  }
);

console.log('Created block:', newBlock.uuid);
;; ClojureScript
(editor/insert-block
  parent-uuid
  "Task description"
  {:properties {:priority 5
                :status "active"
                :tags ["urgent" "client"]}
   :sibling false})
Batch Block Creation with Properties
const batchBlocks = [
  {
    content: 'First task',
    properties: { priority: 3 }
  },
  {
    content: 'Second task',
    properties: { priority: 5 },
    children: [
      {
        content: 'Subtask',
        properties: { status: 'pending' }
      }
    ]
  }
];

const created = await logseq.Editor.insertBatchBlock(
  parentBlock.uuid,
  batchBlocks,
  { sibling: false }
);

console.log(`Created ${created.length} blocks`);
Updating Blocks with Properties
// Update content and properties together
await logseq.Editor.updateBlock(
  block.uuid,
  'Updated task description',
  {
    properties: {
      priority: 4,
      status: 'in-progress'
    }
  }
);
Creating Pages with Properties
const newPage = await logseq.Editor.createPage(
  'Project Alpha',
  {
    status: 'active',
    'start-date': new Date(),
    budget: 50000,
    team: ['Alice', 'Bob']
  },
  {
    redirect: false,
    createFirstBlock: true
  }
);
;; ClojureScript
(editor/create-page
  "Project Alpha"
  {:status "active"
   :budget 50000
   :team ["Alice" "Bob"]}
  {:redirect false
   :create-first-block true})
Property Type Coercion
DB graphs automatically handle type coercion based on property schema:

// If 'priority' is defined as type 'number'
await logseq.Editor.upsertBlockProperty(block.uuid, 'priority', '5');
// Automatically converted to number 5

// If 'tags' has cardinality 'many'
await logseq.Editor.upsertBlockProperty(block.uuid, 'tags', 'urgent');
// Automatically wrapped in array ['urgent']
Advanced Topics
Property Validation
DB graphs enforce schema validation:

try {
  // This will fail if property doesn't exist
  await logseq.Editor.upsertBlockProperty(
    block.uuid,
    'undefined-property',
    'value'
  );
} catch (error) {
  console.error('Property not defined:', error);
}

try {
  // This will fail if type doesn't match
  await logseq.Editor.upsertBlockProperty(
    block.uuid,
    'priority', // type: number
    'not a number'
  );
} catch (error) {
  console.error('Type mismatch:', error);
}
Working with Hidden Properties
// Create a hidden property for internal plugin state
await logseq.Editor.upsertProperty('plugin-state', {
  type: 'json',
  hide: true
});

// Use it to store plugin-specific data
await logseq.Editor.upsertBlockProperty(
  block.uuid,
  'plugin-state',
  {
    lastProcessed: Date.now(),
    version: '1.0.0',
    metadata: { /* ... */ }
  }
);
Property Namespacing Deep Dive
Plugin properties are automatically namespaced to prevent conflicts:

// When you create a property named 'priority'
await logseq.Editor.upsertProperty('priority', { type: 'number' });

// Internally stored as: :plugin.property.my-plugin-id/priority
// But accessed simply as 'priority' in your plugin

// Other plugins can create their own 'priority' without conflict
// :plugin.property.other-plugin/priority
Migrating Data
If migrating from file-based to DB graphs:

async function migrateProperties(block) {
  // File-based: properties are in content as "property:: value"
  const content = block.content;
  const propertyRegex = /(\w+)::\s*(.+)/g;

  let match;
  while ((match = propertyRegex.exec(content)) !== null) {
    const [, key, value] = match;

    // Ensure property exists
    try {
      await logseq.Editor.upsertProperty(key, { type: 'default' });
    } catch (e) {
      // Property might already exist
    }

    // Set property value
    await logseq.Editor.upsertBlockProperty(block.uuid, key, value);
  }

  // Clean content (remove inline properties)
  const cleanContent = content.replace(propertyRegex, '').trim();
  await logseq.Editor.updateBlock(block.uuid, cleanContent);
}
Performance Optimization
Batch Property Updates
// Instead of multiple calls:
// await logseq.Editor.upsertBlockProperty(uuid, 'a', 1);
// await logseq.Editor.upsertBlockProperty(uuid, 'b', 2);
// await logseq.Editor.upsertBlockProperty(uuid, 'c', 3);

// Use update with properties:
await logseq.Editor.updateBlock(uuid, content, {
  properties: {
    a: 1,
    b: 2,
    c: 3
  }
});
Query Optimization
// Use pull patterns to get only what you need
const results = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [:block/uuid :block/content :block/properties])
   :where
   [?b :block/properties ?props]]
`);

// Instead of pulling everything with [*]
Real-Time Collaboration Considerations
DB graphs are designed for real-time collaboration:

// Subscribe to changes to keep UI in sync
logseq.DB.onChanged(({ blocks, txMeta }) => {
  // Check if change was from another user
  if (txMeta?.source !== 'plugin') {
    // Update plugin UI/state
    refreshUI();
  }
});

// Use optimistic updates but be ready to handle conflicts
async function optimisticUpdate(blockUuid, newValue) {
  // Update UI immediately
  updateLocalState(blockUuid, newValue);

  try {
    // Perform actual update
    await logseq.Editor.upsertBlockProperty(blockUuid, 'value', newValue);
  } catch (error) {
    // Revert on conflict
    revertLocalState(blockUuid);
    console.error('Update failed:', error);
  }
}
Best Practices
1. Always Check Graph Type
async function initPlugin() {
  const isDb = await logseq.App.checkCurrentIsDbGraph();

  if (!isDb) {
    logseq.UI.showMsg(
      'This plugin requires a DB graph',
      'warning'
    );
    return;
  }

  // Continue with DB-specific initialization
}
2. Define Properties Early
async function ensureProperties() {
  const requiredProperties = [
    { name: 'priority', schema: { type: 'number' } },
    { name: 'status', schema: { type: 'default' } },
    { name: 'tags', schema: { type: 'default', cardinality: 'many' } }
  ];

  for (const { name, schema } of requiredProperties) {
    try {
      await logseq.Editor.upsertProperty(name, schema);
    } catch (error) {
      console.error(`Failed to create property ${name}:`, error);
    }
  }
}

logseq.ready(ensureProperties);
3. Use Type-Safe Property Values
// Good: Use correct types
await logseq.Editor.upsertBlockProperty(uuid, 'priority', 5);
await logseq.Editor.upsertBlockProperty(uuid, 'completed', true);
await logseq.Editor.upsertBlockProperty(uuid, 'due-date', new Date());

// Avoid: Relying on coercion
await logseq.Editor.upsertBlockProperty(uuid, 'priority', '5');  // Works but not ideal
4. Handle Property Errors Gracefully
async function setPropertySafely(blockUuid, key, value) {
  try {
    // First ensure property exists
    const property = await logseq.Editor.getProperty(key);
    if (!property) {
      console.warn(`Property ${key} doesn't exist, creating...`);
      await logseq.Editor.upsertProperty(key, { type: 'default' });
    }

    // Then set value
    await logseq.Editor.upsertBlockProperty(blockUuid, key, value);
    return true;
  } catch (error) {
    console.error(`Failed to set ${key}:`, error);
    logseq.UI.showMsg(`Failed to set ${key}: ${error.message}`, 'error');
    return false;
  }
}
5. Clean Up Resources
let dbSubscription = null;

async function setupPlugin() {
  // Subscribe to changes
  dbSubscription = logseq.DB.onChanged(handleDatabaseChange);
}

// Clean up when plugin unloads
logseq.beforeunload(() => {
  if (dbSubscription) {
    dbSubscription();  // Unsubscribe
    dbSubscription = null;
  }
});
6. Use Queries Efficiently
// Good: Specific pull pattern
const results = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [:block/uuid :block/content :block/properties])
   :where
   [?b :block/properties ?props]
   [(get ?props :priority) ?p]
   [(= ?p 5)]]
`);

// Avoid: Pulling all data
const allBlocks = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :where
   [?b :block/uuid]]  // Too broad!
`);
7. Namespace Your Properties
// Property names are auto-namespaced, but be descriptive
await logseq.Editor.upsertProperty('task-priority', { type: 'number' });
await logseq.Editor.upsertProperty('task-status', { type: 'default' });

// Better than generic names that might conflict semantically
// 'priority', 'status'
8. Document Your Property Schema
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
9. Validate User Input
async function setPriority(blockUuid, priority) {
  // Validate before setting
  if (typeof priority !== 'number' || priority < 1 || priority > 5) {
    logseq.UI.showMsg('Priority must be a number between 1 and 5', 'warning');
    return false;
  }

  return await setPropertySafely(blockUuid, 'priority', priority);
}
10. Provide User Feedback
async function processBlock(blockUuid) {
  logseq.UI.showMsg('Processing block...', 'info', { timeout: 0, key: 'process' });

  try {
    await performComplexOperation(blockUuid);
    logseq.UI.closeMsg('process');
    logseq.UI.showMsg('Block processed successfully!', 'success');
  } catch (error) {
    logseq.UI.closeMsg('process');
    logseq.UI.showMsg(`Error: ${error.message}`, 'error');
  }
}
Migration Guide
From File-Based Plugins to DB Plugins
1. Conditional Code Paths
async function getValue(block, key) {
  const isDb = await logseq.App.checkCurrentIsDbGraph();

  if (isDb) {
    // DB graph: use property API
    return await logseq.Editor.getBlockProperty(block.uuid, key);
  } else {
    // File-based: parse from content
    const match = block.content.match(new RegExp(`${key}::\\s*(.+)`));
    return match ? match[1] : null;
  }
}

async function setValue(block, key, value) {
  const isDb = await logseq.App.checkCurrentIsDbGraph();

  if (isDb) {
    return await logseq.Editor.upsertBlockProperty(block.uuid, key, value);
  } else {
    // File-based: update content
    const content = block.content.replace(
      new RegExp(`${key}::\\s*.+`),
      `${key}:: ${value}`
    );
    return await logseq.Editor.updateBlock(block.uuid, content);
  }
}
2. Query Migration
// File-based query (limited)
const fileResults = await logseq.DB.q('(property priority)');

// DB graph equivalent (more powerful)
const dbResults = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :where
   [?b :block/properties ?props]
   [(get ?props :priority) ?p]]
`);
3. Feature Detection
const features = {
  hasDbSupport: false,
  hasPropertyApi: false,
  hasClassSupport: false
};

async function detectFeatures() {
  try {
    features.hasDbSupport = await logseq.App.checkCurrentIsDbGraph();

    if (features.hasDbSupport) {
      // Try DB-specific APIs
      features.hasPropertyApi = typeof logseq.Editor.upsertProperty === 'function';
      features.hasClassSupport = typeof logseq.Editor.getTagObjects === 'function';
    }
  } catch (error) {
    console.warn('Feature detection failed:', error);
  }

  return features;
}
Supporting Both Graph Types
class PropertyAdapter {
  async getValue(block, key) {
    if (await this.isDbGraph()) {
      return await logseq.Editor.getBlockProperty(block.uuid, key);
    }
    return this.parseInlineProperty(block.content, key);
  }

  async setValue(block, key, value) {
    if (await this.isDbGraph()) {
      await logseq.Editor.upsertBlockProperty(block.uuid, key, value);
    } else {
      await this.setInlineProperty(block, key, value);
    }
  }

  async isDbGraph() {
    if (this._isDb === undefined) {
      this._isDb = await logseq.App.checkCurrentIsDbGraph();
    }
    return this._isDb;
  }

  parseInlineProperty(content, key) {
    const match = content.match(new RegExp(`${key}::\\s*(.+)`));
    return match ? match[1].trim() : null;
  }

  async setInlineProperty(block, key, value) {
    let content = block.content;
    const regex = new RegExp(`${key}::\\s*.+`);

    if (regex.test(content)) {
      content = content.replace(regex, `${key}:: ${value}`);
    } else {
      content += `\n${key}:: ${value}`;
    }

    await logseq.Editor.updateBlock(block.uuid, content);
  }
}

const properties = new PropertyAdapter();
API Reference
Property Management
upsertProperty(key, schema?, opts?)
Create or update a property definition.

Parameters:

key (string) - Property name (will be namespaced)
schema (object, optional) - Property schema
type (string) - One of: 'default', 'number', 'date', 'datetime', 'checkbox', 'url', 'node', 'json', 'string'
cardinality (string) - 'one' or 'many'
hide (boolean) - Hide from UI
public (boolean) - Visibility in published graphs
opts (object, optional)
name (string) - Display name
Returns: Promise

Example:

await logseq.Editor.upsertProperty('priority', {
  type: 'number',
  cardinality: 'one'
}, {
  name: 'Task Priority'
});
getProperty(key)
Get a property definition.

Parameters:

key (string) - Property name
Returns: Promise<Entity | null>

removeProperty(key)
Remove a property definition (plugin properties only).

Parameters:

key (string) - Property name
Returns: Promise

getAllProperties()
Get all properties in the graph.

Returns: Promise<Array>

Block Properties
upsertBlockProperty(block, key, value)
Set a property value on a block.

Parameters:

block (BlockIdentity) - Block UUID or entity
key (string) - Property name
value (any) - Value (type-checked against schema)
Returns: Promise

getBlockProperty(block, key)
Get a specific property value from a block.

Parameters:

block (BlockIdentity) - Block UUID or entity
key (string) - Property name
Returns: Promise

getBlockProperties(block)
Get all properties of a block.

Parameters:

block (BlockIdentity) - Block UUID or entity
Returns: Promise
removeBlockProperty(block, key)
Remove a property from a block.

Parameters:

block (BlockIdentity) - Block UUID or entity
key (string) - Property name
Returns: Promise

Page Properties
getPageProperties(page)
Get all properties of a page.

Parameters:

page (PageIdentity) - Page name, UUID, or entity
Returns: Promise
Tags/Classes
getAllTags()
Get all tags/classes in the graph.

Returns: Promise<Array>

getTagObjects(identifier)
Get all blocks/pages that belong to a class.

Parameters:

identifier (string) - Class UUID, db/ident, or title
Returns: Promise<Array>

Database Queries
DB.q(dsl)
Run a DSL query.

Parameters:

dsl (string) - Query string
Returns: Promise<Array>

DB.datascriptQuery(query, ...inputs)
Run a Datalog query.

Parameters:

query (string) - Datalog query
inputs (any[]) - Query inputs
Returns: Promise<Array>

DB.onChanged(callback)
Subscribe to all database changes.

Parameters:

callback (function) - Called with { blocks, txData, txMeta }
Returns: Function (unsubscribe)

DB.onBlockChanged(uuid, callback)
Subscribe to specific block changes.

Parameters:

uuid (string) - Block UUID
callback (function) - Called with (block, txData, txMeta)
Returns: Function (unsubscribe)

Graph Type Detection
App.checkCurrentIsDbGraph()
Check if current graph is DB-based.

Returns: Promise

Examples
Example 1: Task Management Plugin
// Initialize properties
async function initTaskManager() {
  const isDb = await logseq.App.checkCurrentIsDbGraph();
  if (!isDb) {
    logseq.UI.showMsg('Task manager requires DB graph', 'warning');
    return;
  }

  // Define properties
  await logseq.Editor.upsertProperty('task-priority', {
    type: 'number',
    cardinality: 'one'
  });

  await logseq.Editor.upsertProperty('task-status', {
    type: 'default',
    cardinality: 'one'
  });

  await logseq.Editor.upsertProperty('task-due', {
    type: 'date',
    cardinality: 'one'
  });

  // Register command
  logseq.Editor.registerSlashCommand('Create Task', async () => {
    const block = await logseq.Editor.getCurrentBlock();
    if (!block) return;

    await logseq.Editor.upsertBlockProperty(block.uuid, 'task-status', 'pending');
    await logseq.Editor.upsertBlockProperty(block.uuid, 'task-priority', 3);

    logseq.UI.showMsg('Task created!', 'success');
  });
}

logseq.ready(initTaskManager);
Example 2: Property Dashboard
async function showPropertyDashboard() {
  // Query all blocks with priorities
  const blocks = await logseq.DB.datascriptQuery(`
    [:find (pull ?b [:block/uuid :block/content :block/properties])
     :where
     [?b :block/properties ?props]
     [(get ?props :task-priority) ?p]]
  `);

  // Group by priority
  const byPriority = blocks.reduce((acc, [block]) => {
    const priority = block.properties['task-priority'] || 0;
    if (!acc[priority]) acc[priority] = [];
    acc[priority].push(block);
    return acc;
  }, {});

  // Display
  let html = '<div><h2>Tasks by Priority</h2>';
  for (let i = 5; i >= 1; i--) {
    const tasks = byPriority[i] || [];
    html += `<h3>Priority ${i} (${tasks.length} tasks)</h3><ul>`;
    tasks.forEach(task => {
      html += `<li>${task.content.substring(0, 50)}...</li>`;
    });
    html += '</ul>';
  }
  html += '</div>';

  logseq.provideUI({
    key: 'task-dashboard',
    slot: 'main',
    template: html,
    reset: true
  });

  logseq.showMainUI();
}
Example 3: Property Sync
// Keep properties in sync with external system
async function syncProperties() {
  const subscription = logseq.DB.onChanged(async ({ blocks }) => {
    for (const block of blocks) {
      const props = await logseq.Editor.getBlockProperties(block.uuid);

      if (props && props['sync-enabled']) {
        // Sync to external system
        await syncToExternalSystem(block.uuid, props);
      }
    }
  });

  // Clean up
  logseq.beforeunload(() => {
    subscription();
  });
}
Example 4: ClojureScript Task Plugin
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
Example 5: Advanced Query Builder
class QueryBuilder {
  constructor() {
    this.conditions = [];
  }

  property(key, value) {
    if (value !== undefined) {
      this.conditions.push(`[(get ?props :${key}) ?${key}]`);
      this.conditions.push(`[(= ?${key} ${JSON.stringify(value)})]`);
    } else {
      this.conditions.push(`[(get ?props :${key})]`);
    }
    return this;
  }

  propertyRange(key, min, max) {
    this.conditions.push(`[(get ?props :${key}) ?${key}]`);
    if (min !== undefined) {
      this.conditions.push(`[(>= ?${key} ${min})]`);
    }
    if (max !== undefined) {
      this.conditions.push(`[(<= ?${key} ${max})]`);
    }
    return this;
  }

  tag(tagName) {
    this.conditions.push(`[?b :block/tags ?t]`);
    this.conditions.push(`[?t :block/original-name "${tagName}"]`);
    return this;
  }

  async execute() {
    const query = `
      [:find (pull ?b [*])
       :where
       [?b :block/properties ?props]
       ${this.conditions.join('\n       ')}]
    `;

    return await logseq.DB.datascriptQuery(query);
  }
}

// Usage
const results = await new QueryBuilder()
  .property('task-status', 'active')
  .propertyRange('task-priority', 3, 5)
  .tag('Project')
  .execute();
Example 6: Property Migration Tool
async function migrateFilePropertiesToDb() {
  const allBlocks = await logseq.Editor.getAllPages();

  let migrated = 0;
  let failed = 0;

  for (const page of allBlocks) {
    const blocks = await logseq.Editor.getPageBlocksTree(page.uuid);

    for (const block of blocks) {
      try {
        await migrateBlockProperties(block);
        migrated++;
      } catch (error) {
        console.error(`Failed to migrate block ${block.uuid}:`, error);
        failed++;
      }
    }
  }

  logseq.UI.showMsg(
    `Migration complete: ${migrated} blocks migrated, ${failed} failed`,
    'success'
  );
}

async function migrateBlockProperties(block) {
  const propertyRegex = /(\w+)::\s*(.+)/g;
  let match;
  const properties = {};

  while ((match = propertyRegex.exec(block.content)) !== null) {
    const [, key, value] = match;
    properties[key] = value;

    // Ensure property exists
    try {
      await logseq.Editor.upsertProperty(key, { type: 'default' });
    } catch (e) {
      // Property exists
    }
  }

  // Set all properties
  if (Object.keys(properties).length > 0) {
    for (const [key, value] of Object.entries(properties)) {
      await logseq.Editor.upsertBlockProperty(block.uuid, key, value);
    }

    // Clean content
    const cleanContent = block.content.replace(propertyRegex, '').trim();
    if (cleanContent !== block.content) {
      await logseq.Editor.updateBlock(block.uuid, cleanContent);
    }
  }
}
Troubleshooting
Common Issues
1. "Property doesn't exist" Error
Problem: Trying to set a property that hasn't been defined.

Solution:

// Always ensure property exists first
try {
  const prop = await logseq.Editor.getProperty('my-property');
  if (!prop) {
    await logseq.Editor.upsertProperty('my-property', { type: 'default' });
  }
  await logseq.Editor.upsertBlockProperty(uuid, 'my-property', 'value');
} catch (error) {
  console.error('Property error:', error);
}
2. Type Mismatch Errors
Problem: Setting a value that doesn't match the property type.

Solution:

// Check property type before setting
const prop = await logseq.Editor.getProperty('priority');
const propType = prop?.type || 'default';

let value = userInput;
if (propType === 'number') {
  value = Number(value);
} else if (propType === 'checkbox') {
  value = Boolean(value);
}

await logseq.Editor.upsertBlockProperty(uuid, 'priority', value);
3. Query Returns No Results
Problem: Datascript query doesn't return expected blocks.

Solution:

// Debug query step by step
const debug = await logseq.DB.datascriptQuery(`
  [:find ?b ?props
   :where
   [?b :block/properties ?props]]
`);
console.log('All blocks with properties:', debug);

// Check property names (case-sensitive!)
const props = await logseq.Editor.getBlockProperties(blockUuid);
console.log('Property keys:', Object.keys(props));
4. Plugin Properties Not Visible
Problem: Properties created by plugin don't appear in UI.

Solution:

// Check if property was created with hide: true
const prop = await logseq.Editor.getProperty('my-property');
console.log('Property hidden?', prop?.hide);

// Recreate without hide flag
await logseq.Editor.upsertProperty('my-property', {
  type: 'default',
  hide: false
});
Debug Helpers
// Log all properties
async function debugProperties() {
  const props = await logseq.Editor.getAllProperties();
  console.table(props.map(p => ({
    name: p.originalName,
    type: p.type,
    cardinality: p.cardinality,
    hidden: p.hide
  })));
}

// Log block properties
async function debugBlock(uuid) {
  const block = await logseq.Editor.getBlock(uuid);
  const props = await logseq.Editor.getBlockProperties(uuid);
  console.log('Block:', block);
  console.log('Properties:', props);
}

// Test query
async function testQuery(query) {
  console.time('query');
  const results = await logseq.DB.datascriptQuery(query);
  console.timeEnd('query');
  console.log('Results:', results);
  return results;
}
Resources
Documentation
Logseq Plugin API Documentation
Datascript Query Reference
Logseq Discord Community
Example Plugins
Plugin Samples Repository
ClojureScript Playground
Related Guides
ClojureScript Plugin SDK Guide
ClojureScript Plugin SDK Detective Guide
License
This guide is part of the Logseq project documentation. MIT License - see the Logseq repository for details.

Contributing: Found an error or want to add examples? Please submit issues or PRs to the Logseq repository.

Questions: Join the Logseq Discord #plugin-dev channel.