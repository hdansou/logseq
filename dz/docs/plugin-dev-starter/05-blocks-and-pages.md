# Working with Blocks and Pages

Complete guide to manipulating blocks and pages in Logseq.

## Table of Contents

- [Block Basics](#block-basics)
- [Creating Blocks](#creating-blocks)
- [Reading Blocks](#reading-blocks)
- [Updating Blocks](#updating-blocks)
- [Deleting Blocks](#deleting-blocks)
- [Page Operations](#page-operations)
- [Block Properties](#block-properties)
- [Advanced Patterns](#advanced-patterns)

## Block Basics

### What is a Block?

A block is the fundamental unit of content in Logseq. Each block:

- Has a unique UUID
- Contains text content
- Can have child blocks
- Can have properties
- Belongs to a page

### Block Structure

```typescript
interface BlockEntity {
  uuid: string // Unique identifier
  content: string // Block text
  parent: { id: number } // Parent block/page
  page: { id: number } // Page it belongs to
  left: { id: number } // Previous sibling
  format: 'markdown' | 'org'
  properties?: Record<string, any>
  children?: BlockEntity[]
}
```

## Creating Blocks

### Insert Block

Basic block insertion:

```typescript
// Insert as child of parent
const block = await logseq.Editor.insertBlock(
  parentUuid, // Parent block or page UUID
  'Block content',
  {
    before: false, // Insert before (true) or after (false)
    sibling: false, // Insert as sibling (true) or child (false)
    isPageBlock: false,
  }
)

console.log('Created block:', block.uuid)
```

### Insert at Cursor

Insert content where user is typing:

```typescript
await logseq.Editor.insertAtEditingCursor('Text to insert')

// Or with properties
await logseq.Editor.insertAtEditingCursor(`
todo:: TODO
tags:: [[important]]
Due date: [[2025-01-15]]
`)
```

### Insert Multiple Blocks

Create a hierarchy:

```typescript
// Create parent block
const parent = await logseq.Editor.insertBlock(pageUuid, 'Parent block')

// Create children
const child1 = await logseq.Editor.insertBlock(
  parent.uuid,
  'Child 1',
  { sibling: false } // Make it a child
)

const child2 = await logseq.Editor.insertBlock(
  child1.uuid,
  'Child 2',
  { sibling: true } // Make it a sibling of child1
)
```

### Batch Block Creation

```typescript
async function createBlockTree(parentUuid: string, tree: any[]) {
  for (const item of tree) {
    const block = await logseq.Editor.insertBlock(parentUuid, item.content)

    if (item.children) {
      await createBlockTree(block.uuid, item.children)
    }
  }
}

// Usage
await createBlockTree(pageUuid, [
  {
    content: '## Project Goals',
    children: [{ content: 'Goal 1' }, { content: 'Goal 2' }],
  },
])
```

## Reading Blocks

### Get Block by UUID

```typescript
const block = await logseq.Editor.getBlock(uuid)

if (block) {
  console.log('Content:', block.content)
  console.log('Page:', block.page)
  console.log('Properties:', block.properties)
}
```

### Get Block with Children

```typescript
const block = await logseq.Editor.getBlock(uuid, {
  includeChildren: true,
})

if (block?.children) {
  block.children.forEach((child) => {
    console.log('Child:', child.content)
  })
}
```

### Get Current Block

```typescript
// Get block where cursor is
const current = await logseq.Editor.getCurrentBlock()

if (current) {
  console.log('Editing:', current.content)
}
```

### Get Selected Blocks

```typescript
const selected = await logseq.Editor.getSelectedBlocks()

selected?.forEach((block) => {
  console.log('Selected:', block.uuid)
})
```

### Get Page Blocks

```typescript
const page = await logseq.Editor.getCurrentPage()

if (page) {
  const pageBlocks = await logseq.Editor.getPageBlocksTree(page.name)
  console.log('Page has', pageBlocks.length, 'top-level blocks')
}
```

## Updating Blocks

### Update Content

```typescript
// Simple update
await logseq.Editor.updateBlock(uuid, 'New content')

// Preserve properties
const block = await logseq.Editor.getBlock(uuid)
if (block) {
  const updatedContent = block.content.replace('old', 'new')
  await logseq.Editor.updateBlock(uuid, updatedContent)
}
```

### Update with Properties

```typescript
// Update content with properties
await logseq.Editor.updateBlock(
  uuid,
  `
Updated content
todo:: DONE
completed:: [[${new Date().toISOString().split('T')[0]}]]
`
)
```

### Upsert Block Property

```typescript
// Add or update a single property
await logseq.Editor.upsertBlockProperty(uuid, 'status', 'in-progress')

// Multiple properties
await logseq.Editor.upsertBlockProperty(uuid, 'priority', 'high')
await logseq.Editor.upsertBlockProperty(uuid, 'assignee', 'John')
```

### Remove Block Property

```typescript
await logseq.Editor.removeBlockProperty(uuid, 'completed')
```

### Move Block

```typescript
// Move block to different parent
await logseq.Editor.moveBlock(blockUuid, targetUuid, {
  before: false,
  children: false,
})
```

## Deleting Blocks

### Remove Single Block

```typescript
await logseq.Editor.removeBlock(uuid)
```

### Remove with Confirmation

```typescript
const confirmed = confirm('Delete this block?')
if (confirmed) {
  await logseq.Editor.removeBlock(uuid)
  logseq.UI.showMsg('Block deleted', 'success')
}
```

### Remove Multiple Blocks

```typescript
const blocksToDelete = ['uuid1', 'uuid2', 'uuid3']

for (const uuid of blocksToDelete) {
  await logseq.Editor.removeBlock(uuid)
}
```

## Page Operations

### Create Page

```typescript
// Simple page creation
const page = await logseq.Editor.createPage('My New Page')

// With properties
const page = await logseq.Editor.createPage('Project Alpha', {
  status: 'active',
  'start-date': '2025-01-01',
  tags: ['project', 'important'],
})
```

### Get Page

```typescript
// By name
const page = await logseq.Editor.getPage('Page Name')

// By ID
const page = await logseq.Editor.getPage(pageId)

// Current page
const current = await logseq.Editor.getCurrentPage()
```

### Update Page

```typescript
// Rename page
await logseq.Editor.renamePage('Old Name', 'New Name')

// Update page properties
await logseq.Editor.upsertBlockProperty(
  page.uuid,
  'description',
  'Project description'
)
```

### Delete Page

```typescript
await logseq.Editor.deletePage('Page Name')
```

### Get All Pages

```typescript
const pages = await logseq.Editor.getAllPages()

pages?.forEach((page) => {
  console.log('Page:', page.name)
})
```

## Block Properties

### Property Types

```typescript
// String property
todo:: TODO

// Date property
due-date:: [[2025-01-15]]

// Page reference
project:: [[Project Alpha]]

// Multi-value
tags:: [[tag1]], [[tag2]], [[tag3]]

// Number
priority:: 1
```

### Set Properties Programmatically

```typescript
async function setBlockProperties(uuid: string, props: Record<string, any>) {
  for (const [key, value] of Object.entries(props)) {
    await logseq.Editor.upsertBlockProperty(uuid, key, value)
  }
}

// Usage
await setBlockProperties(blockUuid, {
  status: 'in-progress',
  priority: 'high',
  assignee: '[[John Doe]]',
  'due-date': '[[2025-02-01]]',
})
```

### Get Block Properties

```typescript
const block = await logseq.Editor.getBlock(uuid)

if (block?.properties) {
  const status = block.properties.status
  const priority = block.properties.priority
  console.log('Status:', status, 'Priority:', priority)
}
```

## Advanced Patterns

### Template Expansion

```typescript
async function expandTemplate(templateName: string) {
  const templates = {
    'meeting-notes': `
## Attendees
- 

## Agenda
- 

## Notes
- 

## Action Items
- 
`,
    'daily-review': `
## ✅ Completed
- 

## 🎯 Focus Today
- 

## 📝 Notes
- 
`,
  }

  const template = templates[templateName]
  if (template) {
    await logseq.Editor.insertAtEditingCursor(template)
  }
}

// Register slash command
logseq.Editor.registerSlashCommand('Meeting Notes', () => {
  expandTemplate('meeting-notes')
})
```

### Block Search and Replace

```typescript
async function searchAndReplace(search: string, replace: string) {
  // Query blocks with search term
  const blocks = await logseq.DB.datascriptQuery(`
    [:find (pull ?b [*])
     :where
     [?b :block/content ?content]
     [(re-find #"${search}" ?content)]]
  `)

  // Replace in each block
  for (const [block] of blocks) {
    const newContent = block.content.replace(new RegExp(search, 'g'), replace)
    await logseq.Editor.updateBlock(block.uuid, newContent)
  }

  logseq.UI.showMsg(`Replaced in ${blocks.length} blocks`, 'success')
}
```

### Block Filtering

```typescript
async function getTodoBlocks() {
  return await logseq.DB.datascriptQuery(`
    [:find (pull ?b [*])
     :where
     [?b :block/content ?content]
     [(re-find #"TODO" ?content)]]
  `)
}

async function getBlocksWithTag(tag: string) {
  return await logseq.DB.datascriptQuery(`
    [:find (pull ?b [*])
     :where
     [?b :block/refs ?r]
     [?r :block/name "${tag}"]]
  `)
}
```

### Block Tree Traversal

```typescript
async function traverseBlockTree(
  uuid: string,
  callback: (block: BlockEntity) => void
) {
  const block = await logseq.Editor.getBlock(uuid, {
    includeChildren: true,
  })

  if (!block) return

  callback(block)

  if (block.children) {
    for (const child of block.children) {
      await traverseBlockTree(child.uuid, callback)
    }
  }
}

// Usage: print all content in tree
await traverseBlockTree(rootUuid, (block) => {
  console.log(block.content)
})
```

### Bulk Operations

```typescript
async function bulkUpdateBlocks(
  filter: (block: BlockEntity) => boolean,
  update: (content: string) => string
) {
  const page = await logseq.Editor.getCurrentPage()
  if (!page) return

  const blocks = await logseq.Editor.getPageBlocksTree(page.name)

  for (const block of blocks) {
    if (filter(block)) {
      const newContent = update(block.content)
      await logseq.Editor.updateBlock(block.uuid, newContent)
    }
  }
}

// Usage: Add emoji to all TODO blocks
await bulkUpdateBlocks(
  (block) => block.content.includes('TODO'),
  (content) => '🔥 ' + content
)
```

## Best Practices

### 1. Always Check for Null

```typescript
const block = await logseq.Editor.getBlock(uuid)
if (!block) {
  logseq.UI.showMsg('Block not found', 'warning')
  return
}
// Use block safely
```

### 2. Handle Errors

```typescript
try {
  await logseq.Editor.updateBlock(uuid, newContent)
} catch (error) {
  console.error('Update failed:', error)
  logseq.UI.showMsg('Failed to update block', 'error')
}
```

### 3. Batch Operations Efficiently

```typescript
// Bad: Sequential
for (const uuid of uuids) {
  await logseq.Editor.updateBlock(uuid, content)
}

// Better: Concurrent with limit
const BATCH_SIZE = 10
for (let i = 0; i < uuids.length; i += BATCH_SIZE) {
  const batch = uuids.slice(i, i + BATCH_SIZE)
  await Promise.all(
    batch.map((uuid) => logseq.Editor.updateBlock(uuid, content))
  )
}
```

### 4. Preserve User Data

```typescript
// Good: Read first, modify, then update
const block = await logseq.Editor.getBlock(uuid)
if (block) {
  const modified = modifyContent(block.content)
  await logseq.Editor.updateBlock(uuid, modified)
}
```

## Next Steps

- ⌨️ Learn [Commands & Shortcuts](07-commands-shortcuts.md)
- 🎨 Build [UI Components](06-ui-components.md)
- 🔍 Master [Database Queries](10-database.md)
- 💡 See [Examples](17-examples.md)

---

You now know how to work with blocks and pages effectively!
