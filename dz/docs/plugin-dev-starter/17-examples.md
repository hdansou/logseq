# Plugin Examples

Real-world examples to help you build Logseq plugins quickly.

## Table of Contents

- [Simple Examples](#simple-examples)
- [Content Manipulation](#content-manipulation)
- [UI Integration](#ui-integration)
- [Data Processing](#data-processing)
- [Complete Plugins](#complete-plugins)

## Simple Examples

### Hello World

```typescript
import '@logseq/libs'

const main = async () => {
  logseq.UI.showMsg('Hello, Logseq!', 'success')
}

logseq.ready(main).catch(console.error)
```

### Insert Text Command

```typescript
logseq.Editor.registerSlashCommand('Insert Date', async () => {
  const date = new Date().toISOString().split('T')[0]
  await logseq.Editor.insertAtEditingCursor(`[[${date}]]`)
})
```

### Keyboard Shortcut

```typescript
logseq.App.registerCommandPalette(
  {
    key: 'quick-note',
    label: 'Quick Note',
    keybinding: { binding: 'mod+shift+n' },
  },
  async () => {
    await logseq.Editor.insertAtEditingCursor('📝 Note: ')
  }
)
```

## Content Manipulation

### Auto-Format Code Blocks

````typescript
logseq.Editor.registerSlashCommand('Format Code', async () => {
  const block = await logseq.Editor.getCurrentBlock()
  if (!block) return

  // Check if it's a code block
  if (block.content.includes('```')) {
    // Format code (simple example)
    const formatted = block.content
      .split('\n')
      .map((line) => line.trim())
      .join('\n')

    await logseq.Editor.updateBlock(block.uuid, formatted)
    logseq.UI.showMsg('Code formatted!', 'success')
  }
})
````

### TODO Counter

```typescript
async function countTodos() {
  const todos = await logseq.DB.datascriptQuery(`
    [:find (pull ?b [*])
     :where
     [?b :block/content ?content]
     [(re-find #"TODO" ?content)]]
  `)

  const done = await logseq.DB.datascriptQuery(`
    [:find (pull ?b [*])
     :where
     [?b :block/content ?content]
     [(re-find #"DONE" ?content)]]
  `)

  logseq.UI.showMsg(`${todos.length} TODOs, ${done.length} DONE`, 'info')
}

logseq.App.registerCommandPalette(
  {
    key: 'count-todos',
    label: 'Count TODOs',
  },
  countTodos
)
```

### Block Duplicator

```typescript
logseq.Editor.registerSlashCommand('Duplicate Block', async () => {
  const block = await logseq.Editor.getCurrentBlock()
  if (!block) return

  // Create duplicate as sibling
  await logseq.Editor.insertBlock(block.uuid, block.content, {
    sibling: true,
    before: false,
  })

  logseq.UI.showMsg('Block duplicated', 'success')
})
```

### Text Transformer

```typescript
const transformers = {
  uppercase: (text: string) => text.toUpperCase(),
  lowercase: (text: string) => text.toLowerCase(),
  title: (text: string) => text.replace(/\b\w/g, (c) => c.toUpperCase()),
  reverse: (text: string) => text.split('').reverse().join(''),
}

for (const [name, fn] of Object.entries(transformers)) {
  logseq.Editor.registerSlashCommand(`Transform: ${name}`, async () => {
    const block = await logseq.Editor.getCurrentBlock()
    if (!block) return

    const transformed = fn(block.content)
    await logseq.Editor.updateBlock(block.uuid, transformed)
  })
}
```

## UI Integration

### Toolbar Button

```typescript
const main = async () => {
  // Register UI model
  logseq.provideModel({
    async handleClick() {
      logseq.UI.showMsg('Button clicked!', 'info')
    },
  })

  // Add toolbar button
  logseq.App.registerUIItem('toolbar', {
    key: 'my-plugin-button',
    template: `
      <a data-on-click="handleClick" 
         class="button"
         title="My Plugin">
        <i class="ti ti-star"></i>
      </a>
    `,
  })
}

logseq.ready(main).catch(console.error)
```

### Settings Panel

```typescript
logseq.useSettingsSchema([
  {
    key: 'apiKey',
    type: 'string',
    default: '',
    title: 'API Key',
    description: 'Enter your API key',
  },
  {
    key: 'enableNotifications',
    type: 'boolean',
    default: true,
    title: 'Enable Notifications',
  },
  {
    key: 'theme',
    type: 'enum',
    default: 'light',
    enumChoices: ['light', 'dark', 'auto'],
    title: 'Theme',
    description: 'Choose your theme preference',
  },
])

// Access settings
const apiKey = logseq.settings?.apiKey
if (apiKey) {
  // Use API key
}
```

### Progress Indicator

```typescript
async function processBlocks() {
  const blocks = await getAllBlocks()
  const total = blocks.length

  for (let i = 0; i < blocks.length; i++) {
    await processBlock(blocks[i])

    // Show progress
    logseq.UI.showMsg(`Processing: ${i + 1}/${total}`, 'info')
  }

  logseq.UI.showMsg('Complete!', 'success')
}
```

## Data Processing

### Export to JSON

```typescript
async function exportPageToJSON() {
  const page = await logseq.Editor.getCurrentPage()
  if (!page) return

  const blocks = await logseq.Editor.getPageBlocksTree(page.name)

  const json = JSON.stringify(
    {
      page: page.name,
      blocks: blocks.map((b) => ({
        content: b.content,
        properties: b.properties,
      })),
    },
    null,
    2
  )

  // Copy to clipboard or save
  navigator.clipboard.writeText(json)
  logseq.UI.showMsg('Exported to clipboard', 'success')
}

logseq.App.registerCommandPalette(
  {
    key: 'export-json',
    label: 'Export Page to JSON',
  },
  exportPageToJSON
)
```

### Word Counter

```typescript
async function countWords() {
  const page = await logseq.Editor.getCurrentPage()
  if (!page) return

  const blocks = await logseq.Editor.getPageBlocksTree(page.name)

  let totalWords = 0
  let totalBlocks = 0

  function countInBlock(block: any) {
    totalBlocks++
    const words = block.content
      .replace(/\[\[.*?\]\]/g, '') // Remove links
      .replace(/[#*`]/g, '') // Remove markdown
      .trim()
      .split(/\s+/)
      .filter((w) => w.length > 0)

    totalWords += words.length

    if (block.children) {
      block.children.forEach(countInBlock)
    }
  }

  blocks.forEach(countInBlock)

  logseq.UI.showMsg(`${totalWords} words in ${totalBlocks} blocks`, 'info')
}

logseq.App.registerCommandPalette(
  {
    key: 'count-words',
    label: 'Count Words',
  },
  countWords
)
```

### Tag Extractor

```typescript
async function extractAllTags() {
  const blocks = await logseq.DB.datascriptQuery(`
    [:find ?tag
     :where
     [?b :block/refs ?r]
     [?r :block/name ?tag]]
  `)

  const uniqueTags = [...new Set(blocks.flat())].sort()

  // Insert as list
  const tagList = uniqueTags.map((tag) => `- [[${tag}]]`).join('\n')

  await logseq.Editor.insertAtEditingCursor(`## All Tags\n${tagList}`)
}

logseq.Editor.registerSlashCommand('List All Tags', extractAllTags)
```

### Find Broken Links

```typescript
async function findBrokenLinks() {
  const allPages = await logseq.Editor.getAllPages()
  const pageNames = new Set(allPages?.map((p) => p.name))

  const blocks = await logseq.DB.datascriptQuery(`
    [:find (pull ?b [*])
     :where
     [?b :block/content ?content]]
  `)

  const brokenLinks: string[] = []

  for (const [block] of blocks) {
    const links = block.content.match(/\[\[(.*?)\]\]/g)
    if (links) {
      links.forEach((link: string) => {
        const pageName = link.slice(2, -2)
        if (!pageNames.has(pageName)) {
          brokenLinks.push(`${pageName} in block ${block.uuid}`)
        }
      })
    }
  }

  if (brokenLinks.length > 0) {
    console.log('Broken links:', brokenLinks)
    logseq.UI.showMsg(`Found ${brokenLinks.length} broken links`, 'warning')
  } else {
    logseq.UI.showMsg('No broken links!', 'success')
  }
}
```

## Complete Plugins

### Template Manager Plugin

```typescript
import '@logseq/libs'

interface Template {
  name: string
  content: string
  description?: string
}

class TemplateManager {
  private templates: Template[] = []

  async load() {
    // Load templates from settings or storage
    const saved = await this.getStoredTemplates()
    this.templates = saved || this.getDefaultTemplates()

    // Register commands
    this.registerCommands()
  }

  getDefaultTemplates(): Template[] {
    return [
      {
        name: 'Meeting Notes',
        description: 'Template for meeting notes',
        content: `## 📅 Meeting Notes
Date:: [[${new Date().toISOString().split('T')[0]}]]

## Attendees
- 

## Agenda
- 

## Notes
- 

## Action Items
- `,
      },
      {
        name: 'Daily Review',
        content: `## ✅ Completed Today
- 

## 🎯 Top Priority Tomorrow
- 

## 💭 Reflections
- `,
      },
    ]
  }

  registerCommands() {
    this.templates.forEach((template) => {
      logseq.Editor.registerSlashCommand(
        `Template: ${template.name}`,
        async () => {
          await logseq.Editor.insertAtEditingCursor(template.content)
          logseq.UI.showMsg(`Inserted: ${template.name}`, 'success')
        }
      )
    })

    // List all templates
    logseq.App.registerCommandPalette(
      {
        key: 'list-templates',
        label: 'List Templates',
      },
      () => {
        const list = this.templates
          .map((t) => `- ${t.name}${t.description ? `: ${t.description}` : ''}`)
          .join('\n')

        logseq.UI.showMsg(`Available:\n${list}`, 'info')
      }
    )
  }

  async getStoredTemplates() {
    // Implementation depends on storage method
    return null
  }
}

const main = async () => {
  const manager = new TemplateManager()
  await manager.load()
  console.log('Template Manager loaded')
}

logseq.ready(main).catch(console.error)
```

### Quick Capture Plugin

```typescript
import '@logseq/libs'

class QuickCapture {
  private inboxPage = 'Inbox'

  async initialize() {
    await this.ensureInboxExists()
    this.registerCommands()
  }

  async ensureInboxExists() {
    const page = await logseq.Editor.getPage(this.inboxPage)
    if (!page) {
      await logseq.Editor.createPage(this.inboxPage)
    }
  }

  registerCommands() {
    // Quick capture
    logseq.App.registerCommandPalette(
      {
        key: 'quick-capture',
        label: 'Quick Capture',
        keybinding: { binding: 'mod+shift+c' },
      },
      async () => {
        await this.captureNote()
      }
    )

    // Capture with TODO
    logseq.Editor.registerSlashCommand('Capture TODO', async () => {
      await this.captureTodo()
    })
  }

  async captureNote() {
    const page = await logseq.Editor.getPage(this.inboxPage)
    if (!page) return

    const timestamp = new Date().toLocaleString()
    const block = await logseq.Editor.insertBlock(
      page.uuid,
      `📥 Captured at ${timestamp}\n`,
      { before: false }
    )

    if (block) {
      await logseq.Editor.editBlock(block.uuid)
    }
  }

  async captureTodo() {
    const page = await logseq.Editor.getPage(this.inboxPage)
    if (!page) return

    const block = await logseq.Editor.insertBlock(page.uuid, 'TODO ', {
      before: false,
    })

    if (block) {
      await logseq.Editor.editBlock(block.uuid)
    }
  }
}

const main = async () => {
  const plugin = new QuickCapture()
  await plugin.initialize()

  logseq.UI.showMsg('Quick Capture ready!', 'success')
}

logseq.ready(main).catch(console.error)
```

### Statistics Dashboard Plugin

```typescript
import '@logseq/libs'

class StatsDashboard {
  async show() {
    const stats = await this.calculateStats()
    await this.displayStats(stats)
  }

  async calculateStats() {
    const [totalBlocks] = await logseq.DB.datascriptQuery(`
      [:find (count ?b)
       :where [?b :block/content]]
    `)

    const [totalPages] = await logseq.DB.datascriptQuery(`
      [:find (count ?p)
       :where [?p :block/name]]
    `)

    const todos = await logseq.DB.datascriptQuery(`
      [:find (pull ?b [*])
       :where
       [?b :block/content ?content]
       [(re-find #"TODO" ?content)]]
    `)

    const dones = await logseq.DB.datascriptQuery(`
      [:find (pull ?b [*])
       :where
       [?b :block/content ?content]
       [(re-find #"DONE" ?content)]]
    `)

    return {
      totalBlocks: totalBlocks[0],
      totalPages: totalPages[0],
      todos: todos.length,
      dones: dones.length,
    }
  }

  async displayStats(stats: any) {
    const statsText = `
## 📊 Graph Statistics

**Content**
- Total Blocks: ${stats.totalBlocks}
- Total Pages: ${stats.totalPages}

**Tasks**
- TODO: ${stats.todos}
- DONE: ${stats.dones}
- Completion Rate: ${(
      (stats.dones / (stats.todos + stats.dones)) *
      100
    ).toFixed(1)}%

Generated: ${new Date().toLocaleString()}
`

    await logseq.Editor.insertAtEditingCursor(statsText)
  }
}

const main = async () => {
  logseq.Editor.registerSlashCommand('Insert Statistics', async () => {
    const dashboard = new StatsDashboard()
    await dashboard.show()
  })
}

logseq.ready(main).catch(console.error)
```

## Tips for Learning

1. **Start Small** - Begin with simple commands
2. **Experiment** - Try modifying examples
3. **Read Plugin Source** - Study popular plugins
4. **Check Console** - Use `console.log()` liberally
5. **Test Incrementally** - Test each feature as you add it

## Resources

- [Official Plugin Samples](https://github.com/logseq/logseq-plugin-samples)
- [Community Plugins](https://github.com/logseq/marketplace)
- [API Documentation](../index.html)

## Next Steps

- 🔍 Learn [Database Queries](10-database.md)
- ⚙️ Add [Settings](11-settings.md)
- 📖 Study [Common Patterns](12-common-patterns.md)
- 🚀 Publish your plugin!

---

Happy coding! Build something awesome! 🎉
