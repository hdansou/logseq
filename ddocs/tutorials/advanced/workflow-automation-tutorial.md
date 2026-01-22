---
title: 'Workflow Automation Tutorial'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'Tutorial'
platform: 'Desktop + Advanced'
related-docs:
  - '[Advanced Queries Tutorial](./advanced-queries-tutorial.md)'
  - '[Plugin Development Guide](../../developer/plugin-development.md)'
  - '[JavaScript API Reference](../../reference/api/javascript-api.md)'
tags: ['automation', 'workflows', 'plugins', 'scripting', 'productivity']
---

# Workflow Automation Tutorial

Automate repetitive tasks in Logseq using queries, plugins, and scripts. This hands-on tutorial teaches you to build custom automation workflows that save time and reduce manual work.

**What You'll Build:**

- Automated task processing
- Smart page templates
- Custom workflows with triggers
- Scheduled operations
- Batch processing scripts
- Cross-graph automation

**Prerequisites:**

- Comfortable with Logseq basics
- Basic JavaScript knowledge (for plugins)
- Understanding of Logseq queries
- Developer tools familiarity

**Time:** 3-4 hours

---

## Table of Contents

1. [Setup](#setup)
2. [Query-Based Automation](#query-based-automation)
3. [Template Automation](#template-automation)
4. [Plugin-Based Workflows](#plugin-based-workflows)
5. [Scheduled Tasks](#scheduled-tasks)
6. [Batch Operations](#batch-operations)
7. [Integration Workflows](#integration-workflows)
8. [Production Deployment](#production-deployment)

---

## Setup

### Development Environment

**Step 1: Enable Developer Mode**

```markdown
1. Settings → Advanced → Developer mode (enable)
2. Help → Developer tools (Ctrl+Shift+I / Cmd+Option+I)
3. Console tab will be our workspace
```

**Step 2: Create Automation Workspace**

```markdown
Create pages:

- [[Automation Hub]] - Central control
- [[Scripts]] - Reusable scripts
- [[Workflows]] - Workflow definitions
- [[Templates/Auto]] - Auto-templates
- [[Logs/Automation]] - Activity logs
```

**Step 3: Install Required Plugins**

```markdown
From Marketplace:

1. **logseq-plugin-developer-tools** - Development helpers
2. **logseq-plugin-template** - Template system
3. **logseq-plugin-task-runner** - Task automation
4. **logseq-plugin-cron** - Scheduled operations (if available)
```

**Step 4: Project Structure**

```
graph-root/
├── pages/
│   ├── automation_hub.md
│   ├── scripts.md
│   └── workflows.md
├── plugins/
│   └── custom-automation/    # Our plugin
│       ├── package.json
│       ├── index.js
│       └── workflows/
│           ├── task-processor.js
│           ├── template-engine.js
│           └── scheduler.js
└── templates/
    └── auto-templates.md
```

---

## Query-Based Automation

### Auto-Task Dashboard

**Goal:** Automatically collect and organize tasks.

**Step 1: Create Smart Queries**

```clojure
;; Overdue Tasks Auto-Collection
#+BEGIN_QUERY
{:title "⚠️ Overdue Tasks"
 :query [:find (pull ?b [*])
         :in $ ?today
         :where
         [?b :block/marker ?marker]
         [(contains? #{"TODO" "DOING"} ?marker)]
         [?b :block/scheduled ?d]
         [(< ?d ?today)]]
 :inputs [:today]
 :collapsed? false
 :result-transform (fn [result]
   (sort-by
     (fn [item]
       (get-in item [:block/scheduled]))
     result))
 :breadcrumb-show? false}
#+END_QUERY

;; Auto-Priority Tasks
#+BEGIN_QUERY
{:title "🔥 High Priority (Auto-Tagged)"
 :query [:find (pull ?b [*])
         :where
         [?b :block/marker ?marker]
         [(contains? #{"TODO" "DOING"} ?marker)]
         (or
           [?b :block/priority "A"]
           [?b :block/refs ?ref]
           [?ref :block/name "urgent"])]
 :collapsed? false}
#+END_QUERY
```

**Step 2: Auto-Categorization Query**

```clojure
;; Auto-Categorize Tasks by Context
#+BEGIN_QUERY
{:title "📋 Tasks by Context"
 :query [:find ?context (pull ?b [*])
         :where
         [?b :block/marker "TODO"]
         [?b :block/refs ?ref]
         [?ref :block/name ?context]
         [(contains? #{"work" "home" "errands" "calls"} ?context)]]
 :result-transform (fn [result]
   (let [grouped (group-by first result)]
     (map
       (fn [[ctx items]]
         {:context ctx
          :tasks (map second items)
          :count (count items)})
       grouped)))
 :view (fn [result]
   [:div
     (for [group result]
       [:div.context-group
         [:h3 (str (get group :context) " (" (get group :count) ")")]
         [:ul
           (for [task (get group :tasks)]
             [:li (get-in task [:block/content])])]])])}
#+END_QUERY
```

**Step 3: Auto-Update Mechanism**

Create a template that auto-refreshes:

```markdown
- template:: auto-dashboard
  template-including-parent:: false
  - ## 🤖 Automated Dashboard
  - Updated: {{currentTime}}
  - {{query (and (task TODO DOING) (not (property status done)))}}
  ***
  - ### Stats
    - Total Tasks: {{sum of tasks}}
    - Due Today: {{today tasks}}
    - Overdue: {{overdue count}}
```

### Auto-Tagging System

**Goal:** Automatically tag blocks based on content patterns.

**Step 1: Define Tagging Rules**

On [[Scripts]] page:

```javascript
// Auto-Tagging Rules
const taggingRules = [
  {
    pattern: /meeting|call|discussion/i,
    tags: ['#meeting'],
    properties: { type: 'sync' },
  },
  {
    pattern: /research|study|learn/i,
    tags: ['#learning'],
    properties: { type: 'async' },
  },
  {
    pattern: /bug|fix|error/i,
    tags: ['#bug'],
    properties: { priority: 'A' },
  },
  {
    pattern: /@\w+/,
    tags: ['#people'],
    properties: { type: 'social' },
  },
]
```

**Step 2: Implement Auto-Tagger**

```javascript
// Run this in Developer Console or as plugin
async function autoTagBlock(blockUuid) {
  const block = await logseq.Editor.getBlock(blockUuid)
  if (!block) return

  const content = block.content
  let modified = false
  let newContent = content
  let properties = {}

  for (const rule of taggingRules) {
    if (rule.pattern.test(content)) {
      // Add tags if not present
      for (const tag of rule.tags) {
        if (!content.includes(tag)) {
          newContent += ` ${tag}`
          modified = true
        }
      }

      // Add properties
      Object.assign(properties, rule.properties)
    }
  }

  if (modified) {
    // Update block
    await logseq.Editor.updateBlock(blockUuid, newContent)

    // Add properties
    for (const [key, value] of Object.entries(properties)) {
      await logseq.Editor.upsertBlockProperty(blockUuid, key, value)
    }

    console.log(`Auto-tagged: ${blockUuid}`)
  }
}

// Hook into block edit events
logseq.Editor.onInputSelectionEnd(async ({ uuid }) => {
  await autoTagBlock(uuid)
})
```

**Step 3: Batch Tag Existing Blocks**

```javascript
// Tag all untagged blocks
async function batchAutoTag(pageFilter = null) {
  const query = `
    [:find (pull ?b [:block/uuid :block/content])
     :where
     [?b :block/content ?content]
     ${
       pageFilter ? `[?b :block/page ?p] [?p :block/name "${pageFilter}"]` : ''
     }]
  `

  const blocks = await logseq.DB.datascriptQuery(query)

  console.log(`Processing ${blocks.length} blocks...`)

  for (const [block] of blocks) {
    await autoTagBlock(block.uuid)
  }

  console.log('Batch tagging complete!')
}

// Run on current page
await batchAutoTag(logseq.Editor.getCurrentPage().name)
```

---

## Template Automation

### Smart Template System

**Goal:** Templates that adapt based on context.

**Step 1: Create Dynamic Template**

```markdown
- template:: smart-meeting-notes
  - # {{date}} - {{input "Meeting Title"}}
  - type:: [[meeting]]
  - participants:: {{input-multiline "Participants (comma separated)"}}
  - project:: [[{{select "Project" [[Project A]], [[Project B]], [[Project C]]}}]]
  ***
  - ## Agenda
    - {{cursor}}
  - ## Notes
    -
  - ## Action Items
    - TODO {{auto-assign-participant}}
  - ## Follow-up
    - scheduled:: [[{{date +7d}}]]
    - {{auto-create-followup-task}}
```

**Step 2: Implement Template Processor**

```javascript
// Template variable processor
const templateVars = {
  date: () => {
    return new Date().toISOString().split('T')[0]
  },

  'date +7d': () => {
    const future = new Date()
    future.setDate(future.getDate() + 7)
    return future.toISOString().split('T')[0]
  },

  currentTime: () => {
    return new Date().toLocaleTimeString()
  },

  input: (prompt) => {
    return logseq.UI.prompt(prompt)
  },

  select: (prompt, options) => {
    return logseq.UI.select(prompt, options)
  },

  'auto-assign-participant': async () => {
    // Get participants from block properties
    const currentBlock = await logseq.Editor.getCurrentBlock()
    const page = await logseq.Editor.getPage(currentBlock.page.id)
    const participants = page.properties?.participants || ''

    // Random assign
    const names = participants.split(',').map((n) => n.trim())
    return names[Math.floor(Math.random() * names.length)]
  },
}

async function processTemplate(templateContent) {
  let processed = templateContent

  // Replace {{variable}} patterns
  const varPattern = /{{([^}]+)}}/g
  const matches = [...processed.matchAll(varPattern)]

  for (const match of matches) {
    const varName = match[1].trim()
    const parts = varName.split(/\s+/)
    const funcName = parts[0]
    const args = parts.slice(1)

    if (templateVars[varName]) {
      const value = await templateVars[varName](...args)
      processed = processed.replace(match[0], value)
    } else if (templateVars[funcName]) {
      const value = await templateVars[funcName](...args)
      processed = processed.replace(match[0], value)
    }
  }

  return processed
}
```

**Step 3: Auto-Template on Page Creation**

```javascript
// Auto-apply template based on page name pattern
logseq.App.onPageCreated(async (page) => {
  const pageName = page.originalName

  // Meeting notes pattern: "YYYY-MM-DD - Meeting"
  if (/^\d{4}-\d{2}-\d{2} - .+/.test(pageName)) {
    const template = await getTemplate('smart-meeting-notes')
    const processed = await processTemplate(template)
    await logseq.Editor.insertBlock(page.uuid, processed)
  }

  // Project pages
  if (pageName.startsWith('Project/')) {
    const template = await getTemplate('project-template')
    await insertTemplate(page.uuid, template)
  }

  // Daily note enhancements
  if (/^\d{4}_\d{2}_\d{2}$/.test(pageName)) {
    await enhanceDailyNote(page)
  }
})

async function getTemplate(name) {
  const template = await logseq.Editor.getBlock(name, { includeChildren: true })
  return template
}
```

### Context-Aware Templates

**Goal:** Templates that adapt to time, location, or other context.

```javascript
// Context-aware template selection
async function selectTemplateByContext() {
  const hour = new Date().getHours()
  const day = new Date().getDay()

  let template

  // Work hours template (9-5, weekdays)
  if (hour >= 9 && hour < 17 && day >= 1 && day <= 5) {
    template = 'work-task-template'
  }
  // Evening template
  else if (hour >= 17 && hour < 22) {
    template = 'evening-reflection-template'
  }
  // Weekend template
  else if (day === 0 || day === 6) {
    template = 'weekend-project-template'
  }
  // Default
  else {
    template = 'general-template'
  }

  return await getTemplate(template)
}

// Usage in slash command
logseq.Editor.registerSlashCommand('smart-template', async () => {
  const template = await selectTemplateByContext()
  const processed = await processTemplate(template)
  await logseq.Editor.insertAtEditingCursor(processed)
})
```

---

## Plugin-Based Workflows

### Build Custom Plugin

**Step 1: Plugin Structure**

Create `plugins/custom-automation/package.json`:

```json
{
  "name": "logseq-automation-workflows",
  "version": "1.0.0",
  "description": "Custom automation workflows",
  "main": "index.js",
  "logseq": {
    "id": "automation-workflows",
    "title": "Automation Workflows",
    "icon": "🤖"
  }
}
```

**Step 2: Main Plugin File**

Create `plugins/custom-automation/index.js`:

```javascript
async function main() {
  console.log('Automation Workflows Plugin Loaded')

  // Register workflows
  registerTaskProcessor()
  registerAutoArchive()
  registerSmartReminders()
  registerBatchOperations()

  // Add toolbar button
  logseq.App.registerUIItem('toolbar', {
    key: 'automation-hub',
    template: `
      <a class="button" data-on-click="openAutomationHub">
        <i class="ti ti-robot"></i>
      </a>
    `,
  })
}

// Task Processor Workflow
function registerTaskProcessor() {
  logseq.Editor.registerSlashCommand('process-tasks', async () => {
    await processTaskWorkflow()
  })

  logseq.App.registerCommand(
    'process-tasks',
    {
      key: 'automation-process-tasks',
      label: 'Process Tasks Workflow',
      keybinding: {
        binding: 'mod+shift+p',
      },
    },
    async () => {
      await processTaskWorkflow()
    }
  )
}

async function processTaskWorkflow() {
  logseq.UI.showMsg('Processing tasks...', 'info')

  // 1. Collect all TODO tasks
  const tasks = await logseq.DB.datascriptQuery(`
    [:find (pull ?b [*])
     :where
     [?b :block/marker "TODO"]]
  `)

  // 2. Categorize
  const categorized = {
    urgent: [],
    important: [],
    routine: [],
    deferred: [],
  }

  for (const [task] of tasks) {
    const category = await categorizeTask(task)
    categorized[category].push(task)
  }

  // 3. Create report
  const reportBlock = await createTaskReport(categorized)

  // 4. Show notification
  logseq.UI.showMsg(
    `Processed ${tasks.length} tasks. Report created.`,
    'success'
  )

  // 5. Navigate to report
  logseq.Editor.scrollToBlockInPage('Automation Hub', reportBlock.uuid)
}

async function categorizeTask(task) {
  const content = task.content.toLowerCase()
  const props = task.properties || {}

  // Urgent: has deadline or priority A
  if (props.deadline || props.priority === 'A') {
    return 'urgent'
  }

  // Important: tagged with important keywords
  if (/critical|important|essential/.test(content)) {
    return 'important'
  }

  // Routine: simple, repetitive
  if (/daily|weekly|monthly|routine/.test(content)) {
    return 'routine'
  }

  // Deferred: someday/maybe
  if (/someday|maybe|later/.test(content)) {
    return 'deferred'
  }

  return 'routine'
}

async function createTaskReport(categorized) {
  const hubPage = await logseq.Editor.getPage('Automation Hub')

  const report = `
## Task Processing Report - ${new Date().toLocaleString()}

### 🔥 Urgent (${categorized.urgent.length})
${categorized.urgent.map((t) => `- [[${t.uuid}]]`).join('\n')}

### ⭐ Important (${categorized.important.length})
${categorized.important.map((t) => `- [[${t.uuid}]]`).join('\n')}

### 📋 Routine (${categorized.routine.length})
${categorized.routine.map((t) => `- [[${t.uuid}]]`).join('\n')}

### 🕐 Deferred (${categorized.deferred.length})
${categorized.deferred.map((t) => `- [[${t.uuid}]]`).join('\n')}
  `.trim()

  return await logseq.Editor.insertBlock(hubPage.uuid, report, { before: true })
}

// Auto-Archive Workflow
function registerAutoArchive() {
  // Archive completed tasks older than 30 days
  logseq.App.registerCommand(
    'auto-archive',
    {
      key: 'automation-auto-archive',
      label: 'Auto-Archive Old Tasks',
    },
    async () => {
      const thirtyDaysAgo = new Date()
      thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30)
      const cutoff = thirtyDaysAgo.getTime()

      const oldCompletedTasks = await logseq.DB.datascriptQuery(`
      [:find (pull ?b [*])
       :where
       [?b :block/marker "DONE"]]
    `)

      const archivePage =
        (await logseq.Editor.getPage('Archive/Tasks')) ||
        (await logseq.Editor.createPage('Archive/Tasks'))

      let archived = 0

      for (const [task] of oldCompletedTasks) {
        const created = task.createdAt || task.updatedAt

        if (created < cutoff) {
          // Move to archive
          await logseq.Editor.moveBlock(task.uuid, archivePage.uuid)
          archived++
        }
      }

      logseq.UI.showMsg(`Archived ${archived} old completed tasks`, 'success')
    }
  )
}

// Smart Reminders Workflow
function registerSmartReminders() {
  // Check reminders every hour
  setInterval(async () => {
    await checkReminders()
  }, 60 * 60 * 1000) // 1 hour

  // Also check on startup
  checkReminders()
}

async function checkReminders() {
  const now = new Date()
  const todayStr = now.toISOString().split('T')[0]

  // Find tasks due today
  const dueTasks = await logseq.DB.datascriptQuery(`
    [:find (pull ?b [*])
     :where
     [?b :block/marker ?m]
     [(contains? #{"TODO" "DOING"} ?m)]
     (or
       [?b :block/deadline ?d]
       [?b :block/scheduled ?d])
     [(= ?d ${now.getTime()})]]
  `)

  if (dueTasks.length > 0) {
    logseq.UI.showMsg(
      `⏰ You have ${dueTasks.length} tasks due today!`,
      'warning',
      { timeout: 10000 }
    )

    // Create daily reminder block
    const today = await logseq.Editor.getPage(todayStr)
    if (today) {
      await logseq.Editor.insertBlock(
        today.uuid,
        `## ⏰ Reminders\n${dueTasks
          .map(([t]) => `- [[${t.uuid}]]`)
          .join('\n')}`,
        { before: true }
      )
    }
  }
}

// Batch Operations
function registerBatchOperations() {
  logseq.Editor.registerSlashCommand('batch-update', async () => {
    const operation = await logseq.UI.prompt(
      'Batch operation:\n1. Add tag\n2. Set property\n3. Change marker\n4. Archive'
    )

    switch (operation) {
      case '1':
        await batchAddTag()
        break
      case '2':
        await batchSetProperty()
        break
      case '3':
        await batchChangeMarker()
        break
      case '4':
        await batchArchive()
        break
    }
  })
}

async function batchAddTag() {
  const tag = await logseq.UI.prompt('Enter tag to add (without #):')
  const query = await logseq.UI.prompt(
    'Enter query selector:\n1. All TODO\n2. All DONE\n3. Current page\n4. Custom'
  )

  let blocks

  switch (query) {
    case '1':
      blocks = await logseq.DB.datascriptQuery(`
        [:find (pull ?b [:block/uuid])
         :where [?b :block/marker "TODO"]]
      `)
      break
    case '3':
      const page = await logseq.Editor.getCurrentPage()
      blocks = await logseq.Editor.getPageBlocksTree(page.name)
      break
    // Add more cases...
  }

  for (const [block] of blocks) {
    const current = await logseq.Editor.getBlock(block.uuid)
    await logseq.Editor.updateBlock(block.uuid, `${current.content} #${tag}`)
  }

  logseq.UI.showMsg(`Tagged ${blocks.length} blocks`, 'success')
}

// Load plugin
logseq.ready(main).catch(console.error)
```

**Step 3: Install and Test**

```markdown
1. Settings → Plugins → Load unpacked plugin
2. Select plugins/custom-automation folder
3. Enable plugin
4. Test slash commands:
   - /process-tasks
   - /batch-update
5. Test keyboard shortcut: Mod+Shift+P
```

---

## Scheduled Tasks

### Cron-Style Automation

**Goal:** Run tasks on a schedule.

**Step 1: Simple Scheduler**

```javascript
// Scheduler configuration
const schedules = [
  {
    name: 'Daily Review Reminder',
    cron: '0 17 * * *', // 5 PM daily
    action: async () => {
      const today = await logseq.Editor.getPage(
        new Date().toISOString().split('T')[0]
      )

      await logseq.Editor.insertBlock(
        today.uuid,
        "## 🔍 Daily Review\n- [ ] Review today's tasks\n- [ ] Plan tomorrow"
      )

      logseq.UI.showMsg('Daily review block added!', 'info')
    },
  },

  {
    name: 'Weekly Archive',
    cron: '0 0 * * 0', // Midnight Sunday
    action: async () => {
      await archiveCompletedTasks()
      await generateWeeklyReport()
    },
  },

  {
    name: 'Backup Reminder',
    cron: '0 9 * * 1', // 9 AM Monday
    action: async () => {
      logseq.UI.showMsg(
        '💾 Weekly backup reminder! Export your graph.',
        'warning',
        { timeout: 30000 }
      )
    },
  },
]

// Simple cron parser (or use library like node-cron)
function shouldRunNow(cronExpr) {
  // Basic implementation
  // In production, use proper cron library
  const now = new Date()
  const [min, hour, day, month, dow] = cronExpr.split(' ')

  return (
    (min === '*' || parseInt(min) === now.getMinutes()) &&
    (hour === '*' || parseInt(hour) === now.getHours()) &&
    (day === '*' || parseInt(day) === now.getDate()) &&
    (month === '*' || parseInt(month) === now.getMonth() + 1) &&
    (dow === '*' || parseInt(dow) === now.getDay())
  )
}

// Check schedules every minute
setInterval(async () => {
  for (const schedule of schedules) {
    if (shouldRunNow(schedule.cron)) {
      console.log(`Running scheduled task: ${schedule.name}`)
      try {
        await schedule.action()
      } catch (error) {
        console.error(`Schedule error: ${schedule.name}`, error)
      }
    }
  }
}, 60 * 1000) // Check every minute
```

**Step 2: Custom Schedule Manager**

```javascript
// Schedule manager with UI
function createScheduleManager() {
  logseq.App.registerCommand(
    'manage-schedules',
    {
      key: 'automation-schedules',
      label: 'Manage Schedules',
    },
    async () => {
      const schedulesList = schedules
        .map((s, i) => `${i + 1}. ${s.name} (${s.cron})`)
        .join('\n')

      const action = await logseq.UI.prompt(
        `Schedules:\n${schedulesList}\n\nAction:\n` +
          'A - Add new\nD - Delete\nR - Run now\nC - Cancel'
      )

      if (action === 'A') {
        await addSchedule()
      } else if (action === 'D') {
        await deleteSchedule()
      } else if (action === 'R') {
        await runScheduleNow()
      }
    }
  )
}

async function addSchedule() {
  const name = await logseq.UI.prompt('Schedule name:')
  const cron = await logseq.UI.prompt('Cron expression (e.g., 0 9 * * *):')
  const actionType = await logseq.UI.prompt(
    'Action type:\n1. Insert block\n2. Run query\n3. Custom function'
  )

  // Create schedule object
  const schedule = {
    name,
    cron,
    action: await buildScheduleAction(actionType),
  }

  schedules.push(schedule)

  // Save to storage
  await logseq.App.setUserConfigs({
    customSchedules: schedules,
  })

  logseq.UI.showMsg(`Schedule "${name}" added!`, 'success')
}
```

---

## Batch Operations

### Mass Block Updates

**Goal:** Update many blocks efficiently.

```javascript
// Batch update utility
class BatchUpdater {
  constructor() {
    this.queue = []
    this.running = false
  }

  async add(operation) {
    this.queue.push(operation)
    if (!this.running) {
      await this.process()
    }
  }

  async process() {
    this.running = true

    while (this.queue.length > 0) {
      const operation = this.queue.shift()

      try {
        await operation()
      } catch (error) {
        console.error('Batch operation failed:', error)
      }

      // Small delay to prevent overwhelming
      await new Promise((resolve) => setTimeout(resolve, 50))
    }

    this.running = false
  }
}

const batchUpdater = new BatchUpdater()

// Example: Add property to all blocks in page
async function batchAddProperty(pageName, propertyKey, propertyValue) {
  const blocks = await logseq.Editor.getPageBlocksTree(pageName)

  console.log(`Queueing ${blocks.length} blocks for update...`)

  for (const block of blocks) {
    await batchUpdater.add(async () => {
      await logseq.Editor.upsertBlockProperty(
        block.uuid,
        propertyKey,
        propertyValue
      )
    })
  }

  logseq.UI.showMsg('Batch update complete!', 'success')
}

// Example: Convert all TODO to DONE in page
async function batchCompleteAllTasks(pageName) {
  const todos = await logseq.DB.datascriptQuery(
    `
    [:find (pull ?b [:block/uuid :block/content])
     :in $ ?page-name
     :where
     [?p :block/name ?page-name]
     [?b :block/page ?p]
     [?b :block/marker "TODO"]]
  `,
    pageName.toLowerCase()
  )

  for (const [block] of todos) {
    await batchUpdater.add(async () => {
      await logseq.Editor.updateBlock(
        block.uuid,
        block.content.replace('TODO', 'DONE')
      )
    })
  }

  logseq.UI.showMsg(`Completed ${todos.length} tasks!`, 'success')
}
```

### Bulk Export/Import

**Goal:** Export and reimport blocks with transformations.

```javascript
// Export blocks to JSON
async function exportBlocksToJSON(pageNames) {
  const exported = []

  for (const pageName of pageNames) {
    const blocks = await logseq.Editor.getPageBlocksTree(pageName)
    exported.push({
      page: pageName,
      blocks: blocks,
    })
  }

  const json = JSON.stringify(exported, null, 2)

  // Save to file
  const blob = new Blob([json], { type: 'application/json' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `logseq-export-${Date.now()}.json`
  a.click()

  return exported
}

// Import with transformation
async function importBlocksWithTransform(jsonData, transformer) {
  const data = typeof jsonData === 'string' ? JSON.parse(jsonData) : jsonData

  for (const pageData of data) {
    const page =
      (await logseq.Editor.getPage(pageData.page)) ||
      (await logseq.Editor.createPage(pageData.page))

    for (const block of pageData.blocks) {
      // Apply transformation
      const transformed = await transformer(block)

      // Insert
      await logseq.Editor.insertBlock(page.uuid, transformed.content, {
        properties: transformed.properties,
        before: false,
      })
    }
  }
}

// Example transformer: Add timestamp to all blocks
const addTimestampTransformer = async (block) => {
  return {
    content: block.content,
    properties: {
      ...block.properties,
      imported: new Date().toISOString(),
      original_uuid: block.uuid,
    },
  }
}
```

---

## Integration Workflows

### External Tool Integration

**Goal:** Connect Logseq with external services.

**Example 1: GitHub Issues to Logseq**

```javascript
// Fetch GitHub issues and create tasks
async function syncGitHubIssues(repo, token) {
  const response = await fetch(`https://api.github.com/repos/${repo}/issues`, {
    headers: {
      Authorization: `token ${token}`,
      Accept: 'application/vnd.github.v3+json',
    },
  })

  const issues = await response.json()

  const tasksPage =
    (await logseq.Editor.getPage('Tasks/GitHub')) ||
    (await logseq.Editor.createPage('Tasks/GitHub'))

  for (const issue of issues) {
    const taskContent = `
TODO #github [${issue.title}](${issue.html_url})
github-id:: ${issue.number}
labels:: ${issue.labels.map((l) => l.name).join(', ')}
created:: ${issue.created_at}
    `.trim()

    // Check if already imported
    const existing = await logseq.DB.datascriptQuery(`
      [:find (pull ?b [:block/uuid])
       :where
       [?b :block/properties ?props]
       [(get ?props :github-id) ?id]
       [(= ?id ${issue.number})]]
    `)

    if (existing.length === 0) {
      await logseq.Editor.appendBlockInPage(tasksPage.name, taskContent)
    }
  }

  logseq.UI.showMsg(`Synced ${issues.length} GitHub issues`, 'success')
}

// Register command
logseq.App.registerCommand(
  'sync-github',
  {
    key: 'automation-sync-github',
    label: 'Sync GitHub Issues',
  },
  async () => {
    const repo = await logseq.UI.prompt('GitHub repo (user/repo):')
    const token = await logseq.UI.prompt('GitHub token:')

    await syncGitHubIssues(repo, token)
  }
)
```

**Example 2: Export to Notion**

```javascript
// Export page to Notion
async function exportToNotion(pageName, notionToken, databaseId) {
  const blocks = await logseq.Editor.getPageBlocksTree(pageName)

  // Convert to Notion format
  const notionBlocks = blocks.map((block) => ({
    object: 'block',
    type: 'paragraph',
    paragraph: {
      rich_text: [
        {
          type: 'text',
          text: { content: block.content },
        },
      ],
    },
  }))

  // Create Notion page
  const response = await fetch('https://api.notion.com/v1/pages', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${notionToken}`,
      'Notion-Version': '2022-06-28',
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      parent: { database_id: databaseId },
      properties: {
        title: {
          title: [
            {
              text: { content: pageName },
            },
          ],
        },
      },
      children: notionBlocks,
    }),
  })

  const result = await response.json()

  logseq.UI.showMsg(`Exported to Notion: ${result.url}`, 'success')

  return result
}
```

### Webhook Automation

**Goal:** Trigger workflows via webhooks.

```javascript
// Simple webhook server (requires external setup)
// This would run as a separate service

// In Logseq, register handler
logseq.App.registerCommand(
  'setup-webhooks',
  {
    key: 'automation-webhooks',
    label: 'Setup Webhooks',
  },
  async () => {
    const webhookUrl = await logseq.UI.prompt(
      'Webhook URL (from external service):'
    )

    // Save configuration
    await logseq.App.setUserConfigs({
      webhookUrl: webhookUrl,
    })

    // Start listening (pseudo-code, needs actual implementation)
    // In practice, you'd use a plugin that runs a local server
    // or connects to a cloud webhook service

    logseq.UI.showMsg('Webhooks configured!', 'success')
  }
)

// Example webhook handler
async function handleWebhook(payload) {
  const { action, data } = payload

  switch (action) {
    case 'create_task':
      await createTaskFromWebhook(data)
      break
    case 'add_note':
      await addNoteFromWebhook(data)
      break
    case 'trigger_workflow':
      await runWorkflow(data.workflowName)
      break
  }
}

async function createTaskFromWebhook(data) {
  const today = await logseq.Editor.getPage(
    new Date().toISOString().split('T')[0]
  )

  const taskContent = `TODO ${data.title}\nsource:: webhook\ndetails:: ${data.details}`

  await logseq.Editor.appendBlockInPage(today.name, taskContent)

  logseq.UI.showMsg('Task created from webhook!', 'success')
}
```

---

## Production Deployment

### Error Handling

```javascript
// Robust error handling wrapper
function withErrorHandling(fn, context = 'Operation') {
  return async (...args) => {
    try {
      return await fn(...args)
    } catch (error) {
      console.error(`${context} failed:`, error)

      // Log to page
      const errorLog =
        (await logseq.Editor.getPage('Logs/Errors')) ||
        (await logseq.Editor.createPage('Logs/Errors'))

      await logseq.Editor.appendBlockInPage(
        errorLog.name,
        `- ${new Date().toISOString()}: ${context} - ${error.message}`
      )

      // Notify user
      logseq.UI.showMsg(`${context} failed. Check error logs.`, 'error')

      throw error // Re-throw if caller needs to handle
    }
  }
}

// Usage
const safeProcessTasks = withErrorHandling(
  processTaskWorkflow,
  'Task Processing'
)
```

### Logging and Monitoring

```javascript
// Centralized logging
class AutomationLogger {
  constructor(logPageName = 'Logs/Automation') {
    this.logPageName = logPageName
  }

  async log(level, message, data = {}) {
    const timestamp = new Date().toISOString()
    const logEntry = `- ${timestamp} [${level}] ${message}`

    const logPage =
      (await logseq.Editor.getPage(this.logPageName)) ||
      (await logseq.Editor.createPage(this.logPageName))

    await logseq.Editor.appendBlockInPage(logPage.name, logEntry)

    // Also log to console
    console.log(`[${level}] ${message}`, data)

    // Store in memory for queries
    this.recentLogs = this.recentLogs || []
    this.recentLogs.push({ timestamp, level, message, data })

    // Keep only last 100
    if (this.recentLogs.length > 100) {
      this.recentLogs.shift()
    }
  }

  async info(message, data) {
    await this.log('INFO', message, data)
  }

  async warn(message, data) {
    await this.log('WARN', message, data)
  }

  async error(message, data) {
    await this.log('ERROR', message, data)
  }

  getRecentLogs(level = null) {
    if (level) {
      return this.recentLogs.filter((log) => log.level === level)
    }
    return this.recentLogs
  }
}

const logger = new AutomationLogger()
```

### Configuration Management

```javascript
// Configuration system
const Config = {
  async get(key, defaultValue = null) {
    const config = await logseq.App.getUserConfigs()
    return config?.[key] ?? defaultValue
  },

  async set(key, value) {
    const config = await logseq.App.getUserConfigs()
    config[key] = value
    await logseq.App.setUserConfigs(config)
  },

  async getAll() {
    return await logseq.App.getUserConfigs()
  },

  async reset() {
    await logseq.App.setUserConfigs({})
  },
}

// Usage
await Config.set('automation.taskProcessing.enabled', true)
await Config.set('automation.schedules', schedules)

const enabled = await Config.get('automation.taskProcessing.enabled', false)
```

### Testing

```javascript
// Test suite for automation workflows
const AutomationTests = {
  async runAll() {
    console.log('Running automation tests...')

    await this.testTaskProcessing()
    await this.testTemplateExpansion()
    await this.testBatchOperations()

    console.log('All tests complete!')
  },

  async testTaskProcessing() {
    console.log('Test: Task Processing')

    // Create test tasks
    const testPage = await logseq.Editor.createPage(`Test-${Date.now()}`)

    await logseq.Editor.appendBlockInPage(
      testPage.name,
      'TODO Test task #urgent'
    )

    // Run processor
    await processTaskWorkflow()

    // Verify results
    // (Add assertions)

    // Cleanup
    await logseq.Editor.deletePage(testPage.name)

    console.log('✓ Task processing test passed')
  },

  async testTemplateExpansion() {
    console.log('Test: Template Expansion')

    const template = '{{date}} - {{currentTime}}'
    const expanded = await processTemplate(template)

    // Verify contains date and time
    if (!expanded.includes('-') || expanded.includes('{{')) {
      throw new Error('Template expansion failed')
    }

    console.log('✓ Template expansion test passed')
  },

  async testBatchOperations() {
    console.log('Test: Batch Operations')

    // Test batch updater
    const results = []

    for (let i = 0; i < 10; i++) {
      await batchUpdater.add(async () => {
        results.push(i)
      })
    }

    // Wait for completion
    await new Promise((resolve) => setTimeout(resolve, 1000))

    if (results.length !== 10) {
      throw new Error('Batch operations failed')
    }

    console.log('✓ Batch operations test passed')
  },
}

// Register test command
logseq.App.registerCommand(
  'run-automation-tests',
  {
    key: 'automation-run-tests',
    label: 'Run Automation Tests',
  },
  async () => {
    await AutomationTests.runAll()
    logseq.UI.showMsg('Tests complete! Check console.', 'info')
  }
)
```

---

## Summary

You've built a complete automation system! 🎉

**What You've Built:**

✅ Query-based auto-dashboards and tagging  
✅ Smart template system with context awareness  
✅ Custom plugin with workflow automation  
✅ Scheduled tasks and reminders  
✅ Batch operations for mass updates  
✅ External integrations (GitHub, webhooks)  
✅ Production-ready error handling and logging

**Key Concepts:**

- **Automation Layers:** Queries → Templates → Plugins → Integrations
- **Event-Driven:** React to user actions and schedule
- **Batch Processing:** Efficient bulk operations
- **Error Handling:** Robust fallbacks and logging
- **Testing:** Verify workflows work correctly

**Next Steps:**

1. **Expand workflows** with your specific needs
2. **Share plugins** with community
3. **Integrate more tools** (Todoist, Calendar, etc.)
4. **Optimize performance** for large graphs
5. **Build UI** for easier workflow management

**Resources:**

- [Logseq Plugin SDK](https://plugins-doc.logseq.com)
- [JavaScript API Reference](../../reference/api/javascript-api.md)
- [Plugin Examples](https://github.com/logseq/logseq-plugin-samples)
- [Community Plugins](https://github.com/logseq/marketplace)
- [Discord #plugin-dev](https://discord.gg/logseq)

**Tips for Automation Success:**

- Start small, automate one pain point at a time
- Test thoroughly before deploying
- Log everything for debugging
- Handle errors gracefully
- Document your workflows
- Share and learn from community

Happy automating! 🤖✨
