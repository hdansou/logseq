---
title: 'Workflow Automation Tutorial'
status: 🟢 Stable
version: 'Schema v65.33 / @logseq/libs v0.3.4'
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
quadrant: 'Tutorial'
platform: '🟠 Desktop Only'
related-docs:
  - '[Advanced Queries Tutorial](./advanced-queries-tutorial.md)'
  - '[Plugin API Reference](../../reference/api/plugin-api.md)'
  - '[Plugin Security Architecture](../../explanation/architecture/plugin-security.md)'
tags: ['automation', 'workflows', 'plugins', 'scripting', 'productivity']
---

# Workflow Automation Tutorial

Automate repetitive tasks in Logseq using queries, templates and a plugin. This hands-on tutorial builds a working automation plugin against a **DB graph**, using APIs verified to exist in `@logseq/libs` v0.3.4.

> **This tutorial targets DB graphs.** The queries below use DB-graph attributes
> (`:block/title`, `:logseq.property/status`, `:logseq.property/deadline`) and Datalog passed to
> `logseq.DB.datascriptQuery`. If you are on a file graph, none of it applies — file graphs use
> `#+BEGIN_QUERY` blocks and `:block/marker` / `:block/content`, which a DB graph cannot parse and
> which no longer exist in the DB schema (they survive only in `deps/graph-parser/`).

> **Before you start, read [Plugin Security Architecture](../../explanation/architecture/plugin-security.md).**
> Logseq does not sandbox plugins. Everything you build here runs with full access to your graph,
> your local files and the network. Automate on a graph you have backed up.

**What You'll Build:**

- Query-driven task dashboards
- Rule-based auto-tagging
- A custom plugin registering slash commands and palette commands
- Interval-based reminders
- Batch update helpers
- An external-service sync (GitHub issues)

**Prerequisites:**

- Comfortable with Logseq basics and a DB graph
- Basic JavaScript knowledge
- Understanding of Datalog queries — see [Advanced Queries Tutorial](./advanced-queries-tutorial.md)
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
1. Settings → Advanced → Developer mode (toggle on)
2. Open the devtools console (Ctrl+Shift+I / Cmd+Option+I)
3. The Console tab is our workspace
```

The toggle is `developer-mode-row` in `src/main/frontend/components/settings.cljs:568-575`; it sets
`:ui/developer-mode?` in app state. Turning it on is what makes the **Load unpacked plugin** button
appear on the Plugins dashboard — that button is gated on developer mode **and** Electron
(`src/main/frontend/components/plugins.cljs:828-838`).

**Step 2: Create Automation Workspace**

```markdown
Create pages:

- [[Automation Hub]] - Central control
- [[Scripts]] - Reusable scripts
- [[Workflows]] - Workflow definitions
- [[Templates/Auto]] - Auto-templates
- [[Logs/Automation]] - Activity logs
```

**Step 3: No marketplace plugins are required**

Everything in this tutorial is built with the SDK and the query engine. Earlier revisions of this
page listed four marketplace plugins as prerequisites; none of them could be verified to exist, so
they have been removed. If you want a scheduler or template plugin from the marketplace, pick one
yourself — and read its source first (see [Plugin Security Architecture](../../explanation/architecture/plugin-security.md)).

**Step 4: Project Structure**

Your plugin lives outside the graph. Marketplace installs land in `~/.logseq/plugins`
(`src/electron/electron/core.cljs:40`); an unpacked plugin you load yourself can live anywhere.
DB graphs themselves live under `~/logseq/graphs`
(`deps/common/src/logseq/common/config.cljs:55`) and are not a directory of Markdown files.

```
~/dev/logseq-automation-workflows/    # our unpacked plugin
├── package.json                      # must contain a `logseq` object
├── index.js
└── workflows/
    ├── task-processor.js
    ├── template-runner.js
    └── scheduler.js
```

The `logseq` object in `package.json` is mandatory — the installer throws `Illegal logseq package`
without it (`src/main/frontend/handler/plugin.cljs:1171-1172`). The plugin id comes from
`logseq.id`, falling back to the package `name` (`:1173-1175`).

---

## Query-Based Automation

### Auto-Task Dashboard

**Goal:** Automatically collect and organize tasks.

These go in **Advanced Query** blocks (`/Advanced Query`). The map keys that do anything are
`:query`, `:inputs`, `:rules`, `:result-transform`, `:view`, `:collapsed?` and
`:remove-block-children?` — `:title` and `:breadcrumb-show?` are silently ignored, so put the
heading in the block above.

**Step 1: Overdue Tasks**

Statuses are **closed-value refs**, not strings, so match the `:db/ident` of the status entity —
never `[(= ?status "TODO")]`. The six status idents are `:logseq.property/status.backlog`, `.todo`,
`.doing`, `.in-review`, `.done`, `.canceled`.

`:logseq.property/deadline` and `/scheduled` are `:datetime` — **epoch milliseconds** — so compare
them against `:start-of-today-ms`, not against `:today` (which is a `YYYYMMDD` integer).

```clojure
;; Block above: ## ⚠️ Overdue Tasks
{:query [:find (pull ?b [:block/uuid :logseq.property/deadline])
         :in $ ?start-of-today
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         (or [?s :db/ident :logseq.property/status.todo]
             [?s :db/ident :logseq.property/status.doing])
         [?b :logseq.property/deadline ?d]
         [(< ?d ?start-of-today)]]
 :inputs [:start-of-today-ms]
 :result-transform (fn [result]
                     (sort-by :logseq.property/deadline result))
 :collapsed? false}
```

**Step 2: High-priority tasks**

Priorities are closed-value refs too: `:logseq.property/priority.low`, `.medium`, `.high`,
`.urgent`.

```clojure
;; Block above: ## 🔥 High Priority
{:query [:find (pull ?b [:block/uuid])
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :logseq.property/status ?s]
         (or [?s :db/ident :logseq.property/status.todo]
             [?s :db/ident :logseq.property/status.doing])
         (or-join [?b]
                  (and [?b :logseq.property/priority ?p]
                       (or [?p :db/ident :logseq.property/priority.high]
                           [?p :db/ident :logseq.property/priority.urgent]))
                  (and [?b :block/tags ?t]
                       [?t :block/title "urgent"]))]
 :collapsed? false}
```

**Step 3: Group tasks by context tag**

Two `:find` columns means rows come back as tuples, so `:view` destructures them directly.
`:view` runs in SCI, which silently drops `map-indexed`, `#()` and keyword-as-function — use plain
`(fn ...)` and `(for ...)`, and give `:style` a **map**, never a string.

```clojure
;; Block above: ## 📋 Tasks by Context
{:query [:find ?context (pull ?b [:block/uuid :block/title])
         :in $ [?context ...]
         :where
         [?b :block/tags :logseq.class/Task]
         [?b :block/tags ?t]
         [?t :block/title ?context]]
 :inputs [["work" "home" "errands" "calls"]]
 :view (fn [rows]
         [:div
          (for [[ctx group] (group-by (fn [row] (first row)) rows)]
            [:div {:style {:margin-bottom "0.75rem"}}
             [:h3 (str ctx " (" (count group) ")")]
             [:ul
              (for [[_ b] group]
                [:li (get b :block/title)])]])])}
```

**Step 4: Auto-refresh**

There is nothing to configure. A query block re-runs when the data it depends on changes; you do
not need a refresh mechanism, and there is no `{{currentTime}}` or `{{sum of tasks}}` template
variable to drive one.

> **Deleted:** an earlier revision of this page showed an "auto-dashboard" template using
> `{{currentTime}}`, `{{sum of tasks}}`, `{{today tasks}}` and `{{overdue count}}`. None of these
> exist. Logseq's `{{...}}` macros are the built-in `{{query ...}}` plus whatever you define under
> `:macros` in `config.edn` (`deps/common/src/logseq/common/util/macro.cljs:1-40`).

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

  // BlockEntity.content is deprecated — use .title (libs/src/LSPlugin.ts, BlockEntity)
  const content = block.title
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

// React to block changes.
// `logseq.DB.onChanged` fires for every transaction and hands you the changed blocks
// (libs/src/LSPlugin.ts:978-983).
logseq.DB.onChanged(async ({ blocks }) => {
  for (const b of blocks) {
    await autoTagBlock(b.uuid)
  }
})
```

> `logseq.Editor.onInputSelectionEnd` (`libs/src/LSPlugin.ts:944-950`) does exist, but its event
> carries `{ caret, point, start, end, text }` — **no `uuid`**. An earlier revision destructured
> `{ uuid }` from it, which is always `undefined`. Use `logseq.DB.onChanged` for
> block-level reactions, or `logseq.DB.onBlockChanged(uuid, cb)` (`:989-996`) for one block.

**Step 3: Batch Tag Existing Blocks**

```javascript
// Tag all blocks on a page. :block/content does not exist in a DB graph — use :block/title.
async function batchAutoTag(pageTitle = null) {
  const query = pageTitle
    ? `[:find (pull ?b [:block/uuid :block/title])
        :in $ ?page-title
        :where
        [?p :block/title ?page-title]
        [?b :block/page ?p]
        [?b :block/title _]]`
    : `[:find (pull ?b [:block/uuid :block/title])
        :where [?b :block/title _]]`

  const rows = pageTitle
    ? await logseq.DB.datascriptQuery(query, pageTitle)
    : await logseq.DB.datascriptQuery(query)

  console.log(`Processing ${rows.length} blocks...`)

  for (const [block] of rows) {
    await autoTagBlock(block.uuid)
  }

  console.log('Batch tagging complete!')
}

// Run on current page — getCurrentPage() is async and may return a block, not a page
const current = await logseq.Editor.getCurrentPage()
if (current) await batchAutoTag(current.title ?? current.name)
```

`logseq.Editor.getCurrentPage()` returns `Promise<PageEntity | BlockEntity | null>`
(`libs/src/LSPlugin.ts:709`) — it must be awaited, and it can hand you a block when the user is
zoomed in.

---

## Template Automation

### What Templates Actually Are

In a DB graph a template is a block tagged **`#Template`** — `:logseq.class/Template`
(`deps/db/src/logseq/db/frontend/class.cljs:100-102`). It carries one built-in property,
`:logseq.property/template-applied-to` ("Apply template to tags",
`deps/db/src/logseq/db/frontend/property.cljs:696`), which makes the template offered automatically
on blocks carrying those tags (`src/main/frontend/worker/pipeline.cljs:107`). Applying a template is
the outliner operation `:apply-template`
(`deps/outliner/src/logseq/outliner/op.cljs:265`, `op/construct.cljc:517-532`).

> **Deleted:** earlier revisions of this page built a template engine on `{{input "..."}}`,
> `{{input-multiline ...}}`, `{{select "..." [...]}}`, `{{cursor}}`, `{{date +7d}}`,
> `{{auto-assign-participant}}` and `{{auto-create-followup-task}}`, driven by
> `logseq.UI.prompt` and `logseq.UI.select`. **None of these exist.** `IUIProxy`
> (`libs/src/LSPlugin.ts:1032-1043`) has exactly six members — `showMsg`, `closeMsg`,
> `queryElementRect`, `queryElementById`, `checkSlotValid`, `resolveThemeCssPropsVals` — and no
> prompt or picker. Logseq's `{{...}}` macros are the built-in `{{query ...}}` plus user macros
> declared under `:macros` in `config.edn`
> (`deps/common/src/logseq/common/util/macro.cljs:1-40`); there is no `input`, `select`, `cursor`
> or `date` macro. The whole engine has been replaced below with the template API that does exist.

### The Template API

Five methods on `IAppProxy` (`libs/src/LSPlugin.ts:532-541`):

```typescript
logseq.App.getTemplate(name)                      // => Promise<BlockEntity | null>
logseq.App.existTemplate(name)                    // => Promise<Boolean>
logseq.App.createTemplate(targetUuid, name, opts) // opts?: { overwrite: boolean }
logseq.App.removeTemplate(name)
logseq.App.insertTemplate(targetUuid, name)       // insert by name into a target block
```

`insertTemplate` takes the **template's name**, not its content — you do not expand it yourself.

**Step 1: Apply a template to a block**

```javascript
async function applyTemplate(targetUuid, name) {
  if (!(await logseq.App.existTemplate(name))) {
    await logseq.UI.showMsg(`No template named "${name}"`, 'warning')
    return
  }
  await logseq.App.insertTemplate(targetUuid, name)
}
```

**Step 2: Choose a template from context**

Since there is no picker API, decide in code and insert by name:

```javascript
function templateForNow() {
  const hour = new Date().getHours()
  const day = new Date().getDay()

  if (day === 0 || day === 6) return 'weekend-project'
  if (hour >= 9 && hour < 17) return 'work-task'
  if (hour >= 17 && hour < 22) return 'evening-reflection'
  return 'general'
}

logseq.Editor.registerSlashCommand('Smart template', async ({ uuid }) => {
  await applyTemplate(uuid, templateForNow())
})
```

`registerSlashCommand(tag, action)` (`libs/src/LSPlugin.ts:666-669`) hands its callback an event
carrying `{ uuid }` — the block the command was invoked in (`BlockCommandCallback`).

**Step 3: Apply on a schedule, not on page creation**

> **Deleted:** `logseq.App.onPageCreated` does not exist. The `IAppProxy` hook events are
> `onCurrentGraphChanged`, `onGraphAfterIndexed`, `onThemeModeChanged`, `onThemeChanged`,
> `onTodayJournalCreated`, `onBeforeCommandInvoked`, `onAfterCommandInvoked`,
> `onBlockRendererSlotted`, `onMacroRendererSlotted`, `onPageHeadActionsSlotted`, `onRouteChanged`
> and `onSidebarVisibleChanged` (`libs/src/LSPlugin.ts:559-605`). There is no page-created hook.

The two hooks that get you close:

```javascript
// Fires when today's journal page is created (libs/src/LSPlugin.ts:565)
logseq.App.onTodayJournalCreated(async ({ title }) => {
  const page = await logseq.Editor.getPage(title)
  if (page) await applyTemplate(page.uuid, 'daily-note')
})

// Fires on every route change (libs/src/LSPlugin.ts:604)
logseq.App.onRouteChanged(({ path, template }) => {
  console.log('navigated to', path, template)
})
```

For anything else — "a page matching this name pattern was created" — watch transactions with
`logseq.DB.onChanged` and filter yourself.

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
  "dependencies": {
    "@logseq/libs": "^0.3.4"
  },
  "logseq": {
    "id": "automation-workflows",
    "title": "Automation Workflows",
    "icon": "🤖"
  }
}
```

The `logseq` object is required; without it the installer throws `Illegal logseq package`
(`src/main/frontend/handler/plugin.cljs:1171-1172`). The loader reads `logseq.entry`, then
`logseq.main`, then the package `main` to find your entry file
(`libs/src/LSPlugin.core.ts:719-728`).

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

  // 1. Collect all open tasks. Status is a closed-value ref, never a string.
  //
  // Priority is a ref too, so bind its :db/ident as its own :find column rather
  // than hoping the pull spec brings it along.
  const tasks = await logseq.DB.datascriptQuery(`
    [:find (pull ?b [:block/uuid :block/title :logseq.property/deadline]) ?priority
     :where
     [?b :block/tags :logseq.class/Task]
     [?b :logseq.property/status ?s]
     [?s :db/ident :logseq.property/status.todo]
     [(get-else $ ?b :logseq.property/priority :none) ?p]
     [(get-else $ ?p :db/ident :none) ?priority]]
  `)

  // 2. Categorize
  const categorized = {
    urgent: [],
    important: [],
    routine: [],
    deferred: [],
  }

  for (const [task, priority] of tasks) {
    const category = await categorizeTask(task, priority)
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

// `priority` is the :db/ident of the priority ref, or ':none'.
//
// Note the key names: datascript_query serializes keywords with camelCase OFF and
// strips the namespace only for the `block`, `db` and `file` namespaces
// (src/main/logseq/sdk/utils.cljs:11-15, :29-52). So `:block/uuid` arrives as
// `uuid`, but `:logseq.property/deadline` keeps its full form, colon included.
async function categorizeTask(task, priority) {
  const content = (task.title ?? '').toLowerCase()

  // Urgent: has a deadline, or an urgent/high priority
  if (
    task[':logseq.property/deadline'] ||
    priority === ':logseq.property/priority.urgent' ||
    priority === ':logseq.property/priority.high'
  ) {
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
      [:find (pull ?b [:block/uuid :block/updated-at])
       :where
       [?b :block/tags :logseq.class/Task]
       [?b :logseq.property/status ?s]
       [?s :db/ident :logseq.property/status.done]]
    `)

      const archivePage =
        (await logseq.Editor.getPage('Archive/Tasks')) ||
        (await logseq.Editor.createPage('Archive/Tasks'))

      let archived = 0

      for (const [task] of oldCompletedTasks) {
        // :block/updated-at → "updated-at" (namespace stripped, no camelCasing)
        const touched = task['updated-at']

        if (touched && touched < cutoff) {
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

  // Find tasks due today. Deadlines are :datetime — epoch milliseconds — so compare
  // against a millisecond window, not against a YYYYMMDD integer.
  const startOfDay = new Date(now)
  startOfDay.setHours(0, 0, 0, 0)
  const endOfDay = startOfDay.getTime() + 24 * 60 * 60 * 1000

  const dueTasks = await logseq.DB.datascriptQuery(
    `[:find (pull ?b [:block/uuid :block/title])
      :in $ ?start ?end
      :where
      [?b :block/tags :logseq.class/Task]
      [?b :logseq.property/status ?s]
      (or [?s :db/ident :logseq.property/status.todo]
          [?s :db/ident :logseq.property/status.doing])
      (or [?b :logseq.property/deadline ?d]
          [?b :logseq.property/scheduled ?d])
      [(>= ?d ?start)]
      [(< ?d ?end)]]`,
    startOfDay.getTime(),
    endOfDay
  )

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
//
// There is no prompt or picker API — IUIProxy has only showMsg, closeMsg,
// queryElementRect, queryElementById, checkSlotValid and resolveThemeCssPropsVals
// (libs/src/LSPlugin.ts:1032-1043). Take input through plugin settings instead:
// useSettingsSchema declares the fields, logseq.settings reads them
// (libs/src/LSPlugin.ts:1201, :1123).
function registerBatchOperations() {
  logseq.useSettingsSchema([
    {
      key: 'batchTag',
      type: 'string',
      title: 'Batch tag',
      description: 'Tag added by the batch-tag command (no leading #)',
      default: 'triage',
    },
  ])

  // One command per operation — the palette is the menu.
  logseq.App.registerCommandPalette(
    { key: 'automation-batch-tag-open-tasks', label: 'Batch: tag open tasks' },
    async () => await batchTagOpenTasks()
  )

  logseq.App.registerCommandPalette(
    { key: 'automation-batch-tag-current-page', label: 'Batch: tag current page' },
    async () => await batchTagCurrentPage()
  )
}

async function batchTagOpenTasks() {
  const tag = logseq.settings?.batchTag
  if (!tag) return logseq.UI.showMsg('Set a batch tag in plugin settings', 'warning')

  const rows = await logseq.DB.datascriptQuery(`
    [:find (pull ?b [:block/uuid :block/title])
     :where
     [?b :block/tags :logseq.class/Task]
     [?b :logseq.property/status ?s]
     [?s :db/ident :logseq.property/status.todo]]
  `)

  for (const [block] of rows) {
    await logseq.Editor.updateBlock(block.uuid, `${block.title} #${tag}`)
  }

  logseq.UI.showMsg(`Tagged ${rows.length} blocks`, 'success')
}

async function batchTagCurrentPage() {
  const tag = logseq.settings?.batchTag
  const page = await logseq.Editor.getCurrentPage()
  if (!tag || !page) return

  // getPageBlocksTree returns BlockEntity[], NOT query tuples — do not destructure.
  const blocks = await logseq.Editor.getPageBlocksTree(page.title ?? page.name)

  for (const block of blocks ?? []) {
    await logseq.Editor.updateBlock(block.uuid, `${block.title} #${tag}`)
  }

  logseq.UI.showMsg(`Tagged ${blocks?.length ?? 0} blocks`, 'success')
}

// Load plugin
logseq.ready(main).catch(console.error)
```

**Step 3: Install and Test**

```markdown
1. Enable Settings → Advanced → Developer mode
2. Open the Plugins dashboard (`t p`)
3. Click "Load unpacked plugin" and select your plugin folder
4. Test the slash command: /process-tasks
5. Test the palette commands: "Batch: tag open tasks"
6. Test the keyboard shortcut: Mod+Shift+P
```

`t p` opens the dashboard — the `:ui/goto-plugins` shortcut
(`src/main/frontend/modules/shortcut/config.cljs:464`). "Load unpacked plugin" only appears in
developer mode on Electron (`src/main/frontend/components/plugins.cljs:828-838`).

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

**Step 2: Persisting schedules**

> **Deleted:** an earlier revision managed schedules through a `logseq.UI.prompt` menu and saved
> them with `logseq.App.setUserConfigs`. **Neither exists.** `IUIProxy` has no prompt
> (`libs/src/LSPlugin.ts:1032-1043`), and `IAppProxy` exposes `getUserConfigs`
> (`:412`) as **read-only** — the only writable config setter is
> `setCurrentGraphConfigs(configs)` (`:510`), which writes the *graph's* `config.edn`, not
> per-plugin state.

Persist plugin state with the plugin settings API instead — `logseq.updateSettings(attrs)`
(`libs/src/LSPlugin.ts:1210`) writes, `logseq.settings` reads (`:1123`), and
`logseq.onSettingsChanged(cb)` (`:1215`) fires on change. Settings are stored per plugin by the host
(`libs/src/LSPlugin.core.ts:573`, `:595`).

```javascript
function loadSchedules() {
  return logseq.settings?.schedules ?? []
}

function saveSchedules(next) {
  logseq.updateSettings({ schedules: next })
}

logseq.App.registerCommandPalette(
  { key: 'automation-list-schedules', label: 'List automation schedules' },
  async () => {
    const list = loadSchedules()
      .map((s, i) => `${i + 1}. ${s.name} (${s.cron})`)
      .join('\n')
    await logseq.UI.showMsg(list || 'No schedules configured', 'info')
  }
)

logseq.onSettingsChanged((next, prev) => {
  console.log('schedules changed', prev?.schedules, '→', next?.schedules)
})
```

For anything a user must type, declare it as a settings field with
`logseq.useSettingsSchema([...])` (`libs/src/LSPlugin.ts:1201`) — that is the only user-input
surface the SDK provides.

> **Note:** `setInterval` in a plugin only runs while Logseq is open and the plugin is enabled.
> Nothing in the SDK schedules work while the app is closed, and there is no cron facility in the
> app.

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

// Example: mark every open task on a page as done.
//
// Status is a closed-value ref, so you cannot flip it by rewriting text. Set the
// property to the target status entity with upsertBlockProperty.
async function batchCompleteAllTasks(pageTitle) {
  const todos = await logseq.DB.datascriptQuery(
    `[:find (pull ?b [:block/uuid])
      :in $ ?page-title
      :where
      [?p :block/title ?page-title]
      [?b :block/page ?p]
      [?b :block/tags :logseq.class/Task]
      [?b :logseq.property/status ?s]
      [?s :db/ident :logseq.property/status.todo]]`,
    pageTitle
  )

  for (const [block] of todos) {
    await batchUpdater.add(async () => {
      await logseq.Editor.upsertBlockProperty(
        block.uuid,
        'logseq.property/status',
        ':logseq.property/status.done'
      )
    })
  }

  logseq.UI.showMsg(`Completed ${todos.length} tasks!`, 'success')
}
```

> **Unverified:** the exact argument shape `upsertBlockProperty` expects for a closed-value ref
> could not be confirmed from `libs/src/LSPlugin.ts` — its signature is
> `(block, key: string, value: any)` (`:918-925`) and the host-side coercion is not documented in
> the type surface. Confirm against your own graph before running this over many blocks.

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

// Example transformer: Add timestamp to all blocks.
// BlockEntity.content is deprecated — read .title.
const addTimestampTransformer = async (block) => {
  return {
    content: block.title,
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

    // Check if already imported. :block/properties does not exist in a DB graph —
    // each property is its own attribute, keyed by the property entity's :db/ident.
    const existing = await logseq.DB.datascriptQuery(
      `[:find (pull ?b [:block/uuid])
        :in $ ?prop-title ?id
        :where
        [?prop :block/tags :logseq.class/Property]
        [?prop :block/title ?prop-title]
        [?prop :db/ident ?ident]
        [?b ?ident ?pv]
        [?pv :logseq.property/value ?id]]`,
      'github-id',
      issue.number
    )

    if (existing.length === 0) {
      await logseq.Editor.appendBlockInPage(tasksPage.uuid, taskContent)
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
    // No prompt API exists. Read the values from plugin settings instead.
    const { githubRepo, githubToken } = logseq.settings ?? {}
    if (!githubRepo || !githubToken) {
      return logseq.UI.showMsg('Set the GitHub repo and token in plugin settings', 'warning')
    }
    await syncGitHubIssues(githubRepo, githubToken)
  }
)

// Declare the settings fields once, at plugin start.
logseq.useSettingsSchema([
  { key: 'githubRepo', type: 'string', title: 'GitHub repo', description: 'user/repo', default: '' },
  { key: 'githubToken', type: 'string', title: 'GitHub token', description: 'Personal access token', default: '' },
])
```

> **Security note.** A token you put in plugin settings is stored in plaintext by the host
> (`libs/src/LSPlugin.core.ts:595`) and readable by any code running in the plugin frame — including
> the plugin's own npm dependencies. See
> [Plugin Security Architecture](../../explanation/architecture/plugin-security.md).

**Example 2: Export to Notion**

```javascript
// Export page to Notion
async function exportToNotion(pageName, notionToken, databaseId) {
  const blocks = await logseq.Editor.getPageBlocksTree(pageName)

  // Convert to Notion format. BlockEntity.content is deprecated — use .title.
  const notionBlocks = (blocks ?? []).map((block) => ({
    object: 'block',
    type: 'paragraph',
    paragraph: {
      rich_text: [
        {
          type: 'text',
          text: { content: block.title },
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

> **Unverified:** Logseq exposes no inbound webhook endpoint to plugins. A plugin can only make
> outbound requests; anything inbound requires a service you run yourself that the plugin polls, or
> a plugin process outside the SDK. The code below is a client-side dispatcher for a payload you
> fetched — not a listener.

```javascript
// In Logseq, register handler
logseq.App.registerCommand(
  'setup-webhooks',
  {
    key: 'automation-webhooks',
    label: 'Setup Webhooks',
  },
  async () => {
    // Read the URL from plugin settings; persist plugin state with updateSettings,
    // not with App.setUserConfigs (which does not exist).
    const webhookUrl = logseq.settings?.webhookUrl
    if (!webhookUrl) {
      return logseq.UI.showMsg('Set the webhook URL in plugin settings', 'warning')
    }

    logseq.updateSettings({ webhookConfiguredAt: Date.now() })
    logseq.UI.showMsg('Webhook configured', 'success')
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

  await logseq.Editor.appendBlockInPage(today.uuid, data.title, {
    properties: { source: 'webhook', details: data.details },
  })

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
        errorLog.uuid,
        `${new Date().toISOString()}: ${context} - ${error.message}`
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

    await logseq.Editor.appendBlockInPage(logPage.uuid, logEntry)

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

`logseq.App.getUserConfigs()` (`libs/src/LSPlugin.ts:412`) is **read-only** — it returns the app's
`AppUserConfigs` (`preferredThemeMode`, `preferredDateFormat`, `currentGraph`, …). There is no
`setUserConfigs`. Plugin state belongs in plugin settings.

```javascript
// Configuration system, built on the plugin settings API
const Config = {
  get(key, defaultValue = null) {
    return logseq.settings?.[key] ?? defaultValue
  },

  set(key, value) {
    logseq.updateSettings({ [key]: value })
  },

  getAll() {
    return logseq.settings ?? {}
  },

  // App-level, read-only: theme mode, date format, current graph, etc.
  async appConfigs() {
    return await logseq.App.getUserConfigs()
  },
}

// Usage
Config.set('taskProcessingEnabled', true)
Config.set('schedules', schedules)

const enabled = Config.get('taskProcessingEnabled', false)
```

Keys are flat — `updateSettings` merges the object you pass into the stored settings
(`libs/src/LSPlugin.ts:1206-1210`), so dotted names like `automation.taskProcessing.enabled` are
stored as literal keys, not nested paths. To write the **graph's** `config.edn` instead, use
`logseq.App.setCurrentGraphConfigs(configs)` (`:510`).

### Testing

```javascript
// Test suite for automation workflows
const AutomationTests = {
  async runAll() {
    console.log('Running automation tests...')

    await this.testTaskProcessing()
    await this.testTemplateApply()
    await this.testBatchOperations()

    console.log('All tests complete!')
  },

  async testTaskProcessing() {
    console.log('Test: Task Processing')

    // Create test tasks
    const testPage = await logseq.Editor.createPage(`Test-${Date.now()}`)

    await logseq.Editor.appendBlockInPage(testPage.uuid, 'Test task #urgent')

    // Run processor
    await processTaskWorkflow()

    // Verify results
    // (Add assertions)

    // Cleanup — deletePage takes a page name (libs/src/LSPlugin.ts:838)
    await logseq.Editor.deletePage(testPage.title ?? testPage.name)

    console.log('✓ Task processing test passed')
  },

  async testTemplateApply() {
    console.log('Test: Template Apply')

    if (!(await logseq.App.existTemplate('work-task'))) {
      throw new Error('Fixture template "work-task" is missing')
    }

    const page = await logseq.Editor.createPage(`Test-tpl-${Date.now()}`)
    await logseq.App.insertTemplate(page.uuid, 'work-task')

    const tree = await logseq.Editor.getPageBlocksTree(page.title ?? page.name)
    if (!tree || tree.length === 0) {
      throw new Error('Template did not insert anything')
    }

    await logseq.Editor.deletePage(page.title ?? page.name)
    console.log('✓ Template apply test passed')
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

You've built an automation system on APIs that exist. 🎉

**What You've Built:**

✅ DB-graph advanced queries for dashboards, using closed-value status/priority refs and
   millisecond datetime comparisons
✅ Rule-based auto-tagging driven by `logseq.DB.onChanged`
✅ A custom plugin registering slash commands, palette commands and a toolbar item
✅ Interval-based reminders (in-session only)
✅ Batch operations over query results
✅ A GitHub sync driven from plugin settings
✅ Error handling and logging into the graph

**Key Concepts:**

- **Automation layers:** advanced queries → templates → plugin commands → integrations
- **Event-driven:** `logseq.DB.onChanged`, `logseq.DB.onBlockChanged`, the `IAppProxy` hooks
- **State lives in plugin settings**, not in app config — `logseq.updateSettings` / `logseq.settings`
- **DB-graph attributes**, not file-graph ones: `:block/title` not `:block/content`,
  `:logseq.property/status` refs not `:block/marker` strings

**What the SDK does not give you** (verified absent at `be800f1711`):

- No prompt or picker (`IUIProxy` has six members — `libs/src/LSPlugin.ts:1032-1043`)
- No writable app config (`getUserConfigs` is read-only — `:412`)
- No page-created hook (`:559-605` is the complete hook list)
- No cron, no background execution while the app is closed
- No inbound webhook endpoint
- No sandbox around any of it — see
  [Plugin Security Architecture](../../explanation/architecture/plugin-security.md)

**Next Steps:**

1. **Expand workflows** with your specific needs
2. **Share plugins** with the community
3. **Optimize performance** on large graphs — every `datascriptQuery` runs over the whole graph
4. **Build settings-driven UI** with `useSettingsSchema` and `provideUI`

**Resources:**

- [Plugin API Reference](../../reference/api/plugin-api.md)
- [Advanced Queries Tutorial](./advanced-queries-tutorial.md)
- [`libs/src/LSPlugin.ts`](../../../../../libs/src/LSPlugin.ts) — the authoritative type surface
- [`libs/guides/`](../../../../../libs/guides/) — in-repo SDK guides
- [Plugin Examples](https://github.com/logseq/logseq-plugin-samples)
- [Community Plugins](https://github.com/logseq/marketplace)

**Tips for Automation Success:**

- Start small, automate one pain point at a time
- Check every API against `libs/src/LSPlugin.ts` before you build on it — a plausible name is not
  an existing method
- Back up before running any batch operation
- Log everything for debugging
- Handle errors gracefully

Happy automating! 🤖✨
