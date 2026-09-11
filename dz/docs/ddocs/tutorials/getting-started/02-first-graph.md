---
title: 'Your First Graph'
status: 🟢
since-version: 'N/A'
quadrant: 'Tutorial'
last-verified: '2026-09-11'
verified-against: 'be800f1711'
review-status: audited
platform: 'All'
related-docs:
  - 'tutorials/getting-started/01-installation.md'
  - 'tutorials/getting-started/03-basic-editing.md'
  - 'explanation/concepts/file-vs-db.md'
---

# Your First Graph

> **Last revised:** 2026-09-11

In this tutorial, you'll create your first Logseq graph and understand the basic concepts of graphs, pages, and blocks.

**Time required:** 5-10 minutes

## What is a Graph?

In Logseq, a **graph** is your personal knowledge base. It's like a notebook, but instead of linear pages, your notes are interconnected through links, forming a graph structure.

Think of it as:

- **Traditional notebook:** Linear pages, one after another
- **Logseq graph:** Interconnected pages that reference each other

## Creating Your First Graph

### Step 1: Launch Logseq

Open the Logseq application you installed in the previous tutorial.

### Step 2: Understand the Graph Type

Every graph you create is a **DB graph** (database graph). On desktop, each graph is a single `db.sqlite` database under `~/logseq/graphs` ([`deps/common/src/logseq/common/config.cljs:55`](../../../../../deps/common/src/logseq/common/config.cljs), [`deps/db/src/logseq/db/common/sqlite.cljs:29-33`](../../../../../deps/db/src/logseq/db/common/sqlite.cljs)), not a folder of Markdown files.

On first launch Logseq has already created a DB graph named `Demo` and opened it ([`src/main/frontend/handler.cljs:189-190`](../../../../../src/main/frontend/handler.cljs)). You can practise in `Demo`, but it's hidden from the **All graphs** list ([`src/main/frontend/components/repo.cljs:319`](../../../../../src/main/frontend/components/repo.cljs)), so create a named graph of your own for real notes.

#### DB Graph

- ✅ Typed properties and tag (class) based workflows
- ✅ Import from SQLite exports, EDN exports and file-based graphs

> **Removed:** "File-Based Mode (Markdown/Org)" as a graph you can create or open. The graph list keeps only DB graphs ([`src/main/frontend/components/repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs)); file-based notes come in through import.

> **Removed:** "Better performance with large graphs" and "Automatic schema management" as DB-graph selling points. No source backs a performance comparison, and file graphs can no longer be opened to compare against.

**For this tutorial, we'll use a DB graph.**

### Step 3: Create the Graph

**Desktop:**

1. Click the graph name at the top of the left sidebar to open the graph switcher ([`src/main/frontend/components/left_sidebar.cljs:164`](../../../../../src/main/frontend/components/left_sidebar.cljs))
2. Click **Create db graph** at the bottom of the menu. In the web version it reads **Create a new graph** ([`src/main/frontend/components/repo.cljs:427-431`](../../../../../src/main/frontend/components/repo.cljs); labels in `src/resources/dicts/en.edn:731`, `:733`). The **All graphs** page (`g` then `Shift+G`) has the same **Create a new graph** button ([`src/main/frontend/components/repo.cljs:341-343`](../../../../../src/main/frontend/components/repo.cljs), shortcut [`src/main/frontend/modules/shortcut/config.cljs:399`](../../../../../src/main/frontend/modules/shortcut/config.cljs))
3. A **Create a new graph** dialog opens ([`src/main/frontend/handler/events/ui.cljs:372-376`](../../../../../src/main/frontend/handler/events/ui.cljs)). Type a name (e.g., "My Knowledge Base") in the **your graph name** field (`en.edn:760`)
4. Press `Enter` or click **Submit** ([`src/main/frontend/components/repo.cljs:724-730`](../../../../../src/main/frontend/components/repo.cljs); `en.edn:1900`)

Graph names can't contain `<`, `>`, `:`, `"`, `/`, `\`, `|`, `?`, `*`, `#` or `+` ([`src/main/frontend/components/repo.cljs:600-624`](../../../../../src/main/frontend/components/repo.cljs)). If your account has sync access, the dialog also shows a **Use Logseq Sync?** checkbox ([`src/main/frontend/components/repo.cljs:700-712`](../../../../../src/main/frontend/components/repo.cljs)). Leave it unchecked for this tutorial.

**Mobile:**

1. Tap the graph title in the header to open the graph switcher ([`src/main/mobile/components/header.cljs:192-194`](../../../../../src/main/mobile/components/header.cljs))
2. Tap **Create new graph** ([`src/main/mobile/components/header.cljs:124-134`](../../../../../src/main/mobile/components/header.cljs); `en.edn:1014`)
3. Enter a name and tap **Submit**

> **Removed:** "Choose Local folder" in first-graph onboarding steps.

If you already have file-based notes, choose **Import existing notes** from the same graph-switcher menu ([`src/main/frontend/components/repo.cljs:436-438`](../../../../../src/main/frontend/components/repo.cljs); `en.edn:940`), then **File to DB graph** (`en.edn:927`).

### Step 4: First Look at Your Graph

You should now see:

- **Main Content Area:** The central panel for editing. With no custom home page set, it shows your journals ([`src/main/frontend/components/container.cljs:178-180`](../../../../../src/main/frontend/components/container.cljs))
- **Left Sidebar:** The graph switcher at the top ([`left_sidebar.cljs:164`](../../../../../src/main/frontend/components/left_sidebar.cljs)), then **Navigations** with **Journals**, **Flashcards**, **Graph view** and **Pages**, then **Favorites** and **Recent** ([`src/main/frontend/components/left_sidebar.cljs:259-386`](../../../../../src/main/frontend/components/left_sidebar.cljs))
- **Right Sidebar:** Its top bar has **Contents**, **Page graph** and **Help** buttons, and pages or blocks you open in the sidebar appear below them ([`src/main/frontend/components/right_sidebar.cljs:514-531`](../../../../../src/main/frontend/components/right_sidebar.cljs))
- **Header:** **Search** ([`src/main/frontend/components/header.cljs:563`](../../../../../src/main/frontend/components/header.cljs)) and a menu with **Settings**, **Plugins**, **Appearance**, **Recycle**, **Export graph** and **Import** ([`src/main/frontend/components/header.cljs:209-233`](../../../../../src/main/frontend/components/header.cljs))

## Understanding the Interface

### The Journal

By default, Logseq opens your **journals** ([`src/main/frontend/components/container.cljs:178-180`](../../../../../src/main/frontend/components/container.cljs)). Logseq creates today's journal page automatically ([`src/main/frontend/handler/page.cljs:321-336`](../../../../../src/main/frontend/handler/page.cljs)). Journal titles use the `MMM do, yyyy` format by default, e.g. "Jan 11th, 2026" ([`deps/common/src/logseq/common/util/date_time.cljs:10`](../../../../../deps/common/src/logseq/common/util/date_time.cljs)).

Think of the journal as your daily scratchpad where you can:

- Write quick notes
- Log tasks for the day
- Capture thoughts that you'll organize later

### Key Concepts

#### 1. Blocks

The fundamental unit in Logseq. Each bullet point is a block:

```
- This is a block
  - This is a nested block (child)
    - This is a deeper nested block (grandchild)
- This is another top-level block
```

#### 2. Pages

Collections of blocks, like traditional notes or documents. Pages have titles and can be linked from anywhere.

**Two types:**

- **Journal pages:** Auto-created daily (e.g., "Jan 11th, 2026")
- **Regular pages:** Created manually for topics (e.g., "Project Ideas", "Meeting Notes")

A new graph also starts with three built-in pages: **Library**, **Quick add** and **Contents** ([`deps/db/src/logseq/db/sqlite/create_graph.cljs:130-133`](../../../../../deps/db/src/logseq/db/sqlite/create_graph.cljs)).

#### 3. Links

Connections between pages using `[[Page Name]]` syntax.

```
- Reading [[Atomic Habits]] by [[James Clear]]
- Key insight: [[Habit stacking]] is powerful
```

This creates three page links: "Atomic Habits", "James Clear", and "Habit stacking". When you save the block, Logseq creates any linked page that doesn't exist yet ([`deps/outliner/src/logseq/outliner/core.cljs:197-208`](../../../../../deps/outliner/src/logseq/outliner/core.cljs)).

## Your First Entry

Let's create your first content:

### Exercise 1: Write in Today's Journal

1. Click in the main content area (you should see today's date as the title)
2. Click the empty row under today's date to start a block ([`src/main/frontend/components/page.cljs:226`](../../../../../src/main/frontend/components/page.cljs); clicking it inserts a block, `:76-80`)
3. Type: `Welcome to my knowledge base!`
4. Press `Enter` to create a new block
5. Type `I'm learning how to use `, then type `[[`. Logseq adds the closing `]]` and opens a page search ([`src/main/frontend/handler/editor.cljs:1943-1947`](../../../../../src/main/frontend/handler/editor.cljs))
6. Type `Logseq`. If no page by that name exists, the first popup item is **New page Logseq** ([`src/main/frontend/components/editor.cljs:174-190`](../../../../../src/main/frontend/components/editor.cljs); `en.edn:499`). Press `Enter` to pick it
7. Press `Esc` to finish editing ([`src/main/frontend/components/editor.cljs:754-757`](../../../../../src/main/frontend/components/editor.cljs))

Notice how "Logseq" became a clickable link? You just created a page link!

### Exercise 2: Create Your First Page

1. Click on the `Logseq` link you just created
2. You'll navigate to a new, empty "Logseq" page
3. Add some content:
   ```
   - Logseq is a knowledge management tool
   - It uses [[blocks]] instead of traditional pages
   - Key features I want to explore:
     - [[Queries]]
     - [[Bidirectional links]]
     - [[Graph view]]
   ```

### Exercise 3: See the Connection

1. Stay on the "Logseq" page and scroll below its content
2. You should see a section called **Linked references** ([`src/main/frontend/components/page.cljs:521-529`](../../../../../src/main/frontend/components/page.cljs); `en.edn:1920`)
3. You'll see your journal entry that mentioned "Logseq"

This is **bidirectional linking** in action! When you link to a page, that page automatically knows what links to it.

> **Removed:** "Open the right sidebar" to find Linked references. On a page, linked references render at the bottom of the page itself ([`src/main/frontend/components/page.cljs:521-529`](../../../../../src/main/frontend/components/page.cljs)).

## Graph Storage Notes (Current)

| Item                    | Current behavior |
| ----------------------- | ---------------- |
| **First graph creation** | A `Demo` DB graph is created automatically; you create named DB graphs from the graph switcher |
| **Storage format**       | One `db.sqlite` per graph ([`deps/db/src/logseq/db/common/sqlite.cljs:29-33`](../../../../../deps/db/src/logseq/db/common/sqlite.cljs)) |
| **Import paths**         | SQLite, SQLite + assets (.zip), File to DB graph, Debug Transit, EDN to DB graph ([`src/main/frontend/components/imports.cljs:572-628`](../../../../../src/main/frontend/components/imports.cljs)) |
| **Migration strategy**   | Use Import/Export workflows |

> **Removed:** Fixed performance thresholds like "100k+ blocks" and "50k+ blocks" in beginner guidance. Performance depends on graph shape, plugins, device, and sync conditions.

> **Removed:** Hard claim that "Semantic Search" is a guaranteed DB-only default capability in first-graph setup.

## Findings: New Capabilities

Validated against `origin/master` (`be800f1711`) on 2026-09-11:

- The Import page offers five entry points: **SQLite**, **SQLite + assets (.zip)**, **File to DB graph**, **Debug Transit** and **EDN to DB graph** ([`src/main/frontend/components/imports.cljs:572-628`](../../../../../src/main/frontend/components/imports.cljs)).
- Desktop and mobile both create graphs through the same DB-graph dialog ([`src/main/mobile/components/header.cljs:80-88`](../../../../../src/main/mobile/components/header.cljs) reuses `repo/new-db-graph`).
- Linked and unlinked references render at the bottom of page views ([`src/main/frontend/components/page.cljs:521-537`](../../../../../src/main/frontend/components/page.cljs)).

**When to choose what:**

- **Create first graph:** Use the graph switcher's create option.
- **Bring existing notes:** Use import options (especially File to DB graph).
- **Move data between graphs/systems:** Use export/import.

> 💡 **Can I switch later?** You can bring a file-based graph into a new DB graph with **File to DB graph**. You can't create or open a file-based graph ([`src/main/frontend/components/repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs)).

## Quick Tips

### Navigation Shortcuts

Shortcuts written as `g j` are sequences: press `g`, release it, then press `j`. They only work when you are not editing a block (the `:shortcut.handler/global-non-editing-only` group, [`config.cljs:703-706`](../../../../../src/main/frontend/modules/shortcut/config.cljs)).

- `Ctrl/Cmd + K`: Search ([`config.cljs:304`](../../../../../src/main/frontend/modules/shortcut/config.cljs))
- `Ctrl/Cmd + Shift + P`: Command palette ([`config.cljs:310`](../../../../../src/main/frontend/modules/shortcut/config.cljs))
- `g j`: Go to journals ([`config.cljs:328`](../../../../../src/main/frontend/modules/shortcut/config.cljs))
- `g h`: Go home, which is the journals unless you set a home page ([`config.cljs:390`](../../../../../src/main/frontend/modules/shortcut/config.cljs))

### Creating Content

- `Enter`: New block at same level ([`config.cljs:107`](../../../../../src/main/frontend/modules/shortcut/config.cljs))
- `Tab`: Indent block (make it a child) ([`config.cljs:220`](../../../../../src/main/frontend/modules/shortcut/config.cljs))
- `Shift + Tab`: Outdent block (move it up a level) ([`config.cljs:223`](../../../../../src/main/frontend/modules/shortcut/config.cljs))
- `[[`: Start typing to create/link a page

### Organizing

- `[[` or `/Node reference`: Create/link pages ([`src/main/frontend/commands.cljs:227-232`](../../../../../src/main/frontend/commands.cljs))
- `#`: Tag a block. In a DB graph a tag is a class, not the same thing as a page link. See [Linking Pages](04-linking-pages.md)
- `/Todo` (in the **TASK STATUS** group) or `Ctrl/Cmd + Enter`: Make the block a task ([`src/main/frontend/commands.cljs:262`](../../../../../src/main/frontend/commands.cljs), [`config.cljs:167`](../../../../../src/main/frontend/modules/shortcut/config.cljs))

> **Removed:** `/page` and `/task` slash commands and typing `TODO`. No slash command is named `page` or `task`. Task statuses are the six built-in values listed under **TASK STATUS** ([`deps/db/src/logseq/db/frontend/property.cljs:313-318`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)).

## What You've Learned

✅ Created your first graph  
✅ Understood blocks, pages, and links  
✅ Made your first journal entry  
✅ Created and linked pages  
✅ Saw bidirectional linking in action

## Next Steps

Now that you have a graph, let's learn how to edit and structure your content:

👉 **Next:** [Basic Note-Taking](03-basic-editing.md)

## Further Reading

- [File-Based vs Database Graphs](../../explanation/concepts/file-vs-db.md) - Deep dive into graph modes
- [Block Editing Philosophy](../../explanation/concepts/block-editing-philosophy.md) - Understanding Logseq's block structure
