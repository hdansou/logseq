---
title: 'Your First Graph'
status: 🟢
since-version: 'N/A'
quadrant: 'Tutorial'
last-verified: '2026-01-11'
platform: 'All'
related-docs:
  - 'tutorials/getting-started/01-installation.md'
  - 'tutorials/getting-started/03-basic-editing.md'
  - 'explanation/concepts/file-vs-db.md'
---

# Your First Graph

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

### Step 2: Choose Graph Type

Logseq offers two graph modes:

#### Database Mode (Recommended for New Users)

- ✅ Better performance with large graphs
- ✅ Advanced features (semantic search, better queries)
- ✅ Automatic schema management
- ⚠️ Less portable (exports to standard formats available)

#### File-Based Mode (Markdown/Org)

- ✅ Human-readable markdown files
- ✅ Easy to sync with Git or cloud storage
- ✅ Works with external text editors
- ⚠️ Performance can degrade with very large graphs

**For this tutorial, we'll use Database Mode.**

### Step 3: Create the Graph

**Desktop:**

1. Click **"Create new graph"** on the welcome screen
2. Choose **"Database graph"**
3. Enter a name (e.g., "My Knowledge Base")
4. Click **"Create"**

**Mobile:**

1. Tap **"+"** or **"Create new graph"**
2. Select **"Database graph"**
3. Enter a name
4. Tap **"Create"**

**File-Based Alternative:**

1. Choose **"Local folder"** instead
2. Select or create a folder on your device
3. Logseq will initialize the folder with config files

### Step 4: First Look at Your Graph

You should now see:

- **Main Content Area:** The central panel for editing
- **Left Sidebar:** Navigation menu (journals, favorites, recent pages)
- **Right Sidebar:** Context panel (backlinks, page graph, etc.)
- **Top Bar:** Search, settings, and graph switcher

## Understanding the Interface

### The Journal

By default, Logseq opens your **daily journal**. Each day gets a new journal page automatically (e.g., "Jan 11th, 2026").

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

#### 3. Links

Connections between pages using `[[Page Name]]` syntax.

```
- Reading [[Atomic Habits]] by [[James Clear]]
- Key insight: [[Habit stacking]] is powerful
```

This creates three page links: "Atomic Habits", "James Clear", and "Habit stacking".

## Your First Entry

Let's create your first content:

### Exercise 1: Write in Today's Journal

1. Click in the main content area (you should see today's date as the title)
2. Click the first bullet point or press `Enter`
3. Type: `Welcome to my knowledge base!`
4. Press `Enter` to create a new block
5. Type: `I'm learning how to use [[Logseq]]`

Notice how "Logseq" became a clickable link? You just created a page link!

### Exercise 2: Create Your First Page

1. Click on the `[[Logseq]]` link you just created
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

1. Open the **right sidebar** (click the expand icon `>` if hidden)
2. You should see a section called **"Linked References"**
3. You'll see your journal entry that mentioned "Logseq"

This is **bidirectional linking** in action! When you link to a page, that page automatically knows what links to it.

## Graph Types Comparison

| Feature               | Database Mode                     | File-Based Mode                 |
| --------------------- | --------------------------------- | ------------------------------- |
| **Storage**           | SQLite database                   | Markdown/Org files              |
| **Performance**       | Excellent, even with 100k+ blocks | Good, can slow with 50k+ blocks |
| **Semantic Search**   | ✅ Yes                            | ❌ No                           |
| **Query Performance** | Fast                              | Slower with large graphs        |
| **External Editors**  | ❌ No direct editing              | ✅ Use any text editor          |
| **Git Integration**   | Via export                        | Native                          |
| **Portability**       | Export to markdown/JSON           | Native markdown                 |
| **Schema Management** | Automatic                         | Manual                          |

**When to choose each:**

- **Database:** You want the best performance and advanced features
- **File-Based:** You need Git integration or external editor support

> 💡 **Can I switch later?** Yes! Logseq provides import/export tools to migrate between graph types.

## Quick Tips

### Navigation Shortcuts

- `Ctrl/Cmd + K`: Quick search (command palette)
- `Ctrl/Cmd + J`: Jump to journal (today)
- `Ctrl/Cmd + Shift + F`: Full-text search
- `G + H`: Go home (journal)

### Creating Content

- `Enter`: New block at same level
- `Tab`: Indent block (make it a child)
- `Shift + Tab`: Outdent block (move it up a level)
- `[[`: Start typing to create/link a page

### Organizing

- `/page` or `[[`: Create/link pages
- `#`: Create/link tags (same as pages)
- `TODO` or `/task`: Create a task

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
- [Block vs Page Model](../../explanation/concepts/block-model.md) - Understanding Logseq's structure
