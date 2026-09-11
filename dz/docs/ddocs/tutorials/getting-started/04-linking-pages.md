---
title: 'Linking Pages'
status: 🟢
since-version: 'N/A'
quadrant: 'Tutorial'
last-verified: '2026-09-11'
verified-against: 'be800f1711'
review-status: audited
platform: 'All'
related-docs:
  - 'tutorials/getting-started/03-basic-editing.md'
  - 'explanation/concepts/linked-references.md'
  - 'explanation/concepts/graph-visualization.md'
---

> **Last revised:** 2026-09-11

# Linking Pages

Learn how to connect your notes through links, tags, and references to build a knowledge graph.

**Time required:** 15-20 minutes

## Why Linking Matters

Traditional note-taking organizes information hierarchically (folders → files). Logseq uses **networked thinking** where notes connect like a web, mirroring how your brain actually works.

**Benefits:**

- **Discover connections** between seemingly unrelated ideas
- **Reduce duplication** by reusing existing notes
- **Build context** by seeing all references to a concept
- **Emergent insights** from unexpected link paths

## Creating Links

### Page Links: `[[Page Name]]`

The most common way to connect notes:

```
- I'm reading [[Atomic Habits]] by [[James Clear]]
- The concept of [[Habit Stacking]] is powerful
```

**What happens:**

- Saving the block creates the pages "Atomic Habits", "James Clear" and "Habit Stacking" if they don't exist yet ([`deps/outliner/src/logseq/outliner/core.cljs:197-208`](../../../../../deps/outliner/src/logseq/outliner/core.cljs))
- Links are clickable to navigate
- Pages show backlinks to sources that mention them

**Keyboard shortcut:** Type `[[`. Logseq adds the closing `]]` and opens a search ([`src/main/frontend/handler/editor.cljs:1943-1947`](../../../../../src/main/frontend/handler/editor.cljs)). Keep typing: the popup lists matching pages, plus a **New page** item when no page has that exact name ([`src/main/frontend/components/editor.cljs:174-190`](../../../../../src/main/frontend/components/editor.cljs)).

### Tags: `#tag-name`

```
- Working on #project-alpha
- Meeting notes #important #review-later
```

In a DB graph, a tag is not just a page link with different syntax. The `#` form **tags the block**: the parser collects hashtags into the block's tags ([`deps/graph-parser/src/logseq/graph_parser/block.cljs:534-537`](../../../../../deps/graph-parser/src/logseq/graph_parser/block.cljs)), and a tag that doesn't exist yet is created as a class ([`deps/outliner/src/logseq/outliner/core.cljs:199-208`](../../../../../deps/outliner/src/logseq/outliner/core.cljs)). When you type `#`, the popup offers **New tag** for a new name ([`src/main/frontend/components/editor.cljs:184-188`](../../../../../src/main/frontend/components/editor.cljs); `en.edn:500`). A `[[page]]` link only references the page.

> **Removed:** "File-based graphs: `#project` and `[[project]]` are equivalent". File-based graphs can no longer be opened ([`src/main/frontend/components/repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs)).

**When to use each:**

- **`[[Links]]`:** For referencing entities, concepts, people (nouns)
- **`#tags`:** For categorizing blocks with types (e.g. `#Book`, `#Meeting`)

### Block References: `[[<block-uuid>]]`

Reference specific blocks. A block reference uses page-link brackets with the block's UUID inside ([`src/main/frontend/util/ref.cljs:5-7`](../../../../../src/main/frontend/util/ref.cljs)).

**Create:**

1. Right-click the target block's bullet and choose **Copy block ref** ([`src/main/frontend/components/content.cljs:288-292`](../../../../../src/main/frontend/components/content.cljs)). Or, while editing that block with no text selected, press `Ctrl/Cmd + C` ([`src/main/frontend/handler/editor.cljs:3662-3666`](../../../../../src/main/frontend/handler/editor.cljs))
2. Click into the block where the reference should go
3. Paste

**Example:**

```
- Original: The key to habits is making them obvious
- Reference: [[3f2f9a5c-1a94-4ec7-a57f-31f9f13b5f9d]]
  - Logseq resolves the UUID to the original block
```

Logseq resolves the UUID inside `[[...]]` to the referenced block ([`src/main/frontend/components/block.cljs:1320-1337`](../../../../../src/main/frontend/components/block.cljs)).

> **Removed:** `((block-id))` block references. Typing `((` shows the warning "To reference a node, please use `[[]]`." ([`src/main/frontend/handler/editor.cljs:1949-1952`](../../../../../src/main/frontend/handler/editor.cljs)), and `((uuid))` text renders literally ([`src/main/frontend/components/block.cljs:1556-1557`](../../../../../src/main/frontend/components/block.cljs)).

**Use cases:**

- Quote from another note
- Reuse content without copying
- Track where ideas are discussed

### Block Embeds: `/Node embed`

Shows a block in place, as a block of its own:

1. In an empty block, type `/Node embed`
2. Search for the block and pick it. Logseq inserts a new block that embeds it ([`src/main/frontend/components/editor.cljs:289-307`](../../../../../src/main/frontend/components/editor.cljs))

`Ctrl/Cmd + Shift + E` copies the block you're editing as an embed. Pasting it into another block creates the embed ([`src/main/frontend/handler/paste.cljs:219-232`](../../../../../src/main/frontend/handler/paste.cljs)).

> **Removed:** `{{embed ((block-id))}}`. The `{{embed}}` macro now renders "{{embed}} is deprecated. Use '/Node embed' command instead." ([`src/main/frontend/components/block.cljs:1917-1918`](../../../../../src/main/frontend/components/block.cljs)).

> **Unverified:** That an embed shows the embedded block's children.

## Bidirectional Linking

The magic of Logseq: **links work both ways**.

### Example

**Page A (Journal - Jan 11th):**

```
- Met with [[Sarah Johnson]] about [[Project Titan]]
- Key decision: Use [[Database Mode]] for better performance
```

**Page B (Sarah Johnson):**

- Automatically shows under "Linked references":
  - "Met with Sarah Johnson about Project Titan" (from Jan 11th journal)

**You didn't need to:**

- Add a backlink manually
- Remember to update both pages
- Maintain a list of references

**Logseq tracks everything automatically.**

### Viewing Backlinks

**Linked references appear at the bottom of the page** ([`src/main/frontend/components/page.cljs:521-529`](../../../../../src/main/frontend/components/page.cljs); label `en.edn:1920`):

1. Navigate to the page you want to explore
2. Scroll down past the page content
3. The "Linked references" section shows all blocks that reference this page

**Example for "Logseq" page:**

```
Linked references:
- From "Jan 11th, 2026":
  - "I'm learning how to use [[Logseq]]"
- From "Tools" page:
  - "My knowledge management tool is [[Logseq]]"
```

To narrow a long list, use the section's **Page filter** button to include or exclude source pages ([`src/main/frontend/components/reference.cljs:14-44`](../../../../../src/main/frontend/components/reference.cljs); `en.edn:1633`).

## Link Types Compared

| Type                | Syntax             | Creates             | Use Case             |
| ------------------- | ------------------ | ------------------- | -------------------- |
| **Page Link**       | `[[Page]]`         | New/existing page   | Concepts, entities   |
| **Tag**             | `#tag`             | Tag (class) on the block | Categories, types |
| **Block Reference** | `[[<block-uuid>]]` (paste from **Copy block ref**) | Reference to block  | Quote, reuse content |
| **Block Embed**     | `/Node embed`      | Block that embeds another | Show full context    |
| **URL**             | `[Text](url)`      | External link       | Web resources        |

## Building Your Knowledge Graph

### Principle 1: Link As You Write

Don't worry about perfect organization. Link naturally:

```
✅ Natural linking:
- Read about [[Zettelkasten]] today
- It's similar to [[Building a Second Brain]]
- Both emphasize [[Progressive Summarization]]

❌ Over-thinking:
- (Should I create a "Methods" folder first?
   What category does this belong to?)
```

**The graph emerges from your links, not pre-planned structure.**

### Principle 2: Use Hub Pages

Create central pages for major topics:

**Example: "Productivity" hub page:**

```
- # Productivity

  - ## Methods
    - [[Getting Things Done (GTD)]]
    - [[Pomodoro Technique]]
    - [[Time Blocking]]

  - ## Tools
    - [[Logseq]]
    - [[Todoist]]

  - ## Principles
    - [[Deep Work]]
    - [[Attention Management]]
```

### Principle 3: Link Related Concepts

When you discover connections, link them:

```
- [[Compounding]] from finance applies to [[Habit Formation]]
  - Small daily improvements compound
  - 1% better each day = 37x better in a year
  - Similar to [[Interest]] in investing
```

## Practical Linking Patterns

Structured fields like "role" or "author" are **properties**. Add them with `/Add property` ([`src/main/frontend/commands.cljs:354-358`](../../../../../src/main/frontend/commands.cljs)), not by typing `key:: value`. A property of type `node` links to pages ([`deps/db/src/logseq/db/frontend/property/type.cljs:21`](../../../../../deps/db/src/logseq/db/frontend/property/type.cljs)).

### Pattern 1: Person → Projects

```
- [[John Smith]] (Engineer) works on [[Project Alpha]] and [[Project Beta]]
- [[Jane Doe]] joined [[Project Alpha]]
```

For structure, open the John Smith page and add a **role** property and a **projects** property (type `node`) pointing at Project Alpha and Project Beta.

### Pattern 2: Book → Concepts

```
- [[Atomic Habits]] #Book by [[James Clear]]
  - Concepts: [[Habit Stacking]], [[Identity-Based Habits]]
- [[Habit Stacking]] comes from [[Atomic Habits]] and relates to [[Tiny Habits]]
```

### Pattern 3: Daily Notes → Topics

In today's journal:

```
- Meeting about [[Product Launch]] with [[Sarah]] and [[Mike]]
  - Decided on a [[Mar 1st, 2026]] launch date
  - Key concern: [[Marketing Strategy]]
```

> **Removed:** `role::`, `projects::`, `team::`, `author::`, `concepts::`, `source::`, `related::` and `attendees::` property lines. They are file-graph syntax.

## Advanced Linking Techniques

### Namespace Pages

Use `/` for hierarchical grouping:

```
- [[Books/Fiction/The Great Gatsby]]
- [[Books/Non-Fiction/Atomic Habits]]
- [[Projects/2026/Website Redesign]]
```

Type `[[`, then the full title, then pick **New page**. Logseq splits the title into one page per segment (`Books`, `Fiction`, `The Great Gatsby`) and organizes them under **Library** ([`src/main/frontend/handler/page.cljs:281`](../../../../../src/main/frontend/handler/page.cljs) passes `:split-namespace? true`; [`deps/outliner/src/logseq/outliner/page.cljs:385-388`](../../../../../deps/outliner/src/logseq/outliner/page.cljs), with the Library parent at `:227-235`).

> **Removed:** "In DB graphs, creating new page names with `/` is no longer supported." The outliner splits `/` titles into per-segment pages, as cited above.

> **Unverified:** What happens when you type a `/` link and save the block without picking **New page**. That save path creates the page without passing `split-namespace?` ([`deps/outliner/src/logseq/outliner/core.cljs:205-208`](../../../../../deps/outliner/src/logseq/outliner/core.cljs)), so the title may not be split.

**Benefits:**

- Logical grouping under **Library**

> **Unverified:** "Auto-complete suggests related pages" and "Can query by namespace". No source confirming either was found.

### Link Text

Show different text for a page link:

```
- Reading [Habits book]([[Atomic Habits]])
- Also known as [James Clear's habit book]([[Atomic Habits]])
```

**Syntax:** `[Display Text]([[Actual Page]])`. It renders a link labelled with your text that points to the page ([`src/main/frontend/components/block.cljs:1559-1563`](../../../../../src/main/frontend/components/block.cljs)).

### Aliases

To give a page alternative names, add the built-in **Alias** property to it with `/Add property` ([`deps/db/src/logseq/db/frontend/property.cljs:68-69`](../../../../../deps/db/src/logseq/db/frontend/property.cljs)).

> **Removed:** Calling `[Display Text]([[Actual Page]])` an alias. It is a labelled link; aliases are the **Alias** property.

> **Unverified:** How alias names behave in links and search.

### Unlinked References

Logseq finds mentions even without `[[]]`. A block counts as an unlinked reference when its text contains the page title, ignoring case, and it doesn't already reference the page ([`deps/db/src/logseq/db/common/reference.cljs:309-326`](../../../../../deps/db/src/logseq/db/common/reference.cljs)). The **Unlinked references** section sits below Linked references on regular pages. It is not shown in the sidebar, on the home view, or on tag and property pages ([`src/main/frontend/components/page.cljs:531-537`](../../../../../src/main/frontend/components/page.cljs)).

**On "Logseq" page:**

```
Unlinked references:
- "I started using Logseq today" (no [[]])
- "logseq is amazing" (lowercase, no [[]])
```

**Convert to links:** Edit the block and wrap the mention in `[[ ]]`.

> **Removed:** The old per-item "Link" action. The unlinked references component has no such action ([`src/main/frontend/components/reference.cljs:64-75`](../../../../../src/main/frontend/components/reference.cljs)).

## Visualizing Your Graph

### Graph View

See your knowledge network visually:

**Open Graph View:**

- Click **Graph view** in the left sidebar ([`src/main/frontend/components/left_sidebar.cljs:316`](../../../../../src/main/frontend/components/left_sidebar.cljs))
- Or press `g g` (press `g` twice while not editing a block) ([`src/main/frontend/modules/shortcut/config.cljs:396`](../../../../../src/main/frontend/modules/shortcut/config.cljs))

> **Unverified:** What the graph view draws (nodes as pages, links as lines, clusters) and how you interact with it (click, hover, zoom, pan). This audit didn't trace the graph renderer.

### Page Graph

See connections for a single page:

**In right sidebar:**

1. Open any page
2. Click **Page graph** in the right sidebar top bar ([`src/main/frontend/components/right_sidebar.cljs:519-526`](../../../../../src/main/frontend/components/right_sidebar.cljs))
3. A **Page graph** panel for the current page appears in the sidebar ([`src/main/frontend/components/right_sidebar.cljs:126-128`](../../../../../src/main/frontend/components/right_sidebar.cljs))

## Practical Exercise

Build a small knowledge network:

**Step 1: Create a reading list**

```
- # Reading List 2026
  - [[Thinking, Fast and Slow]] #Book
    - by [[Daniel Kahneman]], about [[Cognitive Bias]] and [[Decision Making]]
  - [[Deep Work]] #Book
    - by [[Cal Newport]], about [[Focus]] and [[Productivity]]
```

**Step 2: Explore a concept**

```
- # Cognitive Bias
  - Found in [[Thinking, Fast and Slow]]
  - Related to [[System 1 vs System 2 Thinking]]
  - Examples:
    - [[Confirmation Bias]]
    - [[Anchoring Effect]]
    - [[Availability Heuristic]]
```

**Step 3: Check backlinks**

1. Click on "Cognitive Bias" page
2. Scroll down to the "Linked references" section at the bottom
3. See it's referenced in "Reading List 2026"

**Step 4: View the graph**

1. Open Graph View
2. Find the cluster of reading-related notes
3. See how concepts connect

## Link Management Tips

### Renaming Pages

When you rename a page, every link to it shows the new name:

1. Open the page
2. Click the page title
3. Edit the name
4. Click outside the title to save

Saving a changed page title updates the page's name ([`deps/outliner/src/logseq/outliner/core.cljs:362-372`](../../../../../deps/outliner/src/logseq/outliner/core.cljs)). Links are stored as `[[<page-uuid>]]` rather than `[[page name]]` ([`deps/db/src/logseq/db/frontend/content.cljs:129-130`](../../../../../deps/db/src/logseq/db/frontend/content.cljs)), so nothing needs rewriting.

> **Removed:** "All `[[old name]]` links become `[[new name]]` throughout your graph." Links are stored by UUID, not rewritten as text.

### Merging Pages

If you have duplicate pages:

1. Copy content from duplicate to main page
2. Delete the duplicate page
3. Manually review references and relink where needed

> **Removed:** Automatic "merge pages"/auto-retarget behavior is not available.

> **Removed:** The "Finding Broken Links" section (non-existent pages styled differently). In a DB graph, saving a block creates any page its `[[links]]` point to ([`deps/outliner/src/logseq/outliner/core.cljs:197-208`](../../../../../deps/outliner/src/logseq/outliner/core.cljs)), so a link never points to a missing page.

## Common Mistakes to Avoid

### ❌ Over-Linking

```
Bad:
- [[I]] [[am]] [[reading]] [[a]] [[book]] [[about]] [[productivity]]
```

**Link meaningfully:**

```
Good:
- I am reading a [[book]] about [[productivity]]
```

### ❌ No Linking

```
Bad:
- Taking notes from Atomic Habits
- The book talks about habits
- Author is James Clear
```

**Create connections:**

```
Good:
- Taking notes from [[Atomic Habits]]
- The [[book]] talks about [[habits]]
- Author is [[James Clear]]
```

### ❌ Link Everything Immediately

Build links naturally as you write. Don't force it.

## What You've Learned

✅ Create page links with `[[]]`  
✅ Tag blocks with `#`  
✅ Reference blocks with `[[<block-uuid>]]` via **Copy block ref**  
✅ Understand bidirectional linking  
✅ View backlinks at the bottom of a page  
✅ Visualize connections in Graph View  
✅ Apply practical linking patterns

## Findings: New Capabilities

- **Linked reference filters:** The Linked references section has a **Page filter** that includes or excludes source pages ([`src/main/frontend/components/reference.cljs:14-44`](../../../../../src/main/frontend/components/reference.cljs)).
- **Dedicated Page Graph panel:** You can add a per-page graph from the right sidebar top bar using **Page graph** ([`src/main/frontend/components/right_sidebar.cljs:519-526`](../../../../../src/main/frontend/components/right_sidebar.cljs)).
- **Tags are classes:** `#tag` creates a tag class and tags the block ([`deps/outliner/src/logseq/outliner/core.cljs:199-208`](../../../../../deps/outliner/src/logseq/outliner/core.cljs)).
- **Namespace titles split into pages under Library** ([`deps/outliner/src/logseq/outliner/page.cljs:385-388`](../../../../../deps/outliner/src/logseq/outliner/page.cljs)).

## Next Steps

You now have the foundation to use Logseq effectively! Explore these topics:

**Knowledge Management:**

- [Flashcards Basics](../knowledge-management/flashcards-basics.md)

**Task Management:**

- [Todo System Basics](../task-management/todo-basics.md)

**Advanced Features:**

- [Quick Capture](../../how-to/content/quick-capture.md)
- [Advanced Queries](../../how-to/queries/advanced-queries.md)

## Further Reading

- [Linked References Explained](../../explanation/concepts/linked-references.md)
- [Graph Visualization](../../explanation/concepts/graph-visualization.md)
