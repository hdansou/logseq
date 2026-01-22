---
title: 'Linking Pages'
status: 🟢
since-version: 'N/A'
quadrant: 'Tutorial'
last-verified: '2026-01-11'
platform: 'All'
related-docs:
  - 'tutorials/getting-started/03-basic-editing.md'
  - 'explanation/concepts/bidirectional-linking.md'
  - 'explanation/concepts/knowledge-graph.md'
  - 'how-to/content/graph-view.md'
---

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

- Creates three pages: "Atomic Habits", "James Clear", "Habit Stacking"
- Links are clickable to navigate
- Pages show backlinks to sources that mention them

**Keyboard shortcut:** Type `[[` and start typing — Logseq suggests existing pages or creates new ones.

### Tags: `#tag-name`

Tags are just page links with different syntax:

```
- Working on #project-alpha
- Meeting notes #important #review-later
```

**Equivalence:**

- `#project` = `[[project]]`
- Both create the same page
- Use whichever feels more natural

**When to use each:**

- **`[[Links]]`:** For entities, concepts, people (nouns)
- **`#tags`:** For categories, statuses, labels (adjectives)

### Block References: `((block-id))`

Reference specific blocks:

**Create:**

1. Type `((`
2. Search for content in any block
3. Select from dropdown

**Example:**

```
- Original: The key to habits is making them obvious
- Reference: ((above-block))
  - This embeds the original block's content
```

**Use cases:**

- Quote from another note
- Reuse content without copying
- Track where ideas are discussed

### Block Embeds: `{{embed ((block-id))}}`

Shows block + all children:

```
- {{embed ((block-id))}}
  - Shows the block
    - And all nested content
```

## Bidirectional Linking

The magic of Logseq: **links work both ways**.

### Example

**Page A (Journal - Jan 11th):**

```
- Met with [[Sarah Johnson]] about [[Project Titan]]
- Key decision: Use [[Database Mode]] for better performance
```

**Page B (Sarah Johnson):**

- Automatically shows under "Linked References":
  - "Met with Sarah Johnson about Project Titan" (from Jan 11th journal)

**You didn't need to:**

- Add a backlink manually
- Remember to update both pages
- Maintain a list of references

**Logseq tracks everything automatically.**

### Viewing Backlinks

**Open right sidebar:**

1. Click the page you want to explore
2. Right sidebar shows "Linked References"
3. See ALL blocks that mention this page

**Example for "Logseq" page:**

```
Linked References:
- From "Jan 11th, 2026":
  - "I'm learning how to use [[Logseq]]"
- From "Tools" page:
  - "My knowledge management tool is [[Logseq]]"
```

## Link Types Compared

| Type                | Syntax             | Creates             | Use Case             |
| ------------------- | ------------------ | ------------------- | -------------------- |
| **Page Link**       | `[[Page]]`         | New/existing page   | Concepts, entities   |
| **Tag**             | `#tag`             | New/existing page   | Categories, labels   |
| **Block Reference** | `((text))`         | Reference to block  | Quote, reuse content |
| **Block Embed**     | `{{embed ((id))}}` | Embedded block tree | Show full context    |
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

### Pattern 1: Person → Projects

```
- [[John Smith]]
  role:: Engineer
  projects:: [[Project Alpha]], [[Project Beta]]

- [[Project Alpha]]
  team:: [[John Smith]], [[Jane Doe]]
```

### Pattern 2: Book → Concepts

```
- [[Atomic Habits]]
  author:: [[James Clear]]
  concepts:: [[Habit Stacking]], [[Identity-Based Habits]]

- [[Habit Stacking]]
  source:: [[Atomic Habits]]
  related:: [[Tiny Habits]]
```

### Pattern 3: Daily Notes → Topics

```
- [[Jan 11th, 2026]]
  - Meeting about [[Product Launch]]
    attendees:: [[Sarah]], [[Mike]]
    - Decided on [[March 1st]] launch date
    - Key concern: [[Marketing Strategy]]
```

## Advanced Linking Techniques

### Namespace Pages

Use `/` for hierarchical grouping:

```
- [[Books/Fiction/The Great Gatsby]]
- [[Books/Non-Fiction/Atomic Habits]]
- [[Projects/2026/Website Redesign]]
```

**Benefits:**

- Logical grouping
- Auto-complete suggests related pages
- Can query by namespace

### Aliases

Link to a page using different text:

```
- Reading [Habits book]([[Atomic Habits]])
- Also known as [James Clear's habit book]([[Atomic Habits]])
```

**Syntax:** `[Display Text]([[Actual Page]])`

### Unlinked References

Logseq finds mentions even without `[[]]`:

**On "Logseq" page:**

```
Unlinked References:
- "I started using Logseq today" (no [[]])
- "logseq is amazing" (lowercase, no [[]])
```

**Convert to links:** Click "Link" button next to unlinked reference

## Visualizing Your Graph

### Graph View

See your knowledge network visually:

**Open Graph View:**

- Click the graph icon in toolbar
- Or press `Ctrl/Cmd + Shift + G`

**What you see:**

- **Nodes:** Pages
- **Lines:** Links between pages
- **Clusters:** Related concepts

**Interactions:**

- Click nodes to navigate
- Hover to see page title
- Zoom and pan to explore

### Page Graph

See connections for a single page:

**In right sidebar:**

1. Open any page
2. Look for "Page Graph" section
3. See immediate connections

## Practical Exercise

Build a small knowledge network:

**Step 1: Create a reading list**

```
- # Reading List 2026
  - [[Book]] [[Thinking, Fast and Slow]]
    author:: [[Daniel Kahneman]]
    topics:: [[Cognitive Bias]], [[Decision Making]]
  - [[Book]] [[Deep Work]]
    author:: [[Cal Newport]]
    topics:: [[Focus]], [[Productivity]]
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
2. Open right sidebar
3. See it's referenced in "Reading List 2026"

**Step 4: View the graph**

1. Open Graph View
2. Find the cluster of reading-related notes
3. See how concepts connect

## Link Management Tips

### Renaming Pages

When you rename a page, Logseq updates ALL links automatically:

1. Open the page
2. Click the page title
3. Edit the name
4. Press Enter

**All `[[old name]]` links become `[[new name]]` throughout your graph.**

### Merging Pages

If you have duplicate pages:

1. Copy content from duplicate to main page
2. Delete the duplicate page
3. Links remain, now pointing to main page

### Finding Broken Links

Logseq marks non-existent pages differently:

- **Existing page:** Normal link color
- **Non-existent:** Different color/styling

Click to create the page immediately.

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
✅ Use tags with `#`  
✅ Reference blocks with `(())`  
✅ Understand bidirectional linking  
✅ View backlinks in right sidebar  
✅ Visualize connections in Graph View  
✅ Apply practical linking patterns

## Next Steps

You now have the foundation to use Logseq effectively! Explore these topics:

**Knowledge Management:**

- [Building a Knowledge Graph](../knowledge-management/building-knowledge-graph.md)
- [Daily Journaling Workflow](../knowledge-management/daily-journaling.md)

**Task Management:**

- [Todo System Basics](../task-management/todo-basics.md)

**Advanced Features:**

- [Quick Capture](../../how-to/content/quick-capture.md)
- [Advanced Queries](../../how-to/queries/advanced-queries.md)

## Further Reading

- [Bidirectional Linking Explained](../../explanation/concepts/bidirectional-linking.md)
- [What is a Knowledge Graph?](../../explanation/concepts/knowledge-graph.md)
- [Using Graph View](../../how-to/content/graph-view.md)
