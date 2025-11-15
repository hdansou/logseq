# Library and Namespace: Complete User Guide

## Introduction

Logseq provides two powerful features for organizing your pages: **Library** and **Namespaces**. While they serve similar purposes (keeping your content organized), they work differently depending on which type of graph you're using.

This guide will help you understand both features and use them effectively.

---

## Table of Contents

1. [Understanding the Basics](#understanding-the-basics)
2. [Library Page (DB Graphs)](#library-page-db-graphs)
3. [Namespace Pages (All Graphs)](#namespace-pages-all-graphs)
4. [Comparison: Library vs Namespaces](#comparison-library-vs-namespaces)
5. [Common Use Cases](#common-use-cases)
6. [Tips and Best Practices](#tips-and-best-practices)
7. [Troubleshooting](#troubleshooting)

---

## Understanding the Basics

### What's the Difference?

**Library Page** (DB Graphs Only):

- A special built-in page that acts as a container for reusable content
- Think of it as a "filing cabinet" where you store templates, snippets, and reference materials
- You explicitly add pages to the Library

**Namespace Pages** (All Graph Types):

- A way to create hierarchies using forward slashes (/)
- Like folders on your computer: `Projects/Work`, `Books/Fiction/Sci-Fi`
- Automatically creates parent-child relationships

### Which Graph Type Do You Have?

To check your graph type:

1. Click on your graph name in the left sidebar
2. Look for "DB Graph" or "File Graph" in the graph switcher

**DB Graphs**: Modern graphs that use a database. These support the Library page.

**File Graphs**: Traditional graphs that store data as files. These primarily use namespaces.

---

## Library Page (DB Graphs)

### What is the Library Page?

The Library page is a special page in DB graphs where you can organize:

- **Templates**: Reusable block templates
- **Snippets**: Code snippets, quotes, or frequently-used text
- **Reference materials**: Style guides, checklists, workflows
- **Shared resources**: Content you want to access from anywhere

Think of it as your graph's resource center.

### How to Access the Library

1. **Quick Navigation**:

   - Press `Cmd/Ctrl + K` to open the search
   - Type "Library" and press Enter

2. **From the Sidebar**:

   - The Library page appears in your left sidebar (if visible)
   - You can pin it for quick access

### Adding Pages to the Library

#### Method 1: Using the "Add pages" Button

1. Navigate to your Library page
2. Click the **"+ Add existing pages to Library"** button at the bottom
3. A search popup will appear
4. Type to search for pages you want to add
5. Click on a page to add it to the Library
6. The page is now a child of the Library page

#### Method 2: Moving Pages Manually

1. Open the page you want to add
2. Click the page properties (•••) menu
3. Select "Move to..." or "Set parent"
4. Choose "Library" as the parent page
5. The page is now in your Library

### Organizing Pages in the Library

You can create a hierarchy within the Library:

```
Library
├── Templates
│   ├── Meeting Notes Template
│   └── Project Template
├── Code Snippets
│   ├── JavaScript Utils
│   └── CSS Helpers
└── Reference
    ├── Writing Style Guide
    └── Keyboard Shortcuts
```

To create this structure:

1. Create pages like "Templates", "Code Snippets", "Reference"
2. Add these to the Library (they become direct children)
3. Add specific pages like "Meeting Notes Template"
4. Set their parent to "Templates" (not Library)

### Removing Pages from the Library

If you want to remove a page from the Library:

1. Open the page
2. Click the page properties (•••) menu
3. Select "Remove parent" or manually delete the parent property
4. The page is no longer in the Library but still exists in your graph

**Note**: Removing from Library doesn't delete the page!

### Viewing Library Contents

To see everything in your Library:

1. Navigate to the Library page
2. All child pages are shown in the page content
3. Use the outline view (right sidebar) to see the hierarchy
4. Click on any page to navigate to it

### When to Use the Library

Use the Library for:

- ✅ Templates you use across multiple projects
- ✅ Code snippets and examples
- ✅ Style guides and reference materials
- ✅ Shared resources multiple people need
- ✅ Content you want separate from your daily notes

Don't use the Library for:

- ❌ Regular daily notes or journal entries
- ❌ Project-specific pages (use regular hierarchy instead)
- ❌ Temporary or work-in-progress content

---

## Namespace Pages (All Graphs)

### What are Namespace Pages?

Namespace pages use forward slashes (/) to create hierarchical relationships between pages. It's similar to how file systems organize folders:

```
Projects              → Parent page
Projects/Work         → Child of "Projects"
Projects/Personal     → Child of "Projects"
Projects/Work/Client-A → Child of "Projects/Work"
```

### Creating Namespace Pages

#### Creating a New Namespace Page

1. **Using Double Brackets**:

   ```text
   [[Projects/Work/Client-A]]
   ```

   - Logseq automatically creates:
     - "Projects" (if it doesn't exist)
     - "Projects/Work" (if it doesn't exist)
     - "Projects/Work/Client-A" (the new page)

2. **Using the Search Bar**:

   - Press `Cmd/Ctrl + K`
   - Type: `Projects/Work/Client-A`
   - Press Enter to create

3. **Manual Creation**:

   - Create a page named exactly as you want, including slashes
   - Example: `Books/Fiction/Dune`

### Viewing Namespace Hierarchies

#### In File Graphs

1. **Hierarchy View** (if available):

   - Check your left sidebar for a hierarchy/namespace view
   - Click to expand/collapse namespaces
   - See all related pages at a glance

2. **On the Page Itself**:

   - Navigate to a parent namespace page (e.g., "Projects")
   - You'll see links to all child pages listed
   - Automatic backlinks show the relationship

#### In DB Graphs

1. **Page Hierarchy**:

   - When viewing a page, you'll see its "path" in the title
   - Example: A page titled "Client-A" with parents shows as "Projects/Work/Client-A"

2. **Parent-Child View**:

   - Use the page properties to see parent pages
   - Child pages are shown in linked references

### Renaming Namespace Pages

When you rename a namespace parent, all children automatically update!

**Example**:

```text
Before:
- Projects
- Projects/Work
- Projects/Work/Client-A

You rename "Projects" to "MyProjects"

After:
- MyProjects
- MyProjects/Work
- MyProjects/Work/Client-A
```

**How to Rename**:

1. Open the namespace page you want to rename
2. Click the page title to edit
3. Type the new name
4. Press Enter
5. Logseq automatically updates all child pages and references

**Warning**: This is a powerful operation that affects many pages. Make sure you want to rename everything!

### Navigating Namespace Hierarchies

**Quick Navigation Tips**:

1. **Use Search**:

   - Press `Cmd/Ctrl + K`
   - Type part of the namespace path
   - Results show matching pages with their full path

2. **Use Autocomplete**:

   - When typing `[[Projects/`
   - Logseq shows suggestions for existing children
   - Tab to complete

3. **Browse from Parent**:

   - Navigate to the parent page
   - See all children listed
   - Click to navigate down the hierarchy

4. **Use Breadcrumbs** (DB Graphs):

   - Page titles show the full path
   - Click any part of the path to navigate up

### Converting Between Flat and Namespace Structure

**Flatten a Namespace** (Remove the hierarchy):

```text
Before: Projects/Work/Client-A
Rename to: Client-A (just the name, no slashes)
After: Client-A (now a standalone page)
```

**Create a Namespace** (Add hierarchy to flat page):

```text
Before: Client-A (standalone)
Rename to: Projects/Work/Client-A
After: Part of the Projects/Work namespace
```

### When to Use Namespaces

Use namespaces for:

- ✅ Projects and sub-projects
- ✅ Topics with subtopics (Books/Fiction, Books/Non-Fiction)
- ✅ Areas with categories (Work/Meetings, Work/Projects)
- ✅ Taxonomies (Animals/Mammals/Cats)
- ✅ When you want automatic grouping

Don't use namespaces for:

- ❌ Simple relationships (use links instead)
- ❌ Deeply nested hierarchies (keep it 2-3 levels max)
- ❌ When you need many-to-many relationships (one page, multiple categories)

---

## Comparison: Library vs Namespaces

| Feature            | Library Page                   | Namespace Pages              |
| ------------------ | ------------------------------ | ---------------------------- |
| **Availability**   | DB Graphs only                 | All graph types              |
| **Purpose**        | Store reusable content         | Organize related pages       |
| **Structure**      | Explicit parent-child          | Automatic via "/" in name    |
| **Adding Content** | Manually add pages             | Automatic via page naming    |
| **Use Case**       | Templates, snippets, reference | Projects, topics, categories |
| **Visibility**     | Separate "filing cabinet"      | Integrated with graph        |
| **Renaming**       | Rename individual pages        | Cascade renames all children |
| **Best For**       | Shared resources               | Hierarchical organization    |

### Can You Use Both?

**Yes!** In DB graphs, you can:

- Use namespaces for your daily organization (Projects/Work, Books/Fiction)
- Use Library for reusable templates and resources
- Keep them separate or mix them

**Example**:

```
Your Graph:
├── Daily Notes (namespace structure)
│   ├── 2025/01/15
│   └── 2025/01/16
├── Projects (namespace structure)
│   ├── Projects/Work
│   └── Projects/Personal
└── Library (separate)
    ├── Templates
    │   └── Meeting Template
    └── Code Snippets
```

---

## Common Use Cases

### Use Case 1: Managing Projects

**Using Namespaces**:

```text
Projects/
├── Projects/Client-Work
│   ├── Projects/Client-Work/Website-Redesign
│   └── Projects/Client-Work/App-Development
└── Projects/Personal
    ├── Projects/Personal/Home-Renovation
    └── Projects/Personal/Learning-Spanish
```

**Benefits**:

- Easy to see all projects at a glance
- Navigate from general to specific
- Rename "Projects" to "Active-Projects" and everything updates

### Use Case 2: Knowledge Management

**Using Namespaces**:

```text
Knowledge/
├── Knowledge/Programming
│   ├── Knowledge/Programming/JavaScript
│   ├── Knowledge/Programming/Python
│   └── Knowledge/Programming/Databases
└── Knowledge/Writing
    ├── Knowledge/Writing/Blog-Posts
    └── Knowledge/Writing/Book-Notes
```

**Using Library** (DB Graphs):

```text
Library/
├── Templates
│   ├── Book-Note-Template
│   ├── Article-Template
│   └── Code-Review-Template
├── Reference
│   ├── Writing-Style-Guide
│   └── Programming-Best-Practices
└── Checklists
    ├── Publishing-Checklist
    └── Code-Review-Checklist
```

### Use Case 3: Course Notes

**Using Namespaces**:

```text
Courses/
├── Courses/CS101-Intro-to-Programming
│   ├── Courses/CS101-Intro-to-Programming/Week-1
│   ├── Courses/CS101-Intro-to-Programming/Week-2
│   └── Courses/CS101-Intro-to-Programming/Final-Project
└── Courses/PHIL201-Ethics
    ├── Courses/PHIL201-Ethics/Readings
    └── Courses/PHIL201-Ethics/Essays
```

**Using Library** (for reusable content):

```text
Library/
└── Templates
    ├── Lecture-Notes-Template
    ├── Reading-Notes-Template
    └── Essay-Outline-Template
```

### Use Case 4: Meeting Notes

**Using Namespaces**:

```text
Meetings/
├── Meetings/Team
│   ├── Meetings/Team/2025-01-15-Standup
│   └── Meetings/Team/2025-01-17-Sprint-Planning
└── Meetings/Client
    ├── Meetings/Client/2025-01-16-Kickoff
    └── Meetings/Client/2025-01-20-Review
```

**Using Library Template**:

1. Create a "Meeting-Notes-Template" in Library
2. When starting a meeting note:
   - Create page: `Meetings/Team/2025-01-22-Standup`
   - Insert template from Library
   - Fill in meeting details

### Use Case 5: Personal Knowledge Base

**Mixed Approach**:

```text
Daily Notes (journal entries)

Books/                          ← Namespace for book notes
├── Books/Fiction
└── Books/Non-Fiction
    ├── Books/Non-Fiction/Atomic-Habits
    └── Books/Non-Fiction/Deep-Work

Articles/                       ← Namespace for article notes
├── Articles/Technology
└── Articles/Productivity

Library/                        ← Reusable content (DB graphs)
├── Templates
│   ├── Book-Summary-Template
│   └── Article-Notes-Template
└── Reading-Lists
    ├── 2025-Reading-List
    └── Recommended-by-Friends
```

---

## Tips and Best Practices

### Library Page Tips

1. **Keep It Organized**:

   - Use top-level categories: Templates, Snippets, Reference, Checklists
   - Don't let the Library become a dumping ground
   - Regular cleanup: Remove unused templates

2. **Naming Conventions**:

   - Use clear, descriptive names: "Meeting-Notes-Template" not "Template1"
   - Add tags or properties for discoverability
   - Consider prefixes: "Template-", "Snippet-", "Guide-"

3. **Documentation**:

   - Add instructions to templates (explain how to use them)
   - Include examples in code snippets
   - Add last-updated dates to reference materials

4. **Access Speed**:

   - Pin frequently-used Library pages to shortcuts
   - Create a "Quick-Access" page linking to most-used templates
   - Use tags on Library pages for faster search

### Namespace Tips

1. **Keep Hierarchies Shallow**:

   - ✅ Good: `Projects/Client-Work/Website`
   - ❌ Avoid: `Work/Current/Projects/Clients/Active/ABC-Corp/Website/Homepage/Design`
   - Aim for 2-3 levels maximum

2. **Use Consistent Naming**:

   - Decide on a convention: `Projects/Work` or `projects-work`?
   - Stick with it across your graph
   - Use dashes or underscores instead of spaces for cleaner URLs

3. **Think About Growth**:

   - Will `Courses/` work when you have 50 courses?
   - Consider: `Courses/2024`, `Courses/2025` instead
   - Or: `Courses/Computer-Science`, `Courses/Philosophy`

4. **Rename Carefully**:

   - Remember: renaming a parent renames ALL children
   - Check linked references first (what else links to this?)
   - Consider impact before renaming high-level namespaces

5. **Use Namespaces for Groups, Tags for Cross-Cutting Concerns**:

   - Namespaces: `Projects/Work`, `Projects/Personal`
   - Tags: `#urgent`, `#in-progress`, `#completed`
   - This gives you both hierarchy AND flexible categorization

### Search Tips

1. **Search by Namespace Last Part**:

   - Searching "work" finds both "Work" and "Projects/Work"
   - This is intentional - makes finding pages easier

2. **Use Full Path for Precision**:

   - Search "Projects/Work" to find exact namespace
   - Use quotes for exact matches (if supported)

3. **Filter Library Pages**:

   - Most searches exclude Library by default
   - This keeps your search results focused on content
   - Explicitly search "Library" to see Library pages

### Organization Tips

1. **Start Simple, Grow as Needed**:

   - Don't create complex hierarchies on day one
   - Let structure emerge from your usage patterns
   - Refactor when you have 10+ pages that should be grouped

2. **Mix and Match**:

   - Use namespaces for most organization
   - Use Library for truly reusable content
   - Use tags for flexible cross-cutting categories

3. **Regular Maintenance**:

   - Monthly: Review your namespace structure
   - Quarterly: Clean up Library (remove unused templates)
   - Yearly: Consider major restructuring if needed

---

## Troubleshooting

### Library Page Issues

**Problem**: "I don't see the Library page"

**Solutions**:

- Check if you're using a DB graph (Library only works in DB graphs)
- Search for "Library" explicitly - it might not be in sidebar
- The Library page is created automatically - if missing, there may be an issue with your graph

**Problem**: "I added a page to Library but it's not showing up"

**Solutions**:

- Refresh the Library page
- Check the page properties - does it have Library as parent?
- Look in the graph's linked references

**Problem**: "I can't find pages in my Library when searching"

**Solutions**:

- Library pages are often filtered from search results by default
- Search specifically for "Library" to see Library contents
- Use the Library page directly to browse content

### Namespace Issues

**Problem**: "I created `Projects/Work` but they're separate pages, not a hierarchy"

**Solutions**:

- **File Graphs**: The hierarchy is automatic if you use "/" in the name
- **DB Graphs**: You may need to set the parent relationship manually
- Check for extra spaces: `Projects / Work` vs `Projects/Work`

**Problem**: "Renaming a namespace didn't update all children"

**Solutions**:

- Make sure you renamed the parent page itself, not just the title property
- Give Logseq a moment - large renames can take a few seconds
- Check for pages with similar names that didn't actually have the parent relationship

**Problem**: "My namespace page doesn't show child pages"

**Solutions**:

- Child pages appear in linked references, not page content
- Check the sidebar outline view for children
- Use the hierarchy view (if available in your graph type)

### General Issues

**Problem**: "I'm confused when to use Library vs Namespaces"

**Rule of Thumb**:

- **Library**: "Would I reuse this exact content in multiple places?" → Yes = Library
- **Namespaces**: "Is this part of a group of related pages?" → Yes = Namespace

Examples:

- Meeting template → Library
- Specific meeting notes → Namespace (`Meetings/Team/2025-01-15`)
- Code snippet you copy-paste often → Library
- Project page → Namespace (`Projects/Current/Website-Redesign`)

**Problem**: "My graph is a mess, how do I reorganize?"

**Steps**:

1. **Audit**: List your top 20 most-used pages
2. **Categorize**: What natural groups emerge?
3. **Plan**: Sketch out a simple structure (2-3 levels max)
4. **Migrate Slowly**:
   - Start with one area (e.g., "Projects")
   - Rename pages to use namespaces
   - Move reusable content to Library
5. **Update Links**: Fix broken links as you find them
6. **Iterate**: Adjust structure based on usage

---

## Quick Reference

### Library Page Cheat Sheet

```text
Access Library:
  Cmd/Ctrl + K → "Library" → Enter

Add Page to Library:
  1. Open page
  2. Page menu (•••) → "Set parent"
  3. Choose "Library"

Remove from Library:
  1. Open page
  2. Page menu (•••) → "Remove parent"

Organize Library:
  Library/
  ├── Templates/
  ├── Snippets/
  └── Reference/
```

### Namespace Cheat Sheet

```text
Create Namespace Page:
  [[Parent/Child/Grandchild]]
  - Auto-creates all levels

Rename Namespace:
  1. Open parent page
  2. Edit page title
  3. All children auto-update

Navigate Hierarchy:
  - Search: Type partial name
  - Browse: Start from parent
  - Breadcrumbs: Click path segments (DB graphs)

Best Practices:
  ✅ Keep 2-3 levels max
  ✅ Use consistent naming
  ✅ Think before renaming parents
  ❌ Avoid deeply nested hierarchies
```

### When to Use What

```text
Use Library:
  ✓ Templates (Meeting, Project, Book Note)
  ✓ Code snippets and examples
  ✓ Style guides and references
  ✓ Checklists and workflows
  ✓ Shared team resources

Use Namespaces:
  ✓ Projects and sub-projects
  ✓ Topics and subtopics
  ✓ Course notes by course
  ✓ Meeting notes by team/date
  ✓ Knowledge areas and subareas

Use Tags (Complement):
  ✓ Status (#in-progress, #done)
  ✓ Priority (#urgent, #low)
  ✓ Types (#idea, #question)
  ✓ Cross-cutting concerns
```

---

## Conclusion

Both Library and Namespaces are powerful organizational tools in Logseq:

- **Library Page** gives you a dedicated space for reusable content (templates, snippets, references)
- **Namespaces** let you create hierarchical structures for related pages

Start simple:

1. Try creating a basic namespace for your next project: `Projects/YourProject`
2. If you're on a DB graph, add a template to your Library
3. See what works for your workflow
4. Adjust and expand as needed

Remember: **The best organizational system is the one you'll actually use.** Start small, be consistent, and let your structure grow naturally with your needs.

Happy organizing! 🎯

---

## Additional Resources

### Official Documentation

- **[Logseq DB Documentation - Library Section](https://github.com/logseq/docs/blob/master/db-version.md#library)**: Official documentation covering the Library feature in DB graphs
- **[Logseq DB Unofficial FAQ - What is the Library?](https://discuss.logseq.com/t/logseq-db-unofficial-faq/32508#p-65236-what-is-the-library-15)**: Community FAQ explaining the Library feature, including drag-and-drop hierarchy management

### Video Tutorials

- **[Library Feature Video Tutorial](https://www.youtube.com/watch?v=l1xpHrZKGIY)**: Visual walkthrough of the Library feature and how to use it effectively

### For Developers

- **Developer Guide**: See `library-and-namespace-developer-guide.md` in this repository for technical implementation details
- **Plugin API Documentation**: Check the [Logseq Plugin API documentation](https://plugins-doc.logseq.com/) for programmatic access to namespace features

### Community Resources

- **Logseq Discord**: Search for organizational workflow examples and ask questions
- **Logseq Forums**: Browse discussions about Library and namespace usage patterns
- **GitHub Discussions**: Find feature requests and discussions about organizational features

### Key Features from Resources

Based on the community documentation:

**Library Drag-and-Drop** (DB Graphs):

- You can drag and drop pages within the Library outline to reorganize hierarchy
- This is much easier than manually editing `Parent::` properties
- Great for restructuring your organizational system

**Automatic Namespace Collection**:

- When you create `[[computer/apple]]`, it can be organized under `library/computer/apple`
- The Library page shows an outline of all hierarchical relationships
- Helps avoid naming conflicts (e.g., "apple" for computers vs. "apple" for food)

**Why Library Exists**:

- Makes namespace management more intuitive than in file-based graphs
- Addresses the previously underdocumented namespace system
- Provides a visual, interactive way to manage page hierarchies

---

_Last updated: 2025-10-21_
_Written for Logseq users of all experience levels_
