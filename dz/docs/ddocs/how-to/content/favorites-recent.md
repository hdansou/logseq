---
title: 'Managing Favorites & Recent Pages'
status: 🟢
since-version: 'N/A'
quadrant: 'How-To'
last-verified: '2026-05-30'
platform: 'All'
related-docs:
  - 'tutorials/getting-started/04-linking-pages.md'
  - 'how-to/content/quick-capture.md'
---

# Managing Favorites & Recent Pages

> **Last revised:** 2026-05-30

Learn how to organize and quickly access your most important pages using Favorites and Recent Pages features.

## Overview

Logseq provides two navigation features to access pages quickly:

- **Favorites**: Pin important pages for permanent quick access
- **Recent Pages**: A per-graph history of recently visited pages

## Findings: New Capabilities

- On mobile, the bottom **Go to** tab surfaces both Favorites and Recent Pages together.
- The command palette still opens with a **Recently updated pages** group, which is separate from the sidebar Recent Pages history.

## Favorites

### Adding to Favorites

**Method 1: From Page Menu**

1. Open the page you want to favorite
2. Click the **⋯** (more options) menu in the page header
3. Select **"Add to Favorites"**
4. The page appears in left sidebar under "Favorites"

**Method 2: Keyboard Shortcut**

When viewing a page:

- Press `Ctrl/Cmd + Shift + F` (toggles favorite on/off)

### Removing from Favorites

**From Page Menu:**

1. Open the favorited page
2. Click the **⋯** (more options) menu
3. Select **"Unfavorite page"**

**Via Keyboard:**

1. Open the favorited page
2. Press `Ctrl/Cmd + Shift + F` to toggle off

### Organizing Favorites

**Reorder Favorites:**

Drag and drop pages in the sidebar to reorder:

```markdown
Favorites:
├── 📋 Dashboard
├── 📝 Daily Journal
├── 💼 Work Projects
└── 📚 Reading List
```

**Create Hierarchy:**

Use page namespaces for organized favorites:

```markdown
Favorites:
├── Work/Projects
├── Work/Meetings
├── Personal/Health
└── Personal/Learning
```

**Using Properties:**

Add properties to favorite pages for better organization:

```markdown
- # Dashboard
  favorite-category:: important
  access-frequency:: daily
```

Note: Favorites are stored internally via a special `$$$favorites` page whose child blocks link to favorited pages. There is no `:block/favorite?` attribute on pages, so you cannot query favorites directly by a boolean property. Instead, organize favorites using your own custom properties and queries.

## Recent Pages

### Viewing Recent Pages

**Open Recent Sidebar:**

1. Click **"Recent"** in left sidebar
2. See chronologically ordered pages you've visited
3. Click any page to navigate

### How Recent Pages Work

> **Removed:** The old "recently updated pages" explanation is stale. Sidebar Recent Pages now track recently visited pages, not pages sorted by `block/updated-at`.

Recent Pages are a per-graph history of recently visited pages. Logseq records the latest 15 unique pages you navigate to, and reopening a page that is already in the list does not move it to the top.

### Recent Pages Limit

Logseq keeps the **last 15 unique recent pages** in the stored list.

The sidebar may show fewer items after filtering hidden or blank pages.

### Clearing Recent Pages

> **Removed:** The previous restart-based reset advice is stale. Recent Pages are persisted per graph, and the current UI does not provide a supported manual clear action.

## Practical Workflows

### Daily Review Workflow

**Setup favorites for daily pages:**

```markdown
Favorites:
├── 📅 Today's Journal (auto-updates)
├── 📋 Daily Dashboard
├── ✅ Task Inbox
└── 📝 Quick Capture
```

**Navigate quickly:**

1. `g j` - Go to journals page (non-editing shortcut)
2. `Ctrl/Cmd + Shift + J` (macOS) or `Alt + Shift + J` (Windows/Linux) - Open today's page in sidebar
3. Click "Dashboard" from favorites
4. Review tasks and notes

### Project-Based Navigation

**Favorite project hub pages:**

```markdown
Favorites:
├── Project Alpha
├── Project Beta
└── Project Gamma
```

**Use recent for sub-pages:**

- Recent automatically tracks project-related pages
- No need to favorite every sub-page
- Quick access to what you were working on

> **Removed:** `recent-page:: true` is not a supported page property. Recent Pages are managed automatically from navigation history.

### Knowledge Base Navigation

**Favorite main hubs:**

```markdown
Favorites:
├── 📚 Knowledge Hub
├── 🏷️ Tag Index
├── 🔗 Link Graph
└── 🔍 Search Dashboard
```

**Use recent for exploration:**

- Recent tracks your research path
- Easily backtrack through connected notes
- Discover navigation patterns

## Advanced Tips

### Favorites Dashboard

Create a dashboard page with all favorites:

```markdown
- # My Dashboard

  - ## Quick Links

    - {{embed [[Work/Projects]]}}
    - {{embed [[Personal/Goals]]}}
```

Note: Since favorites are stored via an internal system page (`$$$favorites`) rather than a queryable attribute, you cannot query "all favorited pages" with a Datalog query. Instead, curate your dashboard manually or use your own custom properties.

### Finding Recently Updated Pages

You can query for recently updated pages using the `block/updated-at` attribute:

```clojure
#+BEGIN_QUERY
{:query [:find (pull ?p [:block/title :block/updated-at])
         :where
         [?p :block/title ?title]
         [?p :block/updated-at ?t]
         [(> ?t 1700000000000)]]
 :result-transform (fn [result] (sort-by :block/updated-at > result))
 :limit 20}
#+END_QUERY
```

Note: Logseq does not track page visit frequency. The command palette's initial "Recently updated pages" surface is based on `block/updated-at` timestamps. That is separate from the sidebar Recent Pages history.

### Combine with Quick Capture

Link favorites with quick capture templates:

```markdown
- template:: meeting-notes
  project:: [[Project Alpha]]
  favorite:: [[Meeting Notes]]
```

When capturing:

1. Use template
2. Automatically links to favorite project
3. Easy to find in recent pages

## Mobile Considerations

### Mobile Favorites

On mobile, favorites are even more valuable:

**Access:**

- Tap the bottom **Go to** tab
- Favorites and Recent Pages share the same panel
- Instant navigation

**Adding:**

1. Open page
2. Tap the favorite/star action in the header or page menu
3. Select **"Add to Favorites"**

### Mobile Recent Pages

**Access:**

- Tap the bottom **Go to** tab
- Use the **Recent** list in the same panel as Favorites

> **Removed:** The old hamburger-menu and left-edge swipe instructions do not match the current mobile UI.

**Limited space:**

- Mobile shows fewer recent pages
- Prioritize favorites for important pages

## Keyboard Shortcuts Summary

| Action                       | Windows/Linux        | macOS               |
| ---------------------------- | -------------------- | ------------------- |
| Toggle favorite              | `Ctrl + Shift + F`   | `Cmd + Shift + F`   |
| Go to journals               | `g j`                | `g j`               |
| Open today in sidebar        | `Alt + Shift + J`    | `Cmd + Shift + J`   |
| Quick search                 | `Ctrl + K`           | `Cmd + K`           |
| Highlight recent blocks      | `Ctrl + C, Ctrl + R` | `Cmd + C, Cmd + R`  |
| Follow link under cursor     | `Ctrl + O`           | `Cmd + O`           |

## Troubleshooting

### Favorites Not Saving

**Problem:** Favorites disappear after restart

**Solution:**

- Check graph permissions (file/folder access)
- Ensure Logseq has write access
- Try re-adding favorites
- Check for sync conflicts (if using cloud sync)

### Recent Pages Empty

**Problem:** Recent pages list is empty

**Solution:**

- Recent pages track recently visited pages, not pages you merely open from search
- Check you're in the correct graph
- Navigate to a few pages to populate the list
- Confirm the pages are not hidden or blank

### Too Many Favorites

**Problem:** Favorites list is cluttered

**Solution:**

1. Review and remove unused favorites
2. Use page namespaces for organization
3. Create a "Favorites Hub" page instead
4. Keep favorites to 5-10 most-used pages

### Can't Find Recent Page

**Problem:** Recently visited page not in list

**Solution:**

- Page may have aged out of the 15-item history
- The sidebar list is based on page visits, so repeated opens do not move an already-listed page to the top
- Use search instead: `Ctrl/Cmd + K`
- Check if page was renamed/deleted
- Use in-page search: `Ctrl/Cmd + F` (Electron find-in-page)

## Best Practices

### 1. Keep Favorites Minimal

```markdown
✅ Good: 5-10 core pages

- Dashboard
- Task Inbox
- Today's Journal
- Active Project
- Reading List

❌ Avoid: 50+ favorites

- Every project page
- Every note page
- Random pages
```

### 2. Use Hierarchical Organization

```markdown
✅ Good: Organized structure
Favorites:
├── Work/
│ ├── Projects
│ └── Meetings
└── Personal/
├── Goals
└── Learning

❌ Avoid: Flat list
Favorites:

- Projects
- Meetings
- Goals
- Learning
- Random Page 1
- Random Page 2
```

### 3. Leverage Recent for Deep Work

- Don't favorite every page
- Let recent track your workflow
- Favorite only anchor pages
- Use recent for context switching

### 4. Regular Cleanup

Monthly review:

1. Remove unused favorites
2. Check favorite organization
3. Update hub pages
4. Reorganize if needed

## Integration with Other Features

### With Search

Use `Ctrl/Cmd + K` to open the command palette / search. The initial view shows recently updated pages. Typing will fuzzy-search across all pages. Favorites are not given special priority in search results, but they remain easily accessible from the left sidebar.

Note: this command-palette surface is separate from the sidebar Recent Pages history. The command palette shows recently updated pages, while the sidebar shows recently visited pages.

### With Command Palette

> **Removed:** The command palette does not expose a dedicated favorites surface in the current source. Use the left sidebar Favorites list or search for the page name.

### With Graph View

The graph view displays pages and their connections. Use graph search and filter controls to locate important nodes.

## See Also

- [Linking Pages Tutorial](../../tutorials/getting-started/04-linking-pages.md)
- [Quick Capture Guide](quick-capture.md)
- [Using Graph View](graph-view.md)
