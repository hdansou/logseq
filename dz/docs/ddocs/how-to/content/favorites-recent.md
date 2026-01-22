---
title: 'Managing Favorites & Recent Pages'
status: 🟢
since-version: 'N/A'
quadrant: 'How-To'
last-verified: '2026-01-11'
platform: 'All'
related-docs:
  - 'tutorials/getting-started/04-linking-pages.md'
  - 'how-to/content/quick-capture.md'
---

# Managing Favorites & Recent Pages

Learn how to organize and quickly access your most important pages using Favorites and Recent pages features.

## Overview

Logseq provides two navigation features to access pages quickly:

- **Favorites**: Pin important pages for permanent quick access
- **Recent Pages**: Auto-tracked history of recently visited pages

## Favorites

### Adding to Favorites

**Method 1: From Page**

1. Open the page you want to favorite
2. Click the **⭐ star icon** next to the page title
3. The page appears in left sidebar under "Favorites"

**Method 2: From Page Menu**

1. Right-click (or long-press on mobile) any page link
2. Select **"Add to favorites"**
3. Page is added to favorites list

**Method 3: Keyboard Shortcut**

When viewing a page:

- Press `Ctrl/Cmd + Shift + F`

### Removing from Favorites

**From Sidebar:**

1. Find the page in "Favorites" section
2. Right-click → **"Remove from favorites"**
3. Or click the star icon again to toggle off

**From Page:**

1. Open the favorited page
2. Click the ⭐ (filled) star icon to unfavorite

### Organizing Favorites

**Reorder Favorites:**

Drag and drop pages in the sidebar to reorder:

```
Favorites:
├── 📋 Dashboard
├── 📝 Daily Journal
├── 💼 Work Projects
└── 📚 Reading List
```

**Create Hierarchy:**

Use page namespaces for organized favorites:

```
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

Then query favorited pages by category:

```clojure
#+BEGIN_QUERY
{:query [:find (pull ?p [*])
         :where
         [?p :block/favorite? true]
         [?p :favorite-category "important"]]}
#+END_QUERY
```

## Recent Pages

### Viewing Recent Pages

**Open Recent Sidebar:**

1. Click **"Recent"** in left sidebar
2. See chronologically ordered pages you've visited
3. Click any page to navigate

**Keyboard Shortcut:**

- `Ctrl/Cmd + Shift + R` - Open recent pages

### How Recent Pages Work

Recent pages automatically track:

- ✅ Pages you open
- ✅ Pages you edit
- ✅ Pages you create
- ❌ Not tracked: Pages only mentioned in links

**Tracking behavior:**

```markdown
Jan 11th → Opens "Project A" → Added to recent
→ Edits "Meeting Notes" → Added to recent
→ Types [[New Idea]] → NOT added (only link)
→ Clicks [[New Idea]] → NOW added to recent
```

### Recent Pages Limit

Logseq keeps track of the **last 20-30 pages** (varies by version).

Older pages automatically drop off the list.

### Clearing Recent Pages

Recent pages cannot be manually cleared, but you can:

1. **Wait for natural expiration** (they age out)
2. **Visit other pages** to push old ones out
3. **Restart Logseq** (recent list persists but can be reset)

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

1. `Ctrl/Cmd + J` - Jump to today's journal
2. Click "Dashboard" from favorites
3. Review tasks and notes

### Project-Based Navigation

**Favorite project hub pages:**

```markdown
Favorites:
├── Project Alpha
│ recent-page:: true
├── Project Beta
│ recent-page:: true
└── Project Gamma
recent-page:: true
```

**Use recent for sub-pages:**

- Recent automatically tracks project-related pages
- No need to favorite every sub-page
- Quick access to what you were working on

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

  - ## Recent Work
    query-table:: false
    #+BEGIN_QUERY
    {:query [:find (pull ?p [:block/title :block/updated-at])
    :where
    [?p :block/favorite? true]
    [?p :block/updated-at ?t]]
    :order-by [[?t :desc]]
    :limit 10}
    #+END_QUERY
```

### Smart Recent Pages Query

Find pages you frequently visit:

```clojure
#+BEGIN_QUERY
{:query [:find ?title (count ?visit)
         :keys title visit-count
         :where
         [?p :block/title ?title]
         [?v :visit/page ?p]
         [?v :visit/timestamp ?visit]]
 :group-by ?title
 :order-by [[visit-count :desc]]
 :limit 20}
#+END_QUERY
```

_(Note: Visit tracking must be enabled)_

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

- Tap hamburger menu (☰)
- "Favorites" section at top
- Instant navigation

**Adding:**

1. Open page
2. Tap ⋯ (more options)
3. "Add to favorites"

### Mobile Recent Pages

**Access:**

- Swipe right from left edge
- Or tap hamburger menu → "Recent"

**Limited space:**

- Mobile shows fewer recent pages
- Prioritize favorites for important pages

## Keyboard Shortcuts Summary

| Action          | Windows/Linux      | macOS             |
| --------------- | ------------------ | ----------------- |
| Toggle favorite | `Ctrl + Shift + F` | `Cmd + Shift + F` |
| Open recent     | `Ctrl + Shift + R` | `Cmd + Shift + R` |
| Jump to journal | `Ctrl + J`         | `Cmd + J`         |
| Quick search    | `Ctrl + K`         | `Cmd + K`         |
| Go to page      | `Ctrl + O`         | `Cmd + O`         |

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

- Recent pages track actual navigation
- Visit some pages to populate list
- Check you're in correct graph
- Restart Logseq if issue persists

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

- Page may have aged out (only shows ~20-30)
- Use search instead: `Ctrl/Cmd + K`
- Check if page was renamed/deleted
- Use full-text search: `Ctrl/Cmd + Shift + F`

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

Favorites show up first in search results:

```
Search: "project"
Results:
★ Project Alpha (Favorite)
★ Projects Dashboard (Favorite)
- Project Beta
- Old Project
```

### With Command Palette

Access favorites via command palette:

1. Press `Ctrl/Cmd + K`
2. Type favorite name
3. Press Enter to navigate

### With Graph View

Favorited pages highlighted in graph:

- Special color/icon in graph view
- Easy to spot important nodes
- Visual navigation aid

## See Also

- [Linking Pages Tutorial](../../tutorials/getting-started/04-linking-pages.md)
- [Quick Capture Guide](quick-capture.md)
- [Using Graph View](graph-view.md)
