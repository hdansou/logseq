---
title: 'Managing Favorites & Recent Pages'
status: 🟢
since-version: 'N/A'
quadrant: 'How-To'
last-verified: '2026-09-18'
verified-against: '5824434a74'
review-status: audited
graph-type: [db]
platform: 'All'
related-docs:
  - 'tutorials/getting-started/04-linking-pages.md'
  - 'how-to/content/quick-capture.md'
  - 'explanation/concepts/graph-visualization.md'
---

# Managing Favorites & Recent Pages

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

> **Last revised:** 2026-09-15 (pull patterns re-checked for the
> `:block/uuid` renderer requirement; rest of the page unchanged)

Learn how to organize and quickly access your most important pages using Favorites and Recent Pages features.

This page covers DB graphs only. Current Logseq does not open file graphs ([`src/main/frontend/components/repo.cljs:321-323`](../../../../../src/main/frontend/components/repo.cljs)).

## Overview

Logseq provides two navigation features to access pages quickly:

- **Favorites**: Pin important pages for permanent quick access. Favorites are stored in the graph.
- **Recent Pages**: A per-graph history of recently visited pages, stored on this device.

Both appear in the left sidebar under **Favorites** and **Recent** ([`src/resources/dicts/en.edn:1797`](../../../../../src/resources/dicts/en.edn), [`:1799`](../../../../../src/resources/dicts/en.edn)). On mobile, both appear together on the **Go To** tab ([`src/main/mobile/components/app.cljs:275`](../../../../../src/main/mobile/components/app.cljs), [`src/main/mobile/components/favorites.cljs:6-12`](../../../../../src/main/mobile/components/favorites.cljs)).

## Favorites

### Adding to Favorites

**Method 1: From the Page Menu**

1. Open the page you want to favorite
2. Click the **⋯** (more options) menu in the top toolbar
3. Select **"Add to Favorites"**
4. The page appears in the left sidebar under "Favorites"

The menu item is labeled "Add to Favorites", or "Unfavorite page" when the page is already a favorite ([`src/main/frontend/components/page_menu.cljs:116-119`](../../../../../src/main/frontend/components/page_menu.cljs), [`src/main/frontend/components/header.cljs:185-196`](../../../../../src/main/frontend/components/header.cljs), [`src/resources/dicts/en.edn:1106`](../../../../../src/resources/dicts/en.edn), [`:1146`](../../../../../src/resources/dicts/en.edn)).

**Method 2: Keyboard Shortcut**

When viewing a page:

- Press `Ctrl/Cmd + Shift + F` (toggles favorite on/off)

This is the `:page/toggle-favorite` command, bound to `mod+shift+f` ([`src/main/frontend/modules/shortcut/config.cljs:441-442`](../../../../../src/main/frontend/modules/shortcut/config.cljs)). It acts only on the current page, so it does nothing on the Journals view, where there is no current page ([`src/main/frontend/handler/page.cljs:116-122`](../../../../../src/main/frontend/handler/page.cljs)).

### Removing from Favorites

**From the Page Menu:**

1. Open the favorited page
2. Click the **⋯** (more options) menu
3. Select **"Unfavorite page"**

**From the Left Sidebar:**

Open the menu on a page in the Favorites list and choose **"Unfavorite page"** ([`src/main/frontend/components/left_sidebar.cljs:99-105`](../../../../../src/main/frontend/components/left_sidebar.cljs)).

**Via Keyboard:**

1. Open the favorited page
2. Press `Ctrl/Cmd + Shift + F` to toggle off

### Organizing Favorites

**Reorder Favorites:**

Drag and drop pages in the sidebar Favorites list to reorder them. The new order is saved to the graph ([`src/main/frontend/components/left_sidebar.cljs:362-371`](../../../../../src/main/frontend/components/left_sidebar.cljs), [`src/main/frontend/worker/handler/graph.cljs:100-117`](../../../../../src/main/frontend/worker/handler/graph.cljs)).

```markdown
Favorites:
├── 📋 Dashboard
├── 📝 Daily Journal
├── 💼 Work Projects
└── 📚 Reading List
```

The Favorites list is flat. A page titled `Work/Projects` is listed as its own item. It is not nested under a `Work` group.

**Using Properties:**

To group favorite pages yourself, add properties to them in the property UI. Typing `favorite-category:: important` in a DB graph block creates plain text, not a property.

### Where favorites are stored

Favorites are stored in the graph database, not in `config.edn`. Each graph has a hidden built-in page named `$$$favorites` ([`deps/common/src/logseq/common/config.cljs:62`](../../../../../deps/common/src/logseq/common/config.cljs), [`deps/db/src/logseq/db/sqlite/create_graph.cljs:198-203`](../../../../../deps/db/src/logseq/db/sqlite/create_graph.cljs)). Adding a favorite inserts a child block on that page whose `:block/link` points to the favorited page ([`deps/db/src/logseq/db/frontend/db.cljs:36-40`](../../../../../deps/db/src/logseq/db/frontend/db.cljs), [`src/main/frontend/worker/handler/graph.cljs:81-90`](../../../../../src/main/frontend/worker/handler/graph.cljs)). The sidebar lists those links in block order and skips pages that are in the Recycle bin ([`src/main/frontend/worker/handler/graph.cljs:34-44`](../../../../../src/main/frontend/worker/handler/graph.cljs)).

Pages have no favorite attribute, so no boolean property marks a favorite. Favorites are reachable only through the `$$$favorites` page's child blocks and their `:block/link`.

> **Unverified:** whether a user query can read the hidden `$$$favorites` page.

Plugins can read both lists with `getCurrentGraphFavorites()` and `getCurrentGraphRecent()` ([`libs/src/LSPlugin.ts:511-512`](../../../../../libs/src/LSPlugin.ts), host exports [`src/main/logseq/api.cljs:73-74`](../../../../../src/main/logseq/api.cljs), [`src/main/logseq/api/app.cljs:81-90`](../../../../../src/main/logseq/api/app.cljs)).

## Recent Pages

### Viewing Recent Pages

**In the left sidebar:**

1. Find the **Recent** section in the left sidebar. Click its header to expand or collapse it ([`src/main/frontend/components/left_sidebar.cljs:198-213`](../../../../../src/main/frontend/components/left_sidebar.cljs)).
2. The list shows pages you visited recently.
3. Click any page to navigate

### How Recent Pages Work

When Logseq navigates to a page through its page-redirect handler, it records the page ([`src/main/frontend/handler/route.cljs:123`](../../../../../src/main/frontend/handler/route.cljs), [`:140`](../../../../../src/main/frontend/handler/route.cljs)). That handler is also used when you open a page from search ([`src/main/frontend/components/cmdk/core.cljs:611`](../../../../../src/main/frontend/components/cmdk/core.cljs)). Nothing is recorded while a graph is still restoring.

A newly visited page goes to the top of the list. Reopening a page that is already in the list does not move it ([`src/main/frontend/handler/db_based/recent.cljs:5-12`](../../../../../src/main/frontend/handler/db_based/recent.cljs)).

The list is kept per graph in app state and saved to the browser's `localStorage` ([`src/main/frontend/state.cljs:1742-1749`](../../../../../src/main/frontend/state.cljs)). It stays on the device and is not stored in the graph.

### Recent Pages Limit

Logseq keeps the **last 15 unique recent pages** in the stored list ([`src/main/frontend/handler/db_based/recent.cljs:11`](../../../../../src/main/frontend/handler/db_based/recent.cljs)).

The sidebar may show fewer. It skips entries that are not pages, hidden pages, hidden properties and pages with a blank title ([`src/main/frontend/worker/handler/render_resource/basic.cljs:66-83`](../../../../../src/main/frontend/worker/handler/render_resource/basic.cljs)).

### Clearing Recent Pages

There is no UI action to clear Recent Pages. Pages you delete or move to the Recycle bin are removed from the list automatically ([`src/main/frontend/modules/outliner/pipeline.cljs:53-61`](../../../../../src/main/frontend/modules/outliner/pipeline.cljs)).

## Practical Workflows

### Daily Review Workflow

**Setup favorites for daily pages:**

```markdown
Favorites:
├── 📋 Daily Dashboard
├── ✅ Task Inbox
└── 📝 Quick Capture
```

**Navigate quickly:**

1. `g j` - Go to journals page ([`src/main/frontend/modules/shortcut/config.cljs:328`](../../../../../src/main/frontend/modules/shortcut/config.cljs))
2. `Ctrl/Cmd + Shift + J` (macOS) or `Alt + Shift + J` (Windows/Linux) - Open today's page in the right sidebar ([`:340`](../../../../../src/main/frontend/modules/shortcut/config.cljs))
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

- Recent automatically tracks the pages you visit
- No need to favorite every sub-page
- Quick access to what you were working on

Recent Pages are managed automatically from navigation. No page property adds a page to the list.

### Knowledge Base Navigation

**Favorite main hubs:**

```markdown
Favorites:
├── 📚 Knowledge Hub
├── 🏷️ Tag Index
└── 🔍 Search Dashboard
```

**Use recent for exploration:**

- Recent tracks your research path
- Easily backtrack through connected notes

## Advanced Tips

### Favorites Dashboard

Create a dashboard page that embeds your key pages. In a DB graph, use the **/Node embed** command. The old `{{embed [[...]]}}` macro only shows a deprecation warning ([`src/main/frontend/components/block.cljs:1934-1935`](../../../../../src/main/frontend/components/block.cljs), [`src/resources/dicts/en.edn:157`](../../../../../src/resources/dicts/en.edn), [`:549`](../../../../../src/resources/dicts/en.edn)).

1. Create a page such as `My Dashboard`.
2. In a block, type `/Node embed` and pick the page to embed.
3. Repeat for each page.

### Finding Recently Updated Pages

DB graphs do not render `#+BEGIN_QUERY` blocks. In current source that marker appears only in the file-graph importer ([`deps/graph-parser/src/logseq/graph_parser/exporter.cljs:1452`](../../../../../deps/graph-parser/src/logseq/graph_parser/exporter.cljs)). Use the **/Query** command instead.

> **Unverified:** that this advanced query runs as written when entered through `/Query` in a DB graph. It uses `:block/name`, `:block/title` and `:block/updated-at`, and it omits `:limit`.

```clojure
{:query [:find (pull ?p [:block/uuid :block/title :block/updated-at])
         :where
         [?p :block/name]
         [?p :block/updated-at ?t]
         [(> ?t 1700000000000)]]
 :result-transform (fn [result] (sort-by :block/updated-at > result))}
```

Note: Logseq does not track page visit frequency.

## Mobile Considerations

### Mobile Favorites

**Access:**

- Tap the bottom **Go To** tab ([`src/resources/dicts/en.edn:1049`](../../../../../src/resources/dicts/en.edn))
- Favorites and Recent Pages share the same panel, which reuses the desktop sidebar lists ([`src/main/mobile/components/favorites.cljs:6-12`](../../../../../src/main/mobile/components/favorites.cljs))

**Adding:**

1. Open the page
2. Tap the favorite action in the page's top bar. It toggles the page on or off ([`src/main/mobile/components/header.cljs:210-225`](../../../../../src/main/mobile/components/header.cljs)).

### Mobile Recent Pages

**Access:**

- Tap the bottom **Go To** tab
- Use the **Recent** list in the same panel as Favorites

Mobile uses the same Recent list component as desktop, so it applies the same 15-page history and the same filtering.

## Keyboard Shortcuts Summary

| Action                       | Windows/Linux        | macOS               | Source (`shortcut/config.cljs`) |
| ---------------------------- | -------------------- | ------------------- | ------------------------------- |
| Toggle favorite              | `Ctrl + Shift + F`   | `Cmd + Shift + F`   | `:page/toggle-favorite` (441)    |
| Go to journals               | `g j`                | `g j`               | `:go/journals` (328)             |
| Open today in sidebar        | `Alt + Shift + J`    | `Cmd + Shift + J`   | `:sidebar/open-today-page` (340) |
| Quick search                 | `Ctrl + K`           | `Cmd + K`           | `:go/search` (304)               |
| Highlight recent blocks      | `Ctrl + C, Ctrl + R` | `Cmd + C, Cmd + R`  | `:ui/highlight-recent-blocks` (420) |
| Follow link under cursor     | `Ctrl + O`           | `Cmd + O`           | `:editor/follow-link` (113)      |

These match the [Keyboard Shortcuts reference](../../reference/configuration/keyboard-shortcuts.md).

## Troubleshooting

### Favorites Not Saving

**Problem:** Favorites disappear or do not appear

**Solution:**

- Make sure you are in the graph where you added the favorite. Favorites are stored per graph, in that graph's database.
- A favorite page that is in the Recycle bin is hidden from the list. Restore the page to show it again.
- Try re-adding favorites

### Recent Pages Empty

**Problem:** Recent pages list is empty

**Solution:**

- Check you're in the correct graph. The list is kept per graph.
- Recent Pages are kept per device in `localStorage`, so a new device or cleared browser data starts with an empty list.
- Navigate to a few pages to populate the list
- Confirm the pages are not hidden or blank

### Too Many Favorites

**Problem:** Favorites list is cluttered

**Solution:**

1. Review and remove unused favorites
2. Create a "Favorites Hub" page that embeds your key pages with `/Node embed`
3. Keep favorites to 5-10 most-used pages

### Can't Find Recent Page

**Problem:** Recently visited page not in list

**Solution:**

- The page may have dropped out of the 15-page history
- Reopening a page that is already listed does not move it to the top
- Use search instead: `Ctrl/Cmd + K`
- Check whether the page was deleted or moved to the Recycle bin. Those pages are removed from the list.
- Use in-page search: `Ctrl/Cmd + F`. It is available only in the desktop app ([`src/main/frontend/modules/shortcut/config.cljs:316-317`](../../../../../src/main/frontend/modules/shortcut/config.cljs)).

## Best Practices

### 1. Keep Favorites Minimal

```markdown
✅ Good: 5-10 core pages

- Dashboard
- Task Inbox
- Active Project
- Reading List

❌ Avoid: 50+ favorites

- Every project page
- Every note page
- Random pages
```

### 2. Order by Importance

The Favorites list is flat and keeps the order you drag it into. Put the pages you open most often at the top.

### 3. Leverage Recent for Deep Work

- Don't favorite every page
- Let recent track your workflow
- Favorite only anchor pages
- Use recent for context switching

### 4. Regular Cleanup

Monthly review:

1. Remove unused favorites
2. Check favorite order
3. Update hub pages

## Integration with Other Features

### With Search

Use `Ctrl/Cmd + K` to open search. Before you type, it shows a **Recently updated** group. Despite the label, that group lists your Recent Pages, the same visited-pages history as the sidebar, not pages sorted by update time ([`src/main/frontend/components/cmdk/core.cljs:234-239`](../../../../../src/main/frontend/components/cmdk/core.cljs), [`:305-316`](../../../../../src/main/frontend/components/cmdk/core.cljs), [`src/resources/dicts/en.edn:259`](../../../../../src/resources/dicts/en.edn)). As you type, that group narrows to matching recent pages, and search also finds other pages and blocks. Favorites get no special priority in search results.

### With Command Palette

Search has no dedicated Favorites group. Use the left sidebar Favorites list or search for the page name.

### With Graph View

The graph view displays pages and their connections. See [Graph Visualization](../../explanation/concepts/graph-visualization.md).

## See Also

- [Linking Pages Tutorial](../../tutorials/getting-started/04-linking-pages.md)
- [Quick Capture Guide](quick-capture.md)
- [Graph Visualization](../../explanation/concepts/graph-visualization.md)
