# Contents Page Guide

> **Last revised:** 2026-05-29

The Contents page is a special page target in Logseq's right sidebar. It acts as a practical navigation hub and can be pinned/opened like other sidebar items.

## What Is the Contents Page?

The Contents page behaves like a normal page when rendered, with some special UI handling in the right sidebar. It functions as:

- **Navigation Hub**: A central location to organize links to your most important pages
- **Table of Contents**: A structured overview of your knowledge base
- **Dashboard**: A customizable starting point for exploring your notes
- **Right Sidebar Integration**: Always accessible via the right sidebar

## Key Features

### 1. Right Sidebar Access

- Always available as a "Contents" button in the right sidebar topbar
- Displays with a list-details icon for easy identification
- Automatically loads when the right sidebar is empty

### 2. Toggle Command and Shortcut

- You can toggle Contents in the sidebar via command `:ui/toggle-contents`
- Default shortcut is `alt+shift+c`; it works while editing a block as well as when not editing (global handler since `dbe1abb4cf`)
- Plugin API command surface includes `logseq.ui/toggle-contents`

### 3. Regular Page Functionality

Despite its special status, the Contents page works like any other Logseq page:

- You can add blocks, links, and any standard Logseq content
- Supports all formatting options (bullets, headings, properties, etc.)
- Can include queries, block embeds, and references
- Participates in the graph view like other pages

### 4. Creation and Recognition Rules

- **Database graphs**: `Contents` is created in DB graph initialization data.
- **File-based graphs**: `Contents` is recognized from files like `pages/contents.*` and from sidebar/default-home config references.

> **Removed:** "Created automatically for all graphs (both file-based and database graphs)."

## How to Use the Contents Page

### Accessing the Contents Page

1. **Right Sidebar**: Click the "Contents" button in the right sidebar
2. **Direct Navigation**: Search for "Contents" in the global search
3. **Page Links**: Navigate to it like any other page using `[[Contents]]`
4. **Shortcut**: Use `alt+shift+c` to toggle it in the right sidebar (also while editing a block)

### Best Practices for Organization

#### 1. Create a Hierarchical Structure

```markdown
# Contents

## 🎯 Current Projects

- [[Project Alpha]]
- [[Project Beta]]
- [[Weekly Review]]

## 📚 Knowledge Areas

- [[Programming]]
  - [[JavaScript]]
  - [[Python]]
  - [[Web Development]]
- [[Personal]]
  - [[Health]]
  - [[Finance]]
  - [[Reading List]]

## 🔗 Quick Links

- [[Daily Notes]]
- [[Meeting Notes]]
- [[Ideas]]
```

#### 2. Use Page Links and References

- **Page Links**: `[[Page Name]]` - Creates connections in your graph
- **Block References**: `[[<block-uuid>]]` - Links to specific blocks
- **Block Embeds**: `{{embed [[Page Name]]}}` - Shows content inline

#### 3. Add Contextual Information

```markdown
## 📊 Dashboards

- [[Weekly Review]] - Every Sunday planning session
- [[Project Status]] - Current project overviews
- [[Learning Goals]] - Skills and knowledge targets

## 🏷️ Important Tags

- #priority - High-priority items
- #review - Items needing review
- #archive - Completed/archived content
```

#### 4. Include Dynamic Content

You can add queries to make your Contents page dynamic:

```markdown
## Recent Updates

{{query (and (page-property :updated-at) (between (relative-date -7) (relative-date 0)))}}

## Unfinished Tasks

{{query (and (task NOW LATER TODO) (not (page "Archive")))}}
```

### Advanced Usage

#### 1. Multiple Contents Sections

Create different sections for different contexts:

- Work-related content
- Personal projects
- Learning and development
- Reference materials

#### 2. Visual Organization

Use emojis and formatting to make navigation easier:

- 🎯 for goals and objectives
- 📚 for knowledge areas
- 🔧 for tools and resources
- 📊 for dashboards and reports

#### 3. Linked Contents

Create links between related Contents sections:

```markdown
## Related Navigation

- [[Work Contents]] - Professional projects and tasks
- [[Personal Contents]] - Personal life and interests
- [[Learning Contents]] - Study materials and courses
```

## Technical Implementation

### Code Location

- **Right Sidebar render + button + default fallback**: `src/main/frontend/components/right_sidebar.cljs`
- **Open sidebar fallback-to-contents**: `src/main/frontend/state.cljs`
- **Toggle command**: `src/main/frontend/handler/ui.cljs`
- **Shortcut mapping (`alt+shift+c`)**: `src/main/frontend/modules/shortcut/config.cljs`
- **DB graph built-in page creation**: `deps/db/src/logseq/db/sqlite/create_graph.cljs`
- **Graph parser recognition for `pages/contents.*`**: `deps/graph-parser/src/logseq/graph_parser/extract.cljc`
- **Plugin external command type**: `libs/src/LSPlugin.ts`

### How It Works

1. **Sidebar Fallback**: When right sidebar has no blocks, Logseq inserts `:contents`.
2. **Sidebar Integration**: Special `:contents` block type in right sidebar
3. **Page Rendering**: Renders as a normal page using `page-cp` component
4. **DB Graphs**: Built-in `Contents` page is present in initial graph data
5. **File Graphs**: `pages/contents.*` is normalized to title `Contents`

### Special Cases

- The Contents page is protected from delete in page menu logic.
- Some inline page-reference bracket rendering is suppressed in the Contents page context.

### Customization

Since Contents is a regular page, you can:

- Add any standard Logseq content
- Use templates to structure it
- Include properties and metadata
- Reference it from other pages

## Tips and Tricks

### 1. Keep It Current

- Regularly update your Contents page as your knowledge base grows
- Remove outdated links and add new important pages
- Use it as a reflection tool during weekly reviews

### 2. Make It Personal

- Organize it to match your thinking patterns
- Use terminology and categories that make sense to you
- Include motivational quotes or personal reminders

### 3. Link Strategically

- Focus on pages you access frequently
- Include entry points to different knowledge areas
- Balance between comprehensiveness and usability

### 4. Use as a Launchpad

- Start your Logseq sessions by reviewing the Contents page
- Use it to plan your daily work and studies
- Keep it visible in the right sidebar while working

## Common Use Cases

1. **Project Management**: Organize current and planned projects
2. **Knowledge Management**: Categorize learning materials and references
3. **Daily Workflow**: Quick access to frequently used pages
4. **Navigation Aid**: Help others understand your knowledge base structure
5. **Progress Tracking**: Monitor growth and development over time

## Troubleshooting

### Contents Page Not Showing

- Check if the right sidebar is open
- Look for the "Contents" button in the sidebar topbar
- Try searching for "Contents" in global search

### Contents Page Empty

- This is normal for new graphs
- Start adding content using standard Logseq editing
- Use page links `[[Page Name]]` to connect to other pages

### Performance with Large Contents

- If your Contents page becomes very large, consider splitting it
- Create separate Contents pages for different areas
- Use queries instead of manual lists for dynamic content

## Findings: New Capabilities

- **Keyboard toggle** for Contents is now explicit and first-class (`alt+shift+c`, command `:ui/toggle-contents`).
- **Plugin command integration** includes `logseq.ui/toggle-contents`, so plugins can open/close Contents programmatically.

## Conclusion

The Contents page remains a strong navigation tool, with sidebar-first integration and command-level controls. The key implementation nuance is that automatic creation is guaranteed for DB graphs, while file-based graphs depend on recognized files/config references.

Remember: treat it as a normal page for content authoring, but account for its special sidebar behavior and protections when documenting or extending the feature.
