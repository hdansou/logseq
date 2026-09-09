---
title: 'Keyboard Shortcuts Reference'
status: 🟢 Stable
version: 'Logseq v0.10.9'
quadrant: 'Reference'
platform: 'Desktop + Mobile'
related-docs:
  - '[Slash Commands](./slash-commands.md)'
  - '[Block Editing](../../tutorials/getting-started/block-editing.md)'
  - '[Navigation](../../how-to/navigation/graph-navigation.md)'
tags: ['keyboard', 'shortcuts', 'productivity', 'reference']
---

# Keyboard Shortcuts Reference

Complete reference of keyboard shortcuts in Logseq. Master these shortcuts to dramatically increase your note-taking speed and efficiency.

**Platform Notation:**

- `⌘` = Command (macOS) / `Ctrl` (Windows/Linux)
- `⌥` = Option (macOS) / `Alt` (Windows/Linux)
- `⇧` = Shift
- `↵` = Enter/Return
- `⌫` = Backspace/Delete

---

## Table of Contents

1. [Essential Shortcuts](#essential-shortcuts)
2. [Block Operations](#block-operations)
3. [Navigation](#navigation)
4. [Formatting](#formatting)
5. [Search & Commands](#search--commands)
6. [Page Operations](#page-operations)
7. [Journal & Tasks](#journal--tasks)
8. [Customizing Shortcuts](#customizing-shortcuts)

---

## Essential Shortcuts

### Most Used (Learn These First)

| Shortcut | Action                               | Context        |
| -------- | ------------------------------------ | -------------- |
| `⌘ K`    | Quick switcher (go to page)          | Global         |
| `⌘ S`    | Save current block                   | Editing        |
| `⌘ /`    | Toggle between editing and view mode | Any block      |
| `↵`      | Create new block below               | Block selected |
| `⇧ ↵`    | Create new line in same block        | Editing        |
| `Tab`    | Indent block (make child)            | Block selected |
| `⇧ Tab`  | Outdent block (make sibling)         | Block selected |
| `⌘ ⇧ P`  | Command palette                      | Global         |
| `⌘ J`    | Open today's journal                 | Global         |
| `Esc`    | Exit edit mode                       | Editing        |

---

## Block Operations

### Creating & Editing

| Shortcut | Action                | Notes                  |
| -------- | --------------------- | ---------------------- |
| `↵`      | New block below       | Creates sibling        |
| `⇧ ↵`    | New line (soft break) | Within same block      |
| `⌘ ↵`    | New block before      | Creates sibling above  |
| `⌥ ↵`    | New block above       | Alternative            |
| `⌘ ⇧ ↵`  | New child block       | Creates as child       |
| `Tab`    | Indent right          | Make child of above    |
| `⇧ Tab`  | Outdent left          | Make sibling of parent |
| `⌘ ⇧ L`  | Open block in sidebar | View in context        |
| `⌘ .`    | Open block menu       | Context menu           |

### Moving Blocks

| Shortcut | Action                | Notes                      |
| -------- | --------------------- | -------------------------- |
| `⌘ ⇧ ↑`  | Move block up         | Swap with previous sibling |
| `⌘ ⇧ ↓`  | Move block down       | Swap with next sibling     |
| `⌘ ⇧ →`  | Move right (indent)   | Become child               |
| `⌘ ⇧ ←`  | Move left (outdent)   | Become uncle               |
| `⌥ ↑`    | Select previous block | For keyboard navigation    |
| `⌥ ↓`    | Select next block     | For keyboard navigation    |

### Selecting & Deleting

| Shortcut  | Action                    | Notes                                               |
| --------- | ------------------------- | --------------------------------------------------- |
| `⌘ A`     | Select all text in block  | First press: block text<br>Second press: all blocks |
| `⌘ ⇧ A`   | Select all blocks on page | -                                                   |
| `⇧ Click` | Multi-select blocks       | Click multiple blocks                               |
| `⌘ ⌫`     | Delete block              | Moves to recycle bin                                |
| `⌘ ⇧ ⌫`   | Delete block tree         | Delete with children                                |
| `⌫`       | Merge with previous       | At start of empty block                             |

### Copying & Duplicating

| Shortcut | Action               | Notes                        |
| -------- | -------------------- | ---------------------------- |
| `⌘ C`    | Copy block(s)        | Includes block ref           |
| `⌘ X`    | Cut block(s)         | -                            |
| `⌘ V`    | Paste                | Smart paste (blocks or text) |
| `⌘ ⇧ D`  | Duplicate block      | Creates copy below           |
| `⌘ ⇧ C`  | Copy block reference | Copy `((block-uuid))`        |
| `⌘ ⇧ V`  | Copy block embed     | Copy `{{embed ((uuid))}}`    |

---

## Navigation

### Page Navigation

| Shortcut | Action                     | Notes                 |
| -------- | -------------------------- | --------------------- |
| `⌘ K`    | Quick switcher             | Fuzzy search pages    |
| `⌘ ⇧ K`  | Search in current graph    | Full-text search      |
| `⌘ O`    | Open page                  | Same as `⌘ K`         |
| `⌘ [`    | Go back                    | Browser-style back    |
| `⌘ ]`    | Go forward                 | Browser-style forward |
| `⌘ J`    | Jump to today's journal    | -                     |
| `⌘ ⇧ J`  | Jump to tomorrow's journal | -                     |
| `⌘ Home` | Go to top of page          | -                     |
| `⌘ End`  | Go to bottom of page       | -                     |

### Sidebar

| Shortcut | Action                | Notes            |
| -------- | --------------------- | ---------------- |
| `⌘ ⇧ L`  | Open block in sidebar | Right sidebar    |
| `⌘ ⇧ R`  | Open page in sidebar  | Right sidebar    |
| `⌘ ⇧ E`  | Toggle right sidebar  | Show/hide        |
| `⌘ B`    | Toggle left sidebar   | Favorites/recent |

### Zoom

| Shortcut           | Action                | Notes               |
| ------------------ | --------------------- | ------------------- |
| `⌘ ↑`              | Zoom in (focus block) | Makes block root    |
| `⌘ ↓`              | Zoom out              | Return to page view |
| `.` (period twice) | Zoom to block         | Quick zoom shortcut |
| `⌘ .`              | Expand/collapse block | Toggle children     |
| `⌘ ⇧ .`            | Expand all blocks     | On page             |

---

## Formatting

### Text Formatting

| Shortcut | Syntax              | Result                      |
| -------- | ------------------- | --------------------------- |
| `⌘ B`    | `**bold**`          | **bold**                    |
| `⌘ I`    | `*italic*`          | _italic_                    |
| `⌘ U`    | -                   | <u>underline</u> (via menu) |
| `⌘ ⇧ S`  | `~~strikethrough~~` | ~~strikethrough~~           |
| `⌘ ⇧ H`  | `^^highlight^^`     | ==highlight==               |
| `⌘ E`    | `` `code` ``        | `inline code`               |
| `⌘ L`    | `[text](url)`       | Insert link                 |

### Block Formatting

| Shortcut  | Action             | Notes            |
| --------- | ------------------ | ---------------- |
| `/`       | Open slash command | In empty block   |
| `⌘ ⇧ 1-6` | Heading level 1-6  | Add # prefix     |
| `⌘ ⇧ 7`   | Ordered list       | Add 1. prefix    |
| `⌘ ⇧ 8`   | Unordered list     | Add - prefix     |
| `⌘ ⇧ 9`   | Checklist          | Add TODO keyword |

### Special Blocks

| Shortcut | Slash Command | Creates     |
| -------- | ------------- | ----------- |
| `⌘ ⇧ <`  | `/quote`      | Quote block |
| -        | `/code`       | Code block  |
| -        | `/query`      | Query block |
| -        | `/embed`      | Embed block |
| -        | `/draw`       | Whiteboard  |

---

## Search & Commands

### Search

| Shortcut | Action          | Scope                   |
| -------- | --------------- | ----------------------- |
| `⌘ F`    | Find in page    | Current page only       |
| `⌘ ⇧ F`  | Find in graph   | All pages               |
| `⌘ G`    | Find next       | Navigate search results |
| `⌘ ⇧ G`  | Find previous   | Navigate search results |
| `⌘ H`    | Replace in page | Find and replace        |

### Commands

| Shortcut | Action               | Notes               |
| -------- | -------------------- | ------------------- |
| `⌘ ⇧ P`  | Command palette      | Search all commands |
| `/`      | Slash commands       | Block actions       |
| `⌘ ⇧ C`  | Copy block reference | `((block-ref))`     |
| `⌘ ⇧ T`  | Show all TODOs       | Task overview       |
| `⌘ ⇧ A`  | Show all pages       | Graph overview      |

---

## Page Operations

### Page Management

| Shortcut | Action                 | Notes           |
| -------- | ---------------------- | --------------- |
| `⌘ N`    | New page               | Opens quick add |
| `⌘ ⇧ N`  | New page in sidebar    | Create and pin  |
| `⌘ R`    | Rename page            | Current page    |
| `⌘ ⇧ D`  | Delete page            | Moves to trash  |
| `[[`     | Create page link       | Type page name  |
| `((`     | Create block reference | Search blocks   |

### Page Views

| Shortcut | Action               | Notes               |
| -------- | -------------------- | ------------------- |
| `⌘ ⇧ I`  | Toggle document mode | Hide bullets        |
| `⌘ ⇧ M`  | Toggle edit mode     | All blocks editable |
| `t w`    | Wide mode            | Full width          |
| `t r`    | Right sidebar        | Toggle visibility   |
| `t l`    | Left sidebar         | Toggle visibility   |
| `t t`    | Dark/light theme     | Toggle theme        |

---

## Journal & Tasks

### Journal

| Shortcut | Action             | Notes           |
| -------- | ------------------ | --------------- |
| `⌘ J`    | Today's journal    | Jump to today   |
| `⌘ ⇧ J`  | Tomorrow's journal | Create tomorrow |
| `⌘ <`    | Previous journal   | Navigate days   |
| `⌘ >`    | Next journal       | Navigate days   |

### Tasks

| Shortcut     | Action                 | Notes              |
| ------------ | ---------------------- | ------------------ |
| `⌘ ↵`        | Toggle TODO/DOING/DONE | Cycle states       |
| `⌘ ⇧ T`      | Show all tasks         | Task dashboard     |
| `/deadline`  | Add deadline           | Deadline picker    |
| `/scheduled` | Add scheduled date     | Schedule picker    |
| `⌘ ⇧ .`      | Set block priority     | A, B, C priorities |

### Task Markers (Type in block)

| Marker      | Shortcut     | Creates       |
| ----------- | ------------ | ------------- |
| `TODO`      | `/todo`      | TODO item     |
| `DOING`     | `/doing`     | In-progress   |
| `DONE`      | `/done`      | Completed     |
| `LATER`     | `/later`     | Backlog       |
| `NOW`       | `/now`       | High priority |
| `WAITING`   | `/waiting`   | Blocked       |
| `CANCELLED` | `/cancelled` | Cancelled     |

---

## Editor Shortcuts

### Text Editing

| Shortcut   | Action              | Notes        |
| ---------- | ------------------- | ------------ |
| `⌘ Z`      | Undo                | Last change  |
| `⌘ ⇧ Z`    | Redo                | Redo change  |
| `⌘ →`      | End of line         | Move cursor  |
| `⌘ ←`      | Start of line       | Move cursor  |
| `⌥ →`      | Next word           | Jump words   |
| `⌥ ←`      | Previous word       | Jump words   |
| `⌘ ⌫`      | Delete word back    | Quick delete |
| `⌘ Delete` | Delete word forward | -            |

### Selection

| Shortcut | Action            | Notes          |
| -------- | ----------------- | -------------- |
| `⇧ →`    | Select right      | Character      |
| `⇧ ←`    | Select left       | Character      |
| `⌘ ⇧ →`  | Select to end     | End of line    |
| `⌘ ⇧ ←`  | Select to start   | Start of line  |
| `⌥ ⇧ →`  | Select word right | Word selection |
| `⌥ ⇧ ←`  | Select word left  | Word selection |

---

## Special Features

### References & Embeds

| Syntax                | Shortcut | Creates         |
| --------------------- | -------- | --------------- |
| `[[Page]]`            | `[[`     | Page link       |
| `((block))`           | `((`     | Block reference |
| `{{embed ((block))}}` | `⌘ ⇧ V`  | Block embed     |
| `#tag`                | `#`      | Inline tag      |
| `/page-embed`         | -        | Page embed      |

### Queries

| Shortcut | Command  | Action                   |
| -------- | -------- | ------------------------ |
| -        | `/query` | Insert query             |
| `⌘ ⇧ Q`  | -        | Quick query (if enabled) |

### Whiteboard

| Shortcut       | Action           | Context           |
| -------------- | ---------------- | ----------------- |
| `t d`          | Toggle draw mode | Enable whiteboard |
| `Space + Drag` | Pan canvas       | While drawing     |
| `⌘ +`          | Zoom in          | Canvas            |
| `⌘ -`          | Zoom out         | Canvas            |
| `⌘ 0`          | Reset zoom       | Canvas            |

---

## Customizing Shortcuts

### Modify Shortcuts

1. Open Settings (`⌘ ,`)
2. Navigate to **Keymap**
3. Search for command
4. Click shortcut → Press new key combination
5. Click **Save**

### Example Custom Shortcuts

| Custom  | Default | Action           |
| ------- | ------- | ---------------- |
| `⌘ D`   | `⌘ ⇧ D` | Duplicate block  |
| `⌘ /`   | `Esc`   | Exit edit mode   |
| `F1`    | `⌘ ⇧ P` | Command palette  |
| `⌘ 1-9` | -       | Quick page jumps |

### Export/Import Shortcuts

**Export:**

1. Settings → Keymap
2. Click **Export**
3. Save `keymap.edn` file

**Import:**

1. Settings → Keymap
2. Click **Import**
3. Select `keymap.edn` file

---

## Shortcuts by Workflow

### Student Note-Taking

| Task                | Shortcut          | Why                 |
| ------------------- | ----------------- | ------------------- |
| New lecture page    | `⌘ N`             | Quick page creation |
| Link to course      | `[[Course Name]]` | Organize by course  |
| Quick TODO          | `/todo`           | Homework tracking   |
| Highlight key point | `⌘ ⇧ H`           | Visual emphasis     |
| Copy diagram        | `⌘ C` → `/draw`   | Embed whiteboard    |

### Developer Documentation

| Task              | Shortcut     | Why                  |
| ----------------- | ------------ | -------------------- |
| Code snippet      | `/code`      | Syntax highlighting  |
| Link to function  | `((search))` | Block references     |
| Quick search      | `⌘ ⇧ F`      | Find across projects |
| Copy block ref    | `⌘ ⇧ C`      | Reference in code    |
| Sidebar reference | `⌘ ⇧ L`      | Compare code         |

### Task Management

| Task           | Shortcut    | Why            |
| -------------- | ----------- | -------------- |
| All tasks view | `⌘ ⇧ T`     | Dashboard      |
| Toggle status  | `⌘ ↵`       | Quick updates  |
| Add deadline   | `/deadline` | Time tracking  |
| Priority       | `⌘ ⇧ .`     | Set importance |
| Move block     | `⌘ ⇧ ↑↓`    | Reorganize     |

---

## Mobile Shortcuts

**iOS/iPadOS:**

| Gesture               | Action                   |
| --------------------- | ------------------------ |
| Swipe right           | Indent block             |
| Swipe left            | Outdent block            |
| Long press            | Block menu               |
| Two-finger swipe down | Dismiss keyboard         |
| Pinch                 | Zoom in/out (whiteboard) |

**Android:**

| Gesture     | Action       |
| ----------- | ------------ |
| Swipe right | Indent       |
| Swipe left  | Outdent      |
| Long press  | Context menu |

**External Keyboard (iPad):**

- Most desktop shortcuts work with external keyboard
- Use `⌘` instead of `Ctrl` on iOS

---

## Troubleshooting

**Shortcut not working?**

1. Check Settings → Keymap for conflicts
2. Ensure not in special mode (drawing, selecting)
3. Restart Logseq if recently changed
4. Check OS-level shortcuts don't override

**Conflicts with OS shortcuts:**

- macOS: System Preferences → Keyboard → Shortcuts
- Windows: Settings → Keyboard → Advanced keyboard settings
- Linux: System Settings → Keyboard → Shortcuts

**Reset to defaults:**

Settings → Keymap → **Reset all shortcuts**

---

## Quick Reference Card

**Print this for your desk:**

```
ESSENTIAL
⌘ K    Quick switcher
⌘ J    Today's journal
⌘ S    Save block
↵      New block
Tab    Indent

EDITING
⌘ B    Bold
⌘ I    Italic
⌘ L    Link
⌘ C    Copy
⌘ V    Paste

NAVIGATION
⌘ [    Back
⌘ ]    Forward
⌘ ↑    Zoom in
⌘ ↓    Zoom out

BLOCKS
⌘ ⇧ ↑  Move up
⌘ ⇧ ↓  Move down
⌘ ⇧ C  Copy ref
⌘ ⇧ L  Open sidebar
```

---

## Next Steps

**Master Shortcuts:**

1. Learn 5 essential shortcuts this week
2. Practice daily for muscle memory
3. Customize for your workflow
4. Print quick reference card

**Related:**

- [Slash Commands](./slash-commands.md)
- [Block Editing Tutorial](../../tutorials/getting-started/block-editing.md)
- [Productivity Tips](../../how-to/advanced/productivity-tips.md)

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
