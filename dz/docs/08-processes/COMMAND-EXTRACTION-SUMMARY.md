# Logseq Command Palette Extraction - Implementation Summary

> **Last revised:** 2026-03-10

## Overview

Successfully extracted and documented all 135 built-in commands from Logseq's command palette system into a comprehensive reference table.

## Investigation Findings

### Command System Architecture

1. **Central Storage**

   - Commands stored in `:command-palette/commands` atom
   - Location: [src/main/frontend/handler/command_palette.cljs](../../src/main/frontend/handler/command_palette.cljs)

2. **Command Sources**

   - **Built-in shortcuts**: ~135 commands defined in [src/main/frontend/modules/shortcut/config.cljs](../../src/main/frontend/modules/shortcut/config.cljs)
   - **Plugin commands**: Dynamically registered via plugin API
   - **User extensions**: Custom commands via registration API

3. **Command Structure**

   ```clojure
   {:id :editor/bold                    ; Namespaced keyword identifier
    :desc "Bold"                         ; Human-readable description
    :action (fn [] ...)                  ; Execution function
    :shortcut "mod+b"                    ; Keyboard binding
    :handler-id :shortcut.handler/...   ; Handler context
    :tag [:vector]}                      ; Additional metadata
   ```

4. **Categories**

   - Commands organized into 8 categories
   - Defined in `*category` atom
   - Categories: basics, formatting, navigating, block-editing, block-command-editing, block-selection, toggle, others

5. **Translations**
   - Descriptions stored in [src/resources/dicts/en.edn](../../src/resources/dicts/en.edn)
   - Pattern: `:command.<namespace>/<action>`
   - Example: `:command.editor/bold` → "Bold"

## Implementation Approach

### Extraction Script

Created Python script at [dz/scripts/extract_commands.py](../scripts/extract_commands.py) that:

1. **Parses shortcuts** from ClojureScript config files
2. **Extracts categories** from category atom definition
3. **Maps translations** from English dictionary file
4. **Aggregates data** into structured format
5. **Generates markdown table** with sortable columns

### Key Features

- ✅ All 135 built-in commands extracted
- ✅ Keyboard shortcuts parsed (including platform-specific variations)
- ✅ Descriptions from translation keys
- ✅ Category classifications
- ✅ Markdown table format for easy consumption
- ✅ Summary by category
- ✅ Legend explaining conventions

### Data Quality

- **Commands Found**: 135
- **Categorized Commands**: 128
- **Translations**: 454 (includes all command descriptions)
- **Categories**: 8

## Output Files

1. **[dz/docs/command-palette-reference.md](command-palette-reference.md)** - Main reference table (317 lines)

   - Header with legend and instructions
   - Complete table of all commands
   - Summary grouped by category

2. **[dz/scripts/extract_commands.py](../scripts/extract_commands.py)** - Extraction script
   - Reusable for updates
   - Can be run anytime source changes

## Usage

### View the Reference

```bash
cat dz/docs/command-palette-reference.md
```

### Regenerate (if source code changes)

```bash
python3 dz/scripts/extract_commands.py
```

## Command Summary by Category

| Category                  | Count | Examples                                  |
| ------------------------- | ----- | ----------------------------------------- |
| **Basics**                | 13    | Search, Copy, Undo, Redo                  |
| **Formatting**            | 5     | Bold, Italics, Highlight, Link            |
| **Navigating**            | 20    | Go to pages, journals, graph, move cursor |
| **Block Editing**         | 14    | New block, indent, move up/down, zoom     |
| **Block Command Editing** | 14    | Kill line, word navigation, paste         |
| **Block Selection**       | 14    | Select blocks, add properties, tags       |
| **Toggle**                | 12    | Theme, sidebars, wide mode, settings      |
| **Others**                | 43    | PDF, cards, dev tools, plugins, export    |

## Platform-Specific Notes

- **`mod` key**: `Cmd` on Mac / `Ctrl` on Windows/Linux
- Some commands have platform-specific bindings:
  - Example: `:editor/quick-add` - Mac: `Cmd+E` / Win/Linux: `Ctrl+Alt+E`
- Commands marked with `—` have no keyboard shortcut assigned

## Known Limitations

1. **Plugin commands not included** - Only built-in commands extracted
2. **Dynamic commands** - Some commands registered at runtime not captured
3. **Slash commands** - Editor-specific `/` commands not in command palette

## Future Enhancements

1. Add JSON/CSV export formats
2. Include plugin command detection
3. Add search/filter capabilities
4. Generate interactive HTML version
5. Track command usage statistics
6. Compare commands across Logseq versions

## Related Documentation

- [Code Detective Investigation Prompt](./.github/prompts/code-detective-investigation.prompt.md) - Investigation framework used
- [AGENTS.md](../../AGENTS.md) - Repository agent guidelines
- [Command Palette Handler](../../src/main/frontend/handler/command_palette.cljs) - Source code
- [Keyboard Shortcuts Config](../../src/main/frontend/modules/shortcut/config.cljs) - Shortcut definitions

---

**Generated**: January 27, 2026  
**Script**: [dz/scripts/extract_commands.py](../scripts/extract_commands.py)  
**Output**: [command-palette-reference.md](command-palette-reference.md)
