# Extract-Config Skill - Quick Reference

**Skill Type:** Configuration Extraction & Documentation  
**Version:** 1.0  
**Prompt File:** `.github/prompts/extract-config.prompt.md`

---

## Quick Start

### Basic Invocation

```
@workspace /extract-config <system-name>
```

### Common Use Cases

| Use Case               | Command                                | Output                                            |
| ---------------------- | -------------------------------------- | ------------------------------------------------- |
| **Keyboard Shortcuts** | `@workspace /extract-config shortcuts` | Table with bindings, commands, descriptions       |
| **API Endpoints**      | `@workspace /extract-config api`       | REST endpoints with methods, params, responses    |
| **Settings**           | `@workspace /extract-config settings`  | User preferences with types, defaults, validation |
| **Theme Variables**    | `@workspace /extract-config theme`     | CSS variables with values and categories          |
| **Commands**           | `@workspace /extract-config commands`  | Command IDs, shortcuts, descriptions, categories  |

---

## What It Does

1. ✅ **Investigates** - Understands your configuration architecture
2. ✅ **Extracts** - Parses data from multiple source files
3. ✅ **Aggregates** - Combines data from configs, translations, categories
4. ✅ **Generates** - Creates markdown/JSON/CSV reference documentation
5. ✅ **Automates** - Builds reusable extraction script for updates

---

## Output Structure

```markdown
# <System> Configuration Reference

**Total Items:** 135

## All Items

| ID       | Value   | Description | Category |
| -------- | ------- | ----------- | -------- |
| `item-1` | `value` | Description | Category |
| ...      | ...     | ...         | ...      |

## Summary by Category

### Category 1 (20 items)

- **`item-id`** - `value` - Description
- ...
```

---

## Example: Logseq Commands

**Input:**

```
@workspace /extract-config logseq-commands

Extract all command palette commands with shortcuts, descriptions, and categories
```

**Output:**

- ✅ Script: `dz/scripts/extract_commands.py`
- ✅ Documentation: `dz/docs/command-palette-reference.md`
- ✅ 135 commands with shortcuts, descriptions, 8 categories
- ✅ Platform-specific bindings (Mac vs Windows/Linux)
- ✅ Reusable for future updates

---

## Key Features

### Multi-Source Aggregation

Combines data from:

- Configuration files (JSON, YAML, TypeScript, ClojureScript)
- Translation files (i18n, dictionaries)
- Category definitions
- Schema files

### Smart Parsing

- Platform conditionals: `(if mac? "cmd+b" "ctrl+b")`
- Multiple formats: `["option1", "option2"]`
- Fallback generation for missing data
- Namespace-based category inference

### Validation

- Completeness checks
- Required field validation
- Duplicate detection
- Type validation

### Output Formats

- **Markdown** - Human-readable reference tables
- **JSON** - Programmatic access
- **CSV** - Spreadsheet import
- **HTML** - Interactive with search

---

## 5-Phase Process

1. **Investigation** - Understand system architecture
2. **Design** - Plan extraction strategy
3. **Implementation** - Build automated script
4. **Documentation** - Generate reference
5. **Validation** - Quality assurance

---

## Common Patterns

### Pattern 1: Multi-Source

```python
config = parse_config_file()
translations = parse_translations()
categories = parse_categories()
combined = merge_all(config, translations, categories)
```

### Pattern 2: Conditionals

```python
def parse_conditional(value):
    # Input: "(if mac? 'cmd+b' 'ctrl+b')"
    # Output: "Mac: cmd+b / Win: ctrl+b"
```

### Pattern 3: Inference

```python
def infer_category(item_id):
    # "editor/bold" → "Editing"
    # "go/home" → "Navigation"
```

---

## When to Use

✅ **Use this skill when:**

- Documenting keyboard shortcuts
- Creating API reference
- Cataloging settings/preferences
- Extracting theme variables
- Documenting commands/actions
- Building configuration guides

❌ **Don't use for:**

- One-time manual documentation
- Dynamic runtime configuration
- User-specific settings
- Temporary/debug configurations

---

## Success Criteria

A successful extraction has:

- ✅ 100% item coverage
- ✅ 0 parsing errors
- ✅ All required fields present
- ✅ Automated script created
- ✅ Validation passing
- ✅ Can be re-run anytime

---

## Troubleshooting

### Issue: Incomplete extraction

**Fix:** Check source file patterns, add more parsers

### Issue: Parsing errors

**Fix:** Use AST parser instead of regex for complex syntax

### Issue: Missing descriptions

**Fix:** Add fallback logic to generate from IDs

### Issue: Wrong categories

**Fix:** Review category mapping logic, add inference rules

---

## Files Created

After running the skill:

```
project/
├── scripts/
│   └── extract_<config>.py      # Extraction script
├── docs/
│   └── <config>-reference.md    # Reference documentation
└── .github/
    └── prompts/
        └── extract-config.prompt.md  # This skill definition
```

---

## Related Skills

- **@workspace /investigate** - Code detective investigation framework
- **@workspace /new-changelog** - Generate changelogs from git commits

---

## Learn More

**Full Documentation:** `.github/prompts/extract-config.prompt.md`  
**Example:** `dz/docs/EXECUTION-PLAN.md` (Logseq command extraction)  
**Script Example:** `dz/scripts/extract_commands.py`

---

**Remember:** Always create a **reusable script** that can be run again when source code changes. One-time manual extraction is not enough!
