# Extract & Document System Configuration - Reusable Skill

**Purpose:** A reusable skill for extracting structured configuration data from source code and generating comprehensive reference documentation.

**Version:** 1.0  
**Last Updated:** January 27, 2026  
**Based on:** Command Palette Extraction (Logseq)

---

## Overview

This skill automates the process of:

1. **Investigating** configuration system architecture
2. **Extracting** structured data from source code
3. **Aggregating** information from multiple sources
4. **Generating** comprehensive reference documentation
5. **Automating** the process for future updates

**Key Principle:** Always create reusable, automated extraction scripts—not one-time manual documentation.

---

## When to Use This Skill

Use this skill when you need to:

- ✅ Extract configuration data embedded in source code
- ✅ Generate reference tables for commands, shortcuts, settings, or options
- ✅ Document system features comprehensively
- ✅ Create maintainable, automated extraction scripts
- ✅ Produce user-facing reference documentation

### Example Use Cases

| Configuration Type       | Source Format      | Output Fields                               |
| ------------------------ | ------------------ | ------------------------------------------- |
| **Command Palette**      | ClojureScript maps | Command ID, shortcut, description, category |
| **Keyboard Shortcuts**   | JSON/YAML config   | Key binding, command, scope, platform       |
| **API Endpoints**        | Router definitions | Method, path, params, auth, response        |
| **Settings/Preferences** | Config schemas     | Setting name, type, default, validation     |
| **Theme Variables**      | CSS/SCSS files     | Variable name, value, category, usage       |
| **Feature Flags**        | Config files       | Flag name, enabled, rollout %, dependencies |
| **Menu Items**           | UI definitions     | Item ID, label, action, icon, shortcut      |
| **Plugin Registry**      | Manifest files     | Plugin ID, name, version, capabilities      |

---

## Skill Invocation Pattern

### Basic Invocation

```
@workspace /extract-config <SYSTEM_NAME>

Extract and document all <CONFIGURATION_TYPE> from <SOURCE_FILES>.
Generate a comprehensive reference table with:
- Identifiers/names
- Values/bindings
- Descriptions
- Categories/groupings
- Any relevant metadata

Create an automated extraction script for future updates.
```

### Detailed Invocation Template

```
@workspace /extract-config <SYSTEM_NAME>

## Configuration Type
<WHAT_TO_EXTRACT>
Example: keyboard shortcuts, API endpoints, settings, commands

## Data Sources
<SOURCE_FILES_OR_LOCATIONS>
Example: src/config.ts, translations/en.json, schema.yaml

## Required Fields
- Field 1: <DESCRIPTION>
- Field 2: <DESCRIPTION>
- Field 3: <DESCRIPTION>

## Output Format
<TABLE|JSON|CSV|HTML>

## Additional Requirements
- Platform-specific variations (if applicable)
- Categorization/grouping logic
- Translation/description mappings
- Usage examples or notes
```

### Example Invocations

```
@workspace /extract-config logseq-commands
Extract all command palette commands with shortcuts, descriptions, and categories

@workspace /extract-config vscode-keybindings
Extract all keyboard shortcuts from package.json with when clauses

@workspace /extract-config rest-api
Document all REST endpoints with methods, parameters, and response schemas

@workspace /extract-config css-tokens
Extract all design tokens from theme files with values and categories
```

---

## Execution Framework

### Phase 1: Investigation (Discovery)

**Goal:** Understand the configuration system architecture

#### Tasks

1. **Identify Data Sources**

   - Where is configuration defined? (files, databases, constants)
   - What format? (JSON, YAML, ClojureScript, TypeScript, etc.)
   - Are there multiple sources that need combining?
   - Search strategy:
     ```
     - Semantic search: "configuration", "settings", "registry"
     - File search: **/*config*.{js,ts,json,yaml}
     - Grep search: "export const.*COMMANDS", "defonce.*shortcuts"
     ```

2. **Understand Data Structure**

   - What are the key fields?
   - What is the schema/shape?
   - Are there nested structures?
   - Are there conditional values?

3. **Find Related Metadata**

   - Descriptions/labels (translation files, comments)
   - Categories/groupings (separate config, inline metadata)
   - Documentation (existing docs, code comments)
   - Default values, constraints, validators

4. **Map Relationships**
   - How do different sources connect?
   - What is the canonical source?
   - Are there overrides or inheritance?

**Deliverable:** Investigation report documenting findings

---

### Phase 2: Design Extraction Strategy

**Goal:** Plan the extraction approach

#### Tasks

1. **Choose Extraction Method**

   | Method             | Best For                | Tools                          |
   | ------------------ | ----------------------- | ------------------------------ |
   | **Regex Parsing**  | Simple structured text  | Python `re`, grep              |
   | **AST Parsing**    | Complex code structures | babel, typescript, tree-sitter |
   | **JSON/YAML**      | Config files            | json.load(), yaml.load()       |
   | **Database Query** | Runtime data            | SQL, GraphQL                   |

2. **Select Technology/Tools**

   - **Python** - Good for text processing, file I/O, regex
   - **Node.js** - Best for TypeScript/JavaScript parsing
   - **Babashka/Clojure** - Native for Clojure projects
   - **Shell Scripts** - Simple extractions

3. **Design Data Pipeline**

   ```
   Source 1 (Config)     → Parser 1 ──┐
   Source 2 (Translations) → Parser 2 ──→ Aggregator → Validator → Formatter → Output
   Source 3 (Categories) → Parser 3 ──┘
   ```

4. **Plan Output Format**
   - **Markdown Table** - Readable, maintainable, good for docs
   - **JSON** - Programmatic access, API integration
   - **CSV** - Spreadsheet import, data analysis
   - **HTML** - Interactive, searchable, styled

**Deliverable:** Execution plan document with architecture

---

### Phase 3: Implementation

**Goal:** Build the extraction automation

#### Script Structure Template

```python
#!/usr/bin/env python3
"""
Extract <CONFIGURATION_TYPE> from <PROJECT_NAME>

Usage:
    python3 extract_<config_type>.py [--format=markdown|json|csv]
"""

import re
import json
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Any

# Configuration
SOURCE_FILE_1 = Path("src/config/main.ts")
SOURCE_FILE_2 = Path("i18n/en.json")
OUTPUT_DIR = Path("docs/reference")

# Parsers
def parse_primary_config() -> Dict[str, Any]:
    """Extract main configuration data"""
    content = SOURCE_FILE_1.read_text()
    # Parsing logic
    return {}

def parse_translations() -> Dict[str, str]:
    """Extract descriptions from translation files"""
    content = SOURCE_FILE_2.read_text()
    return json.loads(content)

def parse_categories() -> Dict[str, str]:
    """Extract category mappings"""
    # Category extraction logic
    return {}

# Data Processing
def combine_data(config, translations, categories) -> List[Dict]:
    """Merge data from multiple sources"""
    combined = []
    for item_id, item_data in config.items():
        combined.append({
            'id': item_id,
            'value': item_data.get('value'),
            'description': translations.get(item_id, ''),
            'category': categories.get(item_id, 'other')
        })
    return combined

def format_value(value: str) -> str:
    """Clean and format values for display"""
    if not value:
        return "—"
    # Handle platform conditionals, arrays, etc.
    return f"`{value}`"

# Output Generation
def generate_markdown(data: List[Dict]) -> str:
    """Generate markdown reference table"""
    lines = []
    lines.append("# Configuration Reference")
    lines.append("")
    lines.append("| ID | Value | Description | Category |")
    lines.append("|----|-------|-------------|----------|")

    for item in sorted(data, key=lambda x: (x['category'], x['id'])):
        lines.append(
            f"| `{item['id']}` | {format_value(item['value'])} | "
            f"{item['description']} | {item['category']} |"
        )

    return '\n'.join(lines)

# Main
def main():
    print("Extracting configuration...")
    config = parse_primary_config()
    translations = parse_translations()
    categories = parse_categories()

    data = combine_data(config, translations, categories)
    output = generate_markdown(data)

    output_file = OUTPUT_DIR / "reference.md"
    output_file.write_text(output)
    print(f"✓ Generated: {output_file}")

if __name__ == "__main__":
    main()
```

**Deliverable:** Working extraction script

---

### Phase 4: Documentation

**Goal:** Create comprehensive reference documentation

#### Documentation Template

````markdown
# <SYSTEM_NAME> <CONFIGURATION_TYPE> Reference

**Generated automatically from source code**

This document lists all <CONFIGURATION_TYPE> in <SYSTEM_NAME>.

**Legend:**

- `mod` = Modifier key (Cmd on Mac / Ctrl on Windows/Linux)
- `—` = No value assigned
- Platform-specific values are indicated where different

**Total Items:** <COUNT>

---

## All Items

| ID       | Value   | Description      | Category |
| -------- | ------- | ---------------- | -------- |
| `item-1` | `value` | Description text | Category |
| ...      | ...     | ...              | ...      |

---

## Summary by Category

### Category 1 (<COUNT> items)

- **`item-id-1`** - `value` - Description
- **`item-id-2`** - Description

---

## Maintenance

### Regenerating This Reference

```bash
python3 scripts/extract_<config_type>.py
```
````

### When to Update

Regenerate when:

- New items are added
- Values change
- Categories are reorganized

---

**Generated:** <DATE>  
**Script:** `scripts/extract_<config_type>.py`

````

**Deliverable:** User-facing reference documentation

---

### Phase 5: Validation & Quality Assurance

**Goal:** Ensure quality, completeness, and maintainability

#### Validation Checklist

- [ ] All items from source files extracted
- [ ] Values correctly parsed
- [ ] Platform variations captured
- [ ] All descriptions present
- [ ] Categories logical and complete
- [ ] Script can be re-run successfully
- [ ] Handles errors gracefully
- [ ] Edge cases handled

**Deliverable:** Validated, production-ready system

---

## Common Patterns & Best Practices

### Pattern 1: Multi-Source Aggregation

When configuration comes from multiple files:

```python
def aggregate_sources():
    """Collect from all sources, then merge"""
    configs = []
    configs.extend(parse_source1())
    configs.extend(parse_source2())
    configs.extend(parse_source3())

    # Build lookup structures
    by_id = {c['id']: c for c in configs}
    by_category = defaultdict(list)
    for c in configs:
        by_category[c['category']].append(c)

    return by_id, by_category
````

### Pattern 2: Conditional Value Parsing

When values depend on platform/context:

```python
def parse_conditional_value(value_str):
    """
    Parse platform-conditional values.

    Input: "(if mac? 'cmd+b' 'ctrl+b')"
    Output: "Mac: cmd+b / Win: ctrl+b"
    """
    if_match = re.match(r'\(if\s+(\w+)\?\s+"([^"]+)"\s+"([^"]+)"\)', value_str)
    if if_match:
        condition, true_val, false_val = if_match.groups()
        return f"Mac: `{true_val}` / Win/Linux: `{false_val}`"

    return f"`{value_str}`"
```

### Pattern 3: Category Inference

When categories aren't explicitly defined:

```python
def infer_category(item_id: str) -> str:
    """Infer category from ID namespace"""
    namespace = item_id.split('/')[0] if '/' in item_id else item_id.split('_')[0]

    category_map = {
        'editor': 'Editing',
        'go': 'Navigation',
        'ui': 'User Interface',
        'file': 'File Operations',
    }

    return category_map.get(namespace.lower(), 'Other')
```

### Pattern 4: Translation Lookup

When descriptions are in separate i18n files:

```python
def get_description(item_id: str, translations: Dict[str, str]) -> str:
    """Look up description with multiple fallback strategies"""
    # Try exact match
    if item_id in translations:
        return translations[item_id]

    # Try namespaced key
    namespaced = f"config.{item_id}"
    if namespaced in translations:
        return translations[namespaced]

    # Generate from ID as fallback
    return item_id.replace('/', ' ').replace('_', ' ').title()
```

---

## Success Metrics

A successful extraction achieves:

| Metric              | Target                | How to Verify             |
| ------------------- | --------------------- | ------------------------- |
| **Completeness**    | 100% of items         | Compare count with source |
| **Accuracy**        | 0 parsing errors      | Run validation checks     |
| **Maintainability** | Re-runnable anytime   | Test re-execution         |
| **Documentation**   | All fields documented | Review output             |
| **Automation**      | No manual steps       | Run end-to-end            |

---

## Common Challenges & Solutions

### Challenge 1: Complex Source Format

**Problem:** Source uses complex syntax (nested structures, macros)  
**Solution:** Use proper AST parser, break into smaller steps, add logging

### Challenge 2: Data Scattered Across Files

**Problem:** Configuration in 10+ files  
**Solution:** Create aggregator layer, use file discovery, document merge priority

### Challenge 3: Missing Metadata

**Problem:** Some items lack descriptions or categories  
**Solution:** Implement fallback generation, infer from patterns, mark TODOs

### Challenge 4: Platform Variations

**Problem:** Configuration differs by OS/environment  
**Solution:** Extract all variations, document differences, use conditional display

### Challenge 5: Format Evolution

**Problem:** Source format changes over time  
**Solution:** Version the script, add format detection, support multiple versions

---

## Integration Instructions

### Add to Copilot Instructions

In `.github/copilot-instructions.md`:

```markdown
### @workspace /extract-config - Extract System Configuration

Extract and document structured configuration data from source code.

**Usage:**
@workspace /extract-config <system-name>

Extract and document all <config-type> from <source-files>.
Generate reference table with IDs, values, descriptions, and categories.
Create automated extraction script for future updates.

**Examples:**

- `@workspace /extract-config keyboard-shortcuts`
- `@workspace /extract-config api-endpoints`
- `@workspace /extract-config settings`

**See:** `.github/prompts/extract-config.prompt.md` for detailed framework
```

---

## Real-World Example: Logseq Command Extraction

### Context

- **Source**: ClojureScript configuration files
- **Data**: 135 keyboard shortcuts
- **Output**: Markdown reference table

### What Was Extracted

| Field             | Source                                | Example        |
| ----------------- | ------------------------------------- | -------------- |
| Command ID        | `all-built-in-keyboard-shortcuts` map | `:editor/bold` |
| Keyboard Shortcut | `:binding` key                        | `"mod+b"`      |
| Description       | `en.edn` translations                 | "Bold"         |
| Category          | `*category` atom                      | "Formatting"   |

### Implementation

**Script**: `dz/scripts/extract_commands.py` (Python)

**Key Challenges Solved**:

1. Platform-specific conditionals: `(if mac? "cmd+b" "ctrl+b")`
2. Multiple binding formats: `["mod+shift+z" "mod+y"]`
3. Category inference from namespaces
4. ClojureScript syntax parsing with regex

**Output**: [command-palette-reference.md](../../dz/docs/command-palette-reference.md)

**Results**:

- ✅ 135 commands extracted
- ✅ All shortcuts correctly parsed
- ✅ Platform variations documented
- ✅ 8 categories organized
- ✅ Reusable script created

---

## Version History

| Version | Date       | Changes                                                     |
| ------- | ---------- | ----------------------------------------------------------- |
| 1.0     | 2026-01-27 | Initial skill definition based on Logseq command extraction |

---

## Related Resources

- [Code Detective Investigation](code-detective-investigation.prompt.md) - Investigation framework
- [AGENTS.md](../../AGENTS.md) - Repository agent guidelines
- [Example Implementation](../../dz/docs/EXECUTION-PLAN.md) - Command extraction execution plan

---

**Remember:** The key to this skill is **automation**. Always create a reusable script that can be run again when the source changes. One-time manual extraction is not enough!
