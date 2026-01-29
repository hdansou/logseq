# extract-config

Extract and document structured configuration data from source code, generating comprehensive reference tables.

## Description

This skill automates the extraction of configuration data (keyboard shortcuts, API endpoints, settings, theme variables, etc.) from source code and generates comprehensive reference documentation with automated scripts for future updates.

## Instructions

When the user invokes this skill:

1. **Investigate Configuration Architecture**

   - Identify all data sources (config files, translation files, category definitions)
   - Understand data structure and schema
   - Map relationships between sources
   - Document findings

2. **Design Extraction Strategy**

   - Choose appropriate parsing method (regex, AST, JSON/YAML parsers)
   - Select technology (Python, Node.js, Babashka based on project)
   - Design data pipeline from sources → parsers → aggregator → formatter
   - Plan output format (Markdown, JSON, CSV, HTML)

3. **Implement Automated Script**

   - Create extraction script following the template pattern:
     - Parsers for each data source
     - Data aggregation and merging logic
     - Value formatting (handle conditionals, platform variations)
     - Multiple output format generators
     - Built-in validation
   - Save script to `scripts/extract_<config_type>.py` or appropriate location

4. **Generate Reference Documentation**

   - Create comprehensive markdown reference with:
     - Header with legend and metadata
     - Complete table of all items (ID, value, description, category)
     - Summary grouped by category
     - Usage examples where applicable
     - Maintenance instructions
   - Save to `docs/reference/` or appropriate location

5. **Validate & Test**
   - Verify completeness (all items extracted)
   - Check accuracy (no parsing errors)
   - Test script re-execution
   - Validate output format

## Key Principles

- **Always create reusable, automated scripts** - Never do one-time manual extraction
- **Multi-source aggregation** - Combine config + translations + categories
- **Smart parsing** - Handle platform conditionals, arrays, complex formats
- **Fallback generation** - Create descriptions from IDs when missing
- **Built-in validation** - Check completeness, accuracy, duplicates

## Common Patterns

### Multi-Source Aggregation

```python
config = parse_config_file()
translations = parse_translations()
categories = parse_categories()
combined = merge_data(config, translations, categories)
```

### Conditional Value Parsing

```python
def parse_conditional(value):
    # Input: "(if mac? 'cmd+b' 'ctrl+b')"
    # Output: "Mac: cmd+b / Win: ctrl+b"
    if 'if mac?' in value:
        # Parse and format platform-specific
        pass
```

### Category Inference

```python
def infer_category(item_id):
    namespace = item_id.split('/')[0]
    return category_map.get(namespace, 'Other')
```

### Translation Lookup with Fallback

```python
def get_description(item_id, translations):
    return translations.get(item_id) or \
           translations.get(f"config.{item_id}") or \
           humanize(item_id)
```

## Output Structure

### Markdown Reference Template

```markdown
# <System> <Config Type> Reference

**Generated automatically from source code**
**Total Items:** <count>

## All Items

| ID       | Value   | Description | Category |
| -------- | ------- | ----------- | -------- |
| `item-1` | `value` | Description | Category |

## Summary by Category

### Category 1 (<count> items)

- **`item-id`** - `value` - Description

## Maintenance

\`\`\`bash
python3 scripts/extract\_<type>.py
\`\`\`
```

## Success Criteria

- ✅ 100% of items extracted from source
- ✅ 0 parsing errors
- ✅ All required fields present
- ✅ Automated script created
- ✅ Script is re-runnable
- ✅ Validation checks pass

## Example Use Cases

| Use Case               | Output Fields                                        |
| ---------------------- | ---------------------------------------------------- |
| **Keyboard Shortcuts** | Key binding, command, scope, platform, description   |
| **API Endpoints**      | Method, path, params, auth, response schema          |
| **Settings**           | Setting name, type, default, validation, description |
| **Theme Variables**    | Variable name, value, category, usage                |
| **Commands**           | Command ID, shortcut, description, category          |
| **Feature Flags**      | Flag name, enabled, rollout %, dependencies          |

## Reference

For detailed implementation guide, patterns, and troubleshooting:

- Full documentation: `.github/prompts/extract-config.prompt.md`
- Quick reference: `.github/prompts/extract-config-quickref.md`
- Example: `dz/docs/EXECUTION-PLAN.md` (Logseq command extraction)
