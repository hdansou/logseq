# Skill: Regenerate Logseq CLI Documentation

## Purpose

This skill regenerates the comprehensive CLI command reference documentation (`LOGSEQ_COMMANDS.md`) when a new version of the Logseq CLI is released.

## When to Use

- After a new CLI version is released
- When new commands are added to the CLI
- When command options or behavior changes
- When examples need to be updated

## Prerequisites

- The `logseq` CLI must be installed and accessible in PATH
- You must be in the Logseq repository root directory

## Process

### Step 1: Gather Version and Commands Information

```bash
# Get the current version
logseq --version

# Get the main help output with commands list
logseq --help
```

### Step 2: Extract Detailed Help for Each Command

For each command listed in the `Commands:` section, run:

```bash
logseq help <command-name>
```

Extract:

- Usage syntax
- All options with their flags, aliases, defaults, and descriptions
- Full description text

### Step 3: Consult Existing Documentation for Examples

Read the comprehensive user guide for reference examples:

```bash
# Read the CLI user guide
cat docs/06-user-guides/cli-user-guide.md
```

This file contains:

- Detailed examples for each command
- Sample outputs
- Use cases
- Tips and best practices
- Integration examples

### Step 4: Generate or Update LOGSEQ_COMMANDS.md

Create/overwrite `LOGSEQ_COMMANDS.md` in the repository root with:

#### File Structure

````markdown
# Logseq CLI Commands Reference

**Logseq Version:** X.Y.Z

This document provides detailed help for all Logseq CLI commands.

---

## **command-name**

```bash
Usage: logseq command-name [args] [options]
```
````

**Options:**

- `-x, --option-name` - Description (default: value if applicable)

**Description:**

Full description from help output.

**Examples:**

```bash
# Example 1: Basic usage
logseq command-name basic-args

# Example 2: With options
logseq command-name -x value args
```

**Sample Output:**

```text
Expected output
```

**Use Cases:**

- Use case 1
- Use case 2

**Tips:**

- Helpful tip 1
- Helpful tip 2

---

````markdown
### Step 5: Include These Sections for Each Command

#### Required Sections

1. **Command heading** - Bold, level 2
2. **Usage block** - Fenced code block with `bash` language
3. **Options** (if any) - Bulleted list with inline code formatting
4. **Description** - From help output
5. **Examples** - At least 3-5 practical examples
6. **Sample Output** - Show what users should expect

#### Optional Sections (add when relevant)

7. **Requirements** - Prerequisites for the command
8. **Use Cases** - When to use this command
9. **Tips** - Best practices and helpful hints
10. **Technical Details** - Advanced information
11. **Integration Examples** - How to integrate with other tools
12. **Important Notes** - Warnings or critical information

### Step 6: Ensure Quality Standards

#### Markdown Formatting

- ✅ All code blocks must specify language (e.g., `bash`, `text`, `clojure`, `json`)
- ✅ Blank lines before and after code fences
- ✅ URLs wrapped in angle brackets: `<https://example.com>`
- ✅ Consistent heading levels (H2 for commands)
- ✅ Proper list formatting

#### Content Quality

- ✅ Examples use realistic graph names (e.g., `my-notes`, `work-journal`)
- ✅ Token placeholders clearly marked (e.g., `YOUR_TOKEN`)
- ✅ Multi-word search terms use quotes
- ✅ File paths use proper separators and realistic locations
- ✅ Sample outputs match actual CLI behavior
- ✅ Commands show both local mode (`-g`) and API mode (`-a`) where applicable

#### Example Variety

For each command, include:

- Basic usage (simplest form)
- Common options demonstrated
- Multiple related examples showing different scenarios
- Environment variable usage (for API token when relevant)
- Error cases or edge cases (when helpful)

### Step 7: Command-Specific Guidelines

#### `list`

- Show sample output with both DB and File graphs
- Explain use cases (finding graph names, verification)

#### `show`

- Demonstrate single and multiple graphs
- Show formatted table output
- Explain debugging use cases

#### `search`

- Show local (`-g`) and API (`-a`) modes
- Demonstrate limit option
- Show raw output for scripting
- Include environment variable pattern

#### `query`

- Cover entity queries (ID, UUID, keyword)
- Show DataLog query examples
- Include common query patterns (TODO, tags, etc.)
- Explain CLI vs in-app query differences
- Show readable properties option

#### `export`

- Basic and custom filename examples
- Explain what's included
- Show different path options

#### `export-edn`

- Demonstrate both export types (`:graph`, `:graph-ontology`)
- Show all major options (timestamps, exclusions, namespaces)
- Explain EDN structure
- Cover use cases for each type

#### `import-edn`

- Local and API mode examples
- Expected EDN structure
- Important notes about additive nature
- Validation recommendation

#### `append`

- Show requirements clearly
- Multiple content examples
- Automation use cases
- Markdown formatting support

#### `mcp-server`

- HTTP and stdio transport modes
- Integration examples (Claude Desktop config)
- List MCP capabilities
- Security notes
- Both local and API modes

#### `validate`

- Single and multiple graph examples
- Show both valid and invalid outputs
- CI/CD integration tips
- Best practices

#### `help`

- Show general and specific help
- Version check example
- Sample output

### Step 8: Verify and Validate

```bash
# Check for markdown linting errors
# (the get_errors tool will automatically check)

# Verify all commands are documented
logseq --help  # Compare against commands list

# Spot-check examples by running them
logseq list
logseq show <graph-name>
# etc.
```
````

### Step 9: Update Location

Save the file as:

```text
/path/to/logseq/LOGSEQ_COMMANDS.md
```

## Output Format

Return:

1. ✅ File path of created/updated document
2. ✅ Version number documented
3. ✅ Count of commands documented
4. ✅ Confirmation that examples are included
5. ✅ Confirmation that markdown passes linting

## Example Usage

**User request:**

> "Regenerate the CLI docs for the new 0.5.0 release"

**Assistant response:**

1. Run `logseq --version` → 0.5.0
2. Run `logseq --help` → extract commands
3. For each command, run `logseq help <cmd>` → extract details
4. Read `docs/06-user-guides/cli-user-guide.md` → get examples
5. Generate/update `LOGSEQ_COMMANDS.md` with all sections
6. Verify markdown formatting
7. Report completion

## Tips for Success

1. **Always check user guide first** - The `docs/06-user-guides/cli-user-guide.md` file has the most comprehensive examples
2. **Maintain consistency** - Use the same example graph names throughout
3. **Test commands when possible** - Verify examples actually work
4. **Include context** - Don't just list options, explain when/why to use them
5. **Consider the audience** - Write for users who may be new to CLI tools
6. **Use realistic examples** - Avoid `foo`, `bar`, `test` - use `my-notes`, `work-journal`, etc.
7. **Show output** - Users want to know what to expect
8. **Link related commands** - Reference other commands when relevant

## Common Pitfalls to Avoid

- ❌ Bare URLs (use `<https://...>`)
- ❌ Code blocks without language specification
- ❌ Missing blank lines around code blocks
- ❌ Inconsistent option formatting
- ❌ Examples without explanatory comments
- ❌ Missing sample outputs
- ❌ Incomplete or vague descriptions
- ❌ Forgetting to document new commands

## Maintenance Notes

- This skill should be updated if the CLI structure changes significantly
- Keep examples aligned with `docs/06-user-guides/cli-user-guide.md`
- Update if new command categories are added
- Review if CLI output format changes

## Related Files

- `/LOGSEQ_COMMANDS.md` - The output file
- `/docs/06-user-guides/cli-user-guide.md` - Comprehensive user guide with examples
- `/deps/cli/src/logseq/cli/spec.cljs` - Command specifications
- `/deps/cli/src/logseq/cli/commands/` - Command implementations
- `/deps/cli/README.md` - CLI package documentation
