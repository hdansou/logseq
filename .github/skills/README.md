# Claude Skills for Logseq

This directory contains reusable skills (custom instructions) for common tasks in the Logseq repository.

## What are Skills?

Skills are detailed, step-by-step instructions that guide Claude through complex, repeatable tasks. They ensure consistency and quality across similar operations over time.

## Available Skills

### regenerate-cli-docs.md

**Purpose:** Regenerate the CLI command reference documentation (`LOGSEQ_COMMANDS.md`) when a new version of the Logseq CLI is released.

**When to use:**

- After a new CLI version is released
- When new commands are added
- When command options or behavior changes
- When examples need to be updated

**How to use:**

Simply reference the skill in your prompt:

```text
@regenerate-cli-docs.md Please regenerate the CLI docs for version 0.5.0
```

Or:

```text
Using the regenerate-cli-docs skill, update LOGSEQ_COMMANDS.md for the latest CLI version
```

**What it does:**

1. Extracts version and command information from `logseq --help`
2. Gets detailed help for each command
3. Consults the user guide for comprehensive examples
4. Generates/updates `LOGSEQ_COMMANDS.md` with:
   - Version header
   - Complete command reference with examples
   - Sample outputs
   - Use cases and tips
   - Proper markdown formatting
5. Validates markdown formatting

**Output:** Updated `LOGSEQ_COMMANDS.md` file in the repository root

## Creating New Skills

When creating a new skill:

1. **Identify repeatable tasks** - Skills work best for processes that:

   - Are performed multiple times
   - Have clear, defined steps
   - Require consistency across executions
   - Involve gathering information from multiple sources

2. **Document thoroughly** - Include:

   - Clear purpose statement
   - Prerequisites
   - Step-by-step process
   - Quality standards
   - Expected output format
   - Example usage
   - Common pitfalls to avoid

3. **Test the skill** - Verify it works by:

   - Having Claude execute it
   - Checking output quality
   - Ensuring consistency across runs

4. **Name clearly** - Use descriptive kebab-case names that indicate the task

5. **Update this README** - Add your skill to the list above

## Skill Template

```markdown
# Skill: [Task Name]

## Purpose

[Clear description of what this skill does]

## When to Use

- [Scenario 1]
- [Scenario 2]

## Prerequisites

- [Requirement 1]
- [Requirement 2]

## Process

### Step 1: [First Step]

[Detailed instructions]

### Step 2: [Second Step]

[Detailed instructions]

## Output Format

[Description of expected output]

## Example Usage

**User request:**

> "[Example request]"

**Assistant response:**
[What Claude should do]

## Tips for Success

- [Tip 1]
- [Tip 2]

## Common Pitfalls to Avoid

- ❌ [Pitfall 1]
- ❌ [Pitfall 2]
```

## Related Documentation

- `.github/copilot-instructions.md` - Always-active instructions for working in this repo
- `.github/prompts/` - On-demand prompts for specific investigations
- `docs/` - General Logseq documentation
