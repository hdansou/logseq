# Logseq Changelog Generation Workflow

This directory contains tools and guidelines for generating professional changelogs for Logseq releases.

## Quick Start

### Using GitHub Copilot (Recommended)

The easiest way to generate a changelog:

```text
@workspace /new-changelog 2025-01-05 2025-01-15
```

This command will:

1. Fetch git commits for the date range
2. Generate a raw commit log
3. Apply AI analysis to create a user-facing changelog
4. Output a formatted markdown file

### Using the Shell Script

For manual workflow or automation:

```bash
# Generate raw commit log
./scripts/generate-changelog-log.sh 2025-01-05 2025-01-15

# Then use AI to process the output
@workspace /new-changelog --from-file changelog_2025-01-05_to_2025-01-15.md
```

## Changelog vs Release Notes

### Changelog (Default)

**Purpose:** Comprehensive record of all changes for developers and power users

**Format:**

- Detailed commit-by-commit listing
- Organized by category and functional area
- Includes technical details
- All commits are documented

**Example use cases:**

- Developer documentation
- Version control history
- Technical release announcements
- Detailed change tracking

**Command:**

```text
@workspace /new-changelog 2025-01-05 2025-01-15
```

### Release Notes

**Purpose:** User-facing highlights for end users and stakeholders

**Format:**

- Spotlight section with major features
- High-level summaries
- Focus on user benefits
- Excludes minor technical changes

**Example use cases:**

- Public release announcements
- Blog posts
- User notifications
- Marketing materials

**Command:**

```text
@workspace /new-changelog 2025-01-05 2025-01-15 --format=release
```

## Workflow

### 1. Prepare Git Commits

Ensure all commits are properly formatted:

- Use conventional commit messages
- Reference issues/PRs where applicable
- Write clear, descriptive summaries

### 2. Generate Raw Log

Using the script:

```bash
./scripts/generate-changelog-log.sh <start-date> <end-date> [options]
```

**Options:**

- `-o, --output <file>` - Custom output file path
- `-b, --branches <pattern>` - Filter branches (e.g., "origin/master")

**Examples:**

```bash
# Basic usage
./scripts/generate-changelog-log.sh 2025-01-05 2025-01-15

# Custom output
./scripts/generate-changelog-log.sh 2025-01-01 2025-01-31 -o release-notes.md

# Specific branch
./scripts/generate-changelog-log.sh 2025-01-01 2025-01-31 -b "origin/master"
```

### 3. Generate Changelog

Using GitHub Copilot:

```text
@workspace /new-changelog <start-date> <end-date> [--format=<changelog|release>]
```

Or manually review the guidelines in `.github/prompts/changelog-generation.prompt.md` and process the raw log.

### 4. Review and Edit

1. Review the generated changelog for accuracy
2. Verify commit categorization
3. Ensure user-facing language
4. Check for duplicate entries
5. Validate GitHub commit links
6. Confirm contributor list is complete

### 5. Publish

1. Add to `CHANGELOG.md` in the repository
2. Create GitHub release with the content
3. Publish to blog/announcements if needed

## File Structure

```txt
logseq/
├── .github/
│   └── copilot-instructions.md      # Copilot command definitions
├── prompts/
│   └── changelog-generation.md      # Detailed formatting guidelines
└── scripts/
    └── generate-changelog-log.sh    # Raw commit log generator
```

## Changelog Categories

### Primary Categories (Required)

- **New Feature** - New capabilities added
- **Enhancement** - Improvements to existing features
- **Bug Fix** - Corrections to existing functionality
- **Performance** - Speed and efficiency improvements
- **Refactoring** - Code structure improvements
- **Chore** - Maintenance tasks (dependencies, CI/CD, etc.)

### Functional Areas (Secondary)

- Block editing and management
- Graph visualization
- Search and queries
- Plugin system
- Database operations
- UI/UX improvements
- File handling
- Sync and storage
- Mobile support
- Developer tools
- Real-time collaboration (RTC)
- Whiteboards
- PDF annotations
- Settings and configuration

## Best Practices

### Writing Commit Messages

Good commit messages make changelog generation easier:

```bash
# Good ✓
feat(blocks): Add custom properties panel support
fix(mobile): Resolve touch target sizing issues
perf(db): Optimize query execution for large graphs

# Bad ✗
updated stuff
fixes
WIP
```

### Grouping Related Commits

The AI will attempt to group related commits, but you can help by:

- Using consistent terminology
- Referencing related issue numbers
- Writing descriptive commit bodies

### Spotlight Selection

Choose spotlight items that:

- Significantly impact user experience
- Represent major development effort
- Introduce new capabilities
- Solve long-standing issues

## Examples

### Example 1: Monthly Release

```bash
# Generate changelog for January 2025
./scripts/generate-changelog-log.sh 2025-01-01 2025-01-31

# Create release notes
@workspace /new-changelog 2025-01-01 2025-01-31 --format=release
```

### Example 2: Hotfix Release

```bash
# Generate changelog for specific commits
./scripts/generate-changelog-log.sh 2025-01-15 2025-01-16 -b "origin/hotfix"

# Create detailed changelog
@workspace /new-changelog 2025-01-15 2025-01-16
```

### Example 3: Feature Branch

```bash
# Generate log for feature branch
./scripts/generate-changelog-log.sh 2025-01-01 2025-01-31 -b "origin/feature/rtc"

# Create changelog
@workspace /new-changelog 2025-01-01 2025-01-31
```

## Troubleshooting

### Issue: No commits found

**Solution:** Check date format (YYYY-MM-DD) and verify branch names

```bash
# List available branches
git branch -r

# Verify commits exist in date range
git log --since="2025-01-01" --until="2025-01-31" --oneline
```

### Issue: Too many commits

**Solution:** Use branch filtering or shorter date ranges

```bash
# Filter by specific branch
./scripts/generate-changelog-log.sh 2025-01-01 2025-01-31 -b "origin/master"
```

### Issue: Missing contributors

**Solution:** Ensure git fetch is up to date

```bash
git fetch --all
./scripts/generate-changelog-log.sh <dates>
```

## Resources

- **Changelog Guidelines:** `.github/prompts/changelog-generation.prompt.md`
- **Copilot Commands:** `.github/copilot-instructions.md`
- **Conventional Commits:** <https://www.conventionalcommits.org/>
- **Keep a Changelog:** <https://keepachangelog.com/>

## Contributing

When improving the changelog workflow:

1. Update the script if changing log generation
2. Update `.github/prompts/changelog-generation.prompt.md` for formatting changes
3. Update `.github/copilot-instructions.md` for new commands
4. Test with historical date ranges
5. Document any new categories or patterns

## Support

For questions or issues:

1. Check existing changelogs for examples
2. Review `.github/prompts/changelog-generation.prompt.md` guidelines
3. Ask in the Logseq developer community
