# new-changelog

Generate professional, AI-powered changelogs from git commit history.

## Description

This skill creates user-facing changelogs from git commits by fetching commit history, categorizing changes, and generating formatted release notes with spotlight features, categorized changes, and contributor lists.

## Instructions

When the user invokes this skill:

1. **Fetch Git Commits**

   - Parse date range from arguments: `<start-date> <end-date>`
   - Fetch commits from all remote branches for the date range
   - Include commit hash, author, date, message
   - Generate raw commit log in markdown format

2. **Analyze and Categorize**

   - Parse commit messages for conventional commit patterns
   - Categorize changes:
     - **New Feature** - New capabilities added
     - **Enhancement** - Improvements to existing features
     - **Bug Fix** - Issue resolutions
     - **Performance** - Speed/efficiency improvements
     - **Documentation** - Docs updates
     - **Refactor** - Code restructuring
     - **Chore** - Maintenance tasks
   - Group by functional area if applicable

3. **Generate Changelog**

   - Create spotlight section for major features
   - Organize by category and functional area
   - Format entries consistently
   - Add contributor list
   - Include statistics (total commits, contributors)

4. **Format Selection**
   - `--format=changelog` (default): Detailed changelog with all commits
   - `--format=release`: Condensed release notes focusing on user-facing changes

## Output Structure

### Changelog Format

```markdown
# Changelog - [Date Range]

## 🌟 Spotlight

- Major feature highlights

## New Features

### [Functional Area]

- Feature description (#PR)

## Enhancements

### [Functional Area]

- Improvement description

## Bug Fixes

- Fix description

## Contributors

- @username1, @username2

**Total:** X commits by Y contributors
```

### Release Notes Format

```markdown
# Release Notes - Version X.Y.Z

## Highlights

- Key user-facing changes

## What's New

- Notable features

## Improvements

- User-visible enhancements

## Bug Fixes

- Important fixes
```

## Usage Examples

```
@workspace /new-changelog 2025-01-05 2025-01-15
Generate changelog for January 5-15

@workspace /new-changelog 2025-01-01 2025-01-31 --format=release
Generate release notes for January

@workspace /new-changelog 2024-12-01 2025-01-01 --format=changelog
Detailed changelog for December
```

## Reference

Full changelog generation guide: `.github/prompts/changelog-generation.prompt.md`
