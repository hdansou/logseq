# Changelog Generation System

> **Last revised:** 2026-03-10

> AI-powered changelog generation for Logseq releases

## Quick Start

### For GitHub Copilot Users (Recommended)

Simply use the Copilot command in VS Code:

```text
@workspace /new-changelog 2025-01-05 2025-01-15
```

This will:

1. ✓ Fetch git commits from all branches
2. ✓ Analyze and categorize changes
3. ✓ Generate user-facing descriptions
4. ✓ Create a formatted changelog with GitHub links

### For Command Line Users

```bash
# Navigate to Logseq repository
cd ~/Projects/src/github.com/logseq

# Generate raw commit log
./dz/scripts/generate-changelog-log.sh 2025-01-05 2025-01-15

# Then use the output with AI to create the final changelog
```

## Two Formats

### Changelog (Detailed)

**For:** Developers, technical documentation, detailed records

```text
@workspace /new-changelog 2025-01-05 2025-01-15
```

Includes all commits organized by category and functional area.

### Release Notes (Condensed)

**For:** End users, announcements, marketing

```text
@workspace /new-changelog 2025-01-05 2025-01-15 --format=release
```

Highlights major features and user-facing changes only.

## Documentation

| Document                                                                                               | Purpose                               |
| ------------------------------------------------------------------------------------------------------ | ------------------------------------- |
| [CHANGELOG_QUICK_REFERENCE.md](./CHANGELOG_QUICK_REFERENCE.md)                                         | Command cheat sheet and common tasks  |
| [CHANGELOG_WORKFLOW.md](./CHANGELOG_WORKFLOW.md)                                                       | Complete end-to-end workflow guide    |
| [CHANGELOG_SYSTEM_SUMMARY.md](./CHANGELOG_SYSTEM_SUMMARY.md)                                           | Technical implementation details      |
| [../.github/prompts/changelog-generation.prompt.md](../.github/prompts/changelog-generation.prompt.md) | AI processing guidelines and examples |

## Common Commands

```bash
# Monthly release
@workspace /new-changelog 2025-01-01 2025-01-31

# Release notes only
@workspace /new-changelog 2025-01-01 2025-01-31 --format=release

# Hotfix release
./dz/scripts/generate-changelog-log.sh 2025-01-15 2025-01-16 -b "origin/hotfix"
@workspace /new-changelog 2025-01-15 2025-01-16

# Feature branch preview
./dz/scripts/generate-changelog-log.sh 2025-01-01 2025-01-31 -b "origin/feature/my-feature"
@workspace /new-changelog 2025-01-01 2025-01-31
```

## What Gets Generated

### Changelog Structure

```markdown
# Changelog (YYYY-MM-DD to YYYY-MM-DD)

## Spotlight

[Major features and initiatives]

## New Feature

### [Functional Area]

- [User-facing description] ([commit-hash])

## Enhancement

### [Functional Area]

- [User-facing description] ([commit-hash])

## Bug Fix

### [Functional Area]

- [User-facing description] ([commit-hash])

## Performance

- [Improvement description] ([commit-hash])

## Contributors

@user1, @user2, @user3
```

## Categories

- **New Feature** - New capabilities
- **Enhancement** - Improvements to existing features
- **Bug Fix** - Corrections
- **Performance** - Speed/efficiency improvements
- **Refactoring** - Code structure improvements
- **Chore** - Maintenance tasks

## Need Help?

1. **Quick commands:** See [CHANGELOG_QUICK_REFERENCE.md](./CHANGELOG_QUICK_REFERENCE.md)
2. **Complete workflow:** See [CHANGELOG_WORKFLOW.md](./CHANGELOG_WORKFLOW.md)
3. **Technical details:** See [CHANGELOG_SYSTEM_SUMMARY.md](./CHANGELOG_SYSTEM_SUMMARY.md)
4. **Formatting guidelines:** See [../.github/prompts/changelog-generation.prompt.md](../.github/prompts/changelog-generation.prompt.md)

## Examples

See the full documentation for examples of:

- Monthly release changelogs
- Hotfix release notes
- Feature branch previews
- Spotlight sections
- Category organization
