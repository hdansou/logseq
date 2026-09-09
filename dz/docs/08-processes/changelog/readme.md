# Changelog Generation System

> **Last revised:** 2026-05-30

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
cd ~/Projects/src/github.com/logseq-eng-documentation

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
| [quick-reference.md](./quick-reference.md)                                                             | Command cheat sheet and common tasks  |
| [workflow.md](./workflow.md)                                                                           | Complete end-to-end workflow guide    |
| [system-summary.md](./system-summary.md)                                                               | Technical implementation details      |
| [../../../../.github/prompts/changelog-generation.prompt.md](../../../../.github/prompts/changelog-generation.prompt.md) | AI processing guidelines and examples |

> **Removed:** `CHANGELOG_QUICK_REFERENCE.md`, `CHANGELOG_WORKFLOW.md`, and `CHANGELOG_SYSTEM_SUMMARY.md` as canonical filenames in this directory. The current files are `quick-reference.md`, `workflow.md`, and `system-summary.md`.
> **Removed:** `../.github/prompts/changelog-generation.prompt.md` from this document's link path. The correct relative path from this directory is `../../../../.github/prompts/changelog-generation.prompt.md`.

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

1. **Quick commands:** See [quick-reference.md](./quick-reference.md)
2. **Complete workflow:** See [workflow.md](./workflow.md)
3. **Technical details:** See [system-summary.md](./system-summary.md)
4. **Formatting guidelines:** See [../../../../.github/prompts/changelog-generation.prompt.md](../../../../.github/prompts/changelog-generation.prompt.md)

## Findings: New Capabilities

- Source roots include active functional areas for `Publishing`, `Properties`, `Import and export`, `Media / PDF`, `Window management`, and `Real-time collaboration (RTC)`, so these should be treated as first-class changelog grouping candidates.
- `src/`, `deps/`, and `packages/` do not currently implement the changelog-generation pipeline itself; generation remains driven by `.github/skills/` and `dz/scripts/`.

> **Removed:** `PDF annotations` as a standalone capability label in this summary. Current source organization centers on broader PDF/media flows.

## Examples

See the full documentation for examples of:

- Monthly release changelogs
- Hotfix release notes
- Feature branch previews
- Spotlight sections
- Category organization
