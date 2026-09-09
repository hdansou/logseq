# Changelog Generation Quick Reference

> **Last revised:** 2026-05-30

## Commands

### GitHub Copilot Commands

```text
# Generate full changelog
@workspace /new-changelog 2025-01-05 2025-01-15

# Generate full changelog explicitly
@workspace /new-changelog 2025-01-05 2025-01-15 --format=changelog

# Generate release notes (condensed)
@workspace /new-changelog 2025-01-05 2025-01-15 --format=release
```

### Shell Script

```bash
# Basic usage
./dz/scripts/generate-changelog-log.sh 2025-01-05 2025-01-15

# Custom output file
./dz/scripts/generate-changelog-log.sh 2025-01-05 2025-01-15 -o my-changelog.md

# Specific branch only
./dz/scripts/generate-changelog-log.sh 2025-01-05 2025-01-15 -b "origin/master"

# Show help
./dz/scripts/generate-changelog-log.sh --help
```

## Output Formats

### Changelog (Default)

**Use when:** Creating comprehensive developer documentation

**Contains:**

- All commits organized by category
- Technical details included
- Functional area groupings
- Complete contributor list

### Release Notes

**Use when:** Creating user-facing announcements

**Contains:**

- Spotlight section (2-3 major features)
- High-level summaries only
- User benefit focus
- Excludes minor technical changes

## Categories

### Primary

1. **New Feature** - New capabilities
2. **Enhancement** - Improvements to existing features
3. **Bug Fix** - Corrections
4. **Performance** - Speed/efficiency improvements
5. **Refactoring** - Code structure improvements
6. **Chore** - Maintenance tasks

`docs:`, `dev:`, `ci:`, and `test:` commits are currently folded into `Chore` by `dz/scripts/changelog-extend.sh`.

### Functional Areas

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
- Publishing
- Properties
- Import and export
- Media / PDF
- Window management
- Whiteboards
- Settings and configuration

> **Removed:** `PDF annotations` as a standalone bucket. Current source surfaces center on PDF viewer and asset flows rather than a separate annotation subsystem.

## Findings: New Capabilities

- `dz/scripts/changelog-extend.sh` can roll an existing changelog window forward with `--no-merge`, `--max-chars`, and `--dry-run`.
- `dz/scripts/changelog-extend.sh` appends a pre-categorized `## TRIAGE` block, refreshes contributors via `dz/scripts/authors.txt`, and filters noisy commits via `dz/scripts/changelog-exclude.txt`.
- The current changelog style guide expects `## Currently Available`, `## Upcoming`, and `## Contributors` sections, plus a `Total analyzed:` footer in the contributors block.
- Active source areas in `src/`, `deps/`, and `packages/` now clearly include publishing, properties, media / PDF, window management, and RTC-backed sync flows, so those areas should be expected in recent release windows.

## Common Tasks

### Monthly Release

```bash
# 1. Generate raw log
./dz/scripts/generate-changelog-log.sh 2025-01-01 2025-01-31

# 2. Create release notes
@workspace /new-changelog 2025-01-01 2025-01-31 --format=release

# 3. Create detailed changelog
@workspace /new-changelog 2025-01-01 2025-01-31
```

### Hotfix Release

```bash
# 1. Generate log for hotfix branch
./dz/scripts/generate-changelog-log.sh 2025-01-15 2025-01-16 -b "origin/hotfix"

# 2. Create changelog
@workspace /new-changelog 2025-01-15 2025-01-16
```

### Feature Preview

```bash
# 1. Generate log for feature branch
./dz/scripts/generate-changelog-log.sh 2025-01-01 2025-01-31 -b "origin/feature/new-feature"

# 2. Create changelog
@workspace /new-changelog 2025-01-01 2025-01-31
```

## Workflow Checklist

- [ ] Fetch latest git updates: `git fetch --all`
- [ ] Generate raw commit log using script
- [ ] Generate changelog using Copilot command
- [ ] Review and edit for accuracy
- [ ] Verify commit links work
- [ ] Check contributor list is complete
- [ ] Add to `CHANGELOG.md`
- [ ] Create GitHub release
- [ ] Publish announcements (if needed)

## Tips

### Writing Good Commit Messages

```bash
# Good ✓
feat(blocks): Add custom properties panel
fix(mobile): Resolve touch target sizing
perf(db): Optimize query execution

# Bad ✗
updated stuff
fixes
WIP
```

### Spotlight Selection Criteria

- Significantly impacts user experience
- Represents major development effort
- Introduces new capabilities
- Solves long-standing issues

### Common Patterns

```text
[commit-hash] (author) [date]: Summary
  > Detailed description (optional)
```

## Files

- `.github/prompts/changelog-generation.prompt.md` - Full prompt and examples
- `.github/skills/new-changelog.md` - Copilot skill contract and format options
- `dz/scripts/generate-changelog-log.sh` - Raw log generator
- `dz/scripts/changelog-extend.sh` - Extend an existing changelog window and refresh TRIAGE/contributors
- `dz/changelog/STYLE.md` - Current section taxonomy and editorial rules
- `.github/copilot-instructions.md` - Copilot commands
- `dz/docs/08-processes/changelog/workflow.md` - Complete workflow guide

> **Removed:** `prompts/changelog-generation.md`. The current prompt lives at `.github/prompts/changelog-generation.prompt.md`.
> **Removed:** `docs/CHANGELOG_WORKFLOW.md`. The current workflow guide lives at `dz/docs/08-processes/changelog/workflow.md`.

## Troubleshooting

### No commits found

```bash
# Check branch names
git branch -r

# Verify commits in date range
git log --since="2025-01-01" --until="2025-01-31" --oneline
```

### Too many commits

```bash
# Use branch filtering
./dz/scripts/generate-changelog-log.sh 2025-01-01 2025-01-31 -b "origin/master"
```

### Missing contributors

```bash
# Update git remotes
git fetch --all
```

## Examples

See `.github/prompts/changelog-generation.prompt.md` for complete examples of:

- Spotlight sections
- Full changelogs
- Category organization
- Functional area grouping
