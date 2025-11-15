# Changelog Generation Quick Reference

## Commands

### GitHub Copilot Commands

```text
# Generate full changelog
@workspace /new-changelog 2025-01-05 2025-01-15

# Generate release notes (condensed)
@workspace /new-changelog 2025-01-05 2025-01-15 --format=release
```

### Shell Script

```bash
# Basic usage
./scripts/generate-changelog-log.sh 2025-01-05 2025-01-15

# Custom output file
./scripts/generate-changelog-log.sh 2025-01-05 2025-01-15 -o my-changelog.md

# Specific branch only
./scripts/generate-changelog-log.sh 2025-01-05 2025-01-15 -b "origin/master"

# Show help
./scripts/generate-changelog-log.sh --help
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
- Whiteboards
- PDF annotations
- Settings and configuration

## Common Tasks

### Monthly Release

```bash
# 1. Generate raw log
./scripts/generate-changelog-log.sh 2025-01-01 2025-01-31

# 2. Create release notes
@workspace /new-changelog 2025-01-01 2025-01-31 --format=release

# 3. Create detailed changelog
@workspace /new-changelog 2025-01-01 2025-01-31
```

### Hotfix Release

```bash
# 1. Generate log for hotfix branch
./scripts/generate-changelog-log.sh 2025-01-15 2025-01-16 -b "origin/hotfix"

# 2. Create changelog
@workspace /new-changelog 2025-01-15 2025-01-16
```

### Feature Preview

```bash
# 1. Generate log for feature branch
./scripts/generate-changelog-log.sh 2025-01-01 2025-01-31 -b "origin/feature/new-feature"

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

- `prompts/changelog-generation.md` - Full guidelines
- `scripts/generate-changelog-log.sh` - Raw log generator
- `.github/copilot-instructions.md` - Copilot commands
- `docs/CHANGELOG_WORKFLOW.md` - Complete documentation

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
./scripts/generate-changelog-log.sh 2025-01-01 2025-01-31 -b "origin/master"
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
