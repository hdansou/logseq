# Logseq Changelog Generation Prompt

## Task

Generate a professional changelog for Logseq from git commit log data, organizing changes by type and functionality using AI-powered analysis.

## Input Format

Provide git commit log data generated using this shell function:

```bash
function New-LogseqChangeLog {
    if [[ $# -lt 2 ]]; then
        echo "Usage: $0 <start-date> <end-date>"
        echo "Example: $0 2025-01-05 2025-01-15"
        exit 1
    fi
    cd ~/Projects/src/github.com/logseq
    start_date=$1
    end_date=$2
    git fetch --all
    output_file="changelog_${start_date}_to_${end_date}.md"
    echo "## Git Commit Log (${start_date} to ${end_date})" > "$output_file"
    for branch in $(git branch -r | grep -v '\->'); do
        branch_log=$(git log "$branch" --since="$start_date" --until="$end_date" --pretty=format:"- [%h] (%an) [%cd]: %s%n  > %b" --date=short)
        if [[ -n "$branch_log" ]]; then
            echo -e "\n### Branch: $branch" >> "$output_file"
            echo "$branch_log" >> "$output_file"
        fi
    done
    echo "Changelog saved to $output_file"
}
```

## Output Format Requirements

### Structure

1. **Title**: `# Changelog (YYYY-MM-DD to YYYY-MM-DD)`
2. **Spotlight Section**: Highlight 2-3 major initiatives from the release
3. **Primary Categories** (in order):
   - New Feature
   - Enhancement
   - Bug Fix
   - Performance
   - Refactoring
   - Chore
4. **Contributors Section**: Alphabetically sorted

### Formatting Rules

- Use outliner-style formatting with dashes (`-`) for all bullet points
- Nest functional areas under primary categories
- Include commit hashes with GitHub links: `([hash](https://github.com/logseq/logseq/commit/hash))`
- Use clear, user-facing language
- Combine related commits into single entries when appropriate

### Functional Areas (Secondary Categories)

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

## Quality Guidelines

- **Spotlight**: Focus on major features that significantly impact user experience
- **Clarity**: Use present tense and active voice
- **User Impact**: Prioritize user-facing benefits over technical implementation
- **Grouping**: Combine similar commits logically
- **Exclusions**: Skip merge commits, dependency updates, and trivial changes unless significant
- **Consistency**: Maintain consistent terminology throughout

## Example Spotlight

```markdown
## Spotlight

This release delivers three major initiatives that significantly expand Logseq's capabilities:

- **Mobile Experience Overhaul** - Complete redesign of the mobile interface, replacing Ionic with a custom Silk framework. This brings improved navigation, better touch interactions, and enhanced performance across iOS and Android platforms.

- **Semantic Search** - Introduction of local AI-powered semantic search using embedding models and HNSWLIB. Users can now search by meaning and context, not just keywords, with hybrid search combining traditional and semantic results.

- **Logseq CLI** - New command-line interface offering offline search, graph querying, data export, and graph management capabilities. Developers and power users can now interact with Logseq graphs programmatically.
```

## Example Full Changelog

```markdown
# Changelog (2025-01-05 to 2025-01-15)

## Spotlight

This release focuses on database performance and stability improvements, along with enhanced block editing capabilities.

- **Database Performance** - Significant optimizations to query execution and transaction processing, resulting in faster graph loading and smoother navigation.
- **Block Editing Enhancements** - Improved caret positioning, better undo/redo handling, and more intuitive multi-block selection.

## New Feature

### Block Editing
- Add support for custom block properties panel ([a1b2c3d](https://github.com/logseq/logseq/commit/a1b2c3d))
- Implement block folding keyboard shortcuts ([e4f5g6h](https://github.com/logseq/logseq/commit/e4f5g6h))

### Plugin System
- Add new API for custom theme extensions ([i7j8k9l](https://github.com/logseq/logseq/commit/i7j8k9l))

## Enhancement

### Database Operations
- Optimize query execution for large graphs ([m1n2o3p](https://github.com/logseq/logseq/commit/m1n2o3p))
- Improve transaction batching for better performance ([q4r5s6t](https://github.com/logseq/logseq/commit/q4r5s6t))

### UI/UX
- Enhance sidebar navigation with breadcrumbs ([u7v8w9x](https://github.com/logseq/logseq/commit/u7v8w9x))
- Improve mobile touch target sizes ([y1z2a3b](https://github.com/logseq/logseq/commit/y1z2a3b))

## Bug Fix

### Block Editing
- Fix caret position after undo operation ([c4d5e6f](https://github.com/logseq/logseq/commit/c4d5e6f))
- Resolve multi-block selection issues on mobile ([g7h8i9j](https://github.com/logseq/logseq/commit/g7h8i9j))

### File Handling
- Fix file watcher not detecting changes on Windows ([k1l2m3n](https://github.com/logseq/logseq/commit/k1l2m3n))

## Performance

- Reduce memory usage during graph indexing ([o4p5q6r](https://github.com/logseq/logseq/commit/o4p5q6r))
- Optimize rendering for pages with many blocks ([s7t8u9v](https://github.com/logseq/logseq/commit/s7t8u9v))

## Chore

- Update Electron to v28 ([w1x2y3z](https://github.com/logseq/logseq/commit/w1x2y3z))
- Clean up deprecated API calls ([a4b5c6d](https://github.com/logseq/logseq/commit/a4b5c6d))

## Contributors

Thank you to all contributors who made this release possible:

@alice, @bob, @charlie, @diana, @eve
```

## Instructions

1. **Analyze** the git commit log data for major themes and patterns
2. **Create** a compelling spotlight section highlighting the most significant changes
3. **Categorize** commits by primary type, then group by functional area
4. **Write** user-focused descriptions that explain the benefit, not just the change
5. **Include** commit hashes with properly formatted GitHub links
6. **Extract** and alphabetically sort all unique contributors
7. **Review** for clarity, consistency, and user value

Generate a comprehensive, well-organized changelog that helps users understand the value and impact of the release.
