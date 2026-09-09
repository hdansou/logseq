# Logseq Flashcard Implementation Guide

> **Last revised:** 2026-05-28

## Overview

Logseq provides a powerful flashcard system that integrates seamlessly with your knowledge management workflow. The implementation uses spaced repetition algorithms to optimize learning and retention. This guide explains how flashcards work internally and how users can leverage them effectively.

## System Architecture

Logseq's current flashcard system uses the FSRS algorithm for database-based graphs. Legacy file-based graph SRS support (which used block properties like `:card-last-interval`, `:card-ease-factor`, etc.) has been deprecated and the related code removed from the active codebase; those properties now only appear in the graph migration/export layer.

### Database-based Graphs (Current)

- **Algorithm**: FSRS (Free Spaced Repetition Scheduler)
- **Storage**: Database entities with structured properties
- **Review Interface**: 4-button system (Again, Hard, Good, Easy)
- **Status**: Current active implementation

### File-based Graphs (Legacy/Deprecated)

- **Storage**: Block properties in markdown files (e.g., `:card-last-interval`, `:card-repeats`, `:card-ease-factor`)
- **Status**: Deprecated; properties are only handled during migration/export to DB-based graphs

## Creating Flashcards

### Basic Card Creation

1. **Tag Method**: Add `#Card` to any block (capitalized; matches the built-in `:logseq.class/Card` class)

   ```txt
   What is the capital of France? #Card
   Paris
   ```

2. **Context Menu**: Right-click any block and choose **Make a Flashcard** to tag it as `#Card`
3. **Cloze Deletions**: Use `/cloze` for fill-in-the-blank cards

> **Removed:** The `/cards` slash command was planned but never implemented (marked as `TODO: support {{cards ...}}` in `src/main/frontend/extensions/fsrs.cljs`). To create a card collection query, use the **+** button in the flashcards modal.

```txt
The capital of France is {{cloze Paris}}
```

### Advanced Cloze Cards

Cloze deletions support cues for better context:

```txt
The capital of {{cloze France\\country}} is {{cloze Paris\\city}}
```

### Card Types

- **Q&A Cards**: Question-answer format with sided display
- **Cloze Cards**: Fill-in-the-blank with progressive revelation
- **Combined Cards**: Support both Q&A and cloze patterns in one card

## Spaced Repetition Algorithms

### Legacy Properties (File-based, Deprecated)

The following block properties were used by the old file-based SRS system and are now only relevant during migration/export:

- `:card-last-interval` - Days since last review
- `:card-repeats` - Number of successful reviews
- `:card-ease-factor` - Learning difficulty (default: 2.5)
- `:card-next-schedule` - Next review date
- `:card-last-score` - Previous review performance (1-5)

Configuration keys (still recognized in `config.edn` for legacy graphs):

- `:srs/learning-fraction` (default: 0.5)
- `:srs/initial-interval` (default: 4 days)

### FSRS Algorithm (Database-based, Current)

**Key Properties**:

- `:logseq.property.fsrs/state` - Card stability and difficulty metrics
- `:logseq.property.fsrs/due` - Next review timestamp

**Algorithm Features**:

- More sophisticated retention prediction
- Four-level rating system for nuanced feedback
- Better handling of review history patterns
- Adaptive scheduling based on individual learning patterns

## Review Interface

### Review Session Flow

1. **Card Selection**: Due cards are identified by algorithm
2. **Question Phase**: Display question (`s` hides cloze answers, collapses children)
3. **Cloze Phase** (if applicable): Press `s` to reveal cloze fill-ins (children still hidden)
4. **Answer Phase**: Press `s` again to show full answer including children
5. **Rating Phase**: User rates recall difficulty (rating buttons appear once full answer is shown)
6. **Next Card**: Algorithm schedules next review

### User Interface Elements

**Navigation**:

- Modal dialog for focused review sessions
- Breadcrumb navigation for context
- Due card counter in left sidebar

**Interaction**:

- Click to reveal answers/clozes
- Keyboard shortcuts for all actions
- Batch operations for card management

### Keyboard Shortcuts

- `s` - Show clozes / Show answers / Hide answers (toggle reveal, bound to `:cards/toggle-answers`)
- `1` - Again
- `2` - Hard
- `3` - Good
- `4` - Easy

## Configuration Options

### Graph Settings

```clojure
;; config.edn settings
:srs/learning-fraction 0.5        ; Speed of difficulty adjustment (legacy file-based graphs)
:srs/initial-interval 4           ; First review after 4 days (legacy file-based graphs)
:feature/enable-flashcards? true  ; Enable/disable flashcards (default: true)
```

### Built-in Classes (Database-based)

- `:logseq.class/Card` - Individual flashcard entity
- `:logseq.class/Cards` - Card collection queries

## Query System

### Basic Card Queries

Create dynamic card collections:

```txt
{{query (and [[card]] (between [[created-at]] [[7d-ago]] [[now]]))}}
```

### Advanced Filtering

Filter cards by properties, tags, or content:

- Filter by review status
- Sort by due date or difficulty
- Group by topics or hierarchies
- Custom card collections with complex logic

## Integration Features

### Database Integration

- Cards stored as regular blocks with special properties
- Full-text search across card content
- Property-based filtering and sorting
- Query system supports complex card operations

### Plugin API

- Flashcard functionality exposed through Logseq API
- Extensible through plugin system
- Custom algorithms and UI components possible

### UI Integration

- Seamless integration with block editor
- Context-aware card creation
- Navigation between cards and source blocks
- Progress tracking and statistics

## Best Practices

### Card Creation

1. **Atomic Cards**: One concept per card
2. **Clear Questions**: Unambiguous wording
3. **Contextual Cues**: Include necessary background
4. **Progressive Complexity**: Build from simple to complex concepts

### Review Strategy

1. **Consistent Schedule**: Regular review sessions
2. **Honest Rating**: Accurate difficulty assessment
3. **Context Integration**: Connect cards to broader knowledge
4. **Iterative Improvement**: Refine cards based on review experience

### Organization

1. **Tag Hierarchies**: Organize cards by subject/topic
2. **Property Metadata**: Use properties for filtering
3. **Query Collections**: Create focused review sets
4. **Cross-References**: Link related cards and concepts

## Findings: New Capabilities

The following capabilities are present in source code but were not previously documented:

- **Context menu "Make a Flashcard"**: Right-clicking one or more selected blocks exposes a "Make a Flashcard" menu item (`:context-menu/make-a-flashcard`) that calls `fsrs/batch-make-cards!`, tagging the block(s) with `:logseq.class/Card` in one action. Available in both single-block and multi-block selection contexts (`src/main/frontend/components/content.cljs`).

- **"+" button in cards modal**: The flashcards review modal includes a `+` button (`:flashcard/add-query`) that creates a new `:logseq.class/Cards` block on today's journal page and navigates to it, letting users configure a scoped card collection without typing.

- **Card collection selector**: The cards modal contains a dropdown (`shui/select`) listing all Cards collection blocks in the graph. Selecting one scopes the review session to only the due cards matched by that collection's query.

- **Card class inheritance**: The due-card query uses `db-model/get-structured-children` to find all subclasses of `:logseq.class/Card`. This means user-defined classes that extend `Card` are automatically included in review sessions without extra configuration.

## Migration Path

For users transitioning from file-based to database-based graphs:

1. **Assessment**: Evaluate current card collection
2. **Migration**: Export/import cards as needed
3. **Algorithm Adjustment**: Adapt to FSRS rating system
4. **Workflow Update**: Learn new keyboard shortcuts and interface
5. **Configuration**: Adjust settings for optimal performance

## Troubleshooting

### Common Issues

- **Cards Not Appearing**: Check `#Card` tags and due dates
- **Incorrect Scheduling**: Verify algorithm configuration
- **UI Problems**: Ensure flashcards are enabled in settings
- **Performance**: Large card collections may need query optimization

### Performance Considerations

- **Card Count**: Monitor total active cards
- **Query Complexity**: Optimize card collection queries
- **Review Frequency**: Balance learning load with retention
- **Database Size**: Archive old or inactive cards

## Technical Implementation Notes

### Code Organization

- **Core Logic**: `src/main/frontend/extensions/fsrs.cljs` (FSRS algorithm and review UI)
- **SRS Handler**: `src/main/frontend/extensions/srs/handler.cljs` (button click handlers)
- **Class Definition**: `deps/db/src/logseq/db/frontend/class.cljs` (`:logseq.class/Card`)
- **UI Components**: Integrated with block editor and modal system
- **Database Schema**: Uses built-in class system for type safety

### Data Flow

1. Block parsing identifies card markers
2. Properties store scheduling state
3. Algorithm calculates due dates
4. UI queries due cards for review
5. User interactions update scheduling state
6. Process repeats for spaced repetition

This comprehensive system provides flexible, powerful flashcard functionality that scales from simple memory tasks to complex knowledge retention workflows.
