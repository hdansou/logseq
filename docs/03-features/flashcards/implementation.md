# Logseq Flashcard Implementation Guide

## Overview

Logseq provides a powerful flashcard system that integrates seamlessly with your knowledge management workflow. The implementation uses spaced repetition algorithms to optimize learning and retention. This guide explains how flashcards work internally and how users can leverage them effectively.

## System Architecture

Logseq implements a dual-system approach for flashcards, supporting both legacy file-based graphs and modern database-based graphs:

### File-based Graphs (Legacy)

- **Algorithm**: SuperMemo SM-5 spaced repetition
- **Storage**: Block properties in markdown files
- **Review Interface**: 3-button system (Forgotten, Recall, Remembered)
- **Status**: Will be deprecated in favor of database-based system

### Database-based Graphs (Modern)

- **Algorithm**: FSRS (Free Spaced Repetition Scheduler)
- **Storage**: Database entities with structured properties
- **Review Interface**: 4-button system (Again, Hard, Good, Easy)
- **Status**: Current recommended approach

## Creating Flashcards

### Basic Card Creation

1. **Tag Method**: Add `#card` to any block

   ```txt
   What is the capital of France? #card
   Paris
   ```

2. **Slash Command**: Use `/cards` to create card queries
3. **Cloze Deletions**: Use `/cloze` for fill-in-the-blank cards

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

### Legacy SM-5 Algorithm (File-based)

**Key Properties**:

- `:card-last-interval` - Days since last review
- `:card-repeats` - Number of successful reviews
- `:card-ease-factor` - Learning difficulty (default: 2.5)
- `:card-next-schedule` - Next review date
- `:card-last-score` - Previous review performance (1-5)

**Algorithm Logic**:

- Initial interval: 4 days (configurable)
- Learning fraction: 0.5 (configurable)
- Uses optimum factor (OF) matrix for interval calculation
- Adjusts intervals based on review performance

### Modern FSRS Algorithm (Database-based)

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
2. **Question Phase**: Display question or incomplete cloze
3. **Answer Phase**: Show answer after user interaction
4. **Cloze Phase** (if applicable): Progressive cloze revelation
5. **Rating Phase**: User rates recall difficulty
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

- `s` - Start review session
- `n` - Next card / Show answer
- `f` - Forgotten (legacy) / Again (FSRS)
- `r` - Recall (legacy)
- `t` - Remembered (legacy) / Easy (FSRS)
- `1-4` - Direct rating (FSRS only)

## Configuration Options

### Graph Settings

```clojure
;; File-based configuration (config.edn)
:srs/learning-fraction 0.5        ; Speed of difficulty adjustment
:srs/initial-interval 4           ; First review after 4 days
:feature/enable-flashcards? true  ; Enable/disable flashcards
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

## Migration Path

For users transitioning from file-based to database-based graphs:

1. **Assessment**: Evaluate current card collection
2. **Migration**: Export/import cards as needed
3. **Algorithm Adjustment**: Adapt to FSRS rating system
4. **Workflow Update**: Learn new keyboard shortcuts and interface
5. **Configuration**: Adjust settings for optimal performance

## Troubleshooting

### Common Issues

- **Cards Not Appearing**: Check `#card` tags and due dates
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

- **Core Logic**: `/src/main/frontend/extensions/` (srs.cljs, fsrs.cljs)
- **UI Components**: Integrated with block editor and modal system
- **Database Schema**: Uses built-in class system for type safety
- **Algorithm Libraries**: External FSRS library for modern implementation

### Data Flow

1. Block parsing identifies card markers
2. Properties store scheduling state
3. Algorithm calculates due dates
4. UI queries due cards for review
5. User interactions update scheduling state
6. Process repeats for spaced repetition

This comprehensive system provides flexible, powerful flashcard functionality that scales from simple memory tasks to complex knowledge retention workflows.
