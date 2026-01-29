---
title: 'Flashcard System'
description: "Complete reference for Logseq's flashcard and spaced repetition system"
type: reference
status: stable
lastVerified: '2026-01-29'
logseqVersion: '0.11.x'
platforms: [desktop, web, mobile]
graphType: [file, db]
relatedDocs:
  tutorials:
    - '[Flashcards Basics](../../tutorials/knowledge-management/flashcards-basics.md)'
  howTo:
    - '[Create Flashcard Deck](../../how-to/content/create-flashcard-deck.md)'
---

# Flashcard System

> Complete reference for Logseq's flashcard and spaced repetition functionality.

## Overview

Logseq supports two flashcard implementations:

| Feature       | File-Based (Legacy) | Database-Based |
| ------------- | ------------------- | -------------- |
| Algorithm     | SM-5                | FSRS           |
| Rating System | 3-button            | 4-button       |
| Status        | Deprecated          | Recommended    |

---

## Card Creation

### Tag Method

Add `#card` to any block:

```
What is the capital of France? #card
Paris
```

### Reversed Cards

Create bidirectional cards:

```
Bonjour #card #reversed
Hello
```

Creates two cards:

- Bonjour → Hello
- Hello → Bonjour

### Cloze Deletions

Hide parts of text:

```
The capital of France is {{cloze Paris}}. #card
```

### Cloze with Hints

Add context hints:

```
The capital of {{cloze France\\country}} is {{cloze Paris\\city}}. #card
```

### Multiple Clozes

Numbered clozes create separate cards:

```
{{c1 Photosynthesis}} uses {{c2 sunlight}} to produce {{c3 glucose}}. #card
```

Creates 3 cards, each hiding one term.

---

## Card Properties

### File-Based Graph Properties

| Property              | Type   | Description               |
| --------------------- | ------ | ------------------------- |
| `:card-last-interval` | number | Days since last review    |
| `:card-repeats`       | number | Successful review count   |
| `:card-ease-factor`   | float  | Difficulty (default: 2.5) |
| `:card-next-schedule` | date   | Next review date          |
| `:card-last-score`    | number | Last rating (1-5)         |

### Database Graph Properties

| Property                       | Type      | Description              |
| ------------------------------ | --------- | ------------------------ |
| `:logseq.property.fsrs/state`  | map       | Stability and difficulty |
| `:logseq.property.fsrs/due`    | timestamp | Next review time         |
| `:logseq.property.fsrs/reps`   | number    | Total reviews            |
| `:logseq.property.fsrs/lapses` | number    | Times forgotten          |

---

## FSRS Algorithm

### Overview

Free Spaced Repetition Scheduler (FSRS) is a modern algorithm based on the memory forgetting curve.

### Rating Buttons

| Button    | Description              | Effect                     |
| --------- | ------------------------ | -------------------------- |
| **Again** | Forgot completely        | Reset, review soon         |
| **Hard**  | Recalled with difficulty | Slightly longer interval   |
| **Good**  | Normal recall            | Standard interval increase |
| **Easy**  | Instant recall           | Longer interval            |

### State Variables

| Variable         | Description              |
| ---------------- | ------------------------ |
| `stability`      | Days until 90% retention |
| `difficulty`     | Item difficulty (0-10)   |
| `elapsed_days`   | Days since last review   |
| `scheduled_days` | Days until next review   |

### Retention Target

FSRS schedules reviews to maintain ~90% retention:

- Too early: Wasted effort
- Too late: Increased forgetting

---

## Legacy SM-5 Algorithm

### Rating Buttons

| Button         | Score | Description      |
| -------------- | ----- | ---------------- |
| **Forgotten**  | 1     | Complete failure |
| **Recall**     | 3     | Partial recall   |
| **Remembered** | 5     | Full recall      |

### Interval Calculation

```
new_interval = old_interval × ease_factor × optimal_factor
```

Where optimal_factor depends on review history.

---

## Configuration

### Enable Flashcards

Settings → Features → Enable Flashcards

### config.edn Options

```clojure
{;; Enable/disable flashcards
 :feature/enable-flashcards? true

 ;; SM-5 options (file-based only)
 :srs/learning-fraction 0.5
 :srs/initial-interval 4

 ;; New cards per day
 :srs/new-cards-limit 20

 ;; Daily review limit
 :srs/review-limit 200}
```

---

## Keyboard Shortcuts

### Review Session

| Key     | Action                  |
| ------- | ----------------------- |
| `s`     | Start review            |
| `n`     | Show answer / Next card |
| `Space` | Show answer             |

### Rating (FSRS)

| Key | Rating |
| --- | ------ |
| `1` | Again  |
| `2` | Hard   |
| `3` | Good   |
| `4` | Easy   |

### Rating (SM-5)

| Key | Rating     |
| --- | ---------- |
| `f` | Forgotten  |
| `r` | Recall     |
| `t` | Remembered |

---

## Card Queries

### Basic Query

Show cards from a page:

```
{{cards [[MyPage]]}}
```

### All Due Cards

```
{{cards}}
```

### Filter by Tag

```
{{query (and (property :card) [[chemistry]])}}
```

### Custom Card Collection

```clojure
#+BEGIN_QUERY
{:title "Spanish Vocabulary"
 :query [:find (pull ?b [*])
         :where [?b :block/refs ?ref]
                [?ref :block/name "spanish"]
                [?b :block/refs ?card]
                [?card :block/name "card"]]}
#+END_QUERY
```

---

## Built-in Classes (DB Graphs)

### Card Class

`:logseq.class/Card`

Properties:

- FSRS state
- Due date
- Review history

### Cards Collection

`:logseq.class/Cards`

Used for card queries and collections.

---

## Review Statistics

### Card States

| State          | Description         |
| -------------- | ------------------- |
| **New**        | Never reviewed      |
| **Learning**   | Recently introduced |
| **Review**     | Regular rotation    |
| **Relearning** | Failed, re-learning |

### Metrics

- **Due today**: Cards needing review
- **Review forecast**: Upcoming reviews
- **Retention rate**: Success percentage
- **Streak**: Consecutive review days

---

## Data Storage

### File-Based

Properties stored in block metadata:

```markdown
- Question here #card
  card-last-interval:: 7
  card-repeats:: 3
  card-ease-factor:: 2.5
  card-next-schedule:: 2026-02-05
  - Answer here
```

### Database-Based

Stored in SQLite with Transit serialization:

- Card entities reference source blocks
- FSRS state as structured property
- Efficient querying and filtering

---

## Integration

### Plugin API

```javascript
// Get due cards
const dueCards = await logseq.App.getFlashcards()

// Review a card
await logseq.Editor.reviewCard(cardId, rating)

// Create card from block
await logseq.Editor.insertAtEditingCursor('#card')
```

### Slash Commands

| Command  | Action                |
| -------- | --------------------- |
| `/card`  | Add card tag          |
| `/cards` | Insert card query     |
| `/cloze` | Create cloze deletion |

---

## Best Practices

### Card Design

✅ **Do:**

- One concept per card
- Clear, specific questions
- Include context when needed
- Use images for visual content

❌ **Don't:**

- Cram multiple facts
- Use vague questions
- Create orphan cards
- Over-use cloze deletions

### Review Habits

- Daily review sessions
- Honest difficulty ratings
- Consistent schedule
- Regular card maintenance

---

## Troubleshooting

### Cards Not Appearing

1. Check `#card` tag present
2. Verify flashcards enabled
3. Check due date in future
4. Look for syntax errors

### Scheduling Issues

1. Check algorithm settings
2. Review card properties
3. Reset card if corrupted

### Performance

For large collections (>1000 cards):

- Use filtered queries
- Archive old cards
- Optimize query complexity

---

## Migration

### File to Database

When converting:

1. Card content preserved
2. Review history may need manual migration
3. FSRS re-learns your patterns
4. Initial reviews may feel different

### Adapting to FSRS

- 4 buttons instead of 3
- More nuanced ratings
- Different interval calculations
- Generally better long-term retention

---

## Code Locations

| Component     | Path                                        |
| ------------- | ------------------------------------------- |
| SM-5 Logic    | `src/main/frontend/extensions/srs.cljs`     |
| FSRS Logic    | `src/main/frontend/extensions/fsrs.cljs`    |
| UI Components | `src/main/frontend/components/`             |
| Card Classes  | `deps/db/src/logseq/db/frontend/class.cljs` |

---

## See Also

- **[Flashcards Basics](../../tutorials/knowledge-management/flashcards-basics.md)**
- **[Create Flashcard Deck](../../how-to/content/create-flashcard-deck.md)**

---

_Last updated: 2026-01-29_
