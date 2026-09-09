---
title: 'Flashcard System'
description: "Complete reference for Logseq's flashcard and spaced repetition system"
type: reference
status: stable
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
logseqVersion: '0.11.x'
platforms: [desktop, web, mobile]
graphType: [db]
relatedDocs:
  tutorials:
    - '[Flashcards Basics](../../tutorials/knowledge-management/flashcards-basics.md)'
  howTo:
    - '[Create Flashcard Deck](../../how-to/content/create-flashcard-deck.md)'
---

# Flashcard System

> **Last revised:** 2026-09-09

> Complete reference for Logseq's flashcard and spaced repetition functionality.

## Overview

Logseq uses the FSRS (Free Spaced Repetition Scheduler) algorithm for flashcards in database graphs. The legacy SM-5 algorithm and file-based flashcard code have been removed from the codebase.

| Feature       | Database-Based       |
| ------------- | -------------------- |
| Algorithm     | FSRS                 |
| Rating System | 4-button             |
| Graph Type    | DB graphs only       |

---

## Card Creation

### Tag Method

Add `#Card` to any block (note the capital C in DB graphs):

```
What is the capital of France? #Card
Paris
```

### Context Menu ("Make a Card")

When flashcards are enabled, the block right-click menu and the multi-block selection menu both offer **Make a Card**, which tags the selected top-level block(s) with `:logseq.class/Card`.

- Single block: `src/main/frontend/components/content.cljs:341-346`
- Selection: `src/main/frontend/components/content.cljs:134-138`
- Handler: `batch-make-cards!`, `src/main/frontend/extensions/fsrs.cljs:412-424`

### Cloze Deletions

Hide parts of text:

```
The capital of France is {{cloze Paris}}. #Card
```

### Cloze with Hints

Add context hints using `\\` as a separator:

```
The capital of {{cloze France\\country}} is {{cloze Paris\\city}}. #Card
```

Multiple `{{cloze ...}}` macros within a single card are supported. During review the **Show clozes** step reveals all of them at once; an individual cloze can also be toggled by clicking (or focusing and pressing Enter/Space) on it (`cloze-macro-show`, `src/main/frontend/extensions/fsrs.cljs:440-471`).

If a cloze body contains more than one `\\` separator, only the text after the **last** separator is used as the cue (`cloze-parse`, `src/main/frontend/extensions/fsrs.cljs:430-438`).

---

## Card Properties

### Database Graph Properties

| Property                      | Type     | Description                                           |
| ----------------------------- | -------- | ----------------------------------------------------- |
| `:logseq.property.fsrs/state` | map      | Contains stability, difficulty, reps, lapses, state, last-repeat, and last rating |
| `:logseq.property.fsrs/due`   | datetime | Next review time                                      |

The `:logseq.property.fsrs/state` map stores all FSRS scheduling data including:

- `stability` - memory stability, as defined by FSRS (the retention probability it targets is the library's, not Logseq's — see [Scheduling parameters](#scheduling-parameters))
- `difficulty` - item difficulty (0-10)
- `reps` - total review count
- `lapses` - times forgotten
- `state` - card state (`:new`, `:learning`, `:review`, `:relearning`)
- `last-repeat` - timestamp of last review
- `logseq/last-rating` - last rating given (`:again`, `:hard`, `:good`, `:easy`)

---

## FSRS Algorithm

### Overview

Free Spaced Repetition Scheduler (FSRS) is a modern algorithm based on the memory forgetting curve. Logseq uses the [open-spaced-repetition/cljc-fsrs](https://github.com/open-spaced-repetition/cljc-fsrs) library.

### Rating Buttons

| Button    | Description              | Effect                     |
| --------- | ------------------------ | -------------------------- |
| **Again** | Forgot completely        | Reset, review soon         |
| **Hard**  | Recalled with difficulty | Slightly longer interval   |
| **Good**  | Normal recall            | Standard interval increase |
| **Easy**  | Instant recall           | Longer interval            |

The four ratings map to `:again` / `:hard` / `:good` / `:easy` and are passed straight to
the library; the interval effects above describe FSRS behaviour generally, since Logseq
does not compute them itself.

### Scheduling parameters

Logseq does not configure FSRS. `fsrs.cljs` requires
`open-spaced-repetition.cljc-fsrs.core` (`:20`) and calls it without passing weights,
a desired-retention value, or any other parameter, so whatever that library ships as
its defaults is what applies. The dependency is pinned by git SHA in `deps.edn:53-54`,
not by version.

> **Removed:** "FSRS schedules reviews to maintain ~90% retention." That figure is the
> FSRS algorithm's conventional default target, not a value Logseq sets or reads — there
> is no `retention`, `desired-retention` or parameter reference anywhere in
> `src/main/frontend/extensions/fsrs.cljs`. The only occurrence of the word in Logseq's
> source is `true-retention-percent` (`fsrs.cljs:526`), an unrelated statistic — cards in
> review state over all cards — that lives inside the commented-out block and is not
> exposed.

---

## Configuration

### Enable Flashcards

Settings -> Features -> Enable Flashcards

### config.edn Options

```clojure
{;; Enable/disable flashcards
 :feature/enable-flashcards? true

 ;; Legacy SRS options -- explicitly listed as "unused in DB graphs"
 ;; :srs/learning-fraction 0.5
 ;; :srs/initial-interval 4
 }
```

`:srs/learning-fraction` and `:srs/initial-interval` are enumerated in the DB-graph deprecation list (`deps/common/src/logseq/common/config.cljs:123-124`) and have no effect in DB graphs.

---

## Keyboard Shortcuts

### Review Session

| Key | Action                   |
| --- | ------------------------ |
| `s` | Show/hide answer (toggle)|

### Rating

| Key | Rating |
| --- | ------ |
| `1` | Again  |
| `2` | Hard   |
| `3` | Good   |
| `4` | Easy   |

The flashcard dialog can be opened with `g f` or `t c` (`:go/flashcards`, `src/main/frontend/modules/shortcut/config.cljs:414-415`). The rating and show/hide-answer bindings above are the `:cards/*` shortcut namespace (`src/main/frontend/modules/shortcut/config.cljs:82-95`).

---

## Card Collections (DB Graphs)

### Built-in Classes

#### Card Class

`:logseq.class/Card`

Schema properties:

- `:logseq.property.fsrs/state` - FSRS scheduling state
- `:logseq.property.fsrs/due` - next review datetime

#### Cards Class

`:logseq.class/Cards`

Extends `:logseq.class/Query`. Used for defining filtered card collections with custom queries.

### Creating Card Collections

In the flashcard dialog, click the "+" button to create a new Cards block on today's journal page and navigate to it (`<create-cards-block!`, `src/main/frontend/extensions/fsrs.cljs:108-115`). Each Cards block has an associated query (`:logseq.property/query`) that filters which cards to review.

### Selecting Card Sets

Use the dropdown in the flashcard dialog to switch between "All cards" and any defined Cards collections.

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

- **Due today**: Cards needing review (shown in sidebar badge)

Note: Detailed statistics (true retention, passed/lapsed repeats, per-state counts, today / 7-day / 30-day windows) exist only inside a `(comment ...)` form (`src/main/frontend/extensions/fsrs.cljs:484-556`) and are not exposed in the UI.

---

## Data Storage

### Database-Based

Stored in SQLite via Datascript:

- Blocks tagged with `:logseq.class/Card` are flashcards
- FSRS state stored as a map in `:logseq.property.fsrs/state`
- Due date stored as datetime in `:logseq.property.fsrs/due`
- Queried via Datalog for efficient filtering

---

## Integration

### Slash Commands

| Command  | Action                |
| -------- | --------------------- |
| `/Cloze` | Insert cloze template |

Note: `/Cloze` is the only flashcard-related slash command currently registered. There are no `/card` or `/cards` slash commands.

### Macros

`{{cloze ...}}` is the only registered flashcard macro (`component-macro/register`, `src/main/frontend/extensions/fsrs.cljs:482`).

> **Not implemented:** a `{{cards ...}}` query macro. It exists only as a commented-out `TODO` (`src/main/frontend/extensions/fsrs.cljs:477-480`). Card collections are defined with `:logseq.class/Cards` blocks instead (see above).

### Plugin API

There is no flashcard read/write plugin API. The only flashcard-related surface is the read-only `:enabled-flashcards` field returned by `getUserConfigs` (`src/main/logseq/api/app.cljs:65`).

---

## Best Practices

### Card Design

**Do:**

- One concept per card
- Clear, specific questions
- Include context when needed
- Use images for visual content

**Don't:**

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

1. Check `#Card` tag is present (capital C in DB graphs)
2. Verify flashcards are enabled in settings
3. Check due date -- cards not yet due will not appear
4. Look for syntax errors in cloze blocks

### Scheduling Issues

1. Review the `:logseq.property.fsrs/state` property on the card block
2. Check `:logseq.property.fsrs/due` for the next scheduled date

---

## Code Locations

| Component        | Path                                             |
| ---------------- | ------------------------------------------------ |
| FSRS Logic & UI  | `src/main/frontend/extensions/fsrs.cljs`         |
| Card context menu | `src/main/frontend/components/content.cljs:134`, `:341` |
| Shortcut Handler | `src/main/frontend/extensions/srs/handler.cljs`  |
| Shortcut Config  | `src/main/frontend/modules/shortcut/config.cljs` |
| Card Classes     | `deps/db/src/logseq/db/frontend/class.cljs:59-67` |
| FSRS Properties  | `deps/db/src/logseq/db/frontend/property.cljs:610-619` |
| FSRS Library     | `open-spaced-repetition/cljc-fsrs` (dependency)  |

---

## See Also

- **[Flashcards Basics](../../tutorials/knowledge-management/flashcards-basics.md)**
- **[Create Flashcard Deck](../../how-to/content/create-flashcard-deck.md)**

---

_Last updated: 2026-09-09_
