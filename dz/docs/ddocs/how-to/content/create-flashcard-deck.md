---
title: 'Create a Flashcard Deck'
description: 'How to organize flashcards into focused study decks'
status: stable
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
reviewStatus: merged-not-fully-audited
logseqVersion: '0.11.x'
platforms: [desktop, web, mobile]
graphType: [db]
relatedDocs:
  tutorials:
    - '[Flashcards Basics](../../tutorials/knowledge-management/flashcards-basics.md)'
  reference:
    - '[Flashcard System](../../reference/features/flashcards.md)'
---

# Create a Flashcard Deck

> **Last revised:** 2026-09-09 (verified against `be800f1711`)

> How to organize flashcards into focused study decks for efficient learning.

Logseq uses FSRS (Free Spaced Repetition Scheduler) for its flashcard system. In DB-based graphs, cards are blocks tagged with the built-in `#Card` class, and decks are managed through `Cards` objects that contain queries.

## Findings: New Capabilities

- Flashcards can now be opened with both `g f` and `t c`.
- Mobile has a dedicated Flashcards tab, plus deck selection state in the mobile flashcards viewer.
- Creating a new deck still uses the `+` button in the flashcard viewer to create a `Cards` block on today's journal page.

## Creating Cards

### Tag Blocks as Cards

Add the `#Card` tag to any block to make it a flashcard. The block content is the question, and child blocks form the answer:

```markdown
What is H2O? #Card
  Water

What is NaCl? #Card
  Sodium Chloride (Salt)
```

You can also select multiple blocks and use the context menu or command palette to batch-tag them as `#Card`.

### Cloze Deletions

Use the `{{cloze ...}}` macro to create fill-in-the-blank cards:

```markdown
The capital of France is {{cloze Paris}} #Card
```

You can add a cue hint with a backslash separator: `{{cloze answer \\ hint}}`.

Insert a cloze via the `/Cloze` slash command.

---

## Opening the Flashcard Viewer

Access the flashcard review modal in one of these ways:

1. Click the flashcard icon in the left sidebar (if enabled in settings)
2. Use the keyboard shortcut `g f` or `t c`
3. On mobile, open the Flashcards tab

The viewer shows due cards and lets you rate recall with **Again** (1), **Hard** (2), **Good** (3), or **Easy** (4). Press `s` to toggle answers/clozes ([shortcut/config.cljs:82-95](../../../../../src/main/frontend/modules/shortcut/config.cljs#L82-L95)).

> **Note:** Flashcards must be enabled in **Settings > Features > Enable Flashcards** (controlled by `:feature/enable-flashcards?` in config).

---

## Organizing Decks with Cards Objects

Logseq uses `Cards` objects (blocks tagged with the built-in `#Cards` class) to define filtered subsets of your cards. Each `Cards` object contains a query that selects which `#Card` blocks to include.

### Create a Cards Object

1. Open the flashcard viewer (`t c`)
2. Click the **+** button in the viewer toolbar
3. This creates a new `Cards` block on today's journal page with an associated query
4. Edit the query to filter cards (e.g., `[[chemistry]]` to show only cards that reference the chemistry page)

### Switch Between Decks

In the flashcard viewer, use the dropdown at the top to switch between:

- **All cards** -- reviews every `#Card` in the graph
- Any named `Cards` objects you have created

### Example: Tag-Based Decks

Create cards with topic tags, then create `Cards` objects with queries for each topic:

```markdown
What is H2O? #Card #chemistry
  Water

What is NaCl? #Card #chemistry
  Sodium Chloride (Salt)

When was World War II? #Card #history
  1939-1945
```

Then create a `Cards` object with query `[[chemistry]]` to study only chemistry cards.

> **Removed:** The old page-based, tag-based and namespace-based deck recipes built on a `{{cards [[Some Page]]}}` macro. That macro is not registered in DB graphs -- only `{{cloze}}` is, and `{{cards ...}}` remains a commented-out TODO ([fsrs.cljs:473-482](../../../../../src/main/frontend/extensions/fsrs.cljs#L473-L482)). Use `Cards` objects instead.

---

## Property-Based Organization

### Add Properties to Cards

```markdown
What is photosynthesis? #Card
  topic:: biology
  difficulty:: medium
  The process by which plants convert light to energy
```

Properties on card blocks can be used for personal organization and filtering.

---

## Import Cards

### From Existing Notes

Convert existing notes into flashcards:

1. Find key concepts in your notes
2. Add the `#Card` tag to those blocks
3. Ensure the answer is in indented child blocks

### Bulk Creation

Select multiple blocks, then use the command palette or right-click context menu to batch-tag them as `#Card`.

---

## FSRS Scheduling

Logseq uses the FSRS algorithm to schedule card reviews. The scheduling state is stored in two built-in properties on each card block:

- `logseq.property.fsrs/due` -- the next review date (as a timestamp)
- `logseq.property.fsrs/state` -- the persisted FSRS card state; Logseq keeps the internal card map here and records the last rating automatically

These properties are managed automatically when you rate cards during review. You do not need to edit them manually.

> **Removed:** The legacy file-based `:card-last-interval`, `:card-repeats`, `:card-ease-factor`, `:card-next-schedule`, and `:card-last-score` fields do not apply to DB flashcards.

## Configuration

### Enable Flashcards

Settings → Features → Enable Flashcards

> **Removed:** Legacy `:srs/learning-fraction`, `:srs/initial-interval`, `:srs/new-cards-limit`, and `:srs/review-limit` options are not used in DB graphs.

---

## Deck Maintenance

### Review Deck Health

Check for:

- Orphan cards (blocks tagged `#Card` not covered by any `Cards` query)
- Duplicate cards
- Outdated content

### Split Large Decks

If you have many cards on a single topic:

1. Add more specific tags to cards (e.g., `#organic-chemistry`, `#inorganic-chemistry`)
2. Create separate `Cards` objects with queries for each sub-topic
3. Rotate focus between sub-decks

---

## Deck Strategies

### The 20-Card Rule

For new decks:

- Start with 20 cards
- Master before adding more
- Prevents overwhelm

### Topic Rotation

For multiple decks:

- Day 1: Spanish
- Day 2: History
- Day 3: Science
- Repeat

### Focused Sessions

- 15 minutes maximum
- One deck at a time
- Stop when attention wanders

---

## Troubleshooting

### No Cards Appearing

1. Verify blocks are tagged with `#Card` (the built-in class, not a custom tag)
2. Check that flashcards are enabled in Settings
3. If using a `Cards` object, verify the query is correct

### Wrong Cards Appearing

1. Review the query in your `Cards` object
2. Check for unintended tag references on card blocks
3. Switch to "All cards" to see if the issue is query-specific

### Cards Not Scheduled

1. Complete an initial review (rate the card at least once)
2. Check the `logseq.property.fsrs/due` property on the block
3. Ensure flashcards feature is enabled

---

## See Also

- **[Flashcards Basics](../../tutorials/knowledge-management/flashcards-basics.md)**
- **[Flashcard System](../../reference/features/flashcards.md)**

---

_Last updated: 2026-09-09_
