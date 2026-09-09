---
title: 'Flashcards Basics'
description: "Learn to create and study flashcards with Logseq's spaced repetition system"
type: tutorial
status: stable
lastVerified: '2026-09-09'
verifiedAgainst: 'be800f1711'
logseqVersion: '0.11.x'
platforms: [desktop, web, mobile]
graphType: [db]
difficulty: beginner
timeToComplete: '15 minutes'
prerequisites: ['basic-editing']
relatedDocs:
  reference:
    - '[Flashcard System](../../reference/features/flashcards.md)'
  howTo:
    - '[Create Flashcard Deck](../../how-to/content/create-flashcard-deck.md)'
---

# Flashcards Basics

> **Last revised:** 2026-09-09

> Learn to create flashcards and use spaced repetition for effective learning.

## What You'll Learn

- Creating flashcards
- The spaced repetition system (FSRS)
- Studying your cards
- Tracking what is due
- Best practices

**Time:** ~15 minutes

---

## What is Spaced Repetition?

Spaced repetition is a learning technique that shows you information right before you forget it. Logseq uses **FSRS** (Free Spaced Repetition Scheduler), a modern algorithm that:

- Learns how you remember
- Schedules reviews optimally
- Adapts to your performance

> **Note:** The FSRS-based flashcard system only works in **DB-based graphs**.

---

## Step 1: Create Your First Flashcard

### Basic Card (Front/Back)

Tag any block with `#Card` to turn it into a flashcard:

```
What is the capital of France?
#Card
Paris
```

The text before `#Card` becomes the **front** (question).
Child blocks become the **back** (answer).

You can also select one or more blocks, right-click, and choose **Make a Flashcard** from the context menu.

### Try It Now

1. Create a new block
2. Type: `What is 2 + 2?`
3. Add `#Card` tag
4. Press Enter and indent
5. Type: `4`

---

## Step 2: Card Types

### Standard Card

```
Question here
#Card
Answer here
```

### Cloze Deletion

Hide part of the text using the `/Cloze` slash command or by typing `{{cloze ...}}` directly:

```
The {{cloze capital}} of France is Paris.
#Card
```

When studying, you'll see: "The **[...]** of France is Paris."

You can also add an optional cloze cue by separating the hidden answer and cue with `\\`:

```
The {{cloze mitochondria \\ powerhouse of the cell}} generates ATP.
#Card
```

This displays the cue in study mode before reveal: `(powerhouse of the cell)`.

### Multiple Clozes

You can use multiple cloze deletions in a single card:

```
{{cloze Photosynthesis}} converts {{cloze light energy}} into {{cloze chemical energy}}.
#Card
```

All clozes are revealed together as part of a single card.

> **Removed:** Numbered clozes (`{{c1 ...}}`, `{{c2 ...}}`) that split one block into
> several cards, and reversed cards (`#card #reversed`). Neither exists in the current
> source: cloze detection is a plain `{{cloze ` substring test, and there is no
> `reversed` tag or generated inverse card.

---

## Step 3: Study Your Cards

### Access Flashcards

**Method 1: Sidebar**

- Click the flashcard icon in the left sidebar

**Method 2: Keyboard**

- Press `g` then `f`, or `t` then `c` (go to flashcards)

> **Removed:** The `t f` shortcut, a `/Flashcards` slash command, and
> `Cmd/Ctrl+Shift+C` for creating a card. The flashcard shortcut is `g f` / `t c`;
> `Cmd/Ctrl+Shift+C` is bound to Copy text. The only flashcard-related slash command
> is `/Cloze`.

### Study Session

1. A card appears showing the front (question)
2. Think of the answer
3. Click **Show answers** (or press `s`)
4. Rate how well you remembered:
   - **Again** (press `1`) - Forgot completely
   - **Hard** (press `2`) - Struggled but got it
   - **Good** (press `3`) - Remembered correctly
   - **Easy** (press `4`) - Knew it instantly

### Review Schedule

Based on your rating, FSRS calculates when to show the card again. Each rating button displays the projected next review time so you can see how your choice affects scheduling.

For cloze cards, pressing `s` advances in two phases: `Show clozes` then `Show answers`.

---

## Step 4: Organize With Cards Queries

### Using the Cards Selector

When studying, a dropdown at the top lets you switch between "All cards" and any custom Cards queries you have created.

### Creating a Cards Query

Click the "+" button in the study view to create a new Cards query block. This lets you define custom filtered sets of cards to study.

### Tracking What Is Due

- The study view shows a position counter next to the selector, in the form
  `current/total` for the cards due in the selected set.
- The **Flashcards** entry in the left sidebar carries a badge with the total number of
  cards due across the graph. It is hidden when nothing is due.
- When the selected set has no due cards left, the view shows a "you've reviewed all the
  cards for this query" message.

> **Removed:** Per-card status labels (New / Learning / Review / Due) and a statistics
> panel with review forecast, retention rate and streak. The FSRS state is stored on the
> block as `:logseq.property.fsrs/state` and `:logseq.property.fsrs/due`, but no such
> status labels or statistics view are surfaced in the UI.

---

## Step 5: Write Better Cards

### Good Card Principles

**One idea per card**

```
What is the atomic number of Carbon?
#Card
6
```

**Avoid too much info**

```
Describe Carbon including atomic number, mass, and properties
#Card
(Long answer that's hard to review)
```

### Use Context

**Specific question**

```
In JavaScript, what keyword declares a constant?
#Card
const
```

**Avoid vague questions**

```
What is const?
#Card
(Could mean many things)
```

### Add Examples

```
What is a metaphor?
#Card
A figure of speech comparing two unlike things.
Example: "Time is money"
```

---

## Practice Exercise

Create a mini study deck:

1. Create a new page "My First Deck"
2. Add these cards:

```
What is the largest planet in our solar system?
#Card
Jupiter

The Great Wall of China is in {{cloze Asia}}.
#Card

What does CPU stand for?
#Card
Central Processing Unit
```

3. Study your cards using `g f` or the sidebar icon
4. Review again tomorrow

---

## Tips

### Daily Review Habit

Review due cards every day, even just 5 minutes.

### Make Cards From Notes

Turn important concepts into cards as you learn:

```
Notes about photosynthesis...
- Key point: uses sunlight
- Produces: oxygen and glucose

Photosynthesis uses {{cloze sunlight}} to produce oxygen and glucose.
#Card
```

### Use Images

```
What bird is this?
#Card
![robin](image.jpg)
Robin
```

### Review Hard Cards More

If a card keeps failing, rewrite it simpler or break into smaller cards.

---

## Settings

Flashcards have a single setting: **Settings -> Features -> Flashcards**, a toggle that
enables or disables the feature for the current graph.

> **Removed:** A per-deck settings table (new cards/day, reviews/day, card order). No such
> scheduling settings exist; FSRS schedules every card from its own review history.

---

## What's Next?

- **[Create Flashcard Deck](../../how-to/content/create-flashcard-deck.md)** - Advanced deck organization
- **[Flashcard System Reference](../../reference/features/flashcards.md)** - All settings and options
- **[Semantic Search](../getting-started/semantic-search.md)** - Find related content

---

## Findings: New Capabilities

- Cloze cues are supported with `\\` inside `{{cloze ...}}`, for example `{{cloze answer \\ cue}}`.
- Cloze review uses a two-step reveal flow: `Show clozes` -> `Show answers`.

---

## Summary

- Created basic flashcards with `#Card`
- Used cloze deletions with `{{cloze ...}}`
- Studied and rated cards
- Understood the FSRS algorithm
- Organized cards with Cards queries
- Tracked due cards with the sidebar badge and the study-view counter

---

_Last updated: 2026-09-09_
