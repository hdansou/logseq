---
title: 'Flashcards Basics'
description: "Learn to create and study flashcards with Logseq's spaced repetition system"
type: tutorial
status: stable
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
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

> **Last revised:** 2026-09-11

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

Spaced repetition is a learning technique that shows you information just before you would forget it. Logseq uses **FSRS** (Free Spaced Repetition Scheduler). For each card, FSRS:

- Tracks how stable the memory is and how difficult the card is
- Uses your rating to pick the next review time
- Brings a card back sooner after you forget it

Logseq uses the FSRS library's default parameters, and there are no scheduling settings to tune. See [Scheduling parameters](../../reference/features/flashcards.md#scheduling-parameters).

> **Note:** Flashcards work only in **DB graphs**. They are on by default. If yours are off, turn on **Settings → Features → Flashcards**.

---

## Step 1: Create Your First Flashcard

### Basic Card (Front/Back)

Tag a block with `#Card` to turn it into a flashcard. The block is the **front** (question). Its **child blocks** are the **back** (answer):

```
- What is the capital of France? #Card
  - Paris
```

Put the answer in a child block. Text on the card block itself stays visible before you reveal the answer.

You can also select one or more blocks, right-click, and choose **Make a Flashcard**. Only the top-level blocks of the selection become cards.

### Try It Now

1. Create a new block
2. Type: `What is 2 + 2?`
3. Type `#Card` and pick the **Card** tag
4. Press `Enter` to start a new block, then `Tab` to indent it under the question
5. Type: `4`

---

## Step 2: Card Types

### Standard Card

```
- Question here #Card
  - Answer here
```

### Cloze Deletion

Hide part of the text with the `/Cloze` slash command, which inserts `{{cloze }}`, or type `{{cloze ...}}` yourself:

```
- The {{cloze capital}} of France is Paris. #Card
```

When studying, you'll see: "The **[...]** of France is Paris."

You can add an optional cue by putting `\\` between the hidden answer and the cue:

```
- The {{cloze mitochondria \\ powerhouse of the cell}} generates ATP. #Card
```

Before the reveal, study mode shows the cue in place of the answer: `(powerhouse of the cell)`.

### Multiple Clozes

A single card can hold several cloze deletions:

```
- {{cloze Photosynthesis}} converts {{cloze light energy}} into {{cloze chemical energy}}. #Card
```

They are all revealed together, as one card. During review you can also click a single cloze to reveal or hide just that one.

> **Removed:** Numbered clozes (`{{c1 ...}}`, `{{c2 ...}}`) that split one block into
> several cards, and reversed cards (`#card #reversed`). Neither exists in the current
> source: cloze detection is a plain `{{cloze ` substring test, and there is no
> `reversed` tag or generated inverse card.

---

## Step 3: Study Your Cards

### Access Flashcards

**Method 1: Sidebar**

- Click **Flashcards** in the left sidebar

**Method 2: Keyboard**

- Press `g` then `f`, or `t` then `c`. This toggles the flashcards dialog.

**Method 3: Command palette**

- Run **Toggle flashcards**

**Method 4: Mobile app**

- Open the **Cards** tab

> **Removed:** The `t f` shortcut, a `/Flashcards` slash command, and
> `Cmd/Ctrl+Shift+C` for creating a card. The flashcard shortcut is `g f` / `t c`;
> `Cmd/Ctrl+Shift+C` is bound to Copy text. The only flashcard-related slash command
> is `/Cloze`.

### Study Session

1. A card appears, showing the front (question) and a breadcrumb to where it lives
2. Think of the answer
3. Click **Show answers**, or press `s`. For a cloze card, the first press is **Show clozes** and the second is **Show answers**.
4. Rate how well you remembered:
   - **Again** (press `1`): you got it wrong
   - **Hard** (press `2`): correct, but you were not confident or took too long
   - **Good** (press `3`): correct, with some mental effort
   - **Easy** (press `4`): correct, quick and effortless

The number keys and `s` work while the dialog is open and you are not editing a block. `s` only moves the reveal forward. Once the answer is shown, the rating buttons replace the button.

### Review Schedule

FSRS uses your rating to calculate when to show the card again. On desktop, each rating button shows the next review time it would set, so you can see how your choice affects scheduling. The mobile view leaves these times out.

---

## Step 4: Organize With Cards Queries

### Using the Cards Selector

In the study view, a dropdown lets you switch between **All cards** and any Cards queries you have created. On mobile, the selector is in the header.

### Creating a Cards Query

1. Click the **+** button ("Add new query") next to the selector.
2. Logseq closes the dialog, adds an empty block tagged `#Cards` at the end of today's journal page, and takes you to it.
3. Give the block a simple query, such as `[[chemistry]]`, to limit the set to cards that reference or are tagged with `chemistry`. While the query is blank, the set includes every due card.
4. Give the block a title if you want it listed by name. The selector only lists Cards blocks that have a title, or whose query has a title.
5. Click **Practice** on the Cards block to study just that set.

> **Unverified:** the exact gesture for typing the query into a new, empty Cards block was not checked in a running app. What the source shows: Logseq gives the block a query value block, and the due-card lookup uses that value block's title as a simple query.

### Tracking What Is Due

- The study view shows a position counter next to the selector, in the form
  `current/total` for the cards due in the selected set.
- The **Flashcards** entry in the left sidebar carries a badge with the total number of
  cards due now across the graph. It is hidden when nothing is due. It refreshes when you open the
  dialog, when you close it, and every hour.
- When the selected set has no due cards, the view shows "Time to create a card!". After you
  finish the set it shows a "you've reviewed all the cards for this query" message.

> **Removed:** Per-card status labels (New / Learning / Review / Due) and a statistics
> panel with review forecast, retention rate and streak. The FSRS state is stored on the
> block as `:logseq.property.fsrs/state` and `:logseq.property.fsrs/due`, but no such
> status labels or statistics view are surfaced in the UI.

---

## Step 5: Write Better Cards

### Good Card Principles

**One idea per card**

```
- What is the atomic number of Carbon? #Card
  - 6
```

**Avoid too much info**

```
- Describe Carbon including atomic number, mass, and properties #Card
  - (Long answer that's hard to review)
```

### Use Context

**Specific question**

```
- In JavaScript, what keyword declares a constant? #Card
  - const
```

**Avoid vague questions**

```
- What is const? #Card
  - (Could mean many things)
```

### Add Examples

```
- What is a metaphor? #Card
  - A figure of speech comparing two unlike things.
  - Example: "Time is money"
```

---

## Practice Exercise

Create a mini study deck:

1. Create a new page "My First Deck"
2. Add these cards:

```
- What is the largest planet in our solar system? #Card
  - Jupiter
- The Great Wall of China is in {{cloze Asia}}. #Card
- What does CPU stand for? #Card
  - Central Processing Unit
```

3. Study your cards with `g f` or **Flashcards** in the sidebar
4. Come back when the sidebar badge shows cards due again

---

## Tips

### Daily Review Habit

Review due cards every day, even if only for 5 minutes.

### Make Cards From Notes

Turn important concepts into cards as you learn:

```
- Notes about photosynthesis
  - Key point: uses sunlight
  - Produces: oxygen and glucose
- Photosynthesis uses {{cloze sunlight}} to produce oxygen and glucose. #Card
```

### Use Images

Put the image on the card block and the answer in a child block:

```
- What bird is this? #Card
  - Robin
```

Then drag the bird photo into the card block.

### Rewrite Cards You Keep Failing

If a card keeps failing, simplify it or split it into smaller cards.

---

## Settings

Flashcards have a single setting: **Settings → Features → Flashcards**. It is a toggle
that writes `:feature/enable-flashcards?` to the current graph's config. It is on unless set to `false`.

> **Removed:** A per-deck settings table (new cards/day, reviews/day, card order). No such
> scheduling settings exist; FSRS schedules every card from its own review history.

---

## What's Next?

- **[Create Flashcard Deck](../../how-to/content/create-flashcard-deck.md)**: organize cards into decks
- **[Flashcard System Reference](../../reference/features/flashcards.md)**: all properties, shortcuts and source locations
- **[Semantic Search](../getting-started/semantic-search.md)**: find related content

---

## Findings: New Capabilities

- Cloze cues are supported with `\\` inside `{{cloze ...}}`, for example `{{cloze answer \\ cue}}`.
- Cloze review uses a two-step reveal flow: `Show clozes` -> `Show answers`.
- A Cards block has a **Practice** button that opens review for just that set.

---

## Summary

- Created basic flashcards with `#Card`, answers in child blocks
- Used cloze deletions with `{{cloze ...}}`
- Studied and rated cards
- Learned how FSRS schedules reviews
- Organized cards with Cards queries
- Tracked due cards with the sidebar badge and the study-view counter

---

_Last updated: 2026-09-11_
