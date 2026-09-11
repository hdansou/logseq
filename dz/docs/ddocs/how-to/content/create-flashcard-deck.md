---
title: 'Create a Flashcard Deck'
description: 'How to organize flashcards into focused study decks'
status: stable
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
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

> **Last revised:** 2026-09-11 (verified against `be800f1711`)

> How to organize flashcards into focused study decks for efficient learning.

Logseq uses FSRS (Free Spaced Repetition Scheduler) for flashcards. In DB graphs, a card is a block tagged with the built-in `#Card` tag. A deck is a block tagged `#Cards`, and its query selects which cards it contains.

## Findings: New Capabilities

- Flashcards can be opened with both `g f` and `t c`, and from the command palette as **Toggle flashcards**.
- The mobile app has a **Cards** tab, with its own deck selector in the header.
- The `+` button in the flashcard viewer creates a `Cards` block on today's journal page.
- Every `Cards` block has a **Practice** button that opens review for that deck alone ([block.cljs:2539-2549](../../../../../src/main/frontend/components/block.cljs#L2539-L2549)).

## Creating Cards

### Tag Blocks as Cards

Add the `#Card` tag to a block to make it a flashcard. The block is the question. Its child blocks are the answer, which stays hidden until you reveal it:

```markdown
- What is H2O? #Card
  - Water
- What is NaCl? #Card
  - Sodium Chloride (Salt)
```

To tag several blocks at once, select them, right-click, and choose **Make a Flashcard**. Only the top-level blocks of the selection are tagged ([fsrs.cljs:402-424](../../../../../src/main/frontend/extensions/fsrs.cljs#L402-L424)). No command-palette command does this.

### Cloze Deletions

Use the `{{cloze ...}}` macro to create fill-in-the-blank cards:

```markdown
- The capital of France is {{cloze Paris}} #Card
```

You can add a cue after a double-backslash separator: `{{cloze answer \\ hint}}`.

Insert a cloze with the `/Cloze` slash command.

---

## Opening the Flashcard Viewer

Open the flashcard review dialog in any of these ways:

1. Click **Flashcards** in the left sidebar. The entry appears only while flashcards are enabled, and you can hide it from the sidebar navigation.
2. Press `g f` or `t c`
3. Run **Toggle flashcards** from the command palette
4. Click **Practice** on a `Cards` block to review only that deck
5. On mobile, open the **Cards** tab

The viewer shows due cards. Press `s` to reveal clozes and then answers. Rate recall with **Again** (1), **Hard** (2), **Good** (3), or **Easy** (4) ([shortcut/config.cljs:82-95](../../../../../src/main/frontend/modules/shortcut/config.cljs#L82-L95)).

> **Note:** Flashcards are on by default. If the sidebar entry is missing, check **Settings > Features > Flashcards** (`:feature/enable-flashcards?` in config; only an explicit `false` turns it off, [state.cljs:684-688](../../../../../src/main/frontend/state.cljs#L684-L688)).

---

## Organizing Decks with Cards Objects

Logseq uses `Cards` objects (blocks tagged with the built-in `#Cards` tag, which extends `#Query`) to define subsets of your cards. Each `Cards` object has a query. That query is combined with "is a due `#Card`", so it only narrows the set down ([flashcard.cljs:11-38](../../../../../src/main/frontend/worker/handler/flashcard.cljs#L11-L38)).

### Create a Cards Object

1. Open the flashcard viewer (`t c`)
2. Click the **+** button ("Add new query") next to the deck dropdown
3. Logseq closes the viewer, creates an empty `Cards` block at the end of today's journal page, and takes you to it
4. Enter a simple query in the block's query, for example `[[chemistry]]` to select cards that reference or are tagged with the chemistry page. Use the same form you would type after `/Query`. While the query is blank, the deck contains every due card.
5. Give the block a title. The deck dropdown only lists `Cards` blocks with a title, or whose query has a title ([fsrs.cljs:300-321](../../../../../src/main/frontend/extensions/fsrs.cljs#L300-L321)).

> **Unverified:** the exact gesture for entering the query on the new, empty block was not checked in a running app. What the source shows: Logseq gives the block a query value block ([pipeline.cljs:340-371](../../../../../src/main/frontend/worker/pipeline.cljs#L340-L371)), and the worker reads that value block's title as the deck query.

### Switch Between Decks

In the flashcard viewer, use the dropdown at the top to switch between:

- **All cards**, which reviews every due `#Card` in the graph
- Any titled `Cards` objects you have created

On mobile, the deck selector is in the header of the Cards tab.

### Example: Tag-Based Decks

Create cards with topic tags, then create a `Cards` object with a query for each topic:

```markdown
- What is H2O? #Card #chemistry
  - Water
- What is NaCl? #Card #chemistry
  - Sodium Chloride (Salt)
- When was World War II? #Card #history
  - 1939-1945
```

Then create a `Cards` object with the query `[[chemistry]]` to study only chemistry cards. Tags count as references, so `[[chemistry]]` matches blocks tagged `#chemistry` ([pipeline.cljs:118](../../../../../deps/outliner/src/logseq/outliner/pipeline.cljs#L118), [rules.cljc:31-36](../../../../../deps/db/src/logseq/db/frontend/rules.cljc#L31-L36)).

> **Removed:** The old page-based, tag-based and namespace-based deck recipes built on a `{{cards [[Some Page]]}}` macro. That macro is not registered in DB graphs. Only `{{cloze}}` is registered, and `{{cards ...}}` remains a commented-out TODO ([fsrs.cljs:477-482](../../../../../src/main/frontend/extensions/fsrs.cljs#L477-L482)). Use `Cards` objects instead. Importing a file graph turns each `{{cards <query>}}` block into a `Cards` block with that query ([exporter.cljs:1415-1427](../../../../../deps/graph-parser/src/logseq/graph_parser/exporter.cljs#L1415-L1427)).

---

## Property-Based Organization

### Add Properties to Cards

Typing `topic:: biology` into a DB-graph block does not create a property. It stays plain text, and on a child block it would even show up as part of the answer. To add a real property to a card:

1. Click the card block to edit it
2. Run `/Add property` (or press `mod+p` on macOS, `ctrl+alt+p` elsewhere)
3. Choose or create `topic` and set its value, for example `biology`

A deck can then select on the property with a query such as `(property topic biology)`. The `property` query rule matches user properties, which are public unless marked otherwise ([rules.cljc:188-194](../../../../../deps/db/src/logseq/db/frontend/rules.cljc#L188-L194)).

---

## Import Cards

### From Existing Notes

Convert existing notes into flashcards:

1. Find key concepts in your notes
2. Add the `#Card` tag to those blocks
3. Put the answer in indented child blocks

### Bulk Creation

Select multiple blocks, right-click, and choose **Make a Flashcard**.

---

## FSRS Scheduling

Logseq uses the FSRS algorithm to schedule reviews. Each card block stores its scheduling state in two built-in properties:

- `logseq.property.fsrs/due`: the next review time (stored as a timestamp)
- `logseq.property.fsrs/state`: the FSRS card map (stability, difficulty, reps, lapses, state, last review), plus the last rating

Logseq updates these properties when you rate a card during review. You do not need to edit them. A card that has never been rated has no `due` property yet and counts as due.

> **Removed:** The legacy file-based `:card-last-interval`, `:card-repeats`, `:card-ease-factor`, `:card-next-schedule`, and `:card-last-score` fields do not apply to DB flashcards. The file-graph importer drops them, along with `:card-last-reviewed` ([exporter.cljs:1048-1049](../../../../../deps/graph-parser/src/logseq/graph_parser/exporter.cljs#L1048-L1049)).

## Configuration

### Enable Flashcards

Settings → Features → Flashcards (on by default)

> **Removed:** Legacy `:srs/learning-fraction` and `:srs/initial-interval` are listed as not used in DB graphs ([config.cljs:105-127](../../../../../deps/common/src/logseq/common/config.cljs#L105-L127)). This page previously also listed `:srs/new-cards-limit` and `:srs/review-limit`, but neither key appears anywhere in the source. There are no per-deck limits.

---

## Deck Maintenance

### Review Deck Health

Check for:

- Cards that no `Cards` query covers (they still appear under **All cards**)
- Duplicate cards
- Outdated content

### Split Large Decks

If you have many cards on a single topic:

1. Add more specific tags to cards (e.g., `#organic-chemistry`, `#inorganic-chemistry`)
2. Create separate `Cards` objects with queries for each sub-topic
3. Rotate focus between sub-decks

---

## Deck Strategies

The strategies below are study advice. Logseq does not enforce any of them.

### Start Small

For new decks:

- Start with about 20 cards
- Add more once those feel familiar
- This keeps the daily review load manageable

### Topic Rotation

For multiple decks, review a different deck each day:

- Day 1: Spanish
- Day 2: History
- Day 3: Science
- Repeat

### Focused Sessions

- Keep sessions short
- Review one deck at a time
- Stop when your attention wanders

---

## Troubleshooting

### No Cards Appearing

1. Verify blocks are tagged with the built-in `#Card` tag, not a custom tag
2. Check that flashcards are not disabled in Settings
3. If a `Cards` deck is selected, verify its query, or switch to **All cards**
4. Remember that only cards due now are shown

### Wrong Cards Appearing

1. Review the query in your `Cards` object
2. Check for unintended tag references on card blocks
3. Switch to **All cards** to see whether the problem comes from the query

### Answer Shown Before Reveal

The answer is on the card block itself. Move it into a child block.

### Cards Not Scheduled

1. Rate the card at least once. Until then it has no `due` property.
2. Check the `logseq.property.fsrs/due` property on the block
3. Make sure the flashcards feature is enabled

---

## See Also

- **[Flashcards Basics](../../tutorials/knowledge-management/flashcards-basics.md)**
- **[Flashcard System](../../reference/features/flashcards.md)**

---

_Last updated: 2026-09-11_
