---
title: 'Create a Flashcard Deck'
description: 'How to organize flashcards into focused study decks'
status: stable
lastVerified: '2026-09-18'
verifiedAgainst: '5824434a74'
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

> **Last revised:** 2026-09-18 (untitled-deck naming re-checked after #13258
> fixed it; every source line reference on this page relocated to
> `5824434a74`)

> How to organize flashcards into focused study decks for efficient learning.

Logseq uses FSRS (Free Spaced Repetition Scheduler) for flashcards. In DB graphs, a card is a block tagged with the built-in `#Card` tag. A deck is a block tagged `#Cards`, and its query selects which cards it contains.

## Findings: New Capabilities

- Flashcards can be opened with both `g f` and `t c`, and from the command palette as **Toggle flashcards**.
- The mobile app has a **Cards** tab, with its own deck selector in the header.
- The `+` button in the flashcard viewer creates a `Cards` block on today's journal page.
- Every `Cards` block has a **Practice** button that opens review for that deck alone ([block.cljs:2576-2586](../../../../../src/main/frontend/components/block.cljs#L2576-L2586)).

## Creating Cards

### Tag Blocks as Cards

Add the `#Card` tag to a block to make it a flashcard. The block is the question. Its child blocks are the answer, which stays hidden until you reveal it:

```markdown
- What is H2O? #Card
  - Water
- What is NaCl? #Card
  - Sodium Chloride (Salt)
```

To tag several blocks at once, select them, right-click, and choose **Make a Flashcard**. Only the top-level blocks of the selection are tagged ([fsrs.cljs:404-426](../../../../../src/main/frontend/extensions/fsrs.cljs#L404-L426)). No command-palette command does this.

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
5. Give the block a title. The deck dropdown only lists `Cards` blocks with a title, or whose query has a title ([fsrs.cljs:301-322](../../../../../src/main/frontend/extensions/fsrs.cljs#L301-L322)).

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

> **Removed:** The old page-based, tag-based and namespace-based deck recipes built on a `{{cards [[Some Page]]}}` macro. That macro is not registered in DB graphs. Only `{{cloze}}` is registered, and `{{cards ...}}` remains a commented-out TODO ([fsrs.cljs:479-484](../../../../../src/main/frontend/extensions/fsrs.cljs#L479-L484)). Use `Cards` objects instead. Importing a file graph turns each `{{cards <query>}}` block into a `Cards` block with that query ([exporter.cljs:1436-1448](../../../../../deps/graph-parser/src/logseq/graph_parser/exporter.cljs#L1436-L1448)).

### Replacing `{{cards [[Some Page]]}}` from a file graph

The file-graph macro has a direct DB equivalent, and the importer performs
exactly this conversion ([exporter.cljs:1436-1448](../../../../../deps/graph-parser/src/logseq/graph_parser/exporter.cljs#L1436-L1448)).
To do it by hand, anywhere in the graph:

1. Add a `#Cards` tag to a block.
2. Set its query to `[[Some Page]]`.
3. Open **Flashcards** and click **All cards** at the top to pick the new deck.

Choose the query form by what you mean by "the page's cards":

| Goal                                                         | Query                  |
| ------------------------------------------------------------ | ---------------------- |
| Cards **written on** that page, at any depth                  | `(page "some page")`   |
| Cards **referencing** that page, wherever they live (the file-graph macro's meaning) | `[[Some Page]]` |

`(page …)` compiles to `[?b :block/page ?bp] [?bp :block/name ?page-name]`
([query_dsl.cljs:460-465](../../../../../src/main/frontend/worker/query_dsl.cljs#L460-L465),
[rules.cljc:89-92](../../../../../deps/db/src/logseq/db/frontend/rules.cljc#L89-L92)),
so a book page whose cards sit under a `Questions` heading is covered by
`(page "book title")` — nesting does not matter. The DSL has no
descendant-of-block filter, so a single heading within a page cannot be
targeted; tag those cards if you need to separate them from others on the same
page.

**Verified** against `be800f1711` by running the query the worker assembles
(the `#Card` tag clause, the due-or-never-reviewed `or-join`, and the parsed
deck query) over a probe page with a top-level card, a card nested under a
`Questions` heading, and a card on an unrelated page. Both cards on the page
were returned, including the nested one; the unrelated card was excluded.

> **Note:** on builds older than `5824434a74`, an untitled deck shows a raw
> UUID instead of the tag or page name. See
> [Deck names show a UUID](#deck-names-show-a-uuid).

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

> **Removed:** The legacy file-based `:card-last-interval`, `:card-repeats`, `:card-ease-factor`, `:card-next-schedule`, and `:card-last-score` fields do not apply to DB flashcards. The file-graph importer drops them, along with `:card-last-reviewed` ([exporter.cljs:1069-1070](../../../../../deps/graph-parser/src/logseq/graph_parser/exporter.cljs#L1069-L1070)).

## Configuration

### Enable Flashcards

Settings → Features → Flashcards (on by default)

> **Removed:** Legacy `:srs/learning-fraction` and `:srs/initial-interval` are listed as not used in DB graphs ([config.cljs:108-130](../../../../../deps/common/src/logseq/common/config.cljs#L108-L130)). This page previously also listed `:srs/new-cards-limit` and `:srs/review-limit`, but neither key appears anywhere in the source. There are no per-deck limits.

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

### Deck names show a UUID

**Fixed in `5824434a74`** (commit `6007c8e6b5`, PR #13258). On that build and
later, an untitled deck is listed by its query with the names resolved —
`[[LLM]]`, or `(tags [[AI]])`. The brackets stay; only the UUID is replaced.

On an older build, a deck may appear in the dropdown as
`[[6aa9eab5-798f-43f3-bc8c-c65384de1b8f]]` or
`(tags [[6aa9e51c-4a8c-4215-a234-6c77e4eb720f]])` rather than by the page or tag
it selects. This happens when the `Cards` block itself has no title: the
selector falls back to the title of the block's query, and a DB graph stores a
reference in a query as `[[<uuid>]]`, not as the node's name.

**Workaround on an older build:** give the `Cards` block a title. A non-blank
title is used as-is and the fallback never runs.

The fix passes the query title through `recur-replace-uuid-in-block-title`
before listing the deck
([fsrs.cljs:311-316](../../../../../src/main/frontend/extensions/fsrs.cljs#L311-L316)),
which substitutes each reference's page name
([content.cljs:246-265](../../../../../deps/db/src/logseq/db/frontend/content.cljs#L246-L265)).
The dropdown itself still renders the title as plain text without resolving
references ([fsrs.cljs:352-354](../../../../../src/main/frontend/extensions/fsrs.cljs#L352-L354)) —
the resolution now happens before the list is built.

**Verified** against `be800f1711` by reading two untitled `Cards` blocks out of
a live graph: their queries stored `[[<uuid>]]` and `(tags [[<uuid>]])`, and the
UUIDs resolved to a page named `LLM` and a tag named `AI`. Re-checked against
`5824434a74` by reading the fix and the e2e case upstream added with it
([flashcards_basic_test.clj:87-113](../../../../../clj-e2e/test/logseq/e2e/flashcards_basic_test.clj#L87-L113)),
which asserts the option reads `[[fc-tag-a]]` and that no option matches a UUID.

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

_Last updated: 2026-09-15_
