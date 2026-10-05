---
title: 'Flashcard System'
description: "Complete reference for Logseq's flashcard and spaced repetition system"
type: reference
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
  howTo:
    - '[Create Flashcard Deck](../../how-to/content/create-flashcard-deck.md)'
---

# Flashcard System

> **Last revised:** 2026-09-18 (untitled-deck naming re-checked after #13258
> fixed it; every source line reference on this page relocated to
> `5824434a74`)

> Complete reference for Logseq's flashcard and spaced repetition functionality.

All paths below are at `5824434a74`. Paths under `src/` and `deps/` are Logseq source; paths under `cljc-fsrs/` are the FSRS library at the git SHA Logseq pins.

## Overview

Flashcards are scheduled with FSRS (Free Spaced Repetition Scheduler). The namespace docstring says it "only works in db-based graphs" (`src/main/frontend/extensions/fsrs.cljs:2`). No other scheduler exists in the source.

| Feature       | Value                                                    |
| ------------- | -------------------------------------------------------- |
| Algorithm     | FSRS, via `open-spaced-repetition/cljc-fsrs`             |
| Rating System | 4 buttons: Again, Hard, Good, Easy                       |
| Graph Type    | DB graphs only                                           |
| Default       | Enabled unless `:feature/enable-flashcards?` is `false`  |

---

## Card Creation

### Tag Method

Tag a block with the built-in **Card** tag (`:logseq.class/Card`, titled "Card", `deps/db/src/logseq/db/frontend/class.cljs:58-60`). The block is the front of the card. Its **child blocks** are the back:

```
- What is the capital of France? #Card
  - Paris
```

Before the answer is revealed, the review dialog renders the card block with `:hide-children? true`. After it is revealed, it renders the block with its children, even if the block is collapsed (`src/main/frontend/extensions/fsrs.cljs:242-250`). Text on the card block itself is always visible, so the answer must go in a child block. A breadcrumb to the card is shown above it (`fsrs.cljs:240-241`).

Blocks tagged with a tag that extends Card are also listed for review. The due-card query collects Card plus every class that extends it (`src/main/frontend/worker/handler/flashcard.cljs:21-23`, `deps/db/src/logseq/db/frontend/class.cljs:171-181`).

> **Rating works for these cards too** (since `928630b2a9`; earlier builds crashed on Show answer and wrote nothing). `card-block?` accepts Card or any class that reaches Card through `:logseq.property.class/extends`, at any depth (`src/main/frontend/extensions/fsrs.cljs:53-82`), and `repeat-card!` pulls `extends` recursively so the FSRS state is written. The statistics count and `cards?` queries use the same Card-plus-descendants set. Verified in source; not re-run in the app.

### Context Menu ("Make a Flashcard")

When flashcards are enabled, the block right-click menu and the multi-block selection menu both offer **Make a Flashcard** (`src/resources/dicts/en.edn:442`). It adds the Card tag to the selected blocks.

- Single block: `src/main/frontend/components/content.cljs:341-345`
- Selection: `src/main/frontend/components/content.cljs:134-138`
- Handler: `batch-make-cards!` (`src/main/frontend/extensions/fsrs.cljs:414-426`). Only the **top-level** blocks of a selection are tagged, and property blocks are skipped (`fsrs.cljs:404-412`).

No command-palette command or shortcut calls `batch-make-cards!`. The two context-menu entries are its only callers in `src/main`.

### Cloze Deletions

Hide parts of the text with the `cloze` macro. The **Cloze** slash command inserts `{{cloze }}` with the cursor inside it (`fsrs.cljs:26-31`, label `en.edn:521`):

```
- The capital of France is {{cloze Paris}}. #Card
```

A hidden cloze renders as `[...]`, and a revealed one as `[answer]` (`cloze-macro-show`, `fsrs.cljs:442-473`).

### Cloze with Cues

Put a cue after a `\\` separator. A hidden cloze with a cue renders as `(cue)` instead of `[...]` (`fsrs.cljs:470-473`):

```
- The capital of {{cloze France \\ country}} is {{cloze Paris \\ city}}. #Card
```

Surrounding whitespace is trimmed. If the body has more than one `\\`, only the text after the **last** one is the cue (`cloze-parse`, `fsrs.cljs:432-440`).

A card can contain several `{{cloze ...}}` macros. They all belong to one card and are revealed together by the **Show clozes** step. You can also toggle a single cloze by clicking it, or by focusing it and pressing Enter or Space (`fsrs.cljs:442-464`).

A block counts as a cloze card when its title contains the substring `{{cloze ` (`has-cloze?`, `fsrs.cljs:151-153`). Numbered clozes (`{{c1::...}}`) and reversed cards do not exist.

---

## Card Properties

### Database Graph Properties

| Property                      | Type       | Title   | Description                                         |
| ----------------------------- | ---------- | ------- | --------------------------------------------------- |
| `:logseq.property.fsrs/state` | `:map`     | "State" | FSRS card map minus `:due`, plus the last rating    |
| `:logseq.property.fsrs/due`   | `:datetime` | "Due"  | Next review time                                    |

Both are `:public? false` and `:hide? false` (`deps/db/src/logseq/db/frontend/property.cljs:610-619`). The schema for `state` carries the comment `TODO: show for debug now, hide it later`.

After each rating, `repeat-card!` removes `:due` from the new card map, adds `:logseq/last-rating`, and writes the map to `state`. It writes `:due` to `due` (`fsrs.cljs:70-87`). `:last-repeat` and `:due` are stored as epoch milliseconds (`fsrs.cljs:36-41`).

The keys in the `state` map come from the library's card map (`cljc-fsrs/src/open_spaced_repetition/cljc_fsrs/card.cljc:38-48`) plus Logseq's rating:

- `:stability`: memory stability, `0` for a new card
- `:difficulty`: `0` for a new card. After a review the library clamps it to 1–10 (`cljc_fsrs/parameters.cljc:100`)
- `:elapsed-days`, `:scheduled-days`
- `:reps`: review count
- `:lapses`: times forgotten
- `:state`: `:new`, `:learning`, `:review` or `:relearning`
- `:last-repeat`: time of the last review
- `:logseq/last-rating`: `:again`, `:hard`, `:good` or `:easy` (`fsrs.cljs:80-82`)

A Card block with no `state` or `due` yet is treated as a new card whose due time is the block's creation time (`fsrs.cljs:54-68`). The due-card query also counts blocks with no `due` property as due (`flashcard.cljs:28-32`).

---

## FSRS Algorithm

### Overview

Logseq uses the [open-spaced-repetition/cljc-fsrs](https://github.com/open-spaced-repetition/cljc-fsrs) library (`fsrs.cljs:21`). It is pinned by git SHA `eeef3520df664e51c3d0ba2031ec2ba071635442` in `deps.edn:53-54`, not by version.

### Rating Buttons

| Button    | Key | Description shown in the info popover (`en.edn:706-713`)                                     |
| --------- | --- | -------------------------------------------------------------------------------------------- |
| **Again** | `1` | We got the answer wrong. Automatically means that we have forgotten the card. This is a lapse in memory. |
| **Hard**  | `2` | The answer was correct but we were not confident about it or took too long to recall.         |
| **Good**  | `3` | The answer was correct but we took some mental effort to recall it.                           |
| **Easy**  | `4` | The answer was correct and we were confident and quick in our recall without mental effort.   |

On desktop, each button shows the next due time that rating would produce, computed with the library before you click (`fsrs.cljs:187-200`, `:148-149`, preview call at `:195`). The mobile view hides these times (`fsrs.cljs:199`). An **info** icon next to the buttons opens the descriptions above. It is not shown on mobile (`fsrs.cljs:206-221`).

### Scheduling parameters

Logseq does not configure FSRS. It calls `fsrs.core/repeat-card!` with two arguments (`fsrs.cljs:78`, `:195`). The library's two-argument arity applies its `default-params` (`cljc_fsrs/core.cljc:25-30`). Those defaults include `:request-retention 0.9` and `:maximum-interval 36500` days (`cljc_fsrs/parameters.cljc:60`, `:65`). Logseq has no setting that changes them.

---

## Configuration

### Enable Flashcards

**Settings → Features → Flashcards** is a toggle (`src/main/frontend/components/settings.cljs:800-803`, rendered in the Features panel at `:1153`, `:1172`; label `en.edn:1728`). It writes `:feature/enable-flashcards?` to the graph config (`settings.cljs:643-648`).

Flashcards are **on by default**. `state/enable-flashcards?` returns true unless the key is explicitly `false` (`src/main/frontend/state.cljs:684-688`).

### config.edn Options

```clojure
{;; Enable/disable flashcards (default: enabled)
 :feature/enable-flashcards? true}
```

`:srs/learning-fraction` and `:srs/initial-interval` are in the file-only config list, marked "is not used in DB graphs" (`deps/common/src/logseq/common/config.cljs:108-130`, keys at `:126-127`). There are no other flashcard settings: no new-card limit, review limit, desired retention or card order.

---

## Opening the Review Dialog

| Where | How | Source |
| ----- | --- | ------ |
| Left sidebar | **Flashcards** entry, shown when flashcards are enabled. It is one of the default sidebar navigation items and can be unchecked | `src/main/frontend/components/left_sidebar.cljs:234-235`, `:299-311` |
| Keyboard | `g f` or `t c` (`:go/flashcards`, toggles the dialog) | `src/main/frontend/modules/shortcut/config.cljs:414-415`, `src/main/frontend/handler/ui.cljs:298-302` |
| Command palette | **Toggle flashcards** | `en.edn:379`. Registered because `:go/flashcards` is in `:shortcut.handler/global-non-editing-only` (`shortcut/config.cljs:703-708`, `src/main/frontend/handler/command_palette.cljs:23-27`) |
| A `Cards` block | **Practice** button on the block opens the dialog for that set | `src/main/frontend/components/block.cljs:2576-2586`, `en.edn:133-134` |
| Mobile app | **Cards** tab, present only when flashcards are enabled | `src/main/mobile/tabs.cljs:17-20`, `:36-40`; `src/main/mobile/components/app.cljs:253-262`; `en.edn:1048` |

The desktop dialog is opened by the `:modal/show-cards` event (`src/main/frontend/handler/events/ui.cljs:122-126`).

---

## Keyboard Shortcuts

### Review Session

These are the `:cards/*` shortcuts (`shortcut/config.cljs:82-95`). They are active only while the review view is mounted (`fsrs.cljs:298-300`) and not while editing (`shortcut/config.cljs:606-608`).

| Key | Action |
| --- | ------ |
| `s` | Advance the reveal: **Show clozes** (cloze cards only), then **Show answers** |
| `1` | Again  |
| `2` | Hard   |
| `3` | Good   |
| `4` | Easy   |

`s` does not hide the answer again. The reveal button is rendered only while the next phase is `:show-cloze` or `:show-answer`. Once the answer is shown, the rating buttons replace it (`fsrs.cljs:155-164`, `:251-267`). Each shortcut clicks the matching button by id inside `#cards-modal` (`src/main/frontend/extensions/srs/handler.cljs`).

---

## Card Collections (`Cards`)

### Built-in Classes

#### Card Class

`:logseq.class/Card`. Schema properties: `:logseq.property.fsrs/state`, `:logseq.property.fsrs/due` (`class.cljs:58-60`).

#### Cards Class

`:logseq.class/Cards` extends `:logseq.class/Query` (`class.cljs:62-65`). When a block gets a tag that is an instance of Query, which includes Cards, an empty `:logseq.property/query` value block is attached to it (`src/main/frontend/worker/pipeline.cljs:340-371`).

### Creating Card Collections

In the desktop review dialog, the **+** button ("Add new query", `en.edn:698`) creates an empty block tagged Cards at the end of today's journal page. It then closes the dialog and navigates to that block (`fsrs.cljs:109-116`, `:355-367`).

The collection's filter is the **title of its `:logseq.property/query` block**. It is parsed as a simple query (DSL) and ANDed with "is a due Card" (`flashcard.cljs:14-20`, `:34-37`). A blank query selects every due card. Use the same query form you would enter after `/Query`, such as `[[chemistry]]`. Tags are part of `:block/refs` (`deps/outliner/src/logseq/outliner/pipeline.cljs:118`), so `[[chemistry]]` also matches blocks tagged `#chemistry` (`deps/db/src/logseq/db/frontend/rules.cljc:31-36`, `:80-82`).

### Selecting Card Sets

On desktop, a dropdown in the review dialog switches between **All cards** and every Cards block with a non-blank title, or whose query block has a title (`fsrs.cljs:301-322`, `:343-353`). Mobile puts the same selector in the header (`src/main/mobile/components/header.cljs:147-165`).

An untitled Cards block is listed by the text of its query, with references
resolved to names. The fallback reads the query block's title and passes it
through `recur-replace-uuid-in-block-title` (`fsrs.cljs:311-316`), which
rewrites each `[[<uuid>]]` to `[[<page name>]]` and each `#[[<uuid>]]` to
`#<name>` when the name has no space
(`deps/db/src/logseq/db/frontend/content.cljs:246-265`, `:199-206`). The
substitution has data to work with because the worker returns `:block/refs` as
maps carrying `:block/uuid` and `:block/title`
(`src/main/frontend/worker/plain_value.cljs:10-51`).

The **brackets are kept**: a deck selecting a page `LLM` is listed as `[[LLM]]`,
and one selecting a tag `AI` as `(tags [[AI]])`. The select item still renders
the title as plain text with no ref resolution (`fsrs.cljs:352-354`) — the
resolution happens before the list is built, not at render time.

> **Fixed in `5824434a74`** (commit `6007c8e6b5`, PR #13258). Before that
> commit the fallback substituted the query block's raw title, so a deck showed
> as `[[6aa9eab5-798f-43f3-bc8c-c65384de1b8f]]`. On a build older than that,
> give the Cards block a title: a non-blank title skips the fallback. Upstream
> covers the fix with an e2e case asserting the option reads `[[fc-tag-a]]` and
> that no option matches a UUID pattern
> (`clj-e2e/test/logseq/e2e/flashcards_basic_test.clj:87-113`).

### Query forms for a page's cards

`(page "some page")` selects cards **written on** that page at any depth
(`query_dsl.cljs:460-465`, `rules.cljc:89-92`), which is the replacement for a
file graph's `{{cards [[Some Page]]}}` when the cards live on the page itself.
`[[Some Page]]` selects cards **referencing** it, which is what the file-graph
macro matched. The DSL has no descendant-of-block filter, so a deck cannot be
narrowed to one heading within a page.

---

## Review Statistics

### What the UI shows

- **Progress counter** `current/total` next to the selector, for the due cards of the selected set (`fsrs.cljs:290`)
- **Sidebar badge** on the Flashcards entry: the number of due cards across the graph, hidden when zero (`left_sidebar.cljs:301-311`). The count is refreshed when you click the entry, when the review dialog closes, and every hour (`left_sidebar.cljs:308-309`, `fsrs.cljs:329`, `:387-402`). "Due" means due now, not due today (`flashcard.cljs:13`, `:30-31`).
- **Empty state**: "Time to create a card!" when the set has no due cards (`fsrs.cljs:377-381`, `en.edn:703-704`)
- **Finished**: "Congrats, you've reviewed all the cards for this query, see you next time! 💯" (`fsrs.cljs:383-384`, `en.edn:715`)

### Card States

FSRS card states are stored in the `state` map (see above). The UI does not show them.

| State          | Meaning (FSRS)        |
| -------------- | --------------------- |
| `:new`         | Never reviewed        |
| `:learning`    | Recently introduced   |
| `:review`      | Regular rotation      |
| `:relearning`  | Failed, re-learning   |

Detailed statistics exist only inside a `(comment ...)` form and are not exposed in the UI (`fsrs.cljs:486-558`). They cover true retention, passed and lapsed repeats, per-state counts, and today, 7-day and 30-day windows.

---

## Data Storage

- A block tagged with `:logseq.class/Card` is a flashcard.
- FSRS state is stored as a map in `:logseq.property.fsrs/state`.
- The due time is stored in `:logseq.property.fsrs/due`.
- Due cards are selected by a Datalog query in the DB worker (`:thread-api/get-fsrs-due-card-block-ids`, `flashcard.cljs:40-43`).

### Importing a file graph

The file-graph importer handles the old SRS syntax this way:

- A `#card` tag is always converted to a class (`deps/graph-parser/src/logseq/graph_parser/exporter.cljs:257-263`).
- The file-graph SRS properties `card-last-interval`, `card-repeats`, `card-last-reviewed`, `card-next-schedule`, `card-ease-factor` and `card-last-score` are dropped (`exporter.cljs:1069-1070`), so review history is not carried over.
- A block containing `{{cards <query>}}` becomes a Cards block with that query, and the macro is removed from its title (`exporter.cljs:1436-1448`).

---

## Integration

### Slash Commands

| Command  | Action                                   |
| -------- | ---------------------------------------- |
| `/Cloze` | Inserts `{{cloze }}`, cursor inside      |

`/Cloze` is the only flashcard-related slash command. There is no `/Card`, `/Cards` or `/Flashcards` command.

### Macros

`{{cloze ...}}` is the only registered flashcard macro (`component-macro/register`, `fsrs.cljs:482`).

> **Not implemented:** a `{{cards ...}}` query macro. It exists only as a commented-out `TODO` (`fsrs.cljs:477-480`). The importer turns old `{{cards}}` blocks into Cards blocks (see above).

### Plugin API

There is no flashcard read/write plugin API. The only flashcard-related field is the read-only `:enabled-flashcards` returned by `getUserConfigs` (`src/main/logseq/api/app.cljs:65`).

---

## Best Practices

General study advice. None of it is enforced by Logseq.

### Card Design

**Do:**

- One concept per card
- Clear, specific questions
- Put the answer in child blocks
- Use images for visual content

**Don't:**

- Cram multiple facts into one card
- Ask vague questions
- Put the answer on the card block itself, where it is visible before reveal

### Review Habits

- Review when the sidebar badge shows due cards
- Rate honestly
- Keep a consistent schedule

---

## Troubleshooting

### Cards Not Appearing

1. Check that the block is tagged with the built-in **Card** tag.
2. Check that flashcards are not disabled (Settings → Features → Flashcards).
3. Check the due date. Cards that are not yet due do not appear.
4. If a Cards set is selected, check its query, or switch to **All cards**.

### Answer Visible Before Reveal

The answer is on the card block itself. Move it into a child block.

### Scheduling Issues

1. Look at the `:logseq.property.fsrs/state` ("State") property on the card block.
2. Check `:logseq.property.fsrs/due` ("Due") for the next scheduled time.

---

## Code Locations

| Component            | Path                                                         |
| -------------------- | ------------------------------------------------------------ |
| FSRS logic & UI      | `src/main/frontend/extensions/fsrs.cljs`                     |
| Due-card query       | `src/main/frontend/worker/handler/flashcard.cljs`            |
| Card context menu    | `src/main/frontend/components/content.cljs:134`, `:341`      |
| Shortcut handler     | `src/main/frontend/extensions/srs/handler.cljs`              |
| Shortcut config      | `src/main/frontend/modules/shortcut/config.cljs`             |
| Card classes         | `deps/db/src/logseq/db/frontend/class.cljs:59-66`            |
| FSRS properties      | `deps/db/src/logseq/db/frontend/property.cljs:610-619`       |
| Mobile Cards tab     | `src/main/mobile/tabs.cljs`, `src/main/mobile/components/app.cljs` |
| FSRS library         | `open-spaced-repetition/cljc-fsrs` (`deps.edn:53-54`)        |

---

## See Also

- **[Flashcards Basics](../../tutorials/knowledge-management/flashcards-basics.md)**
- **[Create Flashcard Deck](../../how-to/content/create-flashcard-deck.md)**

---

_Last updated: 2026-09-15_
