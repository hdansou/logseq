---
title: 'Templates System'
status: 🟢 Stable
version: 'DB graph schema 65.33'
quadrant: 'Reference'
lastVerified: '2026-09-10'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
related-docs:
  - '[Slash Commands](./slash-commands.md)'
  - '[Properties Reference](../data/properties-reference.md)'
  - '[:block/tags vs :logseq.property/page-tags](../../explanation/concepts/block-tags-vs-page-tags.md)'
tags: ['templates', 'automation', 'productivity', 'reference']
---

# Templates System

This is a reference for templates in Logseq **DB graphs**. A template is a reusable block tree. You can insert it anywhere with the `/Template` slash command, or have Logseq insert it automatically whenever a node gets a particular tag, journal pages included.

All `file:line` citations are at `be800f1711`.

> **File-graph syntax does not apply.** The `template:: name` property, `:default-templates` in `config.edn`, `{{title}}`-style placeholders and date-offset variables such as `<% 3 days ago %>` all belong to file graphs. None of them work in a DB graph. The sections below describe what the DB-graph code actually does.

**What You'll Learn:**

- What makes a block a template
- The five `<% … %>` variables, and what happens to everything else
- Inserting a template by hand, and automatically through tags
- Journal templates
- Troubleshooting

---

## Table of Contents

1. [Template Basics](#template-basics)
2. [Creating Templates](#creating-templates)
3. [Template Variables](#template-variables)
4. [Using Templates](#using-templates)
5. [Tag and Journal Templates](#tag-and-journal-templates)
6. [Example Templates](#example-templates)
7. [Programmatic Access](#programmatic-access)
8. [Troubleshooting](#troubleshooting)

---

## Template Basics

### What is a Template?

A **template** is a block tagged with the built-in `Template` class (`deps/db/src/logseq/db/frontend/class.cljs:100-102`). The template block's **title is its name**. The `/Template` search matches on `:block/title` (`src/main/frontend/search.cljs:45-57`).

When a template is applied, Logseq copies the template block's **children**. The template block itself is not copied:

- `/Template` takes `rest` of `get-block-and-children` (`deps/outliner/src/logseq/outliner/op.cljs:239-251`).
- Tag templates take `next` of the same call (`src/main/frontend/worker/pipeline.cljs:113-116`).

So the template's own title, tags and properties never end up in the inserted copy. That makes the root block a safe place for notes about the template.

A template's children can include:

- Static text and nested block hierarchies
- `<% … %>` variables (see [Template Variables](#template-variables))
- References between blocks of the same template. On insert, each one is rewritten to point at the new copy of the referenced block (`deps/outliner/src/logseq/outliner/core.cljs:631-652`).

Asset blocks are **not** copied. When `insert-template?` is set, insertion removes every block for which `ldb/asset?` is true (`core.cljs:842-844`).

The first inserted block gets the hidden `:logseq.property/used-template` property, pointing back at the template (`deps/db/src/logseq/db/frontend/property.cljs:691-695`, set in `op.cljs:246-248` and `pipeline.cljs:115-116`).

---

## Creating Templates

### Step 1: Create the root block and tag it `Template`

```text
Meeting notes          ← root block; tag it "Template" via the Tags property
  Date: <% today %>
  Attendees:
  Topics:
  Action items:
  Next meeting: <% tomorrow %>
```

> **Inline `#Template` is silently stripped.** `Template` is one of the `block-kind-tags` and so belongs to `disallowed-inline-tags` (`class.cljs:130-140`). When an ordinary block is saved, the outliner removes any disallowed inline tag from `:block/tags` and `:block/refs`, and rewrites the title so `#Template` becomes plain text (`core.cljs:275-331`). No error is shown. Typing `Meeting notes #Template` therefore gives you a block whose title contains the literal text "#Template" and which is **not** a template. Add the tag through the block's **Tags** property instead.

A second route: set the **Apply template to tags** property on a block. The outliner then adds the `Template` tag automatically (`deps/outliner/src/logseq/outliner/property.cljs:207-208`). See [Tag and Journal Templates](#tag-and-journal-templates).

### Step 2: Use the template

Type `/Template` and pick the template by title. See [Using Templates](#using-templates).

### Template Naming

The search is fuzzy over `:block/title` (`search.cljs:55-57`), so any title works. Short, distinct titles are simply easier to find.

---

## Template Variables

### The five variables

Only these five names are substituted (`deps/outliner/src/logseq/outliner/template.cljs:104-112`):

| Variable             | Output                                                                     |
| -------------------- | -------------------------------------------------------------------------- |
| `<% today %>`        | Page reference to today's journal page                                     |
| `<% yesterday %>`    | Page reference to yesterday's journal page                                 |
| `<% tomorrow %>`     | Page reference to tomorrow's journal page                                  |
| `<% time %>`         | Current time as `HH:MM`, 24-hour, in the browser locale (`template.cljs:10-16`) |
| `<% current page %>` | Page reference to the page being inserted into, or today's journal page if there is none |

Details:

- **Page references are UUID refs.** The output is `[[<page-uuid>]]` whenever the page exists. Otherwise it is `[[<title>]]` (`page-ref-for`, `template.cljs:74-79`). It renders as the page title. Journal title formatting comes from the Journal class, not from `config.edn`.
- **Don't wrap a page variable in `[[ ]]`.** `today`, `yesterday`, `tomorrow` and `current page` already produce the brackets through `page-ref/->page-ref`, which is `(str "[[" page-name "]]")` (`deps/common/src/logseq/common/util/page_ref.cljs:30-33`). Writing `[[<% today %>]]` therefore gives doubled brackets in the stored text. Write `<% today %>`. `time` is plain text, not a reference.
- **Journal pages are created on demand.** If a template uses `today`, `yesterday` or `tomorrow` and that journal page does not exist yet, it is created before substitution (`dynamic-template-journal-days`, `template.cljs:94-102`; `op.cljs:259-263`; `pipeline.cljs:78-91`).
- **"current page" means the target's page.** If the target is itself a page, that page is used. Otherwise the target block's `:block/page` is used (`template.cljs:65-72`, `:106-107`).
- **Matching is case- and space-insensitive.** The inner text is trimmed and lower-cased before lookup (`template.cljs:118-119`). `<%Today%>` and `<%  TODAY  %>` both work.
- **Where substitution happens.** Variables are replaced in `:block/title`, `:block/raw-title` and the string values of `:block/properties-text-values` (`template.cljs:151-161`).

### Everything else

`resolve-string` (`template.cljs:114-128`) uses the pattern `#"<%([^%].*?)%>"` (`template.cljs:8`) and replaces each whole match:

| Input                                     | Result                                             |
| ----------------------------------------- | -------------------------------------------------- |
| One of the five names                     | Its value                                          |
| Blank, e.g. `<% %>`                       | Empty string (`template.cljs:121-122`)             |
| Any other name, e.g. `<% week %>`         | The trimmed inner text **without** the `<% %>` wrapper, so `week` (`template.cljs:127-128`) |

So `<% 3 days ago %>` renders as `3 days ago`, `<% cursor %>` renders as `cursor`, and `<% dow %>` renders as `dow`. `week`, `week-start`, `week-end`, `dow`, `previous-week`, `cursor`, `page` and every date-offset phrase are **not** variables. This behavior follows from the code. No test covers unknown names.

### Two substitution engines: templates vs. queries

`<% … %>` is substituted in two places, and the results differ.

| | Template insertion (`template.cljs`) | Simple-query execution (`query_dsl.cljs`) |
| --- | --- | --- |
| When | Once, when the template is applied | Every time the query runs (`resolve-dynamic-template`, `src/main/frontend/worker/query_dsl.cljs:92-110`, called at `:708` and `:805`) |
| `today` / `yesterday` / `tomorrow` | `[[<journal-page-uuid>]]`, fixed at insert time | A page ref to the journal page **by title**, recomputed per run (`query_dsl.cljs:77-90`) |
| `time` | `HH:MM`, 24-hour | `h:mm a` (`query_dsl.cljs:73-75`) |
| Other names | Bare inner text | Tried as a natural-language date (`nld-parse`, `query_dsl.cljs:68-71`). If that fails, bare inner text |

**A query inside a template is frozen to the day of insertion.** A `/Query` block keeps its query text in a `:logseq.property/query` child block (`run-query-command!`, `src/main/frontend/handler/editor.cljs:4377-4396`). Template insertion loads the template with `get-block-and-children … {:include-property-block? true}` (`op.cljs:242-244`). That option defaults to `false` (`deps/db/src/logseq/db.cljs:376-378`), and when set it also walks the `:logseq.property/query` blocks (`db.cljs:381-384`). So the query block is copied, and its title goes through the template engine like any other block (`template.cljs:151-161`). A templated `(between <% yesterday %> <% today %>)` is stored with the UUID refs of that day's journals. The DSL still resolves those refs, because `ldb/get-page` accepts a UUID string (`db.cljs:544-553`, used by `build-page-ref`, `query_dsl.cljs:467-474`). The same query typed into a standalone `/Query` block moves with the calendar.

**Label and query go in separate blocks.** `/Query` moves the block's entire title into the query (`editor.cljs:4388-4395`). A template line such as `Completed: (task done)` therefore cannot become a labelled query block. Put the label in a parent block and the query in a child block.

### No date-format configuration

There is no `:template/date-format` key anywhere in the source. `:journal/page-title-format` appears only in `file-only-config`, which lists keys deprecated in DB graphs (`deps/common/src/logseq/common/config.cljs:105-115`). DB graphs read the journal title format from `:logseq.property.journal/title-format` on the Journal class (`op.cljs:253-257`).

---

## Using Templates

### Slash command

1. Type `/` and choose **Template**. The entry is labelled "Template" and described as "Insert a created template here" (`src/main/frontend/commands.cljs:339-343`, `src/resources/dicts/en.edn:212`, `:563`).
2. The template search opens (`:editor/search-template` → `:template-search`, `commands.cljs:693-694`). It lists nodes tagged `Template`, filtered fuzzily by title, up to 100 results (`search.cljs:45-57`).
3. Choose a template.

What happens next:

- If the editing block has content, it is saved first (`src/main/frontend/handler/editor.cljs:2512-2513`).
- The `:apply-template` outliner op runs (`editor.cljs:2509-2514`).
- **If the current block is empty, the first template block replaces it** (`:replace-empty-target? true`, `editor.cljs:2522-2527`). Replacement applies only when the target's title is blank (`core.cljs:847-853`).
- The template's children are inserted as **children** of the target if the target already has children. Otherwise they are inserted as **siblings** (`op.cljs:273-282`).
- Any failure shows an error notification (`editor.cljs:2517-2519`).

### Keyboard shortcut

None is registered. The shortcut config (`src/main/frontend/modules/shortcut/config.cljs`) has no template entry.

### Context menu

The block context menu (`src/main/frontend/components/content.cljs`) has no "Apply template" item.

---

## Tag and Journal Templates

### Apply template to tags

The `Template` class declares one property, **Apply template to tags** (`:logseq.property/template-applied-to`, `:type :class`, `:cardinality :many`, `property.cljs:696-700`; `class.cljs:100-102`).

After a transaction, `insert-tag-templates` (`pipeline.cljs:93-159`) looks at every `:block/tags` datom that was added:

1. For each newly added tag, it collects templates whose **Apply template to tags** includes that tag **or any class the tag extends** (`ldb/get-class-extends`, `pipeline.cljs:106-107`).
2. Templates with no children are skipped (`pipeline.cljs:112-113`).
3. Matching templates are applied oldest first, by `:block/created-at` (`pipeline.cljs:131-134`).
4. Variables are resolved against the tagged node (`pipeline.cljs:144`).
5. The template's children are inserted as **children of the tagged node** (`:sibling? false`, `pipeline.cljs:146-150`).

The hook does not run for RTC transactions or graph downloads (`pipeline.cljs:528-529`).

Example: set **Apply template to tags** to `Book` on a template. Any node that is later tagged `Book`, or tagged with a class that extends `Book`, gets that template's children.

### Journal templates

`:default-templates` is not a DB-graph key. `file-only-config` says it "is replaced by #Template and the `Apply template to tags` property" (`config.cljs:138-139`).

To make a daily-journal template:

1. Create a template block with children, as above.
2. Set **Apply template to tags** to `Journal`.

`Journal` is a private tag. The class picker hides private tags for most properties but shows them for **Apply template to tags** (`src/main/frontend/worker/handler/property.cljs:128-130`).

When a journal page is created, it gets the `Journal` tag, and the hook above inserts the template. For journal pages, the inserted blocks get **deterministic UUIDs** derived from the journal page and the template block (`common-uuid/gen-journal-template-block`, `pipeline.cljs:121-125`), and insertion keeps those UUIDs (`:keep-uuid? journal-template?`, `pipeline.cljs:149`).

---

## Example Templates

Logseq creates **no** templates in a new graph. `create_graph.cljs` builds only classes, built-in pages, views, favorites and the recycle page (`deps/db/src/logseq/db/sqlite/create_graph.cljs:256-260`). The trees below are examples to build yourself. In each one the top line is the root block tagged `Template`, and only the indented children get inserted.

### Meeting Notes

```text
Meeting
  Date: <% today %>
  Time: <% time %>
  Attendees:
  Agenda:
  Discussion:
  Decisions:
  Action items:
  Next steps:
```

### Daily Note (journal template)

Set **Apply template to tags** = `Journal` on the root block.

```text
Daily note
  Morning
    Intention:
    Top 3 priorities:
  Notes
  Completed
  Evening reflection
    Wins:
    Learnings:
    Tomorrow: <% tomorrow %>
```

### Research Paper (tag template)

Set **Apply template to tags** = `Paper`, your own class, on the root block.

```text
Paper notes
  Abstract
  Key findings
  Methodology
  Conclusions
  Related work
  Read on: <% today %>
```

### Project Brief

```text
Project brief
  Project: <% current page %>
  Start date: <% today %>
  Problem statement
  Objectives
  Success criteria
  Scope
    In scope
    Out of scope
  Stakeholders
  Risks
  Timeline
```

### Sprint Retrospective

```text
Sprint retro
  Retrospective for <% current page %>
  Date: <% today %>
  What went well
  What didn't go well
  Ideas for improvement
  Action items
```

> Target dates such as "in 4 weeks" cannot come from a variable, because offset phrases are not variables (see above). Enter those dates by hand.

---

## Programmatic Access

> **The plugin template API has no implementation at this revision.** `libs/src/LSPlugin.ts` still declares `getCurrentGraphTemplates` (`:513`) and `getTemplate`, `existTemplate`, `createTemplate`, `removeTemplate` and `insertTemplate` (`:533-541`). Their implementations lived in `src/main/logseq/api/file_based.cljs`, which commit `eaa5a57337` ("remove file based api") deleted. No template function exists anywhere under `src/main/logseq/` at `be800f1711`. What a plugin sees when it calls one of these methods was not tested.

Inside the app, templates are applied through the `:apply-template` outliner op (`op.cljs:265-288`). `logseq.Editor.registerTemplate` and a `<% template: name %>` form do not exist.

---

## Troubleshooting

### Template doesn't appear in the `/Template` list

1. **The root block isn't actually tagged `Template`.** The usual cause is typing `#Template` inline, which is silently stripped (`core.cljs:275-331`). Add the tag through the Tags property.
2. **Wrong search text.** The search matches the template block's title (`search.cljs:55-57`).

### Variables not expanding

- **Only `today`, `yesterday`, `tomorrow`, `time` and `current page` expand** (`template.cljs:104-112`). Any other name loses its `<% %>` wrapper and stays as a plain word (`template.cljs:127-128`).
- **Substitution runs only when a template is applied**, through `resolve-dynamic-template-blocks`, called from `op.cljs:271` and `pipeline.cljs:144`. Typing `<% today %>` into an ordinary block does nothing.

### Inserted template is empty, or missing blocks

- The template has no children. The root block is never copied (`op.cljs:239-251`), and tag templates skip childless templates (`pipeline.cljs:112-113`).
- The missing blocks were asset blocks, which template insertion removes (`core.cljs:842-844`).

### Journal template not applied

1. Check that **Apply template to tags** includes `Journal`.
2. Check that the template has children.
3. Journal pages that arrive by sync (RTC) or graph download don't trigger the hook (`pipeline.cljs:528-529`).

### Date offsets don't work

`<% 1 week from now %>` and `<% 3 days ago %>` are not template variables. They render as the bare text `1 week from now` and `3 days ago`.

---

## Next Steps

**Start Using Templates:**

1. Create a block, give it children, and tag it `Template` through the Tags property
2. Test it with `/Template`
3. For automatic insertion, set **Apply template to tags**

**Related:**

- [Slash Commands](./slash-commands.md)
- [Properties Reference](../data/properties-reference.md)
- [:block/tags vs :logseq.property/page-tags](../../explanation/concepts/block-tags-vs-page-tags.md): explains why inline `#Template` is stripped

---

**Questions?** Join [Logseq Discord](https://discord.gg/KpN4eHY) or visit [discuss.logseq.com](https://discuss.logseq.com).
