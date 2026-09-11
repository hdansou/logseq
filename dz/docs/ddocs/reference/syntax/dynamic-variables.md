---
title: 'Dynamic Variables Reference'
description: "Reference for Logseq's <% %> dynamic variables in DB-graph templates and simple queries"
status: stable
lastVerified: '2026-09-11'
verifiedAgainst: 'be800f1711'
reviewStatus: audited
logseqVersion: '0.11.x'
platforms: [desktop, mobile, web]
graphType: [db]
relatedDocs:
  reference:
    - '[Templates System](../interface/templates-system.md)'
    - '[Query System](../advanced/query-system.md)'
---

# Dynamic Variables Reference

> **Last revised:** 2026-09-11

> Reference for the `<% … %>` variables in Logseq **DB graphs**: which names exist, what each one produces, and why the result differs between templates and simple queries. All `file:line` citations are at `be800f1711`.

## Overview

A dynamic variable is text of the form `<% name %>`. Typing one into an ordinary block does nothing. Only two code paths substitute it, and they produce different results.

| Where | Resolver | When it runs | Names it knows |
| ----- | -------- | ------------ | -------------- |
| Template insertion (`/Template`, or a template applied to a tag) | `resolve-dynamic-template-blocks` (`deps/outliner/src/logseq/outliner/template.cljs:163-170`), called only from `deps/outliner/src/logseq/outliner/op.cljs:271` and `src/main/frontend/worker/pipeline.cljs:144` | Once, at insertion. The result is stored as ordinary text | The five built-ins |
| Simple-query execution | `resolve-dynamic-template` (`src/main/frontend/worker/query_dsl.cljs:92-110`), called from `parse-query` (`:708`) and `execute-custom-query` (`:805`) | Every time the query runs | The five built-ins, then any text chrono-node can read as a date |

Datalog ("advanced") queries use neither. They take dates through `:inputs`. See [Advanced Queries Use `:inputs`](#advanced-queries-use-inputs-not-variables).

> **Correction:** an earlier version named `frontend.template/resolve-dynamic-template!` (`src/main/frontend/template.cljs:23-37`) as the query resolver. That function still exists, but its only caller is `frontend.db.query-dsl/pre-transform-query` (`src/main/frontend/db/query_dsl.cljs:17-20`). The query builder calls it to parse a query string into its editing UI (`src/main/frontend/components/query/builder.cljs:564-567`). The query itself runs in the DB worker (`src/main/frontend/db/query_dsl.cljs:40-50`, `src/main/frontend/worker/handler/render_resource/query.cljs:137-145`), and the worker resolver above does the substitution.

---

## Quick Reference

| Variable | Inserted by a template | Substituted in a simple query |
| -------- | ---------------------- | ----------------------------- |
| `<% today %>` | `[[<uuid>]]`, a reference to today's journal page | `[[Jan 29th, 2026]]` |
| `<% yesterday %>` | `[[<uuid>]]` of yesterday's journal page | `[[Jan 28th, 2026]]` |
| `<% tomorrow %>` | `[[<uuid>]]` of tomorrow's journal page | `[[Jan 30th, 2026]]` |
| `<% time %>` | `14:30` (24-hour, plain text) | `2:30 PM` (plain text) |
| `<% current page %>` | Reference to the page the template is inserted into | See [current page](#current-page) |

A `[[<uuid>]]` reference is displayed as the page's title.

---

## Built-in Variables

### today, yesterday, tomorrow

**In a template**, `variable-rules` finds the journal page for the day (`template.cljs:104-110`). `page-ref-for` then wraps the page's UUID as `[[<uuid>]]` (`template.cljs:74-79`). Before resolving, Logseq creates any journal page the template names that does not exist yet (`ensure-template-journal-pages!`, `op.cljs:259-263`; for tag templates, `pipeline.cljs:140`), so the stored reference is normally the UUID form. If the page is still missing, `page-ref-for` falls back to the journal title, or to the bare `YYYYMMDD` number when no title is found (`template.cljs:39-48`).

**In a simple query**, the result is a reference by title, and the title is always formatted as `MMM do, yyyy` (`query_dsl.cljs:61-66`, `deps/common/src/logseq/common/util/date_time.cljs:10`), whatever date format you have chosen. The day comes from the `:today-day` the caller passes, or from the current date when there is none (`query_dsl.cljs:79-88`).

Don't write `[[<% today %>]]`. These variables already produce the brackets through `page-ref/->page-ref` (`deps/common/src/logseq/common/util/page_ref.cljs:30-33`), so the brackets would be doubled.

### time

**In a template**: `toLocaleTimeString` with the browser language, a two-digit hour and minute, and `hourCycle "h23"`, which gives 24-hour `HH:MM` (`template.cljs:10-16`).

**In a simple query**: cljs-time formatted as `h:mm a`, for example `2:30 PM` (`query_dsl.cljs:73-75`).

`time` is plain text in both cases, not a reference.

> **Correction:** an earlier version said both resolvers pass `hourCycle: "h23"` and cited `src/main/frontend/context/i18n.cljs:198-202`. That i18n function belongs to neither resolver, and the query resolver uses the 12-hour `h:mm a` pattern.

<a id="current-page"></a>

### current page

**In a template**: a reference to the page holding the block the template is inserted into. If the target is itself a page, that page is used. If there is no page, today's journal page is used (`template.cljs:65-72`, `:107`, `:112`).

**In a simple query**: the `:current-page-title` passed with the query, or today's journal title when none is passed (`query_dsl.cljs:90`). The renderer sets `:current-page-title` only for the `:default-queries :journals` queries on today's journal page (`src/main/frontend/components/page.cljs:233-252`), and forwards it to the worker only when it is set (`src/main/frontend/components/query/result.cljs:37-38`). A `/Query` block's config does not include it (`src/main/frontend/components/block.cljs:4099-4101`). So inside an ordinary `/Query` block, `<% current page %>` resolves to today's journal, not to the page the query is on. This follows from the code and has not been reproduced in the app.

### Blank variable

A variable with only whitespace inside, such as `<% %>`, resolves to an empty string (`template.cljs:121-122`, `query_dsl.cljs:100-101`). `<%%>` with nothing inside is left as literal text, because the pattern `#"<%([^%].*?)%>"` needs one character that is not `%` right after `<%` (`template.cljs:8`, `query_dsl.cljs:59`).

> **Correction:** an earlier version said `<%%>` resolves to an empty string. It does not match the pattern at all.

### Complete built-in list

The five names above are the **entire** set. Both resolvers hard-code them in a `variable-rules` map with exactly the keys `today`, `yesterday`, `tomorrow`, `time` and `current page` (`template.cljs:104-112`, `query_dsl.cljs:77-90`). There is no `date`, `week`, `uuid` or `cursor` variable, and no way to add your own.

---

## Natural-Language Dates

> **Scope:** natural-language dates are resolved **only in simple queries**. In a template, any name that is not a built-in becomes bare text. See [Unrecognized Variables](#unrecognized-variables).

In a simple query, a name that is not a built-in is passed to chrono-node's `parseDate` (`query_dsl.cljs:68-71`, `:106-110`). chrono-node is pinned at `2.9.0` (`package.json:160`). If the text parses, the date becomes a `[[MMM do, yyyy]]` reference. If it does not parse, the trimmed text is returned without the `<% %>` delimiters.

The results below come from calling chrono-node 2.9.0's `parseDate` directly, not through Logseq, with Thursday 2026-01-29 as "now". They are shown as the reference the query would receive.

| Expression | Result |
| ---------- | ------ |
| `next Monday` | `[[Feb 2nd, 2026]]` |
| `last Friday` | `[[Jan 23rd, 2026]]` |
| `this Wednesday` | `[[Feb 4th, 2026]]` |
| `in 3 days`, `3 days from now` | `[[Feb 1st, 2026]]` |
| `5 days ago` | `[[Jan 24th, 2026]]` |
| `2 weeks ago` | `[[Jan 15th, 2026]]` |
| `next week`, `in 7 days`, `1 week from now` | `[[Feb 5th, 2026]]` |
| `3 months from now` | `[[Apr 29th, 2026]]` |
| `next month` | `[[Mar 1st, 2026]]`. One month after Jan 29 is past the end of February, so the date rolls over into March |
| `March 15` | `[[Mar 15th, 2026]]` |
| `Jan 1 2027` | `[[Jan 1st, 2027]]` |
| `end of month`, `beginning of year` | Not parsed. The query receives the bare text |
| `today + 1` | Read as `today`. The `+ 1` is ignored |

> **Removed:** an earlier version listed `end of month` and `beginning of year` as supported expressions. chrono-node 2.9.0 parses neither.

---

## Usage in Templates

A template is a block tagged `#Template`. Its **child** blocks are what gets inserted (`template-children-blocks` drops the root with `rest`, `op.cljs:239-251`). Insert it with `/Template` (`src/main/frontend/commands.cljs:339-343`), or let "Apply template to tags" insert it when a node gets a tag (`pipeline.cljs:93-150`). The [Templates System](../interface/templates-system.md) reference covers templates in full.

What the template path does:

1. **Only the five built-ins are resolved.** `deps/outliner/src/logseq/outliner/template.cljs` has no chrono-node fallback.
2. **Missing journal pages are created first** (`op.cljs:259-263`), so date references point at real pages.
3. **Every inserted block's text is resolved.** `resolve-block` rewrites `:block/title` and `:block/raw-title` (`template.cljs:151-158`). The template is loaded with `:include-property-block? true` (`op.cljs:242-243`). That option walks every child entity, including blocks created from a property (`deps/db/src/logseq/db.cljs:376-387`; `:block/_raw-parent` keeps the children that `:block/_parent` filters out, `deps/db/src/logseq/db/common/entity_plus.cljc:171-178`), and a `/Query` block's query (`db.cljs:384`). A `/Query` inside a template is therefore resolved once, at insertion, and stops moving with the calendar.
   > **Unverified:** a variable inside a text property's value block goes through the same rewrite. Whether the resolved text then appears correctly as the property value after insertion was not checked.
4. `resolve-block` also rewrites `:block/properties-text-values` (`template.cljs:160-161`). That attribute is not in the DB-graph schema (`deps/db/src/logseq/db/frontend/schema.cljs` has no such attribute), so this branch does nothing in a DB graph.

### Example

Tag a block `#Template` and give it children:

```
Meeting notes #Template
  Meeting on <% today %>
  Started at <% time %>
  Follow up on <% tomorrow %>
  Filed under <% current page %>
```

Inserted into a block on the page **Project Alpha** on 2026-01-29 at 14:30, the children become:

```
Meeting on [[<uuid of the Jan 29 journal page>]]
Started at 14:30
Follow up on [[<uuid of the Jan 30 journal page>]]
Filed under [[<uuid of Project Alpha>]]
```

Each reference displays as the page's title.

> **Removed:** the earlier examples used `Attendees::`, `due:: <% tomorrow %>` and `TODO` lines. Those are file-graph syntax. In a DB graph, `key:: value` text is not a property, so add properties with `/Add property` (`commands.cljs:354-358`) and set task status with `/Todo`.

> **Removed:** an earlier "Weekly Review" template used `<% last Monday %>`, `<% last Sunday %>` and `<% next Monday %>`. Natural-language dates do not resolve in templates. Those lines would insert the bare text `last Monday` and so on.

<a id="unrecognized-variables"></a>

### Unrecognized Variables

In a template, a name that is not one of the five built-ins is replaced by its **trimmed inner text, without the `<%` / `%>` delimiters** (`template.cljs:127-128`). It is not left as literal `<% … %>`.

```
<% next Monday %>   (in a template)
→ next Monday
```

A simple query does the same when chrono-node also cannot parse the text (`query_dsl.cljs:106-110`).

---

## Usage in Queries

### Simple Queries

Type `/Query`, then enter:

```clojure
(between <% yesterday %> <% tomorrow %>)
```

With two arguments, `between` matches blocks on journal pages whose journal day falls in the range (`query_dsl.cljs:292-298`; rule at `deps/db/src/logseq/db/frontend/rules.cljc:94-100`). A page-reference argument is read as a journal title in any built-in journal format (`query_dsl.cljs:124-129`). `between` also accepts `today`, `yesterday`, `tomorrow` and offsets such as `-7d` or `+2w` directly (`query_dsl.cljs:112-139`), so `(between -7d today)` needs no variable.

Natural-language dates work here:

```clojure
(between <% 2 weeks ago %> <% today %>)
```

> **`{{query …}}` does not work in a DB graph.** Typed into a block, it renders only the warning "{{query}} is deprecated. Use '/Query' command instead." (`block.cljs:1870-1871`, `src/resources/dicts/en.edn:159`). `/Query` moves the block's whole title into the query (`src/main/frontend/handler/editor.cljs:4377-4395`), so enter only the bare form. Put a label in a parent block.

> **Possible defect (from reading the source, not reproduced):** the query resolver always writes `MMM do, yyyy` titles. A date variable used as a bare page-reference filter, such as `(and <% today %> (task todo))`, looks the page up by exactly that name (`build-page-ref`, `query_dsl.cljs:467-474`). If your journal titles use a different format, no page is found, `build-page-ref` returns nothing, and that filter is silently left out of the query. `between` is not affected, because it parses the title against all built-in formats.

<a id="advanced-queries-use-inputs-not-variables"></a>

### Advanced Queries Use `:inputs`, Not Variables

Create an advanced query with `/Advanced Query` (`commands.cljs:125-132`, `:326`) and write a bare EDN map (`block.cljs:4094-4096`). A Datalog query is executed by `execute-custom-query` (`src/main/frontend/worker/handler/query.cljs:243-261`), which does no `<% %>` substitution. Each `:inputs` keyword is resolved by `resolve-custom-query-input` (`handler/query.cljs:158-183`) through `logseq.db.frontend.inputs` (`deps/db/src/logseq/db/frontend/inputs.cljs:69-82`).

```clojure
{:query [:find (pull ?b [*])
         :in $ ?start ?end
         :where
         [?b :logseq.property/scheduled ?d]
         [(>= ?d ?start)]
         [(<= ?d ?end)]]
 :inputs [:start-of-today-ms :end-of-today-ms]}
```

`:datetime` values are stored as numbers, not as refs. `:datetime` is not one of the value-ref property types (`deps/db/src/logseq/db/frontend/property/type.cljs:58-61`), and Logseq's own scheduled/deadline query compares the attribute directly in the same way (`src/main/frontend/worker/handler/property.cljs:864-867`).

> **Removed:** the earlier example wrapped the map in `#+BEGIN_QUERY` / `#+END_QUERY` and set `:title`. In a DB graph, `#+BEGIN_QUERY` is deprecated in favour of `/Advanced Query` (`en.edn:120`), and `:title` is not a query key.

**Pick the input that matches the attribute's type:**

| Attribute | Type | Input to use |
| --------- | ---- | ------------ |
| `:block/journal-day` | `YYYYMMDD` integer | `:today`, `:yesterday`, `:tomorrow` (`inputs.cljs:100-107`); `:+7d`, `:-3d` (`:73`, `:121-125`) |
| `:logseq.property/deadline`, `:logseq.property/scheduled` | `:datetime`, epoch ms (`deps/db/src/logseq/db/frontend/property.cljs:340-353`) | `:start-of-today-ms`, `:end-of-today-ms` (`inputs.cljs:76-77`, `:113-118`); `:today-start`, `:today-end` (`:78`); `:+7d-ms` (`:74`, `:128`); `:right-now-ms` (`:71`, `:109-110`) |

> **Removed:** an earlier version of this page said `:today` resolves to "a date integer suitable for comparison with `:block/scheduled` and `:block/deadline`". Those are not DB-graph attributes. The idents are `:logseq.property/scheduled` and `:logseq.property/deadline` (`property.cljs:340-353`). Both are `:datetime`, epoch milliseconds, while `:today` resolves to a `YYYYMMDD` integer (`inputs.cljs:100-101`), so comparing the two never matches. Use `:today` for `:block/journal-day`, and the `-ms` / `-start` / `-end` inputs for the datetime properties.

---

## Case Sensitivity

Variable names are **case-insensitive**. Both resolvers trim and lower-case the name before looking it up (`template.cljs:118-119`, `query_dsl.cljs:97-98`). `<% TODAY %>`, `<% Today %>` and `<%today%>` are all the same variable.

---

## Output Format

- **Templates** store `[[<uuid>]]` references (`template.cljs:74-79`), which display as the journal page's title.
- **Simple queries** always receive `[[MMM do, yyyy]]` titles such as `[[Jan 29th, 2026]]` (`query_dsl.cljs:61-66`).

**Settings → Editor → Preferred date format** (`src/main/frontend/components/settings.cljs:448-471`, shown in the Editor tab at `:888-903`; label at `en.edn:1713`) sets the Journal tag's Title Format (`settings.cljs:460-466`), which decides how journal pages are titled. The simple-query resolver ignores this setting.

---

## Limitations

1. **Template values are fixed at insertion.** Query values are recomputed every time the query runs.
2. **No nesting.** `<% <% today %> %>` is not supported.
3. **Date variables always produce page references.** A variable cannot choose its own date format.
4. **No arithmetic.** In a template, `<% today + 1 %>` becomes the bare text `today + 1`. In a query, chrono-node 2.9.0 reads it as today. Use `<% tomorrow %>`.
5. **No natural language in templates.** Only `today`, `yesterday`, `tomorrow`, `time` and `current page` resolve when a template is applied.
6. **Only five built-ins,** hard-coded, with no extension point (`template.cljs:104-112`, `query_dsl.cljs:77-90`).
7. **`current page` in a `/Query` block** falls back to today's journal. See [current page](#current-page).

---

## Troubleshooting

### Variable Not Resolving

**Symptom A**: `<% today %>` appears literally.

Either the pattern `#"<%([^%].*?)%>"` never matched, or the text was never passed through a resolver.

**Causes**:

- The variable is in an ordinary block. Only template insertion and simple queries substitute variables.
- Extra spaces inside the delimiters: `< % today % >`
- Missing closing: `<% today`
- A `%` immediately after `<%`, as in `<%%>`

**Symptom B**: the delimiters are gone but the text is unchanged. For example, `<% next Monday %>` became `next Monday`.

The pattern matched but the name was not recognized. In a template, that is any name outside the five built-ins. In a simple query, it means chrono-node could not parse the text either.

### Unexpected Date Format

- In a template, the reference shows the journal page's current title.
- In a simple query, the substituted title is always `MMM do, yyyy`. See [Output Format](#output-format).

### Natural Language Not Parsing

**Symptom**: `<% in 3 days %>` comes out as the bare text `in 3 days`.

**Possible causes**:

- **You are in a template.** Templates have no chrono-node fallback, so this is expected.
- chrono-node 2.9.0 does not recognize the phrase. `end of month` and `beginning of year` are two phrases it does not recognize.

In a template, use one of the five built-ins. In an advanced query, use an `:inputs` keyword instead.

---

## Implementation Details

Both resolvers use the pattern `#"<%([^%].*?)%>"` and trim and lower-case the captured name before lookup.

### Template path: `deps/outliner/src/logseq/outliner/template.cljs`

1. `dynamic-template-journal-days` scans the blocks for `today`/`yesterday`/`tomorrow` (`template.cljs:94-102`), and the caller creates any missing journal page (`op.cljs:259-263`).
2. `variable-rules` builds the five bindings against the live DB and the target block's page (`template.cljs:104-112`).
3. `resolve-block` rewrites `:block/title`, `:block/raw-title` and `:block/properties-text-values` (`template.cljs:151-161`).
4. Blank match → `""`. Known name → its binding. Unknown name → trimmed inner text (`template.cljs:114-128`).

### Simple-query path: `src/main/frontend/worker/query_dsl.cljs`

1. `parse-query` calls `resolve-dynamic-template` on the query string before parsing it (`query_dsl.cljs:705-709`).
2. `variable-rules` builds the five bindings from `:today-day` and `:current-page-title` (`query_dsl.cljs:77-90`).
3. Blank match → `""`. Known name → its binding. Otherwise chrono-node `parseDate`, emitted as a `[[MMM do, yyyy]]` reference. Otherwise the trimmed inner text (`query_dsl.cljs:92-110`).

---

## See Also

- **Templates**: [reference/interface/templates-system.md](../interface/templates-system.md)
- **Queries and query inputs**: [reference/advanced/query-system.md](../advanced/query-system.md)
- **Date parsing**: [chrono-node library](https://github.com/wanasit/chrono)

---

_Last updated: 2026-09-11 | [Report an issue](https://github.com/logseq/logseq/issues)_
