# Query Syntax: DB vs File-Based Graphs

> **Last revised:** 2026-05-29

This page compares how query text is authored in file-based graphs and DB graphs, validated against current source in `src/`, `deps/`, and `packages/`.

## What Is Actually Different

The query language itself is shared. The main difference is how query content is attached to a block.

- File-based graph: query text is typically authored with `{{query ...}}` or a `query` code block.
- DB graph: recommended flow is slash command `/Query`, which creates a Query-typed block with a `:logseq.property/query` value.

## File-Based Graph Syntax

### Simple DSL Query

```clojure
{{query (and [[project]] (task todo doing))}}
```

### Advanced Query

````markdown
```query
{:title "My Query"
 :query [:find (pull ?b [*])
         :where [?b :block/title ?t]]
 :inputs [:today]}
```
````

## DB Graph Syntax

### Recommended Creation: `/Query`

`/Query` currently applies these editor steps:

- `:block/tags` -> `:logseq.class/Query`
- `:logseq.property/query` -> `""`
- query property display type -> `:code`
- query property code language -> `"clojure"`

### Query Value Examples

Simple DSL value:

```clojure
(and [[project]] (task done doing))
```

Advanced query value:

```clojure
{:title "My Query"
 :query [:find (pull ?b [*])
         :in $ ?today
         :where [?b :block/journal-day ?today]]
 :inputs [:today]}
```

## Task Marker Behavior (Current)

Task marker tokens in DSL `task` filters are normalized by the query DSL implementation via capitalization.

That means these forms are accepted by current tests/implementation:

- `(task done)`
- `(task DOING)`
- `(task done doing)`
- `(task [done doing])`

Practical guidance: use canonical display values (`Todo`, `Doing`, `Done`) for readability, but query parsing is not strictly case-sensitive in current source.

## Scheduled and Deadline Notes

For DB graphs, scheduled/deadline are built-in datetime properties:

- `:logseq.property/scheduled`
- `:logseq.property/deadline`

The DB built-ins are queryable and typed as `:datetime` in current property definitions.

## Template Variables and Query Inputs

Both query surfaces still use the same underlying resolution path for template variables and query inputs at execution time.

## Side-by-Side Quick Reference

| Aspect                          | File-Based                           | DB-Based                               |
| ------------------------------- | ------------------------------------ | -------------------------------------- |
| Primary authoring surface       | `{{query ...}}` / `query` code block | `/Query` command                       |
| Storage shape                   | Inline content in markdown           | Query class + `:logseq.property/query` |
| Query language                  | DSL or datalog map                   | DSL or datalog map                     |
| Query property display metadata | N/A                                  | `:code` + `"clojure"`                  |

## Removed / Corrected Claims

> **Removed:** "DB graphs do not support `{{query}}` and always show a deprecation warning". Current source still includes `{{query}}` examples and DSL query entry points that mention both `/query` and `{{query }}` usage.

> **Removed:** "DB task filters must use title-case only and uppercase is invalid". Current tests show marker normalization and acceptance of multiple casings/forms.

> **Removed:** The hard requirement to write DB queries as literal `query:: ...` text syntax. Current DB-native creation path in source is the `/Query` slash flow that sets Query class/property metadata.

> **Removed:** Reference to `src/main/frontend/components/file_based/query.cljs`. That path is not present in current source tree.

> **Removed:** The strict file-vs-DB scheduled/deadline storage comparison as a validated source claim in this page. In current validation scope, DB built-ins are directly verifiable; legacy file-attribute storage details are not asserted here as source-verified facts.

## Findings: New Capabilities

1. Query-input time support includes rich millisecond-level variants (for example `:-1d-ms`, `:+1w-ms`, `:start-of-today-ms`, `:today-115959999`) validated in current tests.
2. `/Query` auto-configures code-editor rendering metadata on the query property (`display-type :code`, language `clojure`).
3. DSL task filters support both list and vector marker forms and normalize marker casing in current implementation.

## Code Locations (Validation Sources)

- `/Query` setup steps: `src/main/frontend/commands.cljs`
- Query class schema: `deps/db/src/logseq/db/frontend/class.cljs`
- Query property definition: `deps/db/src/logseq/db/frontend/property.cljs`
- DSL task normalization: `src/main/frontend/db/query_dsl.cljs`
- DSL task behavior tests: `src/test/frontend/db/query_dsl_test.cljs`
- Query input resolution: `deps/db/src/logseq/db/frontend/inputs.cljs`
- Millisecond/date input behavior tests: `src/test/frontend/db/query_react_test.cljs`
