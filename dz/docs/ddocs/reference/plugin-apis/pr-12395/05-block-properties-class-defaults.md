# `Editor.getBlockProperties` — class default values are now merged in

## Summary

`logseq.Editor.getBlockProperties` (and its page counterpart) now returns the **effective** property map for a block: its own properties **merged with** any properties defined on the block's classes (tags) that have a `:logseq.property/default-value`.

Previously a plugin had to manually walk a block's tags, fetch each tag's properties, and resolve defaults itself.

This applies to **DB graphs** only.

## Behavior

For a block:

1. Read the block's own properties (`:block/properties`).
2. Look up properties contributed by every class on the block (via `outliner-property/get-block-classes-properties`).
3. Filter to those with `:logseq.property/default-value`.
4. Merge: **class defaults first, own properties override.**

So if a block is tagged `#Task` and `Task` defines `status` with default `"TODO"`, calling `getBlockProperties` on a freshly created task returns `{ status: "TODO", … }` — even though no `status` was set on the block.

## Example

Setup (DB graph, conceptual):

```text
Class: Task
  property status
    default-value: "TODO"
  property priority
    default-value: "B"

Block:
  title: "Write release notes"
  tags: [#Task]
  properties:
    priority: "A"      ← overrides class default
```

Plugin code:

```ts
const props = await logseq.Editor.getBlockProperties(blockUuid)
// {
//   status:   'TODO',   ← from class default
//   priority: 'A'       ← block's own value wins over class default 'B'
// }
```

Same call against a non-DB graph just returns the block's own properties — no class merge.

## Use cases

- **Status badges / dashboards** — render a badge for every `Task` block without needing to special-case unset properties.
- **Default-value-aware exports** — exporters previously emitted blank fields for unset properties; now they emit the class defaults consistently.
- **Validation plugins** — diff actual vs. expected (defaulted) properties to flag stale or missing values.

## What hasn't changed

- `getBlockProperty(uuid, key)` for a single property — unchanged signature; returns whatever `get_block_property` resolves (own value, may be undefined if not set).
- Setting / removing properties — unchanged; you still write to the block, not the class.
- File-based graphs — unchanged behavior.

## Source

- Helper: `src/main/logseq/api/editor.cljs:443` (`get-block-classes-properties-has-default-value`)
- Public API: `src/main/logseq/api/editor.cljs:488` (`get_block_properties`)
- Underlying classes/property walk: `logseq.outliner.property/get-block-classes-properties`
- Commit: `fix(apis): simplify block property retrieval and enhance default value handling`
