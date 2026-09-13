# Query DSL and template probe

Scratch probe behind investigation
[`029-upstream-defects-from-ddocs-review.md`](../../docs/07-investigations/029-upstream-defects-from-ddocs-review.md).
It prints, it does not assert. Same convention as
[`../markdown-mirror-probe/`](../markdown-mirror-probe/README.md).

It exists so that four entries in 029 rest on execution rather than on reading source:

| Claim in 029 | What the probe shows |
|---|---|
| `(between deadline …)` drops its clause instead of filtering | `get-timestamp-property` returns nil for a bare `deadline`, and the parsed query has no clause for it |
| `get-timestamp-property` maps only `created-at`/`updated-at` | the same call for `created_at` returns `:block/created-at` |
| `<% current page %>` resolves to today's journal in a standalone `/Query` | the resolver's output with and without `:current-page-title` |
| `mod+shift+h` wraps with `==` while `^^` is used elsewhere | `config/get-highlight :markdown` |

## Why it can run on a checkout that is not master

029 cites `be800f1711`. The files this probe exercises are **byte-identical** between
`be800f1711` and the `d2ab7726ab` checkout it was run on:

```bash
git diff --stat d2ab7726ab be800f1711 -- \
  src/main/frontend/worker/query_dsl.cljs \
  deps/db/src/logseq/db/frontend/query_dsl.cljs \
  src/main/frontend/config.cljs \
  deps/db/src/logseq/db/frontend/property.cljs   # empty
```

Re-run that check before trusting a later run. **Do not** extend this probe to
`handler/editor.cljs` or `worker/markdown_mirror.cljs`: both differ between those revisions,
so a run would not be evidence for what 029 cites.

## Running it

```bash
# in a checkout of logseq with node_modules installed
cp dz/scripts/query-dsl-probe/query_dsl_probe_test.cljs src/test/frontend/worker/
clojure -M:test compile test
node static/tests.js -n frontend.worker.query-dsl-probe-test
rm src/test/frontend/worker/query_dsl_probe_test.cljs
```

The compile is the slow part (minutes cold). The probe itself needs no graph on disk and
writes nothing: `db-test/create-conn` builds an in-memory DB graph, and the template
resolver is pure.
