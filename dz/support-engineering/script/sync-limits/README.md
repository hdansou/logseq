# sync-limits

Drift check for `ddocs/reference/features/sync-limits.md`. Every number on that page is declared in `sync-limits.edn` with the source file and symbol (or regex) it comes from; `check_sync_limits.clj` re-reads the sources and fails when any value has changed.

```
bb check_sync_limits.clj --root <logseq-checkout>
```

Exit 0 when every declared value still matches source; 1 when any has drifted or can no longer be found; 2 when the declaration file is missing.

Two entry kinds in `sync-limits.edn`:

- `{:symbol max-asset-size :file "…assets.cljs" :expected 104857600}` — finds `(def max-asset-size …)` (also `defonce`, `^:private`), folds simple arithmetic like `(* 100 1024 1024)`, compares.
- `{:regex "…(\\d+)" :file "…wrangler.toml" :expected "120"}` — first capture group, compared as a string. Used for TOML, dictionary strings, and inline literals that are not top-level defs.

When it fails, fix the source if the change was accidental; otherwise update **both** `sync-limits.edn` and the reference page. The declaration and the page are meant to be edited together.

Requires babashka (edamame bundled).
