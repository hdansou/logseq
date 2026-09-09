# Import/Export Best Practices

> **Last revised:** 2026-05-30

**Document Type:** Process Guide
**Related Investigation:** [Schema Evolution and Data Migration](../07-investigations/schema-evolution-and-data-migration.md)

---

## Overview

Logseq currently ships several distinct import/export paths, and they are not interchangeable:

- **SQLite database export/import** is the highest-fidelity graph backup and restore path.
- **SQLite zip export/import** packages `db.sqlite` plus assets.
- **DB EDN export/import** is the graph-agnostic interchange format used for DB-graph round trips, migration work, and validation tests.
- **Debug Transit export/import** is a debugging path for datoms and in-memory inspection, not the main backup format.
- **Markdown export** is a separate text export path.
- **Public HTML export** is an Electron-only publishing/export path.

The owning product code lives primarily in:

- [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs)
- [deps/db/src/logseq/db/sqlite/util.cljs](../../../deps/db/src/logseq/db/sqlite/util.cljs)
- [src/main/frontend/handler/db_based/export.cljs](../../../src/main/frontend/handler/db_based/export.cljs)
- [src/main/frontend/handler/db_based/import.cljs](../../../src/main/frontend/handler/db_based/import.cljs)
- [src/main/frontend/handler/export.cljs](../../../src/main/frontend/handler/export.cljs)
- [src/main/frontend/components/export.cljs](../../../src/main/frontend/components/export.cljs)
- [src/main/frontend/components/imports.cljs](../../../src/main/frontend/components/imports.cljs)
- [src/main/logseq/cli/command/graph.cljs](../../../src/main/logseq/cli/command/graph.cljs)

## Current Supported Formats

| Format                         | Current role                        | Main entry points                                                                                        | Notes                                                    |
| ------------------------------ | ----------------------------------- | -------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| **SQLite (`.sqlite`)**         | Full DB backup/restore              | `export-repo-as-sqlite-db!`, `graph export --type sqlite`, `graph import --type sqlite`                  | Best fidelity for DB graphs                              |
| **SQLite zip (`.zip`)**        | Full DB backup/restore with assets  | `export-repo-as-zip!`, `import-from-sqlite-zip!`                                                         | Exports `db.sqlite` and asset files together             |
| **DB EDN (`.edn`)**            | Graph-agnostic DB graph interchange | `export-repo-as-db-edn!`, `:thread-api/export-edn`, `graph export --type edn`, `graph import --type edn` | Used by export/import tests and validation               |
| **Debug Transit (`.transit`)** | Debugging and support workflow      | `export-repo-as-debug-transit!`, `import-from-debug-transit!`                                            | Serializes with Transit JSON, not a binary backup format |
| **Markdown**                   | Text export                         | `export-repo-as-markdown!` from the export UI                                                            | Separate from DB-graph restore workflows                 |
| **Public HTML**                | Publish/export static pages         | `download-repo-as-html!`                                                                                 | Electron-only in current UI                              |

> **Removed:** The previous version listed JSON as a current graph import/export format. There is no product-owned JSON graph export/import path in current `src/`, `deps/`, or `packages/` code. The old Roam JSON code in [src/main/frontend/handler/export.cljs](../../../src/main/frontend/handler/export.cljs) is commented out and should not be documented as an active capability.

## DB EDN Format

The current DB EDN path is implemented in [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs) via `build-export` and `build-import`.

For a full graph export, the public shape is centered on these top-level keys:

```clojure
{:pages-and-blocks [...]
 :properties {...}
 :classes {...}}
```

For graph exports, Logseq may also include namespaced implementation keys such as:

```clojure
{:logseq.db.sqlite.export/schema-version ...
 :logseq.db.sqlite.export/export-type ...
 :logseq.db.sqlite.export/graph-files [...]
 :logseq.db.sqlite.export/kv-values [...]
 :logseq.db.sqlite.export/property-history #{...}}
```

Treat those namespaced keys as part of Logseq's internal round-trip format, not as a stable third-party contract.

Key behavior in the current exporter:

- Pages and blocks are exported under `:pages-and-blocks`, not `:blocks`.
- User properties and classes are exported separately under `:properties` and `:classes`.
- Graph exports can include file payloads, `:kv/value` records, and property history.
- `:logseq.kv/schema-version` is intentionally excluded from exported kv values so the importing app keeps control of its own schema version.
- Graph options currently supported by the exporter include `:include-timestamps?`, `:exclude-built-in-pages?`, `:exclude-namespaces`, and `:exclude-files?`.

> **Removed:** The previous version documented a top-level `{:version 1 :blocks ... :closed-values ... :property-values ... :metadata ...}` structure and pointed to `sqlite.build.cljs` as the primary implementation. That no longer matches the current owning export path. The graph export contract is built in [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs), while `sqlite.build` is the lower-level tx/validation layer used underneath it.

## SQLite and Zip Workflows

### SQLite database

SQLite export/import is the canonical full-fidelity backup path for DB graphs.

- UI export uses `export-repo-as-sqlite-db!` in [src/main/frontend/handler/export.cljs](../../../src/main/frontend/handler/export.cljs).
- CLI export uses `graph export --type sqlite` in [src/main/logseq/cli/command/graph.cljs](../../../src/main/logseq/cli/command/graph.cljs).
- UI sqlite import reads the selected file as an `ArrayBuffer` and hands it to `import-from-sqlite-db!` in [src/main/frontend/handler/db_based/import.cljs](../../../src/main/frontend/handler/db_based/import.cljs).
- CLI sqlite import uses `:thread-api/import-db-binary`.

Use SQLite export when you need the closest thing to a raw DB restore.

### SQLite zip

Zip export/import is the current asset-aware packaging path.

- Zip export gathers `db.sqlite` plus asset files in `db-based-export-repo-as-zip!` in [src/main/frontend/handler/export.cljs](../../../src/main/frontend/handler/export.cljs).
- Zip import locates `db.sqlite` inside the archive and then copies asset files into the graph's assets directory in `import-from-sqlite-zip!` and `<copy-zip-assets!` in [src/main/frontend/handler/db_based/import.cljs](../../../src/main/frontend/handler/db_based/import.cljs).

Use zip when the graph must arrive with both database state and local assets.

## Transit Serialization

Transit is still used, but its current role is narrower than the earlier document implied.

- Transit read/write helpers live in [deps/db/src/logseq/db/sqlite/util.cljs](../../../deps/db/src/logseq/db/sqlite/util.cljs).
- `write-transit-str` and `read-transit-str` use Transit **JSON** with custom handlers for DataScript entities and errors.
- The user-facing debug export path writes a `.transit` file from `ldb/write-transit-str` in [src/main/frontend/handler/export.cljs](../../../src/main/frontend/handler/export.cljs).
- The matching import path is `import-from-debug-transit!` in [src/main/frontend/handler/db_based/import.cljs](../../../src/main/frontend/handler/db_based/import.cljs).

Use Transit for support/debug workflows, not for the main graph portability story.

> **Removed:** The previous version described Transit as the main performance-oriented backup format and called it binary. Current product code uses SQLite for full-fidelity backups and uses Transit JSON for the dedicated debug import/export path.

## Import Implementation

Current DB EDN import is not a separate version-dispatch pipeline. The actual path is:

1. Build or read export data.
2. Convert it to import txs with `sqlite-export/build-import`.
3. Execute those txs through the outliner/import transaction path.
4. Validate behavior through round-trip tests and explicit export validation.

The main entry points are:

- UI DB EDN import: `import-from-edn-file!` in [src/main/frontend/handler/db_based/import.cljs](../../../src/main/frontend/handler/db_based/import.cljs)
- Outliner operation: `import-edn-data` in [deps/outliner/src/logseq/outliner/op.cljs](../../../deps/outliner/src/logseq/outliner/op.cljs)
- Core import builder: `build-import` in [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs)
- CLI DB EDN import: `graph import --type edn` in [src/main/logseq/cli/command/graph.cljs](../../../src/main/logseq/cli/command/graph.cljs)

Important current behaviors:

- Existing pages are matched by journal day or page title and can be imported onto existing page UUIDs.
- Existing properties and classes are reconciled by ident.
- Import rejects conflicting property definitions when cardinality or type no longer match.
- Graph imports may carry files, kv values, and property history through `:misc-tx`.
- There is an internal `:existing-pages-keep-properties?` import option for preserving properties already on an existing page.

> **Removed:** The previous version documented explicit format-version dispatch and migration examples such as `import-format-v1`, `import-format-v2`, and `migrate-format-v1->v2`. Those do not exist in the current import pipeline.

## Validation and Compatibility

The current source relies on executable validation more than on declarative format-version metadata.

- `build-export` performs structural checks via `basic-validate-export`.
- `validate-export` creates an in-memory DB, imports the exported EDN, and runs DB validation.
- The UI DB EDN export path validates the current graph before writing the file.
- Round-trip tests in [deps/db/test/logseq/db/sqlite/export_test.cljs](../../../deps/db/test/logseq/db/sqlite/export_test.cljs) cover graph export/import behavior, existing-page reconciliation, empty `:build/properties` omission, ontology handling, and asset-related cases.

Current compatibility guidance:

- Do not invent a new top-level export version field unless the importer is updated to use it.
- If you change graph export shape, update both `build-export` and `build-import` together.
- Prefer additive shape changes that preserve current importer behavior.
- Back new export/import behavior with round-trip tests, not just examples in docs.
- Keep CLI and UI semantics aligned when adding or changing graph options.

## Best Practices for Code Changes

1. Treat SQLite as the default backup/restore path for DB graphs.
2. Use zip when assets must round-trip with the database.
3. Use DB EDN for graph-agnostic interchange, migration work, and test fixtures.
4. Treat debug Transit as a support format, not the main user backup format.
5. Preserve UUIDs only where the exporter/importer already models them; current code deliberately removes non-reference UUIDs from parts of graph export to keep payloads smaller and cleaner.
6. Do not export `:logseq.kv/schema-version`; the importer should not overwrite the app's schema version from payload data.
7. When changing property/class export behavior, verify both export validation and import conflict handling.
8. When changing zip workflows, test both `db.sqlite` extraction and asset copying.
9. When changing CLI import/export flags, update [src/main/logseq/cli/command/graph.cljs](../../../src/main/logseq/cli/command/graph.cljs) and keep EDN-only options invalid for sqlite export.
10. Prefer executable round-trip tests in [deps/db/test/logseq/db/sqlite/export_test.cljs](../../../deps/db/test/logseq/db/sqlite/export_test.cljs) over prose examples.

## Findings: New Capabilities

Compared with the previous document revision, the current source now clearly exposes capabilities that were either missing from the doc or underspecified:

- The CLI has first-class `graph export` and `graph import` commands for both `edn` and `sqlite`, plus graph backup list/create/restore/remove commands in [src/main/logseq/cli/command/graph.cljs](../../../src/main/logseq/cli/command/graph.cljs).
- The UI has an asset-aware sqlite zip workflow, with export in [src/main/frontend/handler/export.cljs](../../../src/main/frontend/handler/export.cljs) and import in [src/main/frontend/handler/db_based/import.cljs](../../../src/main/frontend/handler/db_based/import.cljs).
- Graph export can carry graph files, kv values, and property history through namespaced export keys in [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs).

## Removed Claims From The Previous Revision

> **Removed:** The old document's EDN examples were based on a `:blocks` vector and top-level `:version` field that do not match current graph export output.
> **Removed:** The old document described Transit as the main backup format and as a binary serialization path. Current product backup flows center on sqlite and sqlite zip; Transit is used for debug import/export and uses Transit JSON helpers.
> **Removed:** The old document described JSON as an active product import/export capability. No current product-owned graph JSON import/export implementation exists.
> **Removed:** The old document described an explicit format-version migration layer with example dispatcher functions that are not present in the current importer.
