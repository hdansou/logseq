# Schema Changes & Data Integrity Investigation

> **Update 2026-10-04:** this investigation analyzed schema **65.33**. The schema is now **65.34** (`deps/db/src/logseq/db/frontend/schema.cljs:33`), which adds a repair migration, `db-order/missing-internal-page-parent-order-tx`, that gives nested pages with a parent but no `:block/order` a deterministic order (`68a68263b7`). Statements below that call 65.33 the current version describe the analyzed snapshot; nothing else here was re-verified.

## Part 4: Import/Export & Validation Analysis

> **Last revised:** 2026-05-30

---

## Table of Contents

1. [Export System Architecture](#export-system-architecture)
2. [Import System Architecture](#import-system-architecture)
3. [Validation System](#validation-system)
4. [EDN/Transit Format Notes](#edntransit-format-notes)
5. [SQLite Export/Import](#sqlite-exportimport)
6. [Schema Version Compatibility](#schema-version-compatibility)
7. [Findings: New Capabilities](#findings-new-capabilities)
8. [Failure Scenarios](#failure-scenarios)

---

## Export System Architecture

### Current architecture (validated)

The DB export pipeline is implemented in `deps/db/src/logseq/db/sqlite/export.cljs` and is based on `sqlite.build`-compatible maps rather than a single fixed `{:version ... :entities ...}` envelope.

Primary entry point:

- `build-export` dispatches by `:export-type` (`:block`, `:page`, `:view-nodes`, `:selected-nodes`, `:graph-ontology`, `:graph`).

For `:graph` export specifically, output is assembled from:

- `:properties`
- `:classes`
- `:pages-and-blocks`
- namespaced metadata keys (for example `::schema-version`, `::graph-files`, `::kv-values`, `::property-history`)

The graph export path also supports options such as:

- `:include-timestamps?`
- `:exclude-namespaces`
- `:exclude-built-in-pages?`
- `:exclude-files?`

and performs internal consistency checks via `basic-validate-export`.

> **Removed:** Prior claim that exports are built as a fixed map with top-level keys `:version`, `:schema-version`, `:entities`, and `:datoms` from `build-export`.
> **Removed:** Prior claim that export validation blocks all export paths by throwing before save as a universal behavior.

---

## Import System Architecture

### Current import architecture (validated)

Import for EDN data is built around `build-import` in `deps/db/src/logseq/db/sqlite/export.cljs`.

`build-import`:

- accepts an export map and current DB
- resolves existing entities to avoid duplicate pages/classes/properties
- checks property schema conflicts (`:logseq.property/type`, `:db/cardinality`)
- returns transaction groups `:init-tx`, `:block-props-tx`, `:misc-tx`, or returns `{:error ...}` on conflicts

Main app import handlers live in:

- `src/main/frontend/handler/db_based/import.cljs`

Supported import paths include:

- EDN import into a new graph
- pasted EDN import via dialog
- SQLite DB import
- SQLite ZIP import (`db.sqlite` + asset extraction)
- debug transit import

> **Removed:** Prior claim that import is primarily a version-gated `import-with-migration` flow in this file.
> **Removed:** Prior claim that import uses a generic `transform-entity` function with hard-coded attribute rename/add/remove branches.

---

## Validation System

### What is currently true

Validation behavior is split across transaction-time validation and explicit full-db validation:

1. Transaction-time validation

- Implemented in `deps/db/src/logseq/db.cljs` (`transact-sync` + `should-validate-tx?`)
- Uses `logseq.db.frontend.validate/validate-tx-report`
- Rejects invalid tx with `ex-info` and debug payload

1. Full DB validation

- Implemented in `deps/db/src/logseq/db/frontend/validate.cljs`
- `validate-db` and `validate-local-db!` run full-graph checks

1. Export validation helper

- `validate-export` in `deps/db/src/logseq/db/sqlite/export.cljs` imports EDN into a fresh in-memory conn and runs `validate-local-db!`
- returns `{:error ...}` instead of throwing for validation failures

Important nuance:

- Transaction validation is not literally unconditional. It can be skipped via tx-meta and internal flags (for example `:skip-validate-db?`, initial/reset/RTC cases).

> **Removed:** Prior claim that transaction-time validation "cannot be disabled".

---

## EDN/Transit Format Notes

Current serialization helpers are in `deps/db/src/logseq/db/sqlite/util.cljs`:

- `write-transit-str`
- `read-transit-str`

They include handlers for DataScript entities and error objects.

The current EDN export payload for DB graph operations is a `sqlite.build`-style structure (properties/classes/pages-and-blocks + namespaced metadata), not the old fixed datom envelope shown previously.

> **Removed:** Prior claim that `serialize-export`/`deserialize-export` are defined in `sqlite/export.cljs` as top-level Transit JSON wrappers.

---

## SQLite Export/Import

SQLite export/import flows are active and expanded:

- SQLite file export (`export-repo-as-sqlite-db!`) in `src/main/frontend/handler/export.cljs`
- DB+assets ZIP export (`db-based-export-repo-as-zip!`) in `src/main/frontend/handler/export.cljs`
- SQLite DB import (`import-from-sqlite-db!`) in `src/main/frontend/handler/db_based/import.cljs`
- SQLite ZIP import with asset copy (`import-from-sqlite-zip!`) in `src/main/frontend/handler/db_based/import.cljs`

---

## Schema Version Compatibility

Current frontend schema version constant is:

- `logseq.db.frontend.schema/version` = `65.33`

Compatibility checks are done via:

- `parse-schema-version`
- `compare-schema-version`

Graph export stamps namespaced schema metadata (`::schema-version`) during `:graph` export generation.

> **Removed:** Prior matrix and examples based on `65.16` as current schema.

---

## Findings: New Capabilities

Since the prior investigation version, the current codebase includes notable import/export capabilities that should be tracked explicitly:

1. SQLite ZIP round-trip with assets

- Export: `db.sqlite` plus assets bundled into zip (`db-based-export-repo-as-zip!`)
- Import: zip parsing, SQLite extraction, and asset copy progress/error reporting (`import-from-sqlite-zip!`)

1. CLI graph backup lifecycle

- `graph backup list/create/restore/remove` flows are implemented in `src/main/logseq/cli/command/graph.cljs`

1. CLI EDN export controls

- `--include-timestamps`
- `--exclude-built-in-pages`
- `--exclude-namespaces`

1. Import conflict safety

- `build-import` detects property definition conflicts and returns readable import errors instead of silently coercing

1. Export/import integrity test depth

- `deps/db/test/logseq/db/sqlite/export_test.cljs` includes a broad suite for graph/page/block/ontology/view-node/selected-node and validation consistency cases

---

## Failure Scenarios

### 1) Property definition conflicts during import

If imported property definitions disagree with existing graph definitions (`:logseq.property/type` or `:db/cardinality`), import returns an error map. This is now an explicit guardrail.

### 2) EDN payload not build-compatible

Invalid EDN shape or unresolved references can fail during `build-import` or during tx application.

### 3) Validation failures after import simulation

`validate-export` can return `{:error ...}` if the exported payload cannot be cleanly imported and validated in a fresh graph.

### 4) Asset partial failures in ZIP import

SQLite DB may import successfully while some assets are skipped; user notifications report copied/skipped/partial counts.

---

## Key Takeaways (Updated)

1. DB export/import is `sqlite.build`-map based, not fixed entity/datoms envelope based.
2. Validation exists at transaction-time, full-db, and export-simulation layers, each with different semantics.
3. Transaction validation has explicit skip pathways for specific internal flows.
4. Schema version constant is currently `65.33`.
5. SQLite ZIP + assets is a first-class path, not just plain `.sqlite` copy.
6. CLI now supports graph backup lifecycle commands and richer EDN export options.

---

## Scope Notes

Validation for this revision was performed against current sources in:

- `src/`
- `deps/`
- `packages/`

No import/export schema-integrity control plane was found in `packages/`; relevant behavior remains concentrated in `src/` and `deps/`.
