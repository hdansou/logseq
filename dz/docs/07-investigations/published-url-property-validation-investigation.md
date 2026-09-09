# Investigation: `:logseq.property/view-context` rejected on `Published URL` property

**Investigation Date:** February 17, 2026  
**Last Verified:** May 30, 2026

> **Last revised:** 2026-05-30

## 1) Executive Summary

- Validation error is raised in closed-schema malli validation for property entities and reported as `{"user-property" ["disallowed key"]}` for `:logseq.property/view-context`.
- `Published URL` is defined as a built-in property with `:view-context :page`, so that key is expected for internal properties.
- The failure mode occurs when the property classifier routes an entity into `user-property` schema instead of `internal-property` schema.
- Root-risk is schema dispatch/classification drift (especially across versions/migrations), not the `Published URL` data itself.
- Current protection is namespace/db-attribute based dispatch (`logseq-property?` and db-attribute checks), not a dedicated `:logseq.property/built-in?` guard.

## Current Status (Verified 2026-05-30)

- No dedicated `:logseq.property/built-in?` dispatch guard exists in `property-page`.
- `Published URL` remains protected by internal classification because `:db/ident` is in the whitelisted `logseq.property.publish` namespace.
- Migration entry `65.17` remains the introduction point for `:logseq.property.publish/published-url`.
- Keep this investigation as a schema-dispatch reference.

> **Removed:** Earlier revisions that described a dedicated `:logseq.property/built-in?` dispatch guard in `property-page`.

## Findings: New Capabilities

- Publish now persists a deterministic fallback URL (`/page/{graph-id}/{page-uuid}`) when the publish API response does not include `:short_url`, and still writes it to `:logseq.property.publish/published-url`.

## 2) Technical Deep Dive

### Component A — Built-in property definition

- **Purpose:** Declares `Published URL` as a built-in property and schema metadata.
- **Location:** `deps/db/src/logseq/db/frontend/property.cljs`.
- **Key references:**
  - `:logseq.property.publish/published-url` definition: includes `:type :url`, `:view-context :page`, `:public? true`.
  - Namespace whitelist (`logseq-property-namespaces`) includes `"logseq.property.publish"`.
- **Data structure:** built-in property map keyed by `:db/ident`.

### Component B — Property schema dispatch (critical)

- **Purpose:** Selects `internal`, `plugin`, or `user` malli schema for each property page/entity.
- **Location:** `deps/db/src/logseq/db/frontend/malli_schema.cljs`.
- **Key functions:** `property-page`, `internal-property`, `user-property`.
- **Behavior difference that matters:**
  - `internal-property` allows `:logseq.property/view-context`.
  - `user-property` does not allow `:logseq.property/view-context`.

### Component C — Validation engine and error reporting

- **Purpose:** Runs closed-schema validation and groups humanized errors by entity.
- **Location:** `deps/db/src/logseq/db/frontend/validate.cljs`.
- **Key functions:** `validate-db!`, `validate-local-db!`.
- **Observed error shape:** `{:logseq.property/view-context {"user-property" ["disallowed key"]}}`.

### Component D — Runtime usage path for `Published URL`

- **Purpose:** Publish action writes/removes the property on pages.
- **Location:** `src/main/frontend/handler/publish.cljs`.
- **Key functions:** `publish-page!`, `unpublish-page!`.
- **Persistence touchpoint:** property is written with `set-block-property!` and removed with `remove-block-property!`.

### Component E — Migration introduction point

- **Purpose:** Adds the built-in property in schema migrations.
- **Location:** `src/main/frontend/worker/db/migrate.cljs`.
- **Key reference:** `schema-version->updates` entry `65.17` adds `:logseq.property.publish/published-url`.

## 3) Flow Diagrams

### A. Validation flow for this error

```text
Entity read from DB
   ↓
update-properties-in-ents
   ↓
Data(:property) dispatch
   ↓
property-page dispatch
   ├─ internal-property  (allows :view-context)
   └─ user-property      (disallows :view-context)
   ↓
closed-schema explain + humanize
   ↓
"user-property disallowed key"
```

### B. Publish runtime flow

```text
User clicks Publish
   [0ms]
      ↓
publish-page! builds payload + sends API request
      ↓
on success sets :logseq.property.publish/published-url
      ↓
entity persisted + later validation run checks schema
```

## 4) Comparison Table

| Aspect                          | Internal property schema   | User property schema  | Impact for this error                          |
| ------------------------------- | -------------------------- | --------------------- | ---------------------------------------------- |
| `:logseq.property/view-context` | Allowed                    | Disallowed            | Error appears only if misrouted to user schema |
| `:db/ident` expectation         | logseq/db-attribute idents | user namespace idents | Classifier must route correctly                |
| Intended for built-ins          | Yes                        | No                    | `Published URL` should be internal             |

## 5) Scenario Analysis

### Scenario A — Expected behavior (no error)

- Entity is classified as internal property.
- `:logseq.property/view-context :page` is accepted.
- Validation passes for this entity.
- **Risk:** 🟢 Low.

### Scenario B — Misclassification to user-property

- Entity is classified as user property.
- Same `:view-context` key becomes illegal in that schema.
- Validation reports `disallowed key` and entity is flagged invalid.
- **Risk:** 🟡 Medium (false-positive invalidation, noisy repair path).

## 6) Code Location Reference

| File                                               | Purpose                                                   | Key symbols                                           |
| -------------------------------------------------- | --------------------------------------------------------- | ----------------------------------------------------- |
| `deps/db/src/logseq/db/frontend/property.cljs`     | Built-in property declarations + namespace classification | `built-in-properties`, `logseq-property-namespaces`   |
| `deps/db/src/logseq/db/frontend/malli_schema.cljs` | Property malli schemas + dispatch                         | `internal-property`, `user-property`, `property-page` |
| `deps/db/src/logseq/db/frontend/validate.cljs`     | DB/entity validation + error grouping                     | `validate-db!`, `validate-local-db!`                  |
| `src/main/frontend/worker/db/migrate.cljs`         | Schema version migrations                                 | `schema-version->updates` (`65.17`)                   |
| `src/main/frontend/handler/publish.cljs`           | Publish/unpublish write path                              | `publish-page!`, `unpublish-page!`                    |

## 7) Recommendations

### For Users

- If this appears on existing graphs, re-run validation after upgrading to a recent build.
- If still present, inspect the affected property entity for namespace/classification drift (for example, incorrect `:db/ident` namespace).

### For Developers

- Dispatch currently relies on `:db/ident` namespace and db-attribute classification.
- `Published URL` is classified as internal because `logseq-property-namespaces` includes `logseq.property.publish`.
- Keep migration-introduced built-ins in sync with classifier assumptions.
- If future migrations expose mixed legacy shapes, consider an explicit temporary compatibility branch with clear removal criteria.

### For Testing

- Keep/expand validation coverage around built-in property entities that include `:view-context`.
- Add compatibility tests for migrated built-ins across schema versions.

## 8) Appendices

### Glossary

- **Internal property:** Logseq-owned property (`logseq.property.*` or db-attribute) validated by `internal-property` schema.
- **User property:** Custom property in user namespace, validated by `user-property` schema.
- **Closed schema:** Malli mode where unknown keys are rejected.

### Change History

- 2026-02-17: Investigation created for `Published URL` validation mismatch.
- 2026-05-25: Verified current code paths; corrected earlier note that claimed a `:logseq.property/built-in?` dispatch guard.

---

## Investigated Error Payload (verbatim)

```clojure
{:dispatch-key :property,
 :errors {:logseq.property/view-context {"user-property" ["disallowed key"]}}}
```
