# Graph Validation Developer Guide

> **Last revised:** 2026-05-29
> **Previously revised:** 2026-05-28
> **Change summary:** Fixed stale line numbers (`shortcut/config.cljs` 516-519→521-524, `db_core.cljs` 1140-1143→1472-1475); corrected `validate-db!` → `validate-db` in `deps/db/…/validate.cljs`; fixed `validate-tx-report!` → `validate-tx-report`; corrected `:skip-validate-db?` tx-meta key (was incorrectly namespaced as `::validate/skip-validate-db?`); removed wrong `validate.cljs:43` claim for `:block/warning` auto-retract; added `validate-local-db!` under New Capabilities.

## Overview

The "Validate current Graph" feature is a developer-only tool that validates the integrity of a Logseq DB-based graph's database. It checks for schema violations, structural inconsistencies, and invalid entities, then attempts to automatically fix detected issues.

**Command**: `:dev/validate-db`
**UI Label**: "(Dev) Validate current graph"
**Access**: Developer Mode only
**Performance**: ~1-5 seconds for typical graphs

## Architecture Overview

```
User triggers validation
        ↓
Command Palette (:dev/validate-db)
        ↓
frontend.handler.common.developer/validate-db
        ↓
DB Worker Thread API (:thread-api/validate-db)
        ↓
worker.db.validate/validate-db
        ↓
db.frontend.validate/validate-db
        ↓
Schema Validation (Malli)
        ↓
Auto-fix (fix-invalid-blocks!)
        ↓
User Notification
```

## How to Use

### Accessing the Feature

1. **Enable Developer Mode**: Settings → Advanced → Enable Developer Mode
2. **Open Command Palette**: `Cmd+Shift+P` (Mac) or `Ctrl+Shift+P` (Windows/Linux)
3. **Search**: Type "validate" or "validate graph"
4. **Execute**: Select `:dev/validate-db`

### Expected Output

**Success Case**:

```
Your graph is valid! {:entities 5000, :pages 100, :blocks 4500, :classes 50, :properties 200, :objects 120, :property-pairs 800, :datoms 120000}
```

**Error Case**:

```
Validation detected 15 invalid block(s). These blocks may be buggy.
Attempting to fix invalid blocks. Run validation again to see if they were fixed.
```

## Code Walkthrough

### 1. UI Entry Point

**File**: `src/main/frontend/handler/common/developer.cljs:91-92`

```clojure
(defn ^:export validate-db []
  (state/<invoke-db-worker :thread-api/validate-db (state/get-current-repo)))
```

This public function is triggered from the command palette and invokes the worker thread.

**Command Registration**: `src/main/frontend/modules/shortcut/config.cljs:521-524`

```clojure
:dev/validate-db {:binding []
                  :desc "(Dev) Validate current graph"
                  :inactive (not (state/developer-mode?))
                  :fn :frontend.handler.common.developer/validate-db}
```

### 2. Worker Thread API

**File**: `src/main/frontend/worker/db_core.cljs:1472-1475`

> **Note:** `db_worker.cljs` is a thin bootstrap entrypoint; all thread API definitions live in `db_core.cljs`.

```clojure
(def-thread-api :thread-api/validate-db
  [repo & [opts]]
  (when-let [conn (worker-state/get-datascript-conn repo)]
    (worker-db-validate/validate-db conn opts)))
```

The optional `opts` map is forwarded to `validate-db`. Returns `nil` silently if the conn isn't found.

### 3. Main Validation Orchestrator

**File**: `src/main/frontend/worker/db/validate.cljs:286-325`

```clojure
(defn validate-db
  [conn & {:keys [fix] :or {fix true}}]
  (when fix
    (fix-extends-cardinality! conn)
    (fix-icon-wrong-type! conn)
    (db-migrate/ensure-built-in-data-exists! conn)
    (fix-non-closed-values! conn)
    (fix-num-prefix-db-idents! conn))

  (let [{:keys [errors datom-count entities invalid-entity-ids]}
        (if fix
          (validate-and-fix-invalid-blocks! conn)   ; loops until stable or no progress
          (let [{:keys [errors] :as result} (validate-db-result @conn)]
            (log-validation-errors! errors)
            result))
        db @conn]
    (if errors
      (do
        (shared-service/broadcast-to-clients! :log [:db-invalid :error ...])
        (shared-service/broadcast-to-clients! :notification
          [(str "Validation detected " (count errors) " invalid block(s)...") :warning false]))
      (shared-service/broadcast-to-clients! :notification
        [(str "Your graph is valid! " (assoc (db-validate/graph-counts db entities) :datoms datom-count))
         :success false]))
    {:errors errors
     :datom-count datom-count
     :invalid-entity-ids invalid-entity-ids}))
```

### 4. Core Schema Validation

**File**: `deps/db/src/logseq/db/frontend/validate.cljs:90-109`

```clojure
(defn validate-db
  "Validates all the entities of the given db using :eavt datoms. Returns a map
  with info about db being validated. If there are errors, they are placed on
  :errors and grouped by entity"
  [db]
  (let [datoms (d/datoms db :eavt)
        ent-maps* (db-malli-schema/datoms->entities datoms)
        ent-maps (mapv
                  ;; Remove some UI interactions adding this e.g. import
                  #(dissoc % :block.temp/load-status :block.temp/has-children?)
                  (db-malli-schema/update-properties-in-ents db ent-maps*))
        errors (binding [db-malli-schema/*db-for-validate-fns* db]
                 (-> (map (fn [e] (dissoc e :db/id)) ent-maps)
                     closed-db-schema-explainer :errors))]
    (cond-> {:datom-count (count datoms)
             :entities ent-maps*}
      (some? errors)
      (assoc :errors (map #(update % :errors (fn [errs] (me/humanize {:errors errs})))
                          (group-errors-by-entity db ent-maps errors))))))
```

This function:

1. Converts all datoms to entity maps
2. Validates each entity against the Malli schema
3. Returns errors and statistics

## What Gets Validated

### 1. Block Structure

- **Block Title**: All blocks must have a `:block/title`
- **Block Page**: All blocks must reference a `:block/page`
- **Block Parent**: All blocks must have a `:block/parent`

### 2. Property/Class References

- User class references (`:user.class/*`) must be valid
- Table sized-columns must be properly structured
- Property values must be correctly linked

### 3. Entity Integrity

- Classes must extend from valid parent classes
- Properties must have proper tags
- Internal pages must have correct tags
- Assets must have proper type metadata

### 4. Schema Compliance

All entities are validated against the Malli schema defined in:
`deps/db/src/logseq/db/frontend/malli_schema.cljs`

## Auto-Fix Capabilities

**File**: `src/main/frontend/worker/db/validate.cljs`

The `fix-invalid-blocks!` function attempts to repair issues:

### Fix Categories

1. **Missing Block Title** → Retract the entity
2. **Missing Block Page** → Add to latest journal or parent's page
3. **Missing Block Parent** → Set parent to the block's page
4. **Invalid Property References** → Fix user.class references to proper property IDs
5. **Temporary Attributes** → Remove `block.temp/*` attributes
6. **Missing Class Extends** → Add default `:logseq.class/Root` extension
7. **Invalid Tags** → Remove inappropriate tags from internal pages
8. **Corrupted Assets** → Retract assets with invalid metadata

### Example Fix Logic

> **Note:** The code below is illustrative — the actual implementation (line 35) uses `mapcat` to accumulate all tx-data and applies it in a single `d/transact!` call. There is no `block-entity?` helper; dispatch is done via the `:dispatch-key` field on the error map.

```clojure
;; Illustrative sketch of fix-invalid-blocks! shape
;; See src/main/frontend/worker/db/validate.cljs:35 for full implementation
(defn- fix-invalid-blocks!
  [conn errors]
  (let [db @conn
        fix-tx-data (mapcat
                     (fn [{:keys [entity dispatch-key]}]
                       (let [entity (d/entity db (:db/id entity))]
                         (cond
                           ;; Legacy attributes
                           (some? (:logseq.property/parent entity))
                           [[:db/retract (:db/id entity) :logseq.property/parent]]
                           ;; block missing page (dispatch-key :block)
                           (and (= dispatch-key :block) (nil? (:block/page entity)))
                           (let [page-id (or (:db/id (:block/page (:block/parent entity)))
                                             (:db/id (first (ldb/get-latest-journals db))))]
                             (when page-id
                               [[:db/add (:db/id entity) :block/page page-id]]))
                           ;; ... many more cases ...
                           :else nil)))
                     errors)]
    (when (seq fix-tx-data)
      (d/transact! conn fix-tx-data {:fix-db? true}))))
```

## Automatic Validation During Development

**File**: `deps/db/src/logseq/db/frontend/validate.cljs`

Transaction-level validation is available via `validate-tx-report` which validates entities affected by a specific transaction:

```clojure
(defn validate-tx-report
  "Validates the datascript tx-report for entities that have changed. Returns
  boolean indicating if db is valid"
  [{:keys [db-after tx-data tx-meta]} {:keys [closed-schema?]}]
  ...)
```

> **Note:** The pipeline-level `validate-db!` function that previously lived in `src/main/frontend/worker/pipeline.cljs` has been removed. Transaction validation is now handled directly in the DB validation layer.

## Configuration

### Graph Config Options

**File**: `src/main/frontend/handler/events.cljs` and `src/main/frontend/persist_db/browser.cljs`

```clojure
:validate-db-options (:dev/validate-db-options (state/get-config))
```

Add to your graph's `logseq/config.edn`:

```clojure
:dev/validate-db-options {:fail-on-invalid? true
                          :closed-schema? true}
```

Options:

- `:fail-on-invalid?` - Throw errors on validation failures (default: false in prod)
- `:closed-schema?` - Use closed schema validation (stricter)

## Notification System

### Success Notification

```clojure
{:type :success
 :msg "Your graph is valid! {:entities 5000, :pages 100, :blocks 4500, :classes 50, :properties 200, :datoms 120000}"}
```

### Error Notification

```clojure
{:type :warning
 :msg "Validation detected 15 invalid block(s). These blocks may be buggy. Attempting to fix invalid blocks. Run validation again to see if they were fixed."}
```

### Console Logging

Detailed error logs are sent to the browser console:

```clojure
{:level :error
 :payload [:div "Invalid entities:" "[1234 5678 9012]"]}
```

## Validation vs. Garbage Collection

| Feature         | Validate DB               | GC Graph                 |
| --------------- | ------------------------- | ------------------------ |
| **Purpose**     | Check schema integrity    | Remove orphaned data     |
| **Trigger**     | Manual only               | Auto (30 days) + Manual  |
| **Auto-fixes**  | Yes, attempts repairs     | No, just deletes         |
| **Report Type** | Validation errors         | Space reclaimed          |
| **Dev Mode**    | Required                  | Required                 |
| **Performance** | ~1-5 seconds              | ~5-30 seconds            |
| **Destructive** | Fixes orphaned entities   | Deletes unused addresses |
| **Location**    | `worker/db/validate.cljs` | `deps/db/sqlite/gc.cljs` |

## Related Validation Layers

### 1. Transaction Validation

**File**: `deps/db/src/logseq/db/frontend/validate.cljs`

```clojure
(defn validate-tx-report
  "Validates the datascript tx-report for entities that have changed. Returns
  boolean indicating if db is valid"
  [{:keys [db-after tx-data tx-meta]} {:keys [closed-schema?]}]
  ;; Validates only entities affected by the transaction
  )
```

### 2. Block Title Validation

**File**: `deps/outliner/src/logseq/outliner/validate.cljs`

```clojure
(defn validate-block-title
  "Validates block title meets requirements"
  [title]
  ;; Check for illegal characters, length limits, etc.
  )
```

### 3. Property Name Validation

```clojure
(defn validate-property-title
  "Validates property naming rules"
  [title]
  ;; Ensures property names follow conventions
  )
```

### 4. Unique Constraint Validation

Prevents duplicate:

- Page names
- Property names
- Class names

### 5. Page Title Character Validation

Prevents illegal characters:

- `#` (hash)
- `/` (slash)

## Debugging Tips

### 1. Finding Invalid Entities

When validation fails, check the console for invalid entity IDs:

```javascript
// In browser console
logseq.api.datascript_query(`
  [:find ?e ?title
   :where
   [?e :db/id ?id]
   [(contains? #{1234 5678 9012} ?id)]
   [(get-else $ ?e :block/title "") ?title]]
`)
```

### 2. Inspecting Schema

View the Malli schema:

```clojure
(require '[logseq.db.frontend.malli-schema :as schema])
(clojure.pprint/pprint schema/closed-db-schema)
```

### 3. Manual Validation in REPL

```clojure
(require '[logseq.db.frontend.validate :as validate])
(require '[datascript.core :as d])

(let [conn (get-conn "graph-name")
      db @conn]
  (validate/validate-db db))
```

### 4. Skipping Validation

For performance-critical operations:

```clojure
(d/transact! conn
  [{:db/id 123 :block/title "foo"}]
  {:skip-validate-db? true})
```

## Common Issues

### Issue 1: Orphaned Blocks

**Symptom**: Blocks without `:block/page` or `:block/parent`

**Fix**: Validation will attempt to:

1. Assign to latest journal
2. Or assign to parent's page
3. Or retract if unfixable

### Issue 2: Invalid Property References

**Symptom**: `:user.class/*` references pointing to non-existent properties

**Fix**: Auto-converts to proper property IDs or retracts

### Issue 3: Missing Class Extensions

**Symptom**: Classes without `:logseq.class/parent`

**Fix**: Adds `:logseq.class/Root` as default parent

### Issue 4: Temporary Attributes Leaking

**Symptom**: `block.temp/*` attributes persisting in DB

**Fix**: Retracts all temporary attributes

## Performance Considerations

### Validation Speed

- **Small graphs** (< 1000 entities): < 1 second
- **Medium graphs** (1000-10000 entities): 1-3 seconds
- **Large graphs** (> 10000 entities): 3-5 seconds

### Optimization Tips

1. **Run during off-peak**: Validation blocks writes temporarily
2. **Skip import validation**: Already disabled by default
3. **Batch fixes**: Auto-fix runs in single transaction
4. **Use in dev only**: Production has validation disabled

## File Reference

| File                                               | Purpose                                |
| -------------------------------------------------- | -------------------------------------- |
| `src/main/frontend/worker/db_core.cljs`            | Worker thread API (line 1472)          |
| `src/main/frontend/handler/common/developer.cljs`  | UI entry point (lines 91-92)           |
| `src/main/frontend/worker/db/validate.cljs`        | Main orchestration & auto-fix          |
| `deps/db/src/logseq/db/frontend/validate.cljs`     | Core schema validation + tx validation |
| `deps/db/src/logseq/db/frontend/malli_schema.cljs` | Entity schema definitions              |
| `src/resources/dicts/en.edn`                       | UI labels                              |

## Testing

### Unit Tests

Check for existing tests in:

- `src/test/frontend/worker/db/validate_test.cljs`
- `deps/db/test/logseq/db/frontend/validate_test.cljs`

### Manual Testing

1. **Create Invalid Data**:

   ```clojure
   ;; Directly insert invalid block
   (d/transact! conn [{:block/uuid (random-uuid)}])
   ```

2. **Run Validation**: Execute `:dev/validate-db`

3. **Check Logs**: Review browser console for errors

4. **Re-run Validation**: Verify auto-fixes worked

## Programmatic Access

### From Developer Console

The validation function is exported and can be called directly:

```javascript
// Direct call to the exported function
window.frontend.handler.common.developer.validate_db()
```

This is the most reliable method from the browser console.

### From Plugins

Plugins can trigger validation using the external command API:

```javascript
// Trigger validation from a plugin
await logseq.App.invokeExternalCommand('logseq.dev/validate-db')
```

**Important Notes:**

- The `validate-db` function is **not** exposed in `logseq.api` (the plugin SDK)
- It's marked with `^:export` but not registered in `src/main/logseq/api.cljs`
- Plugins must use `invokeExternalCommand` to trigger it indirectly
- The command ID requires the `logseq.` namespace prefix: `'logseq.dev/validate-db'`

### Alternative: Query-Based Cleanup

For specific validation issues (e.g., clearing `:block/warning` properties), you can query and manually clean up:

```clojure
;; Query to find blocks with warnings
{:query [:find (pull ?b [*])
         :where
         [?b :block/warning :multiple-blocks]]}
```

After fixing the underlying issue (e.g., removing multiple block markers from titles), run validation to auto-retract the warning:

```javascript
// Console
window.frontend.handler.common.developer.validate_db()

// Plugin
await logseq.App.invokeExternalCommand('logseq.dev/validate-db')
```

> **Removed:** The claim that validation auto-fix retracts `:block/warning` via `validate.cljs:43` is incorrect. `fix-invalid-blocks!` (line 35) does not contain any `:block/warning` handling. The `:block/warning` attribute is valid in the schema (`[:keyword]`) but is not automatically cleared by the validation pass.

## Findings: New Capabilities

### `validate-local-db!`

**File**: `deps/db/src/logseq/db/frontend/validate.cljs:127`

A separate public entry point for CLI, importer, and test use-cases that wraps `validate-db` with default local-DB behaviour:

```clojure
(defn validate-local-db!
  "Validates a local (non-RTC) DB like validate-db! but with default behavior,
  options and logging specific to a local DB. Used by CLI, importer and tests"
  [db & {:keys [db-name open-schema verbose]}]
  ...)
```

This function is not exposed through the UI path; it is called from the Logseq CLI and integration tests.

### Additional fix categories in `fix-invalid-blocks!`

The actual `fix-invalid-blocks!` implementation (line 35) handles many more cases than previously documented. Newly discovered fix branches include:

- `:logseq.property/parent` present → retract it
- `:hide?` / `:public?` / `:block/pre-block?` present → retract each
- Embedding properties (`:logseq.property.embedding/hnsw-label*`) → retract
- Normal page missing `:block/updated-at` → set it to `:block/created-at`
- "External URL" node without tags → retract entity
- Property erroneously tagged with `:logseq.class/Tag` → retract that tag
- User-class ident entity missing class tag → add `:logseq.class/Tag`, retract `:logseq.class/Page`
- Class with spurious `:kv/value` → retract it
- Property with `:logseq.property.class/extends` datoms → retract them
- `:block/level` present → retract it
- Class missing `:db/ident` → synthesise from title via `db-class/create-user-class-ident-from-name`
- Whiteboard-shape entities → retract entity
- `:logseq.property/created-by-ref` pointing to non-entity → retract entity
- Block missing `:block/uuid` (but otherwise intact) → generate and add a new UUID
- Property-value-block with `:block/title` → retract title
- Normal page with `:block/page` (shouldn't have one) → retract `:block/page`

All branches accumulate tx-data via `mapcat`; a **single** `d/transact!` call is issued at the end.

## Future Improvements

Potential enhancements:

1. **Validation Report UI**: Dedicated panel showing all errors
2. **Selective Fix**: Let users choose which fixes to apply
3. **Validation History**: Track validation runs over time
4. **Performance Profiling**: Report which entity types are slow to validate
5. **Plugin API Exposure**: Add `logseq.api.validate_db()` for direct plugin access
6. **Custom Validators**: Plugin API for custom validation rules

## See Also

- [Garbage Collection Developer Guide](./garbage-collection-developer-guide.md)
- [Plugin API Overview](../../04-api-reference/plugin-api/overview.md)
- [DB Schema Documentation](../deps/db/src/logseq/db/frontend/malli_schema.cljs)
- [Outliner Validation](../deps/outliner/src/logseq/outliner/validate.cljs)
- [Worker Thread Architecture](../src/main/frontend/worker/README.md)
