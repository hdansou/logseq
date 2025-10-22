# Graph Validation Developer Guide

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
db.frontend.validate/validate-db!
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
Your graph is valid! {:entities 5000, :pages 100, :blocks 4500, :classes 50, :properties 200, :datoms 120000}
```

**Error Case**:
```
Validation detected 15 invalid block(s). These blocks may be buggy.
Attempting to fix invalid blocks. Run validation again to see if they were fixed.
```

## Code Walkthrough

### 1. UI Entry Point

**File**: `src/main/frontend/handler/common/developer.cljs:94-95`

```clojure
(defn ^:export validate-db []
  (state/<invoke-db-worker :thread-api/validate-db (state/get-current-repo)))
```

This public function is triggered from the command palette and invokes the worker thread.

**Command Registration**: `src/main/frontend/modules/shortcut/config.cljs:656-659`

```clojure
:dev/validate-db {:binding []
                  :db-graph? true
                  :inactive (not (state/developer-mode?))
                  :fn :frontend.handler.common.developer/validate-db}
```

### 2. Worker Thread API

**File**: `src/main/frontend/worker/db_worker.cljs:677-680`

```clojure
(def-thread-api :thread-api/validate-db
  [repo]
  (when-let [conn (worker-state/get-datascript-conn repo)]
    (worker-db-validate/validate-db conn)))
```

The worker thread handles validation to avoid blocking the UI thread.

### 3. Main Validation Orchestrator

**File**: `src/main/frontend/worker/db/validate.cljs:99-119`

```clojure
(defn validate-db
  [conn]
  (let [db @conn
        {:keys [errors datom-count entities]} (db-validate/validate-db! db)
        invalid-entity-ids (distinct (map (fn [e] (:db/id (:entity e))) errors))]
    (if errors
      (do
        (fix-invalid-blocks! conn errors)
        (shared-service/broadcast-to-clients! :notification
          {:type :warning
           :msg (str "Validation detected " (count errors) " invalid block(s)...")})
        (shared-service/broadcast-to-clients! :log
          {:level :error
           :payload [:div "Invalid entities:" (pr-str invalid-entity-ids)]}))
      (shared-service/broadcast-to-clients! :notification
        {:type :success
         :msg (str "Your graph is valid! "
                   (assoc (db-validate/graph-counts db entities) :datoms datom-count))}))))
```

### 4. Core Schema Validation

**File**: `deps/db/src/logseq/db/frontend/validate.cljs:91-111`

```clojure
(defn validate-db!
  "Validates all the entities of the given db using :eavt datoms. Returns a map
  with info about db being validated."
  [db]
  (let [datoms (d/datoms db :eavt)
        ent-maps* (db-malli-schema/datoms->entities datoms)
        ent-maps (map (fn [e] (assoc e :db/id (:e e))) ent-maps*)
        entities (map (fn [e] (dissoc e :db/id)) ent-maps)
        errors (binding [db-malli-schema/*db-for-validate-fns* db]
                 (-> entities closed-db-schema-explainer :errors))]
    {:errors errors
     :datom-count (count datoms)
     :entities ent-maps}))
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

**File**: `src/main/frontend/worker/db/validate.cljs:31-92`

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

```clojure
(defn fix-invalid-blocks!
  [conn errors]
  (doseq [error errors]
    (let [entity (:entity error)
          eid (:db/id entity)]
      (cond
        ;; Missing block title - retract the entity
        (and (block-entity? entity)
             (not (seq (:block/title entity))))
        (d/transact! conn [[:db/retractEntity eid]])

        ;; Missing block page - assign to journal/parent page
        (and (block-entity? entity)
             (not (:block/page entity)))
        (when-let [page-id (or (some-> (db-model/get-latest-journals @conn 1) first :db/id)
                               (:db/id (:block/parent entity)))]
          (d/transact! conn [{:db/id eid :block/page page-id}]))

        ;; Add more fix patterns...
        ))))
```

## Automatic Validation During Development

**File**: `src/main/frontend/worker/pipeline.cljs:104-119`

```clojure
(defn validate-db!
  "Validate db is slow, we probably don't want to enable it for production."
  [repo conn tx-report tx-meta context]
  (when (and (not (::skip-validate-db? tx-meta false))
             (or (:dev? context) (:undo? tx-meta) (:redo? tx-meta))
             (not (:importing? context))
             (sqlite-util/db-based-graph? repo))
    (let [valid? (if (get-in tx-report [:tx-meta :reset-conn!])
                   true
                   (db-validate/validate-tx-report! tx-report (:validate-db-options context)))]
      (when-not valid?
        (throw (ex-info "Invalid transaction" {:tx-report tx-report}))))))
```

### When Automatic Validation Runs

- ✅ Every undo/redo operation
- ✅ In dev mode for all transactions
- ❌ Not during data import
- ❌ Can be skipped with `::skip-validate-db?` tx-meta flag

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

| Feature | Validate DB | GC Graph |
|---------|-------------|----------|
| **Purpose** | Check schema integrity | Remove orphaned data |
| **Trigger** | Manual only | Auto (3 days) + Manual |
| **Auto-fixes** | Yes, attempts repairs | No, just deletes |
| **Report Type** | Validation errors | Space reclaimed |
| **Dev Mode** | Required | Required |
| **Performance** | ~1-5 seconds | ~5-30 seconds |
| **Destructive** | Fixes orphaned entities | Deletes unused addresses |
| **Location** | `worker/db/validate.cljs` | `worker/rtc/client.cljs` |

## Related Validation Layers

### 1. Transaction Validation

**File**: `deps/db/src/logseq/db/frontend/validate.cljs`

```clojure
(defn validate-tx-report!
  "Validates entities in a transaction report"
  [tx-report options]
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
  (validate/validate-db! db))
```

### 4. Skipping Validation

For performance-critical operations:

```clojure
(d/transact! conn
  [{:db/id 123 :block/title "foo"}]
  {::validate/skip-validate-db? true})
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

| File | Purpose | Lines |
|------|---------|-------|
| `src/main/frontend/handler/common/developer.cljs` | UI entry point | 94-95 |
| `src/main/frontend/modules/shortcut/config.cljs` | Command registration | 656-659 |
| `src/main/frontend/worker/db_worker.cljs` | Worker thread API | 677-680 |
| `src/main/frontend/worker/db/validate.cljs` | Main orchestration & auto-fix | 31-119 |
| `src/main/frontend/worker/pipeline.cljs` | Auto-validation in transactions | 104-119 |
| `deps/db/src/logseq/db/frontend/validate.cljs` | Core schema validation | 91-111 |
| `deps/db/src/logseq/db/frontend/malli_schema.cljs` | Entity schema definitions | - |
| `src/resources/dicts/en.edn` | UI labels | - |

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

## Future Improvements

Potential enhancements:

1. **Validation Report UI**: Dedicated panel showing all errors
2. **Selective Fix**: Let users choose which fixes to apply
3. **Validation History**: Track validation runs over time
4. **Performance Profiling**: Report which entity types are slow to validate
5. **Custom Validators**: Plugin API for custom validation rules

## See Also

- [Garbage Collection Developer Guide](./garbage-collection-developer-guide.md)
- [DB Schema Documentation](../deps/db/src/logseq/db/frontend/malli_schema.cljs)
- [Outliner Validation](../deps/outliner/src/logseq/outliner/validate.cljs)
- [Worker Thread Architecture](../src/main/frontend/worker/README.md)
