# Schema Changes & Data Integrity Investigation

## Part 6: Failure Patterns & Recommendations

> **Last revised:** 2026-05-30

---

## Table of Contents

1. [Common Failure Patterns](#common-failure-patterns)
2. [Failure Cascade Analysis](#failure-cascade-analysis)
3. [Risk Assessment](#risk-assessment)
4. [Mitigation Strategies](#mitigation-strategies)
5. [Recovery Procedures](#recovery-procedures)
6. [Monitoring & Detection](#monitoring--detection)
7. [Recommendations](#recommendations)

---

## Common Failure Patterns

### Pattern 1: Migration Cascade Failure

**Description:** A single failed migration leaves graph in partially migrated state

**Trigger:** Migration bug or data inconsistency

**Impact Chain:**

```
Migration v65.15 -> v65.16 fails
        ↓
Graph stuck at v65.15
        ↓
Cannot open with v65.16+ clients
        ↓
Cannot export (validation fails)
        ↓
User loses access to graph
```

**Evidence:** [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs#L329-L366)

**Risk Level:** 🔴 CRITICAL

**Frequency:** Low (but high impact when occurs)

### Pattern 2: Invalid Graph Export Block

**Description:** Export can fail on structural export-map validation; full-graph validation is available as a separate validation path

**Trigger:** Missing/undefined export references or invalid export shape during export-map build

**Impact Chain:**

```
Graph contains inconsistent references
        ↓
User tries to export for backup
        ↓
Structural export validation fails
        ↓
Export blocked
        ↓
User cannot create backup
```

**Evidence:**

- [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs#L1010-L1053) (`basic-validate-export` in `build-export`)
- [src/main/frontend/worker/db_core.cljs](../../../src/main/frontend/worker/db_core.cljs#L1496-L1512) (`:thread-api/export-edn` uses `build-export`)
- [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs#L1200-L1220) (`validate-export` exists as separate full import+validate helper)

**Risk Level:** 🔴 CRITICAL

**Frequency:** Medium

### Pattern 3: Version Incompatibility Lock-out

**Description:** User cannot access graph due to version mismatch

**Trigger:** Attempting to open newer graph with older client

**Impact Chain:**

```
User upgrades app to v66
        ↓
Graph migrated to v66.0
        ↓
User downgrades app to v65 (trying to fix issue)
        ↓
v65 app cannot open v66 graph
        ↓
User locked out of graph
```

**Evidence:** [deps/db/src/logseq/db/frontend/schema.cljs](../../../deps/db/src/logseq/db/frontend/schema.cljs#L34-L40)

**Risk Level:** 🟠 HIGH

**Frequency:** High (common user behavior)

### Pattern 4: Old Export Import Failure

**Description:** Import can fail on property/type conflicts or invalid imported entities, but old backups are not categorically rejected

**Trigger:** Import-time conflicts (e.g. property schema mismatch) or tx validation failures

**Impact Chain:**

```
User creates export with v65.10
        ↓
Time passes (6 months)
        ↓
User tries to import with v65.16
        ↓
Import detects property/type conflict
        ↓
Import rejected
```

**Evidence:**

- [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs#L1080-L1162) (`build-import` conflict checks)
- [deps/db/src/logseq/db/frontend/validate.cljs](../../../deps/db/src/logseq/db/frontend/validate.cljs#L24-L126) (transaction/local DB validation paths)

**Risk Level:** 🟠 HIGH

**Frequency:** Medium

### Pattern 5: RTC Branch Divergence

> **Removed:** Legacy evidence path `src/main/frontend/worker/rtc/branch_graph.cljs` no longer exists in current source layout.
>
> **Replacement (current behavior):** Sync failure/recovery now centers on websocket message handling (`tx/reject`, `pull`, checksum mismatch logging) under `src/main/frontend/worker/sync/`.
>
> **Evidence:**
>
> - [src/main/frontend/worker/sync/handle_message.cljs](../../../src/main/frontend/worker/sync/handle_message.cljs#L169-L231)
> - [src/main/frontend/worker/sync/log_and_state.cljs](../../../src/main/frontend/worker/sync/log_and_state.cljs#L13-L24)

**Description:** Rejected or stale tx batches can stall collaboration until pull/retry resolves divergence

**Trigger:** Server rejects inflight txs (`tx/reject`) or reports stale state

**Impact Chain:**

```
Team collaboration active
        ↓
Client sends tx batch
        ↓
Server responds `tx/reject` / stale
        ↓
Client marks failed/inflight txs and requests pull
        ↓
Local state appears stuck until pull/apply catches up
        ↓
Collaboration disrupted
```

**Evidence:** [src/main/frontend/worker/sync/handle_message.cljs](../../../src/main/frontend/worker/sync/handle_message.cljs#L169-L231)

**Risk Level:** 🟡 MEDIUM

**Frequency:** High (common in team settings)

### Pattern 6: Partial Migration Sync Failure

> **Removed:** Legacy evidence path `src/main/frontend/worker/rtc/migrate.cljs` no longer exists.
>
> **Replacement:** Snapshot download/import and tx application are handled by `src/main/frontend/worker/sync/download.cljs` and `src/main/frontend/worker/sync/apply_txs.cljs`.

**Description:** Snapshot download/import failure can leave client behind until retry succeeds

**Trigger:** Snapshot transfer/decrypt/import failure during sync bootstrap

**Impact Chain:**

```
Client requests snapshot download
        ↓
Download/decrypt/import fails
        ↓
Client remains stale
        ↓
Retry/reconnect required
```

**Evidence:**

- [src/main/frontend/worker/sync/download.cljs](../../../src/main/frontend/worker/sync/download.cljs#L468-L559)
- [src/main/frontend/worker/sync/apply_txs.cljs](../../../src/main/frontend/worker/sync/apply_txs.cljs)

**Risk Level:** 🟡 MEDIUM

**Frequency:** Low

---

## Failure Cascade Analysis

### Cascade 1: Migration → Validation → Export Failure

```
┌─────────────────────┐
│  Migration Fails    │
│  (Bug in v65.16)    │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│  Graph Stuck at     │
│  v65.15             │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│  Validation Errors  │
│  (Incomplete state) │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│  Export Blocked     │
│  (Can't backup)     │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│  User Helpless      │
│  (No recovery path) │
└─────────────────────┘
```

**Recovery Options:**

1. ❌ Cannot export (blocked by validation)
2. ❌ Cannot downgrade (v65.15 state requires v65.16+ app)
3. ⚠️ Manual SQLite file copy (bypasses validation)
4. ✅ Wait for bugfix release

### Cascade 2: Strict Validation → Data Rejection

> **Removed:** Previous example claiming `:block/priority` changed from string to number is stale.
>
> **Current evidence:** Priority is modeled as a closed-value default property in current schema definitions.
>
> **Evidence:** [deps/db/src/logseq/db/frontend/property.cljs](../../../deps/db/src/logseq/db/frontend/property.cljs#L323-L341)

```
┌─────────────────────┐
│  Imported data uses │
│  old/conflicting    │
│  property shape     │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│  Current schema     │
│  validates tx/import│
│  shape and conflicts│
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│  Import/tx validate │
│  or conflict checks │
│  reject data        │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│  Transaction        │
│  Rejected           │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│  User Cannot Save   │
│  Legitimate Data    │
└─────────────────────┘
```

**Recovery Options:**

1. ✅ Migration or import transforms for known incompatibilities
2. ⚠️ Validation relaxation (temporary)
3. ✅ User manually fixes data

### Cascade 3: Version Mismatch → Collaboration Breakdown

> **Removed:** The older branch-based v65/v66 narrative referenced removed RTC implementation files.
>
> **Current compatibility gate:** local DB migration explicitly warns and blocks edits when app schema is older than graph schema.
>
> **Evidence:** [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs#L292-L301)

```
┌─────────────────────┐
│  Graph schema       │
│  already upgraded   │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│  User opens graph   │
│  with older app     │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│  Incompatible       │
│  version detected   │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│  Editing blocked    │
│  with update notice │
└──────────┬──────────┘
           │
           ↓
┌─────────────────────┐
│  User must upgrade  │
│  app before edits   │
└─────────────────────┘
```

**Recovery Options:**

1. ❌ Downgrade further (still incompatible)
2. ✅ Upgrade app to current schema-compatible version
3. ⚠️ Use backup/export workflow only after opening with compatible app

---

## Risk Assessment

### Risk Scoring Matrix

| Risk Factor              | Weight | Score (1-10) | Weighted Score |
| ------------------------ | ------ | ------------ | -------------- |
| **Data Loss Potential**  | 0.35   | 9            | 3.15           |
| **User Impact**          | 0.25   | 8            | 2.00           |
| **Frequency**            | 0.15   | 4            | 0.60           |
| **Recovery Difficulty**  | 0.15   | 8            | 1.20           |
| **Detection Difficulty** | 0.10   | 6            | 0.60           |
| **Total**                | 1.00   | -            | **7.55/10**    |

**Overall Risk:** 🔴 HIGH

### Individual Risk Analysis

#### Migration System

- **Data Loss Risk:** 🔴 HIGH
- **User Impact:** 🔴 CRITICAL
- **Detection:** 🟢 EASY (fails visibly)
- **Recovery:** 🔴 HARD (requires code fix)

#### Validation System

- **Data Loss Risk:** 🟡 MEDIUM
- **User Impact:** 🟠 HIGH (blocks operations)
- **Detection:** 🟢 EASY (error messages)
- **Recovery:** 🟡 MEDIUM (data transform)

#### Import/Export

- **Data Loss Risk:** 🟠 HIGH
- **User Impact:** 🟠 HIGH (cannot backup/restore)
- **Detection:** 🟢 EASY (import/export fails)
- **Recovery:** 🟠 MODERATE (manual data fix)

#### RTC Sync

- **Data Loss Risk:** 🟡 LOW
- **User Impact:** 🟡 MEDIUM (collaboration issues)
- **Detection:** 🟡 MODERATE (silent divergence)
- **Recovery:** 🟢 EASY (upgrade coordination)

---

## Mitigation Strategies

### Strategy 1: Defensive Migration Design

**Problem:** Migrations can fail and leave graph corrupted

**Solution:**

```clojure
(defn migrate-with-checkpoint
  "Migrate with rollback checkpoint"
  [conn from-version to-version]
  ;; 1. Create checkpoint
  (let [checkpoint (create-checkpoint conn)]
    (try
      ;; 2. Run migration
      (migrate conn from-version to-version)

      ;; 3. Validate result
      (when-let [errors (validate-db! @conn)]
        (throw (ex-info "Migration produced invalid graph" {:errors errors})))

      ;; 4. Success - delete checkpoint
      (delete-checkpoint checkpoint)

      (catch :default e
        ;; 5. Failure - restore checkpoint
        (log/error "Migration failed, restoring checkpoint" e)
        (restore-checkpoint conn checkpoint)
        (throw e)))))
```

### Strategy 2: Graceful Export with Warnings

**Problem:** Invalid graphs cannot be exported for backup

**Solution:**

```clojure
(defn export-with-option
  "Export with force option for invalid graphs"
  [db path {:keys [force? warn-only?]}]
  (if-let [errors (validate-db! db)]
    (cond
      force?
      (do
        (log/warn "Force-exporting invalid graph" {:errors errors})
        (export-without-validation db path)
        {:success true :warnings errors})

      warn-only?
      (do
        (export-without-validation db path)
        {:success true :warnings errors})

      :else
      (throw (ex-info "Graph invalid, use :force? true to export anyway"
                     {:errors errors})))
    (export-without-validation db path)))
```

### Strategy 3: Import with Transform

**Problem:** Old exports fail validation under new rules

**Solution:**

```clojure
(defn import-with-transform
  "Import with automatic data transformation"
  [conn export-data]
  (let [export-version (:schema-version export-data)
        transforms (get-transforms-for-version export-version)]

    ;; Transform entities to match current schema
    (let [transformed-entities
          (for [entity (:entities export-data)]
            (reduce apply-transform entity transforms))]

      ;; Import transformed data
      (transact! conn transformed-entities))))

(defn get-transforms-for-version
  "Get data transforms needed for old version"
  [old-version]
  [(when (< (:minor old-version) 15)
     ;; Transform string priority to number
     (fn [entity]
       (if (string? (:block/priority entity))
         (update entity :block/priority parse-priority-string)
         entity)))

   (when (< (:minor old-version) 14)
     ;; Add missing timestamps
     (fn [entity]
       (cond-> entity
         (nil? (:block/created-at entity))
         (assoc :block/created-at 0)
         (nil? (:block/updated-at entity))
         (assoc :block/updated-at 0))))])
```

### Strategy 4: RTC Version Coordination

**Problem:** Team members on different versions cannot sync

**Solution:**

```clojure
(defn coordinate-team-upgrade
  "Notify team before major version upgrade"
  [graph-id]
  ;; 1. Detect pending upgrade
  (when (pending-major-upgrade? graph-id)

    ;; 2. Notify all clients
    (broadcast-notification graph-id
      {:type :major-upgrade-pending
       :message "New version available. Please upgrade within 7 days."
       :deadline (+ (js/Date.now) (* 7 24 60 60 1000))})

    ;; 3. Block branch creation until all upgraded
    (set-branch-creation-policy graph-id :require-coordination)

    ;; 4. After deadline, allow forced upgrade
    (schedule-task (* 7 24 60 60 1000)
                  #(allow-forced-upgrade graph-id))))
```

### Strategy 5: Pre-Migration Validation

**Problem:** Migrations run on invalid graphs and fail

**Solution:**

```clojure
(defn migrate-safely
  "Validate before and after migration"
  [conn from-version to-version]
  ;; Pre-migration validation
  (log/info "Validating graph before migration")
  (when-let [errors (validate-db! @conn)]
    (log/warn "Graph has validation errors before migration" {:count (count errors)})
    ;; Attempt to fix common issues
    (fix-common-issues! conn errors))

  ;; Run migration
  (log/info "Running migration" {:from from-version :to to-version})
  (let [start-time (js/Date.now)
        result (migrate conn from-version to-version)
        duration (- (js/Date.now) start-time)]
    (log/info "Migration completed" {:duration duration})

    ;; Post-migration validation
    (log/info "Validating graph after migration")
    (when-let [errors (validate-db! @conn)]
      (log/error "Migration produced invalid graph" {:errors errors})
      (throw (ex-info "Post-migration validation failed" {:errors errors})))

    result))
```

### Strategy 6: Schema Change Impact Analysis

**Problem:** Schema changes have unexpected side effects

**Solution:**

```clojure
(defn analyze-schema-change-impact
  "Analyze impact of proposed schema change"
  [current-schema proposed-change]
  {:breaking-changes
   (detect-breaking-changes current-schema proposed-change)

   :migration-required?
   (requires-migration? proposed-change)

   :affected-entities
   (count-affected-entities proposed-change)

   :validation-impact
   (analyze-validation-impact proposed-change)

   :rtc-impact
   (analyze-rtc-impact proposed-change)

   :import-export-impact
   (analyze-import-export-impact proposed-change)})
```

---

## Recovery Procedures

### Procedure 1: Recover from Failed Migration

**Symptoms:** Graph stuck at intermediate version, cannot open

**Steps:**

1. **Identify Failed Migration**

   ```clojure
   (get-current-schema-version conn)  ;; Returns partial version
   (check-migration-log conn)         ;; Shows last attempted migration
   ```

2. **Create SQLite Backup**

   ```bash
   cp graph.db graph.db.backup
   ```

3. **Attempt Manual Fix**

   ```clojure
   ;; Option A: Complete migration manually
   (complete-partial-migration conn)

   ;; Option B: Rollback to last known good version
   (rollback-to-version conn last-good-version)
   ```

   > **Removed:** `complete-partial-migration` and `rollback-to-version` are not present in current source; this remains a conceptual recommendation, not an existing supported API.

4. **Verify Graph Health**

   ```clojure
   (validate-db! @conn)  ;; Should return nil (no errors)
   ```

5. **Export as Backup**
   ```clojure
   (export-graph @conn "recovery-backup.edn")
   ```

### Procedure 2: Recover from Invalid Graph

**Symptoms:** Cannot export, validation errors

**Steps:**

1. **Identify Validation Errors**

   ```clojure
   (def errors (validate-db! @conn))
   (group-by :attribute errors)  ;; See which attributes are problematic
   ```

2. **Force Export (if possible)**

   ```clojure
   (export-with-option @conn "backup.edn" {:force? true})
   ```

   > **Removed:** `export-with-option` force-export flow is not implemented in current export API.

3. **Fix Data Issues**

   ```clojure
   ;; Fix specific validation errors
   (fix-validation-errors! conn errors)
   ```

4. **Retry Export**
   ```clojure
   (export-graph @conn "clean-backup.edn")
   ```

### Procedure 3: Recover from Version Lock-out

**Symptoms:** Cannot open graph, "incompatible version" error

**Steps:**

1. **Check Version Mismatch**

   ```
   Graph version: 66.0
   App version: 65.16
   Status: INCOMPATIBLE
   ```

2. **Options:**
   - **Option A:** Upgrade app to v66+
   - **Option B:** Find v66 export and import into v65 graph
   - **Option C:** Use dev tools to force downgrade (RISKY)

3. **If Upgrading:**

   ```bash
   # Update app to latest version
   # Reopen graph - should auto-migrate
   ```

4. **If Using Export:**

   ```clojure
   ;; Export with v66 app
   (export-graph @conn "v66-export.edn")

   ;; Import into new v65 graph
   ;; (Some features may be lost)
   ```

### Procedure 4: Merge Diverged RTC Branches

> **Removed:** This procedure assumes explicit RTC branch-export merge functions (`export-branch`, `merge-exports`) that are not present in current source tree.

**Symptoms:** Team members cannot see each other's changes

**Steps:**

1. **Identify Branch Versions**

   ```clojure
   Branch A: v65.16 (3 users)
   Branch B: v66.0  (1 user)
   ```

2. **Coordinate Upgrade**

   ```
   Option 1: All users upgrade to v66
   Option 2: v66 user downgrades to v65
   ```

3. **Export Both Branches**

   ```clojure
   (export-branch "branch-a.edn" branch-a-id)
   (export-branch "branch-b.edn" branch-b-id)
   ```

4. **Manual Merge (if needed)**

   ```clojure
   (merge-exports "branch-a.edn" "branch-b.edn" "merged.edn")
   ```

5. **Import Merged Data**
   ```clojure
   (import-graph conn "merged.edn")
   ```

---

## Monitoring & Detection

## Findings: New Capabilities

1. **Schema head advanced to 65.33**
   - Migration table now includes versions through `65.33`, including fixes for comment class/target repair and newer view/gallery properties.
   - Evidence: [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs#L116-L170)

2. **Sync stack migrated to `worker/sync/*` architecture**
   - Legacy `worker/rtc/*` evidence paths are obsolete; active logic now uses websocket handlers, pull/retry queues, checksum diagnostics, and staged sync tasks.
   - Evidence: [src/main/frontend/worker/sync.cljs](../../../src/main/frontend/worker/sync.cljs), [src/main/frontend/worker/sync/handle_message.cljs](../../../src/main/frontend/worker/sync/handle_message.cljs)

3. **Explicit tx rejection and checksum-mismatch logging in sync pipeline**
   - Current sync logic records and reacts to rejected tx batches and checksum mismatches, improving failure detection and triage.
   - Evidence: [src/main/frontend/worker/sync/handle_message.cljs](../../../src/main/frontend/worker/sync/handle_message.cljs#L169-L231), [src/main/frontend/worker/sync/log_and_state.cljs](../../../src/main/frontend/worker/sync/log_and_state.cljs#L13-L24)

4. **Dedicated graph download bootstrap pipeline for sync recovery**
   - Snapshot-based download/import path exists for recovery/bootstrap instead of older RTC migration-file assumptions.
   - Evidence: [src/main/frontend/worker/sync/download.cljs](../../../src/main/frontend/worker/sync/download.cljs#L468-L559)

### Key Metrics to Monitor

```clojure
(defn collect-schema-metrics
  "Collect metrics for monitoring"
  []
  {:current-schema-version (get-schema-version @conn)

   :validation-stats
   {:errors-per-day (count-validation-errors-today)
    :most-common-errors (top-validation-errors)
    :error-trend (validation-error-trend)}

   :migration-stats
   {:success-rate (migration-success-rate)
    :average-duration (average-migration-duration)
    :failure-count (migration-failure-count)}

   :export-import-stats
   {:exports-per-day (count-exports-today)
    :exports-failed (count-export-failures-today)
    :imports-per-day (count-imports-today)
    :imports-failed (count-import-failures-today)}

   :rtc-stats
   {:active-branches (count-active-branches)
    :clients-per-branch (clients-by-branch)
    :sync-conflicts (count-sync-conflicts-today)
    :diverged-branches (count-diverged-branches)}})
```

### Alerting Rules

```clojure
(def alert-rules
  [{:name "High Migration Failure Rate"
    :condition #(> (:migration-failure-rate %) 0.1)
    :severity :critical
    :action send-pagerduty-alert}

   {:name "Export Validation Blocking Users"
    :condition #(> (:export-failures-today %) 5)
    :severity :high
    :action send-slack-alert}

   {:name "RTC Branch Proliferation"
    :condition #(> (:active-branches %) 5)
    :severity :medium
    :action send-email-alert}

   {:name "Validation Error Spike"
    :condition #(> (:validation-errors-today %)
                  (* 2 (:validation-errors-yesterday %)))
    :severity :high
    :action send-slack-alert}])
```

### Health Check Dashboard

```clojure
(defn schema-health-check
  "Overall schema system health"
  []
  {:status (calculate-overall-status)

   :checks
   [{:name "Migration System"
     :status (check-migration-health)
     :last-failure (:last-migration-failure @state)
     :success-rate (:migration-success-rate @state)}

    {:name "Validation System"
     :status (check-validation-health)
     :error-rate (:validation-error-rate @state)
     :common-errors (:top-validation-errors @state)}

    {:name "Import/Export"
     :status (check-import-export-health)
     :exports-today (:exports-count @state)
     :exports-failed (:exports-failed-count @state)}

    {:name "RTC Sync"
     :status (check-rtc-health)
     :active-branches (:branches-count @state)
     :sync-lag (:average-sync-lag @state)}]})
```

---

## Recommendations

### Immediate Actions (Priority: 🔴 CRITICAL)

1. **Add Migration Rollback**
   - Implement checkpoint/restore mechanism
   - Test rollback in development
   - Deploy in next release

2. **Enable Force Export**
   - Add `--force` flag to export
   - Show validation warnings
   - Document risks

3. **Add Pre-Migration Validation**
   - Check graph health before migration
   - Auto-fix common issues
   - Warn user of risks

### Short-term Actions (Priority: 🟠 HIGH)

4. **Implement Import Transforms**
   - Auto-transform old data formats
   - Maintain compatibility layer
   - Document transforms

5. **Add Schema Change Impact Analysis**
   - Analyze breaking changes
   - Estimate affected entities
   - Generate migration preview

6. **Improve Error Messages**
   - Provide actionable recovery steps
   - Link to documentation
   - Show contact support option

### Medium-term Actions (Priority: 🟡 MEDIUM)

7. **Build Schema Evolution Dashboard**
   - Monitor migration health
   - Track validation errors
   - Alert on anomalies

8. **Implement Gradual Rollout**
   - Beta test migrations
   - Staged deployment
   - Rollback capability

9. **Add RTC Coordination**
   - Notify teams of upgrades
   - Block premature branching
   - Facilitate synchronized upgrades

### Long-term Actions (Priority: 🟢 LOW)

10. **Design Schema Versioning V2**
    - Support backward compatibility
    - Graceful degradation
    - Automatic transforms

11. **Implement Branch Auto-Merge**
    - Detect mergeable branches
    - Auto-merge when safe
    - Notify users of merge

12. **Build Recovery Tools**
    - Graph repair utilities
    - Data migration tools
    - Forensics capabilities

---

## Key Takeaways

1. **No Silver Bullet:** Schema evolution is inherently risky
2. **Defense in Depth:** Multiple safety layers needed
3. **User Communication:** Clear error messages and recovery paths
4. **Monitoring Critical:** Early detection prevents disasters
5. **Testing Essential:** Migrations must be thoroughly tested
6. **Rollback Plan:** Always have a way back
7. **Documentation:** Users need guidance for recovery

---

## Next Steps

Proceed to **Part 7: PR Review Standards & Guiding Principles** for actionable team guidelines to prevent data migration issues.
