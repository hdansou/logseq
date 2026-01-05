# Schema Changes & Data Integrity Investigation

## Part 5: RTC Synchronization Analysis

---

## Table of Contents

1. [RTC Architecture Overview](#rtc-architecture-overview)
2. [Schema Version Branching](#schema-version-branching)
3. [Migration Operation Conversion](#migration-operation-conversion)
4. [Sync Conflict Resolution](#sync-conflict-resolution)
5. [Multi-Client Scenarios](#multi-client-scenarios)
6. [Branch Management](#branch-management)
7. [Failure Recovery](#failure-recovery)

---

## RTC Architecture Overview

### Purpose

RTC (Real-Time Collaboration) enables multiple clients to work on the same graph simultaneously. Schema versioning adds complexity: clients may be running different app versions with different schema requirements.

**Key Files:**

- [src/main/frontend/worker/rtc/branch_graph.cljs](../../../src/main/frontend/worker/rtc/branch_graph.cljs) - Branching logic
- [src/main/frontend/worker/rtc/migrate.cljs](../../../src/main/frontend/worker/rtc/migrate.cljs) - Migration ops

### RTC Components

```
┌─────────────────────────────────────────────────────────────┐
│                       RTC Server                            │
│                                                             │
│  ┌────────────┐    ┌────────────┐    ┌────────────┐       │
│  │ Branch 65  │    │ Branch 66  │    │ Branch 67  │       │
│  │ (v65.16)   │    │ (v66.0)    │    │ (v67.2)    │       │
│  └────────────┘    └────────────┘    └────────────┘       │
│         ↑                 ↑                 ↑              │
└─────────│─────────────────│─────────────────│──────────────┘
          │                 │                 │
          │                 │                 │
    ┌─────┴────┐      ┌─────┴────┐      ┌─────┴────┐
    │ Client A │      │ Client B │      │ Client C │
    │ v65.16   │      │ v66.0    │      │ v67.2    │
    └──────────┘      └──────────┘      └──────────┘
```

### Sync Flow

```
Client makes change
        ↓
Create operation
        ↓
Send to RTC server
        ↓
Server validates schema compatibility
        ↓
Broadcast to other clients in same branch
        ↓
Clients apply operation
        ↓
Resolve conflicts if needed
```

---

## Schema Version Branching

### Branch Decision Logic

**Location:** [src/main/frontend/worker/rtc/branch_graph.cljs](../../../src/main/frontend/worker/rtc/branch_graph.cljs#L13-L40)

```clojure
(defn compare-schemas
  "Compare client schema with server graph schema.
   Returns action to take based on version compatibility."
  [client-schema server-schema]
  (let [client-major (:major client-schema)
        server-major (:major server-schema)
        client-minor (:minor client-schema)
        server-minor (:minor server-schema)]
    (cond
      ;; Client has older major version - must download
      (< client-major server-major)
      :download

      ;; Client has newer major version - create new branch
      (> client-major server-major)
      :create-branch

      ;; Same major version - compatible regardless of minor
      (= client-major server-major)
      :compatible)))
```

### Branch Creation

```clojure
(defn create-branch
  "Create new schema version branch on server"
  [graph-id client-schema]
  {:graph-id graph-id
   :branch-id (str "v" (:major client-schema))
   :schema-version client-schema
   :created-at (js/Date.now)
   :created-by (get-user-id)
   :base-branch (get-current-branch graph-id)})
```

### Branch Selection

```clojure
(defn select-branch
  "Select which branch to connect to"
  [graph-id client-schema available-branches]
  (let [compatible-branches
        (filter #(= (:major (:schema-version %))
                   (:major client-schema))
               available-branches)]
    (cond
      ;; No compatible branches - create new one
      (empty? compatible-branches)
      {:action :create-branch
       :reason "No compatible branch exists"}

      ;; Single compatible branch - use it
      (= 1 (count compatible-branches))
      {:action :join-branch
       :branch (first compatible-branches)}

      ;; Multiple compatible branches - choose latest
      :else
      {:action :join-branch
       :branch (apply max-key #(-> % :schema-version :minor)
                     compatible-branches)})))
```

### Decision Matrix

| Client | Server | Action           | Result                |
| ------ | ------ | ---------------- | --------------------- |
| 65.16  | 65.16  | `:compatible`    | Join existing branch  |
| 65.15  | 65.16  | `:compatible`    | Join existing branch  |
| 65.16  | 65.15  | `:compatible`    | Join existing branch  |
| 66.0   | 65.16  | `:create-branch` | Create new v66 branch |
| 65.16  | 66.0   | `:download`      | Download v66 graph    |
| 64.5   | 65.16  | `:download`      | Download v65 graph    |

---

## Migration Operation Conversion

### Purpose

When a local migration runs, it must be converted to RTC operations that can be synced to other clients in the same branch.

**Location:** [src/main/frontend/worker/rtc/migrate.cljs](../../../src/main/frontend/worker/rtc/migrate.cljs#L8-L45)

### Migration to Ops Conversion

```clojure
(defn migration-results=>client-ops
  "Convert local migration results to RTC operations"
  [migration-results]
  (for [result migration-results]
    (case (:type result)
      :properties
      (property-migration=>ops result)

      :classes
      (class-migration=>ops result)

      :rename-db-idents
      (rename-migration=>ops result)

      :fix
      (fix-migration=>ops result)

      :version-update
      (version-update=>op result))))
```

### Property Migration Ops

```clojure
(defn property-migration=>ops
  "Convert property migration to RTC ops"
  [{:keys [properties]}]
  (for [[prop-key prop-def] properties]
    {:op-type :create-property
     :property-name (name prop-key)
     :property-schema (:schema prop-def)
     :timestamp (js/Date.now)}))
```

### Class Migration Ops

```clojure
(defn class-migration=>ops
  "Convert class migration to RTC ops"
  [{:keys [classes]}]
  (for [[class-key class-def] classes]
    {:op-type :create-class
     :class-name (name class-key)
     :schema-properties (:schema-properties class-def)
     :extends (:extends class-def)
     :timestamp (js/Date.now)}))
```

### Rename Migration Ops

```clojure
(defn rename-migration=>ops
  "Convert rename migration to RTC ops"
  [{:keys [renames]}]
  (for [[old-attr new-attr] renames]
    {:op-type :rename-attribute
     :old-name (name old-attr)
     :new-name (name new-attr)
     :timestamp (js/Date.now)}))
```

### Fix Migration Ops

```clojure
(defn fix-migration=>ops
  "Convert fix migration to RTC ops"
  [{:keys [entities-modified]}]
  (for [entity entities-modified]
    {:op-type :update-entity
     :entity-id (:db/id entity)
     :changes (:modifications entity)
     :timestamp (js/Date.now)}))
```

### Version Update Op

```clojure
(defn version-update=>op
  "Create version update operation"
  [{:keys [new-version]}]
  {:op-type :update-schema-version
   :version new-version
   :timestamp (js/Date.now)})
```

---

## Sync Conflict Resolution

### Conflict Types

1. **Schema Conflict:** Different clients in same branch apply conflicting migrations
2. **Data Conflict:** Two clients edit same attribute simultaneously
3. **Version Conflict:** Client's local version diverges from server
4. **Operation Conflict:** Operations applied in different order

### Conflict Detection

```clojure
(defn detect-conflicts
  "Detect conflicts in incoming operations"
  [local-state remote-ops]
  (for [op remote-ops
        :let [conflict (check-conflict local-state op)]
        :when conflict]
    conflict))

(defn check-conflict
  "Check if operation conflicts with local state"
  [local-state op]
  (case (:op-type op)
    :create-property
    (when (property-exists? local-state (:property-name op))
      {:type :duplicate-property
       :op op})

    :rename-attribute
    (when-not (attribute-exists? local-state (:old-name op))
      {:type :missing-attribute
       :op op})

    :update-entity
    (when (entity-modified-locally? local-state (:entity-id op))
      {:type :concurrent-modification
       :op op})))
```

### Resolution Strategies

#### Last-Write-Wins (LWW)

```clojure
(defn resolve-lww
  "Resolve conflict by timestamp"
  [local-op remote-op]
  (if (> (:timestamp remote-op) (:timestamp local-op))
    {:winner :remote
     :action :accept-remote
     :discard :local}
    {:winner :local
     :action :reject-remote
     :keep :local}))
```

#### Schema Merge

```clojure
(defn resolve-schema-merge
  "Merge non-conflicting schema changes"
  [local-changes remote-changes]
  (let [local-properties (set (keys (:properties local-changes)))
        remote-properties (set (keys (:properties remote-changes)))
        overlapping (clojure.set/intersection local-properties remote-properties)]
    (if (empty? overlapping)
      {:merged-properties (merge (:properties local-changes)
                                (:properties remote-changes))
       :conflicts nil}
      {:merged-properties nil
       :conflicts overlapping})))
```

#### Operational Transformation (OT)

```clojure
(defn transform-operations
  "Transform operations to resolve conflicts"
  [local-op remote-op]
  (case [(:op-type local-op) (:op-type remote-op)]
    [:rename-attribute :rename-attribute]
    (if (= (:old-name local-op) (:old-name remote-op))
      ;; Both rename same attribute - keep remote's target
      {:transformed-local nil
       :transformed-remote remote-op}
      ;; Different attributes - both can proceed
      {:transformed-local local-op
       :transformed-remote remote-op})

    [:update-entity :update-entity]
    (if (= (:entity-id local-op) (:entity-id remote-op))
      ;; Same entity - merge changes
      (merge-entity-updates local-op remote-op)
      ;; Different entities - no conflict
      {:transformed-local local-op
       :transformed-remote remote-op})))
```

---

## Multi-Client Scenarios

### Scenario 1: All Clients Same Version

```
Server: Branch v65 (v65.16)
Client A: v65.16 ← Connected to v65 branch
Client B: v65.16 ← Connected to v65 branch
Client C: v65.16 ← Connected to v65 branch

Result: All clients share operations seamlessly
```

### Scenario 2: Mixed Minor Versions

```
Server: Branch v65 (v65.16)
Client A: v65.16 ← Connected to v65 branch
Client B: v65.15 ← Connected to v65 branch (compatible)
Client C: v65.14 ← Connected to v65 branch (compatible)

Result: All compatible, but newer features unavailable to older clients
```

### Scenario 3: New Major Version Client

```
Server: Branch v65 (v65.16)
        Branch v66 (v66.0) ← Created by Client B
Client A: v65.16 ← Connected to v65 branch
Client B: v66.0  ← Created v66 branch, connected
Client C: v65.16 ← Connected to v65 branch

Result: Clients on different branches, no sync between A/C and B
```

### Scenario 4: Client Upgrades Mid-Session

```
Initial:
  Server: Branch v65 (v65.16)
  Client A: v65.16 ← Connected

User upgrades app:
  Client A: v66.0 ← Disconnected from v65
                  ← Server detects major version change
                  ← Creates new v66 branch
                  ← Runs migration locally
                  ← Syncs migration ops to v66 branch
                  ← Reconnects to v66 branch

Result: Client A now on separate v66 branch
```

---

## Branch Management

### Branch Lifecycle

```
1. Creation
   └─ Client with new major version connects
   └─ Server creates new branch
   └─ Initial graph state copied to branch

2. Active Use
   └─ Multiple clients connect to branch
   └─ Operations synced within branch
   └─ Schema evolves through minor versions

3. Merge (Optional)
   └─ Newer branch migrated to even newer version
   └─ Both branches become compatible
   └─ Branches merged

4. Archival
   └─ No clients using branch
   └─ Branch marked inactive
   └─ Preserved for historical access
```

### Branch Merging

```clojure
(defn can-merge-branches?
  "Check if two branches can be merged"
  [branch-a branch-b]
  (let [schema-a (:schema-version branch-a)
        schema-b (:schema-version branch-b)]
    (cond
      ;; Same major version - can merge
      (= (:major schema-a) (:major schema-b))
      {:can-merge? true
       :strategy :minor-version-merge}

      ;; Different major - need migration
      (not= (:major schema-a) (:major schema-b))
      {:can-merge? false
       :reason "Major version mismatch"
       :suggestion "Migrate lower version branch"})))

(defn merge-branches
  "Merge two compatible branches"
  [target-branch source-branch]
  (let [operations (get-operations-since source-branch
                                        (:last-merge-point source-branch))]
    ;; Replay operations into target branch
    (doseq [op operations]
      (apply-operation target-branch op))

    ;; Update merge metadata
    (update-branch-metadata! target-branch
                            {:merged-from (:branch-id source-branch)
                             :merged-at (js/Date.now)})))
```

### Branch Pruning

```clojure
(defn prune-inactive-branches
  "Remove branches with no active clients"
  [graph-id]
  (let [branches (get-branches graph-id)
        inactive (filter #(zero? (count-active-clients %)) branches)
        old (filter #(> (days-since-activity %) 90) inactive)]
    (doseq [branch old]
      (archive-branch! branch))))
```

---

## Failure Recovery

### Scenario 1: Sync Interrupted During Migration

```
Client A running migration v65.15 -> v65.16
  Step 1: Add property ✅ (synced)
  Step 2: Add class ✅ (synced)
  Step 3: Fix data ❌ (connection lost)

Recovery:
  Client A: Completes migration locally
  Client A: Reconnects to server
  Client A: Resumes sync from last successful op
  Client A: Sends remaining migration ops
  Other clients: Receive and apply remaining ops
```

### Scenario 2: Conflicting Migrations

```
Client A and Client B both at v65.15
Both independently upgrade to v65.16 simultaneously

Client A migrations:
  - Add property :block/priority
  - Add class :task

Client B migrations:
  - Add property :block/priority (same name!)
  - Add class :task (same name!)

Server resolution:
  - Detect duplicate property/class creation
  - Keep first client's version (Client A)
  - Transform Client B's ops to no-ops
  - Notify Client B of conflict
```

### Scenario 3: Branch Version Rollback

```
Problem: v66 branch has critical bug
Solution: Revert users to v65 branch

Process:
1. Mark v66 branch as deprecated
2. When v66 clients connect, force disconnect
3. Present "please downgrade app" message
4. Users downgrade to v65 client
5. Users reconnect to v65 branch
6. Data from v66 branch migrated back (if possible)
```

### Recovery Strategies

```clojure
(defn recover-from-sync-failure
  "Recover from sync failure"
  [client-state server-state]
  (cond
    ;; Client ahead of server
    (> (:version client-state) (:version server-state))
    {:action :push-missing-ops
     :ops (get-ops-after client-state (:version server-state))}

    ;; Server ahead of client
    (< (:version client-state) (:version server-state))
    {:action :pull-missing-ops
     :ops (get-ops-after server-state (:version client-state))}

    ;; Diverged (conflict)
    (diverged? client-state server-state)
    {:action :resolve-divergence
     :strategy :rebase-on-server}))
```

---

## Key Insights

### 1. Branch-Based Isolation

**Benefit:** Different major versions can coexist without conflicts  
**Cost:** Users on different branches cannot collaborate

### 2. Minor Version Flexibility

**Benefit:** Clients with different minor versions can sync  
**Cost:** Feature availability varies by client version

### 3. Migration Op Sync

**Benefit:** All clients see schema changes in real-time  
**Cost:** Migration failures affect all clients in branch

### 4. No Automatic Branch Merge

**Limitation:** Manual intervention needed to merge diverged branches  
**Workaround:** Export/import or migrate to common version

### 5. Last-Write-Wins Simplicity

**Benefit:** Simple conflict resolution, no complex OT  
**Cost:** Potential data loss on concurrent edits

---

## RTC Schema Evolution Best Practices

### 1. Coordinate Major Version Upgrades

```clojure
;; Notify all clients before major version upgrade
(defn prepare-major-upgrade
  [graph-id new-major-version]
  (broadcast-to-all-clients graph-id
    {:type :major-upgrade-pending
     :new-version new-major-version
     :deadline (+ (js/Date.now) (* 7 24 60 60 1000))  ;; 7 days
     :message "Please upgrade to latest version"}))
```

### 2. Test Migrations on Branches

```clojure
;; Create test branch for migration testing
(defn create-test-branch
  [graph-id target-version]
  {:branch-id (str "test-v" (:major target-version))
   :schema-version target-version
   :is-test? true
   :auto-delete-after (* 24 60 60 1000)})  ;; 24 hours
```

### 3. Graceful Degradation

```clojure
;; Handle features not available in older clients
(defn sync-entity-to-client
  [entity client-version]
  (if (supports-feature? client-version :block/priority)
    entity
    (dissoc entity :block/priority)))  ;; Remove unsupported attributes
```

### 4. Monitor Branch Health

```clojure
(defn branch-health-metrics
  [branch]
  {:active-clients (count-active-clients branch)
   :schema-version (:schema-version branch)
   :ops-per-minute (calculate-op-rate branch)
   :conflict-rate (calculate-conflict-rate branch)
   :last-activity (:last-activity branch)})
```

---

## Key Takeaways

1. **Major Version = New Branch:** Different major versions cannot sync
2. **Minor Version = Compatible:** Same major, different minor versions can coexist
3. **Migration Ops:** Local migrations converted to sync operations
4. **Conflict Resolution:** Last-write-wins for simplicity
5. **Branch Isolation:** Protects against incompatible changes
6. **No Auto-Merge:** Manual intervention required for branch merging
7. **Graceful Degradation:** Older clients ignore newer features

---

## Next Steps

Proceed to **Part 6: Failure Patterns & Recommendations** for comprehensive analysis of common failure scenarios and mitigation strategies.
