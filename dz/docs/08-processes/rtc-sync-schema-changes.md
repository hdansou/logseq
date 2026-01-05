# RTC/Sync Schema Change Considerations

**Document Type:** Process Guide  
**Last Updated:** December 28, 2025  
**Related Investigation:** [Schema Evolution and Data Migration](../07-investigations/schema-evolution-and-data-migration.md)

---

## Overview

Real-Time Collaboration (RTC) in Logseq enables multiple devices to sync graph changes in real-time. Schema changes can significantly impact RTC behavior, causing sync conflicts, data loss, or synchronization failures if not handled properly.

**Key Principle:** Every property must have explicit RTC configuration, and schema changes must consider multi-device scenarios where different schema versions may interact.

---

## Table of Contents

1. [RTC Architecture Overview](#rtc-architecture-overview)
2. [Property RTC Configuration](#property-rtc-configuration)
3. [Schema Version Synchronization](#schema-version-synchronization)
4. [Conflict Resolution](#conflict-resolution)
5. [Multi-Device Compatibility](#multi-device-compatibility)
6. [Remote Update Processing](#remote-update-processing)
7. [Adding RTC-Synced Properties](#adding-rtc-synced-properties)
8. [Testing RTC Behavior](#testing-rtc-behavior)
9. [Common RTC Issues](#common-rtc-issues)
10. [Best Practices](#best-practices)

---

## RTC Architecture Overview

### Core Components

**Location:** [src/main/frontend/worker/rtc/](src/main/frontend/worker/rtc/)

```
┌─────────────────────────────────────────────────┐
│              RTC Sync System                    │
├─────────────────────────────────────────────────┤
│                                                 │
│  ┌──────────────┐         ┌─────────────────┐ │
│  │ Local Device │◄────────┤  WebSocket      │ │
│  │              │         │  Connection     │ │
│  │  - Detects   │         └────────┬────────┘ │
│  │    changes   │                  │          │
│  │  - Generates │         ┌────────▼────────┐ │
│  │    ops       │         │  Remote Server  │ │
│  │  - Applies   │         │                 │ │
│  │    remote    │         │  - Broadcasts   │ │
│  │    updates   │         │  - Resolves     │ │
│  └──────┬───────┘         │    conflicts    │ │
│         │                 └────────┬────────┘ │
│         │                          │          │
│         │                 ┌────────▼────────┐ │
│         └────────────────►│ Other Devices   │ │
│                           │                 │ │
│                           │  - Receive ops  │ │
│                           │  - Apply        │ │
│                           │    updates      │ │
│                           └─────────────────┘ │
└─────────────────────────────────────────────────┘
```

### Key Files

| File                                                                | Purpose                                  |
| ------------------------------------------------------------------- | ---------------------------------------- |
| [core.cljs](src/main/frontend/worker/rtc/core.cljs)                 | RTC coordination and state management    |
| [op_mem_layer.cljs](src/main/frontend/worker/rtc/op_mem_layer.cljs) | Applying remote changes to local DB      |
| [client_op.cljs](src/main/frontend/worker/rtc/client_op.cljs)       | Client operation handlers                |
| [local_change.cljs](src/main/frontend/worker/rtc/local_change.cljs) | Detecting and broadcasting local changes |
| [ws.cljs](src/main/frontend/worker/rtc/ws.cljs)                     | WebSocket communication                  |

### Sync Flow

```
Local Edit
    ↓
Detect Change
    ↓
Generate Client Op
    ↓
Send to Server (WebSocket)
    ↓
Server Broadcasts
    ↓
Other Devices Receive
    ↓
Apply Remote Op
    ↓
Resolve Conflicts (if any)
    ↓
Update Local DB
```

---

## Property RTC Configuration

### RTC Configuration Keys

Every property definition must include an `:rtc` map:

```clojure
{:db/ident :logseq.property/my-property
 :title "My Property"
 :schema {:type :default
          :cardinality :one}
 :rtc {:rtc/ignore-attr-when-init-upload true
       :rtc/ignore-attr-when-init-download true
       :rtc/ignore-attr-when-syncing true}}
```

### RTC Configuration Options

| Key                                   | Values         | Meaning                                                  |
| ------------------------------------- | -------------- | -------------------------------------------------------- |
| `:rtc/ignore-attr-when-init-upload`   | `true`/`false` | Skip this property when uploading graph for first time   |
| `:rtc/ignore-attr-when-init-download` | `true`/`false` | Skip this property when downloading graph for first time |
| `:rtc/ignore-attr-when-syncing`       | `true`/`false` | Skip this property during ongoing sync                   |

### Common RTC Patterns

#### Pattern 1: Never Sync (Device-Local)

**Use for:** Device-specific settings, UI state, local cache

```clojure
:rtc {:rtc/ignore-attr-when-init-upload true
      :rtc/ignore-attr-when-init-download true
      :rtc/ignore-attr-when-syncing true}
```

**Example properties:**

- `:logseq.property/collapsed?` - Local UI state
- `:logseq.property/view-mode` - Device-specific preference

#### Pattern 2: Always Sync (Collaborative)

**Use for:** User content, graph structure, shared data

```clojure
:rtc {:rtc/ignore-attr-when-init-upload false
      :rtc/ignore-attr-when-init-download false
      :rtc/ignore-attr-when-syncing false}
```

**Example properties:**

- `:block/content` - Block text
- `:block/tags` - Page tags
- `:logseq.property/author` - Metadata

#### Pattern 3: Sync Initial, Ignore Updates

**Use for:** Initial metadata that shouldn't change after creation

```clojure
:rtc {:rtc/ignore-attr-when-init-upload false
      :rtc/ignore-attr-when-init-download false
      :rtc/ignore-attr-when-syncing true}
```

**Example properties:**

- `:block/created-at` - Creation timestamp
- `:logseq.property/creator` - Original author

#### Pattern 4: Skip Initial Upload, But Sync Later

**Use for:** Properties populated by server or after initial setup

```clojure
:rtc {:rtc/ignore-attr-when-init-upload true
      :rtc/ignore-attr-when-init-download false
      :rtc/ignore-attr-when-syncing false}
```

### Default RTC Configuration

**Location:** [deps/db/src/logseq/db/frontend/property/build.cljc](deps/db/src/logseq/db/frontend/property/build.cljc)

```clojure
;; Ignore completely (never sync)
(def ^:private property-ignore-rtc
  {:rtc/ignore-attr-when-init-upload true
   :rtc/ignore-attr-when-init-download true
   :rtc/ignore-attr-when-syncing true})

;; Ignore upload and ongoing sync
(def ^:private property-ignore-rtc-upload-sync
  {:rtc/ignore-attr-when-init-upload true
   :rtc/ignore-attr-when-syncing true})
```

### Decision Tree for RTC Configuration

```
Should property sync?
    │
    ├─ No (device-local) → property-ignore-rtc
    │
    └─ Yes
        │
        ├─ User content? → Sync all (false/false/false)
        │
        ├─ Creation metadata? → Sync initial, ignore updates
        │
        └─ Server-managed? → Skip upload, sync download
```

---

## Schema Version Synchronization

### Schema Version in RTC

Three schema version values tracked:

```clojure
;; Local app schema version
:logseq.kv/schema-version "65.17"

;; Schema version when graph was created
:logseq.kv/graph-initial-schema-version "65.10"

;; Remote (server) schema version
:logseq.kv/remote-schema-version "65.16"
```

### Version Mismatch Scenarios

#### Scenario 1: Local Newer Than Remote

```
Device A: v65.17 (has new property)
     ↓ sync
Device B: v65.16 (doesn't know about property)
```

**What happens:**

- Device A can send property updates
- Device B ignores unknown properties (graceful degradation)
- Device B won't display new property
- No data loss, but feature unavailable

**Handling:**

```clojure
(defn handle-unknown-property-update
  "Gracefully handle updates to properties not in local schema"
  [property-ident value]
  (if (property-exists-in-schema? property-ident)
    (apply-property-update property-ident value)
    (do
      (log/warn "Ignoring update to unknown property:" property-ident)
      ;; Store in pending updates for later?
      nil)))
```

#### Scenario 2: Remote Newer Than Local

```
Device A: v65.16 (outdated)
     ↓ sync
Device B: v65.17 (has new property)
```

**What happens:**

- Device B sends property updates
- Device A receives unknown property updates
- Device A should prompt user to upgrade

**Handling:**

```clojure
(defn check-remote-schema-version
  "Check if remote schema is newer, prompt upgrade"
  [remote-version local-version]
  (when (> (parse-version remote-version)
           (parse-version local-version))
    (show-upgrade-prompt
     "A newer version of Logseq is available. Please upgrade to sync properly.")))
```

### Gradual Rollout Strategy

When releasing schema changes:

1. **Version N-1:** Add property with RTC ignore (not synced yet)
2. **Wait for adoption** (2-4 weeks)
3. **Version N:** Enable RTC sync for property
4. **Deprecation:** After N+3 versions, can safely assume all users upgraded

### Schema Version Validation

```clojure
(defn validate-sync-compatibility
  "Check if local and remote schemas are compatible for sync"
  [local-version remote-version]
  (let [local (parse-version local-version)
        remote (parse-version remote-version)
        major-diff (- (:major remote) (:major local))
        minor-diff (- (:minor remote) (:minor local))]
    (cond
      ;; Major version mismatch - incompatible
      (not= (:major local) (:major remote))
      {:compatible? false
       :message "Major version mismatch. Sync disabled."}

      ;; Remote more than 3 minor versions ahead
      (> minor-diff 3)
      {:compatible? false
       :message "Please upgrade Logseq to continue syncing."}

      ;; Remote slightly ahead - degraded mode
      (pos? minor-diff)
      {:compatible? true
       :degraded? true
       :message "Some features unavailable until you upgrade."}

      ;; Compatible
      :else
      {:compatible? true
       :degraded? false})))
```

---

## Conflict Resolution

### Types of Conflicts

#### 1. Content Conflicts

**Scenario:** Same block edited on multiple devices simultaneously

```clojure
;; Device A
{:block/uuid uuid
 :block/content "Version A"}

;; Device B (same time)
{:block/uuid uuid
 :block/content "Version B"}
```

**Resolution:** Last-write-wins (based on timestamp)

#### 2. Order Conflicts

**Scenario:** Blocks reordered differently on multiple devices

```clojure
;; Device A
{:block/uuid uuid-1 :block/order "a0"}
{:block/uuid uuid-2 :block/order "a1"}

;; Device B (same time)
{:block/uuid uuid-1 :block/order "a1"}
{:block/uuid uuid-2 :block/order "a0"}
```

**Resolution:** Fractional indexing to create new order values

**Location:** [src/main/frontend/worker/rtc/op_mem_layer.cljs](src/main/frontend/worker/rtc/op_mem_layer.cljs)

```clojure
(defn resolve-order-conflict
  "Generate new order value between two existing orders"
  [order-before order-after]
  (fractional-index/between order-before order-after))
```

#### 3. Property Conflicts

**Scenario:** Same property set to different values

```clojure
;; Device A
{:logseq.property/status "done"}

;; Device B (same time)
{:logseq.property/status "in-progress"}
```

**Resolution:** Last-write-wins

#### 4. Structural Conflicts

**Scenario:** Block moved to different parents on different devices

```clojure
;; Device A - block moved under page 1
{:block/uuid uuid :block/parent page-1-uuid}

;; Device B - same block moved under page 2
{:block/uuid uuid :block/parent page-2-uuid}
```

**Resolution:** Last-write-wins, may cause unexpected structure

### Conflict Detection

```clojure
(defn detect-datom-conflicts
  "Find conflicting datoms (same entity, same attribute, different values)"
  [local-datoms remote-datoms]
  (let [local-by-ea (group-datoms-by-entity-attr local-datoms)
        remote-by-ea (group-datoms-by-entity-attr remote-datoms)]
    (->> (keys local-by-ea)
         (keep (fn [ea-key]
                 (let [local-val (get local-by-ea ea-key)
                       remote-val (get remote-by-ea ea-key)]
                   (when (and local-val remote-val
                             (not= local-val remote-val))
                     {:entity-attr ea-key
                      :local-value local-val
                      :remote-value remote-val
                      :conflict-type :value-conflict})))))))
```

### Conflict Resolution Functions

**Location:** [src/main/frontend/worker/rtc/op_mem_layer.cljs](src/main/frontend/worker/rtc/op_mem_layer.cljs)

```clojure
(defn remove-conflict-same-block-datoms
  "Remove conflicting datoms from remote update.
   Keep local changes (last-write on local device wins for perceived responsiveness)"
  [local-tx-data remote-tx-data]
  (let [local-block-updates (group-by-block local-tx-data)
        conflict-block-ids (keys local-block-updates)]
    ;; Remove remote updates for blocks being edited locally
    (remove (fn [remote-datom]
              (contains? conflict-block-ids (:e remote-datom)))
            remote-tx-data)))
```

### User-Visible Conflict Handling

```clojure
(defn notify-conflict-resolution
  "Inform user about conflict and resolution"
  [conflict-type resolved-value]
  (case conflict-type
    :content-conflict
    (log/info "Content conflict resolved using latest version")

    :order-conflict
    (log/info "Block order adjusted due to concurrent reordering")

    :structural-conflict
    (log/warn "Block location changed due to concurrent moves")

    (log/info "Conflict resolved:" conflict-type)))
```

---

## Multi-Device Compatibility

### Testing Multi-Device Scenarios

#### Test Setup

```clojure
(deftest test-multi-device-sync
  (testing "Schema change syncs correctly across devices"
    (let [device-a (create-test-device {:schema-version "65.17"})
          device-b (create-test-device {:schema-version "65.16"})
          rtc-server (create-test-rtc-server)]

      ;; Device A adds property (new schema)
      (add-property device-a :logseq.property/new-prop "value-a")
      (sync-to-server device-a rtc-server)

      ;; Device B receives update
      (sync-from-server device-b rtc-server)

      ;; Device B should handle gracefully
      (is (no-errors? device-b))
      (is (or (property-ignored? device-b :logseq.property/new-prop)
              (upgrade-prompted? device-b))))))
```

### Compatibility Matrix

| Local Version | Remote Version | Behavior                                           |
| ------------- | -------------- | -------------------------------------------------- |
| v65.17        | v65.17         | ✅ Full sync                                       |
| v65.17        | v65.16         | ✅ Local has newer features (not synced to remote) |
| v65.16        | v65.17         | ⚠️ Remote has newer features (ignored or degraded) |
| v66.0         | v65.x          | ❌ Major version mismatch - sync disabled          |

### Graceful Degradation

```clojure
(defn handle-schema-version-mismatch
  "Handle sync when schema versions differ"
  [local-version remote-version]
  (cond
    ;; Major version mismatch - abort sync
    (not= (major local-version) (major remote-version))
    (throw (ex-info "Incompatible schema versions"
                   {:local local-version
                    :remote remote-version}))

    ;; Local newer - filter out new properties before sending
    (> (minor local-version) (minor remote-version))
    (filter-properties-by-version remote-version)

    ;; Remote newer - ignore unknown properties from remote
    (< (minor local-version) (minor remote-version))
    (ignore-unknown-properties-from-remote)

    ;; Same version - full sync
    :else
    (full-sync)))
```

---

## Remote Update Processing

### Update Types

**Location:** [src/main/frontend/worker/rtc/op_mem_layer.cljs](src/main/frontend/worker/rtc/op_mem_layer.cljs)

```clojure
(defmulti apply-remote-update
  "Apply remote update based on operation type"
  :op-type)

;; Block deletion
(defmethod apply-remote-update :delete-blocks [update db]
  ...)

;; Block movement
(defmethod apply-remote-update :move-blocks [update db]
  ...)

;; Order update
(defmethod apply-remote-update :update-block-order-directly [update db]
  ...)

;; Block insertion
(defmethod apply-remote-update :insert-blocks [update db]
  ...)

;; General transaction
(defmethod apply-remote-update :transact-data [update db]
  ...)
```

### Applying Remote Updates

```clojure
(defn apply-remote-ops
  "Apply batch of remote operations to local database"
  [conn remote-ops]
  (doseq [op remote-ops]
    ;; Validate operation
    (when (valid-remote-op? op)
      ;; Check for conflicts
      (let [conflicts (detect-conflicts @conn op)]
        (if (seq conflicts)
          ;; Resolve conflicts first
          (let [resolved-op (resolve-conflicts op conflicts)]
            (apply-update conn resolved-op))
          ;; No conflicts, apply directly
          (apply-update conn op))))))
```

### Property-Specific Sync Handling

```clojure
(defn should-sync-property?
  "Determine if property should be synced based on RTC config and context"
  [property-ident context]
  (let [property (get-property-definition property-ident)
        rtc-config (:rtc property)]
    (case context
      :init-upload
      (not (:rtc/ignore-attr-when-init-upload rtc-config))

      :init-download
      (not (:rtc/ignore-attr-when-init-download rtc-config))

      :syncing
      (not (:rtc/ignore-attr-when-syncing rtc-config))

      ;; Default: sync
      true)))
```

---

## Adding RTC-Synced Properties

### Step-by-Step Process

#### Step 1: Define Property with RTC Config

```clojure
;; In deps/db/src/logseq/db/frontend/property/build.cljc

{:db/ident :logseq.property/my-sync-property
 :title "My Sync Property"
 :schema {:type :default
          :cardinality :one
          :public? true}

 ;; Critical: Explicit RTC configuration
 :rtc {:rtc/ignore-attr-when-init-upload false  ; Do sync on initial upload
       :rtc/ignore-attr-when-init-download false ; Do sync on initial download
       :rtc/ignore-attr-when-syncing false}}     ; Do sync during ongoing sync
```

#### Step 2: Add to RTC Constants (If Needed)

**Location:** [src/main/frontend/worker/rtc/const.cljs](src/main/frontend/worker/rtc/const.cljs)

```clojure
;; If property needs special RTC handling, add to constants
(def rtc-synced-properties
  #{:block/content
    :block/tags
    :logseq.property/my-sync-property})  ; Add here
```

#### Step 3: Handle in Conflict Resolution

If property can have conflicts, add resolution logic:

```clojure
(defmethod resolve-property-conflict :logseq.property/my-sync-property
  [local-value remote-value local-timestamp remote-timestamp]
  ;; Strategy: last-write-wins
  (if (> remote-timestamp local-timestamp)
    remote-value
    local-value))
```

#### Step 4: Test Multi-Device Sync

```clojure
(deftest test-my-sync-property-rtc
  (testing "Property syncs between devices"
    (let [device-a (create-test-device)
          device-b (create-test-device)
          rtc-server (create-test-rtc-server)]

      ;; Set property on device A
      (set-property device-a :logseq.property/my-sync-property "value-a")
      (sync-to-server device-a rtc-server)

      ;; Sync to device B
      (sync-from-server device-b rtc-server)

      ;; Verify property synced
      (is (= "value-a"
             (get-property device-b :logseq.property/my-sync-property)))))

  (testing "Property conflicts resolve correctly"
    ;; Test concurrent edits
    ...))
```

### RTC Configuration Decision Flowchart

```
New Property
    │
    ├─ Is it user content/collaborative data?
    │   └─ Yes → Sync all (false/false/false)
    │
    ├─ Is it device-specific (UI state, local cache)?
    │   └─ Yes → Never sync (true/true/true)
    │
    ├─ Is it creation metadata (created-at, creator)?
    │   └─ Yes → Sync initial, ignore updates (false/false/true)
    │
    └─ Is it computed/server-managed?
        └─ Yes → Skip upload, sync download (true/false/false)
```

---

## Testing RTC Behavior

### Test Categories

#### 1. Basic Sync Test

```clojure
(deftest test-property-basic-sync
  (testing "Property value syncs from device A to device B"
    (let [devices (create-two-device-setup)
          property :logseq.property/test-prop]
      ;; Set on device A
      (set-property! (devices :a) property "test-value")
      (wait-for-sync)

      ;; Verify on device B
      (is (= "test-value"
             (get-property (devices :b) property))))))
```

#### 2. Conflict Resolution Test

```clojure
(deftest test-property-conflict
  (testing "Concurrent edits resolve correctly"
    (let [devices (create-two-device-setup)
          property :logseq.property/test-prop
          block-uuid (random-uuid)]
      ;; Simultaneous edits
      (set-property! (devices :a) property "value-a" :on-block block-uuid)
      (set-property! (devices :b) property "value-b" :on-block block-uuid)
      (wait-for-sync)

      ;; Both should converge to same value (last-write-wins)
      (let [value-a (get-property (devices :a) property :on-block block-uuid)
            value-b (get-property (devices :b) property :on-block block-uuid)]
        (is (= value-a value-b))))))
```

#### 3. Schema Version Mismatch Test

```clojure
(deftest test-schema-version-mismatch
  (testing "Newer schema sends property, older schema ignores gracefully"
    (let [device-new (create-device {:schema-version "65.17"})
          device-old (create-device {:schema-version "65.16"})
          new-property :logseq.property/v65-17-only]
      ;; New device sets property
      (set-property! device-new new-property "value")
      (sync-between device-new device-old)

      ;; Old device should not error
      (is (no-sync-errors? device-old))
      ;; Old device should not have property
      (is (nil? (get-property device-old new-property))))))
```

#### 4. RTC Configuration Test

```clojure
(deftest test-rtc-configuration-respected
  (testing "Property with ignore-when-syncing doesn't sync"
    (let [devices (create-two-device-setup)
          no-sync-property :logseq.property/local-only]
      ;; Ensure property configured to not sync
      (is (get-in (get-property-def no-sync-property)
                  [:rtc :rtc/ignore-attr-when-syncing]))

      ;; Set on device A
      (set-property! (devices :a) no-sync-property "local-value")
      (wait-for-sync)

      ;; Should NOT appear on device B
      (is (nil? (get-property (devices :b) no-sync-property))))))
```

#### 5. Order Conflict Test

```clojure
(deftest test-order-conflict-resolution
  (testing "Concurrent block reordering resolves without data loss"
    (let [devices (create-two-device-setup)
          blocks [(random-uuid) (random-uuid) (random-uuid)]]
      ;; Both devices reorder blocks simultaneously
      (reorder-blocks! (devices :a) blocks [0 2 1])  ; New order: 0, 2, 1
      (reorder-blocks! (devices :b) blocks [2 1 0])  ; New order: 2, 1, 0
      (wait-for-sync)

      ;; All blocks should still exist
      (is (= 3 (count-blocks (devices :a))))
      (is (= 3 (count-blocks (devices :b))))

      ;; Orders should converge (may not match original intent, but consistent)
      (is (= (get-block-order (devices :a) blocks)
             (get-block-order (devices :b) blocks))))))
```

### Test Environment Setup

```clojure
(defn create-two-device-setup
  "Create two test devices connected to same RTC server"
  []
  (let [server (create-test-rtc-server)
        device-a (create-test-device {:name "Device A"
                                      :schema-version "65.17"})
        device-b (create-test-device {:name "Device B"
                                      :schema-version "65.17"})]
    ;; Connect both to server
    (connect-to-server device-a server)
    (connect-to-server device-b server)

    {:a device-a
     :b device-b
     :server server}))
```

---

## Common RTC Issues

### Issue 1: Property Not Syncing

**Symptoms:**

- Property changes on device A don't appear on device B
- No errors shown

**Causes:**

```clojure
;; ❌ Missing RTC configuration
{:db/ident :logseq.property/my-prop
 :schema {:type :default}}
;; No :rtc key!

;; ❌ RTC explicitly disabled
:rtc {:rtc/ignore-attr-when-syncing true}
```

**Fix:**

```clojure
;; ✅ Add explicit RTC configuration
:rtc {:rtc/ignore-attr-when-init-upload false
      :rtc/ignore-attr-when-init-download false
      :rtc/ignore-attr-when-syncing false}
```

### Issue 2: Sync Conflicts Causing Data Loss

**Symptoms:**

- Changes disappear after sync
- Last edit wins, earlier edits lost

**Cause:**

- Last-write-wins conflict resolution
- No merge strategy for complex properties

**Mitigation:**

```clojure
;; Option 1: Timestamp-based resolution (built-in)
;; Newer edit always wins

;; Option 2: Custom merge for specific properties
(defmethod resolve-property-conflict :logseq.property/tags
  [local-value remote-value local-ts remote-ts]
  ;; Merge tags instead of overwrite
  (set/union (set local-value) (set remote-value)))
```

### Issue 3: Schema Version Mismatch Errors

**Symptoms:**

- Sync stops working after app upgrade
- Error about incompatible versions

**Cause:**

- Major schema version mismatch
- Remote too many versions ahead

**Fix:**

```clojure
;; Prompt user to upgrade
(when (incompatible-schema-versions? local-version remote-version)
  (show-modal
   :title "Upgrade Required"
   :message "Please upgrade Logseq to continue syncing."
   :action (fn [] (open-download-page))))
```

### Issue 4: Circular Sync Loop

**Symptoms:**

- Same changes sync back and forth endlessly
- High network/CPU usage

**Cause:**

- Local change triggers remote update, which triggers local change again

**Fix:**

```clojure
(defn apply-remote-update [update]
  ;; Mark as remote-originating to prevent re-broadcasting
  (binding [*suppress-local-change-detection* true]
    (apply-update-to-db update)))
```

### Issue 5: Order Conflicts Creating Invalid State

**Symptoms:**

- Blocks appear in wrong order
- Duplicate order values

**Cause:**

- Concurrent reordering on multiple devices

**Fix:**

```clojure
;; Use fractional indexing to avoid conflicts
(defn resolve-order-conflict [existing-orders new-position]
  (let [before (nth existing-orders (dec new-position))
        after (nth existing-orders new-position)]
    (fractional-index/between before after)))
```

---

## Best Practices

### 1. Always Specify RTC Configuration

```clojure
;; ❌ Bad - implicit behavior
{:db/ident :logseq.property/my-prop
 :schema {:type :default}}

;; ✅ Good - explicit configuration
{:db/ident :logseq.property/my-prop
 :schema {:type :default}
 :rtc {:rtc/ignore-attr-when-init-upload false
       :rtc/ignore-attr-when-init-download false
       :rtc/ignore-attr-when-syncing false}}
```

### 2. Test Multi-Device Scenarios

Every property change should be tested with:

- Two devices, same schema version
- Two devices, different schema versions
- Concurrent edits on same property
- Network interruption and recovery

### 3. Consider Conflict Resolution

For properties with complex values:

- Provide custom merge logic
- Document conflict behavior
- Test conflict scenarios

### 4. Gradual Rollout for Sync Changes

When enabling sync for existing property:

1. Release with sync disabled first
2. Wait for majority adoption (2-4 weeks)
3. Enable sync in next release

### 5. Monitor Schema Version Distribution

```clojure
;; Track schema versions of connected devices
(defn log-schema-version-stats []
  (let [versions (get-connected-device-versions)]
    (log/info "Schema versions in use:"
              (frequencies versions))))
```

### 6. Handle Graceful Degradation

```clojure
(defn sync-with-version-awareness
  "Sync only properties compatible with remote version"
  [local-version remote-version changes]
  (let [compatible-properties
        (filter-properties-by-version remote-version changes)]
    (send-to-remote compatible-properties)))
```

### 7. Document RTC Behavior

In property definition, comment sync behavior:

```clojure
{:db/ident :logseq.property/my-prop
 :title "My Property"
 :description "Stores user preference. Syncs across devices."  ; Document!
 :schema {:type :default}
 :rtc {:rtc/ignore-attr-when-syncing false}}  ; Syncs
```

### 8. Use Version Gates for New Features

```clojure
(defn use-new-sync-feature? []
  (and (>= (parse-version (get-schema-version)) "65.17")
       (rtc-enabled?)))
```

---

## Quick Reference

### RTC Configuration Checklist

- [ ] Property has explicit `:rtc` configuration
- [ ] RTC behavior matches property purpose (device-local vs collaborative)
- [ ] Conflict resolution strategy defined (if complex value)
- [ ] Multi-device testing completed
- [ ] Schema version compatibility considered
- [ ] Documentation includes sync behavior
- [ ] Added to RTC constants if special handling needed

### Property RTC Patterns

| Property Type     | RTC Config                  | Example                       |
| ----------------- | --------------------------- | ----------------------------- |
| User content      | All false                   | `:block/content`              |
| Device UI state   | All true                    | `:logseq.property/collapsed?` |
| Creation metadata | Init false, sync true       | `:block/created-at`           |
| Server-managed    | Upload true, download false | `:logseq.property/server-id`  |

### Testing Scenarios

- [ ] Two devices, same version, simultaneous edit
- [ ] Two devices, different versions, new property sync
- [ ] Conflict resolution (concurrent same-property edits)
- [ ] Network interruption and recovery
- [ ] Order conflicts (block reordering)
- [ ] Schema version mismatch (major difference)

---

## Related Documentation

- [Investigation Overview](../07-investigations/schema-evolution-and-data-migration.md)
- [Schema Migration Best Practices](./schema-migration-best-practices.md)
- [Validation Guidelines](./validation-best-practices.md)
- [Import/Export Best Practices](./import-export-best-practices.md)
- [Testing Strategies](./schema-change-testing.md)
- [PR Review Checklist](./schema-change-pr-checklist.md)

---

## Changelog

| Date       | Changes                                         |
| ---------- | ----------------------------------------------- |
| 2025-12-28 | Initial version based on investigation findings |
