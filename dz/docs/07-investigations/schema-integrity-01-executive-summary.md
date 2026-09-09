# Schema Changes & Data Integrity Investigation

## Part 1: Executive Summary & Overview

**Investigation Date:** 2026-05-30  
**Schema Version Analyzed:** 65.33  
**Investigator:** Development Team

> **Last revised:** 2026-05-30

---

## Executive Summary

This investigation examines how schema changes impact data integrity across Logseq's database graph system, focusing on upgrades, imports, exports, validation, and real-time collaboration (RTC) synchronization.

### Critical Findings

#### 1. **No Backward Compatibility**

- **Impact:** CRITICAL
- **Finding:** Older Logseq versions cannot open graphs created with newer schema versions
- **Consequence:** Users forced to upgrade all clients or lose access to graphs
- **Evidence:** [deps/db/src/logseq/db/frontend/schema.cljs](../../../deps/db/src/logseq/db/frontend/schema.cljs#L34-L40) - Schema comparison logic has no downgrade path

#### 2. **Strict Validation Enforcement**

- **Impact:** HIGH
- **Finding:** DB-graph transactions are validated by default against Malli schemas, with explicit skip flags for specific internal flows
- **Consequence:** Validation regressions can block legitimate writes unless operations are in explicitly exempted paths
- **Evidence:** [deps/db/src/logseq/db.cljs](../../../deps/db/src/logseq/db.cljs#L103-L172) - Validation gate and `validate-tx-report` path

#### 3. **Sequential Migration Risk**

- **Impact:** HIGH
- **Finding:** Migrations run sequentially without rollback mechanism
- **Consequence:** A failed later step can leave the graph only partially upgraded across versions
- **Evidence:** [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs#L287-L324) - Per-version upgrades run in order with no cross-version rollback wrapper

#### 4. **Export as Last Resort Backup**

- **Impact:** MEDIUM
- **Finding:** Export performs structural validation and fails on undefined refs/classes/properties by default
- **Consequence:** Invalid graph structures can still block standard exports
- **Evidence:** [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs#L1010-L1054) - `basic-validate-export` and `build-export` validation flow

#### 5. **Expanded Migration Surface (65.17-65.33)**

- **Impact:** MEDIUM
- **Finding:** Migration coverage now includes delete-property migrations, comment model/class migrations, and view-gallery property additions through 65.33
- **Consequence:** Schema evolution support is broader, but migration test coverage remains critical
- **Evidence:** [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs#L116-L152) - Current `schema-version->updates`

> **Removed:** Previous RTC branching references to `src/main/frontend/worker/rtc/branch_graph.cljs` and `src/main/frontend/worker/rtc/migrate.cljs` are stale; these files are no longer present.

## Findings: New Capabilities

- **Schema advanced to 65.33:** Current frontend DB schema version is now `65.33` ([deps/db/src/logseq/db/frontend/schema.cljs](../../../deps/db/src/logseq/db/frontend/schema.cljs#L33)).
- **Delete-property migrations:** Migration maps now support `:delete-properties` (used at 65.25) to actively retract deprecated attributes ([src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs#L142-L147)).
- **Comment domain migrations:** New class/property and repair migrations cover comments data integrity (65.27-65.32) ([src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs#L148-L151)).
- **Gallery view property migrations:** 65.33 adds gallery view configuration properties ([src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs#L151-L152)).
- **Export validation control hook:** Export supports an optional `:graph-options :catch-validation-errors?` path for controlled error handling ([deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs#L1047-L1052)).

---

## System Architecture Overview

### Dual Schema System

Logseq uses two complementary schema systems:

1. **DataScript Schema** (Structural)
   - Defines entity attributes, cardinality, types
   - Enforces referential integrity
   - Controls indexing behavior
   - Location: [deps/db/src/logseq/db/schema.cljs](../../../deps/db/src/logseq/db/schema.cljs)

2. **Malli Schema** (Business Logic)
   - Validates entity properties
   - Enforces business rules
   - Provides detailed error messages
   - Location: [deps/db/src/logseq/db/frontend/malli_schema.cljs](../../../deps/db/src/logseq/db/frontend/malli_schema.cljs)

### Schema Version Format

```clojure
{:major 65  ;; Breaking changes - increment forces all clients to upgrade
 :minor 33} ;; Non-breaking additions - backward compatible within major version
```

**Current Version:** 65.33  
**Location:** [deps/db/src/logseq/db/frontend/schema.cljs](../../../deps/db/src/logseq/db/frontend/schema.cljs#L33)

### Version Comparison Logic

```clojure
(defn compare-schema-version
   [x y]
   (apply compare
             (map (juxt :major :minor)
                     [(parse-schema-version x) (parse-schema-version y)])))
```

**Key Insight:** Major version mismatches always reject compatibility, regardless of minor version.

---

## Five Interconnected Systems

Schema changes cascade through these systems:

### 1. **Migration System**

- **Purpose:** Upgrade graphs to new schema versions
- **Trigger:** On graph open if version mismatch detected
- **Key File:** [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs)
- **Risk:** No rollback on failure

### 2. **Validation System**

- **Purpose:** Enforce schema constraints at runtime
- **Trigger:** Every transaction, manual full-graph validation, pre-export
- **Key File:** [deps/db/src/logseq/db/frontend/validate.cljs](../../../deps/db/src/logseq/db/frontend/validate.cljs)
- **Risk:** Overly strict validation can block legitimate operations

### 3. **Import System**

- **Purpose:** Load external graphs (EDN, SQLite)
- **Trigger:** User-initiated import
- **Key File:** [src/main/frontend/handler/db_based/import.cljs](../../../src/main/frontend/handler/db_based/import.cljs)
- **Risk:** Old exports incompatible with new schema versions

### 4. **Export System**

- **Purpose:** Create portable graph backups
- **Trigger:** User-initiated export
- **Key File:** [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs)
- **Risk:** Invalid graphs cannot be exported (backup blocked)

### 5. **RTC Sync System**

- **Purpose:** Real-time multi-client collaboration
- **Trigger:** Continuous background sync
- **Key Files:** [src/main/frontend/worker/sync/upload.cljs](../../../src/main/frontend/worker/sync/upload.cljs), [src/main/frontend/worker/sync/download.cljs](../../../src/main/frontend/worker/sync/download.cljs)
- **Risk:** Schema/version mismatches across clients and server can require full snapshot re-sync flows

> **Removed:** Legacy RTC worker path references under `src/main/frontend/worker/rtc/`.

---

## Schema Change Impact Matrix

| Change Type             | Migration | Validation | Import    | Export    | RTC Sync            |
| ----------------------- | --------- | ---------- | --------- | --------- | ------------------- |
| **New Entity Type**     | Optional  | Required   | Required  | Auto      | Re-sync/Migrate Ops |
| **New Property**        | Required  | Required   | Required  | Auto      | Ops Conversion      |
| **Rename Property**     | Required  | Required   | Required  | Auto      | Ops Conversion      |
| **Change Cardinality**  | Required  | Required   | Break     | Break     | Re-sync Required    |
| **New Validation Rule** | Optional  | Required   | May Break | May Break | Compatible          |
| **Remove Entity Type**  | Required  | Required   | Break     | Break     | Re-sync Required    |
| **Schema Version Bump** | Trigger   | Check      | Check     | Record    | Snapshot Decision   |

**Legend:**

- **Required:** System must be updated
- **Optional:** May need update for data transform
- **Auto:** System handles automatically
- **Break:** Old data incompatible
- **Re-sync Required:** RTC requires snapshot/re-download to recover compatibility
- **May Break:** Depends on strictness

---

## Investigation Scope

This investigation analyzed:

- **25+ source files** across 5 subsystems
- **~3,000 lines of ClojureScript code**
- **27 schema versions** (65.7 through 65.33)
- **4 migration types** (properties, classes, fix, delete-properties)
- **3 validation modes** (transaction, full-graph, pre-export)
- **2 import formats** (EDN, SQLite)
- **1 export format** (EDN with Transit serialization)

---

## Key Terminology

- **Graph:** A Logseq database (SQLite file + DataScript in-memory)
- **Schema Version:** Major.Minor version (e.g., 65.33)
- **Migration:** Code that transforms data from one schema version to another
- **DataScript:** In-memory Datalog database used by Logseq
- **Malli:** Data-driven schema validation library
- **EDN:** Extensible Data Notation (Clojure's data format)
- **Transit:** Serialization format for EDN
- **RTC:** Real-Time Collaboration (sync system)
- **Snapshot Re-sync:** Full download/import path used when incremental sync can't safely continue

---

## Document Organization

This investigation is split across multiple documents:

1. **Executive Summary & Overview** (this document)
2. **Schema System Technical Analysis** - Deep dive into schema definitions
3. **Migration System Deep Dive** - Migration registry and execution flow
4. **Import/Export & Validation Analysis** - Data flow and validation points
5. **RTC Synchronization Analysis** - Multi-client schema coordination
6. **Failure Patterns & Recommendations** - Common issues and solutions
7. **PR Review Standards & Guiding Principles** - Team development guidelines
8. **Investigation Index** - Quick reference and navigation

---

## Quick Reference: Critical Files

### Schema Management

- [deps/db/src/logseq/db/frontend/schema.cljs](../../../deps/db/src/logseq/db/frontend/schema.cljs) - Version management
- [deps/db/src/logseq/db/schema.cljs](../../../deps/db/src/logseq/db/schema.cljs) - DataScript schema
- [deps/db/src/logseq/db/frontend/malli_schema.cljs](../../../deps/db/src/logseq/db/frontend/malli_schema.cljs) - Validation schema

### Migration System

- [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs) - Migration registry

> **Removed:** [deps/db/src/logseq/db/frontend/migrate_65_7_through_65_16.cljs](../../../deps/db/src/logseq/db/frontend/migrate_65_7_through_65_16.cljs) (no longer present; migrations are maintained in the registry and linked handlers)

### Validation

- [deps/db/src/logseq/db/frontend/validate.cljs](../../../deps/db/src/logseq/db/frontend/validate.cljs) - Core validation
- [deps/db/src/logseq/db.cljs](../../../deps/db/src/logseq/db.cljs) - Transaction validation

### Import/Export

- [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs) - Export/import logic
- [src/main/frontend/handler/db_based/import.cljs](../../../src/main/frontend/handler/db_based/import.cljs) - UI import handlers

### RTC Sync

- [src/main/frontend/worker/sync/upload.cljs](../../../src/main/frontend/worker/sync/upload.cljs) - Upload/create-graph sync path (includes schema version in graph creation)
- [src/main/frontend/worker/sync/download.cljs](../../../src/main/frontend/worker/sync/download.cljs) - Snapshot/datoms download and import ordering

> **Removed:** [src/main/frontend/worker/rtc/branch_graph.cljs](../../../src/main/frontend/worker/rtc/branch_graph.cljs) and [src/main/frontend/worker/rtc/migrate.cljs](../../../src/main/frontend/worker/rtc/migrate.cljs) (paths removed).

---

## Next Steps

Proceed to **Part 2: Schema System Technical Analysis** for detailed examination of schema definitions and version management.
