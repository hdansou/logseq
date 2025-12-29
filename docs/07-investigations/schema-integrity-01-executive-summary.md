# Schema Changes & Data Integrity Investigation

## Part 1: Executive Summary & Overview

**Investigation Date:** December 2024  
**Schema Version Analyzed:** 65.16  
**Investigator:** Development Team

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
- **Finding:** All transactions are validated against Malli schemas; invalid data cannot be saved
- **Consequence:** Bugs in validation logic can prevent legitimate operations
- **Evidence:** [deps/db/src/logseq/db.cljs](../../../deps/db/src/logseq/db.cljs#L118-L143) - Every transaction runs through validation

#### 3. **Sequential Migration Risk**

- **Impact:** HIGH
- **Finding:** Migrations run sequentially without rollback mechanism
- **Consequence:** A single failed migration can corrupt the entire graph
- **Evidence:** [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs#L329-L366) - Migration execution is transactional but no rollback on partial failure

#### 4. **Export as Last Resort Backup**

- **Impact:** MEDIUM
- **Finding:** Export validation prevents saving corrupt graphs
- **Consequence:** Users with invalid graphs cannot create backups
- **Evidence:** [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs#L1100-L1113) - Pre-export validation blocks invalid graphs

#### 5. **RTC Version Branching**

- **Impact:** MEDIUM
- **Finding:** RTC servers support multiple schema versions through branching
- **Consequence:** Mixed-version clients can coexist but may have sync conflicts
- **Evidence:** [src/main/frontend/worker/rtc/branch_graph.cljs](../../../src/main/frontend/worker/rtc/branch_graph.cljs#L13-L40) - Schema comparison determines branch or download

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
 :minor 16} ;; Non-breaking additions - backward compatible within major version
```

**Current Version:** 65.16  
**Location:** [deps/db/src/logseq/db/frontend/schema.cljs](../../../deps/db/src/logseq/db/frontend/schema.cljs#L40)

### Version Comparison Logic

```clojure
(defn compare-schema-version [v1 v2]
  (let [major-cmp (compare (:major v1) (:major v2))]
    (if (zero? major-cmp)
      (compare (:minor v1) (:minor v2))
      major-cmp)))
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
- **Key Files:** [src/main/frontend/worker/rtc/branch_graph.cljs](../../../src/main/frontend/worker/rtc/branch_graph.cljs), [src/main/frontend/worker/rtc/migrate.cljs](../../../src/main/frontend/worker/rtc/migrate.cljs)
- **Risk:** Schema version conflicts require branching or re-download

---

## Schema Change Impact Matrix

| Change Type             | Migration | Validation | Import    | Export    | RTC Sync        |
| ----------------------- | --------- | ---------- | --------- | --------- | --------------- |
| **New Entity Type**     | Optional  | Required   | Required  | Auto      | Branch/Migrate  |
| **New Property**        | Required  | Required   | Required  | Auto      | Ops Conversion  |
| **Rename Property**     | Required  | Required   | Required  | Auto      | Ops Conversion  |
| **Change Cardinality**  | Required  | Required   | Break     | Break     | Branch          |
| **New Validation Rule** | Optional  | Required   | May Break | May Break | Compatible      |
| **Remove Entity Type**  | Required  | Required   | Break     | Break     | Branch          |
| **Schema Version Bump** | Trigger   | Check      | Check     | Record    | Branch Decision |

**Legend:**

- **Required:** System must be updated
- **Optional:** May need update for data transform
- **Auto:** System handles automatically
- **Break:** Old data incompatible
- **Branch:** RTC creates new branch
- **May Break:** Depends on strictness

---

## Investigation Scope

This investigation analyzed:

- **25+ source files** across 5 subsystems
- **~3,000 lines of ClojureScript code**
- **9 schema versions** (65.7 through 65.16)
- **4 migration types** (properties, classes, rename, fix)
- **3 validation modes** (transaction, full-graph, pre-export)
- **2 import formats** (EDN, SQLite)
- **1 export format** (EDN with Transit serialization)

---

## Key Terminology

- **Graph:** A Logseq database (SQLite file + DataScript in-memory)
- **Schema Version:** Major.Minor version (e.g., 65.16)
- **Migration:** Code that transforms data from one schema version to another
- **DataScript:** In-memory Datalog database used by Logseq
- **Malli:** Data-driven schema validation library
- **EDN:** Extensible Data Notation (Clojure's data format)
- **Transit:** Serialization format for EDN
- **RTC:** Real-Time Collaboration (sync system)
- **Branch:** Separate schema version maintained on RTC server

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
- [deps/db/src/logseq/db/frontend/migrate_65_7_through_65_16.cljs](../../../deps/db/src/logseq/db/frontend/migrate_65_7_through_65_16.cljs) - Migration implementations

### Validation

- [deps/db/src/logseq/db/frontend/validate.cljs](../../../deps/db/src/logseq/db/frontend/validate.cljs) - Core validation
- [deps/db/src/logseq/db.cljs](../../../deps/db/src/logseq/db.cljs) - Transaction validation

### Import/Export

- [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs) - Export/import logic
- [src/main/frontend/handler/db_based/import.cljs](../../../src/main/frontend/handler/db_based/import.cljs) - UI import handlers

### RTC Sync

- [src/main/frontend/worker/rtc/branch_graph.cljs](../../../src/main/frontend/worker/rtc/branch_graph.cljs) - Schema branching
- [src/main/frontend/worker/rtc/migrate.cljs](../../../src/main/frontend/worker/rtc/migrate.cljs) - Migration ops conversion

---

## Next Steps

Proceed to **Part 2: Schema System Technical Analysis** for detailed examination of schema definitions and version management.
