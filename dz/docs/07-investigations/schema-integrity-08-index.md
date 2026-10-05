# Schema Changes & Data Integrity Investigation

> **Update 2026-10-04:** this investigation analyzed schema **65.33**. The schema is now **65.34** (`deps/db/src/logseq/db/frontend/schema.cljs:33`), which adds a repair migration, `db-order/missing-internal-page-parent-order-tx`, that gives nested pages with a parent but no `:block/order` a deterministic order (`68a68263b7`). Statements below that call 65.33 the current version describe the analyzed snapshot; nothing else here was re-verified.

## Investigation Index & Quick Reference

**Investigation Date:** December 2024  
**Schema Version Analyzed:** 65.33  
**Total Files Analyzed:** 25+  
**Lines of Code Reviewed:** ~3,000

> **Last revised:** 2026-05-30

---

## Document Series Overview

This investigation is split into 8 comprehensive documents covering schema evolution, data integrity, and team development standards for Logseq's database graph system.

### How to Use This Investigation

- **For Executives/PMs:** Start with [Part 1: Executive Summary](#part-1-executive-summary--overview)
- **For Developers:** Read [Part 2-5](#technical-deep-dive) for implementation details
- **For Code Reviewers:** Use [Part 7: PR Review Standards](#part-7-pr-review-standards--guiding-principles)
- **For Troubleshooting:** Refer to [Part 6: Failure Patterns](#part-6-failure-patterns--recommendations)

---

## Complete Document List

### [Part 1: Executive Summary & Overview](schema-integrity-01-executive-summary.md)

**Purpose:** High-level findings and critical issues

**Key Sections:**

- Critical findings (5 major issues)
- System architecture overview
- Five interconnected systems
- Schema change impact matrix
- Quick reference to critical files

**Audience:** Everyone - start here

**Estimated Reading Time:** 15 minutes

---

### [Part 2: Schema System Technical Analysis](schema-integrity-02-schema-system.md)

**Purpose:** Deep dive into schema definitions and version management

**Key Sections:**

- Schema version management (major.minor)
- DataScript schema (structural constraints)
- Malli validation schema (business logic)
- Entity type classification (8 types)
- Built-in properties system
- Schema comparison logic

**Key Files Covered:**

- [deps/db/src/logseq/db/frontend/schema.cljs](../../../deps/db/src/logseq/db/frontend/schema.cljs)
- [deps/db/src/logseq/db/frontend/malli_schema.cljs](../../../deps/db/src/logseq/db/frontend/malli_schema.cljs)

> **Removed:** Reference to `deps/db/src/logseq/db/schema.cljs` (file no longer exists).

**Audience:** Developers working on schema definitions

**Estimated Reading Time:** 30 minutes

---

### [Part 3: Migration System Deep Dive](schema-integrity-03-migration-system.md)

**Purpose:** How schema changes are applied to existing graphs

**Key Sections:**

- Migration architecture and design principles
- Migration registry (version → updates map)
- Four migration types (properties, classes, rename, fix)
- Migration execution flow
- Error handling and recovery
- Testing strategy and best practices

**Key Files Covered:**

- [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs)

> **Removed:** Reference to `deps/db/src/logseq/db/frontend/migrate_65_7_through_65_16.cljs` (file no longer exists).

**Code Examples:**

- Adding properties (migration v65.15)
- Renaming attributes (migration v65.9)
- Fixing data (migration v65.14)

**Audience:** Developers implementing migrations

**Estimated Reading Time:** 40 minutes

---

### [Part 4: Import/Export & Validation Analysis](schema-integrity-04-import-export-validation.md)

**Purpose:** Data portability and integrity checks

**Key Sections:**

- Export system architecture
- Import system architecture
- Three validation modes (transaction, full-graph, pre-export)
- EDN format specification
- SQLite vs EDN comparison
- Schema version compatibility matrix
- Common failure scenarios

**Key Files Covered:**

- [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs)
- [src/main/frontend/handler/db_based/import.cljs](../../../src/main/frontend/handler/db_based/import.cljs)
- [deps/db/src/logseq/db/frontend/validate.cljs](../../../deps/db/src/logseq/db/frontend/validate.cljs)
- [deps/db/src/logseq/db.cljs](../../../deps/db/src/logseq/db.cljs)

**Critical Finding:** Invalid graphs cannot be exported (blocks backup when most needed)

**Audience:** Developers working on import/export features

**Estimated Reading Time:** 35 minutes

---

### [Part 5: RTC Synchronization Analysis](schema-integrity-05-rtc-synchronization.md)

**Purpose:** Multi-client collaboration with schema versioning

**Key Sections:**

- RTC / DB sync architecture overview
- Schema version branching logic
- Migration operation conversion
- Sync conflict resolution
- Multi-client scenarios (4 examples)
- Branch management lifecycle
- Failure recovery procedures

**Key Files Covered:**

- [src/main/frontend/worker/sync/upload.cljs](../../../src/main/frontend/worker/sync/upload.cljs)
- [src/main/frontend/worker/sync/download.cljs](../../../src/main/frontend/worker/sync/download.cljs)
- [src/main/frontend/worker/sync/apply_txs.cljs](../../../src/main/frontend/worker/sync/apply_txs.cljs)

> **Removed:** References to `src/main/frontend/worker/rtc/branch_graph.cljs` and `src/main/frontend/worker/rtc/migrate.cljs` (directory no longer exists).

**Decision Matrix:** When to branch vs download vs stay compatible

**Audience:** Developers working on RTC sync

**Estimated Reading Time:** 35 minutes

---

### [Part 6: Failure Patterns & Recommendations](schema-integrity-06-failure-patterns-recommendations.md)

**Purpose:** Common issues, risks, and mitigation strategies

**Key Sections:**

- Six common failure patterns
- Three failure cascade analyses
- Risk assessment matrix (7.55/10 overall risk)
- Six mitigation strategies (with code)
- Four recovery procedures (step-by-step)
- Monitoring & detection setup
- 12 prioritized recommendations

**Critical Patterns:**

1. Migration cascade failure (🔴 CRITICAL)
2. Invalid graph export block (🔴 CRITICAL)
3. Version incompatibility lock-out (🟠 HIGH)

**Audience:** Tech leads, SREs, support engineers

**Estimated Reading Time:** 40 minutes

---

### [Part 7: PR Review Standards & Guiding Principles](schema-integrity-07-pr-review-standards.md)

**Purpose:** Actionable guidelines for code reviews and development

**Key Sections:**

- Five general principles
- Schema change checklist (21 items)
- Migration review standards (5 standards)
- Validation review standards (3 standards)
- Import/export review standards (3 standards)
- RTC sync review standards (3 standards)
- Testing requirements (4 categories)
- Deployment guidelines (3 phases)
- Code review examples (good vs bad)

**Quick Reference Card:**

- Change type → required actions
- PR review quick checks
- When to reject PRs

**Audience:** All developers, code reviewers

**Estimated Reading Time:** 30 minutes

---

### Part 8: Investigation Index (This Document)

**Purpose:** Navigation and quick reference

**Sections:**

- Document overview
- Quick navigation
- Key findings summary
- File location index
- Common patterns reference
- Glossary

**Audience:** Everyone

---

## Findings: New Capabilities

- Frontend schema and migration chain now run through `65.33` in [deps/db/src/logseq/db/frontend/schema.cljs](../../../deps/db/src/logseq/db/frontend/schema.cljs) and [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs).
- Migration coverage includes post-`65.16` additions such as comments/reactions/recycle and gallery-view property updates in [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs).
- Export validation now includes in-memory import-and-validate flow via `validate-export` in [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs).
- Sync implementation is centered in `worker/sync` (`upload`, `download`, `apply_txs`) instead of the removed `worker/rtc` paths.

---

## Quick Navigation

### By Role

**Product Manager / Executive**

- Read: Part 1 (Executive Summary)
- Skim: Part 6 (Failure Patterns)
- Reference: This index

**Software Developer**

- Read: Part 2, 3, 4, 5 (Technical Deep Dive)
- Reference: Part 7 (PR Standards)
- Troubleshoot: Part 6 (Failure Patterns)

**Code Reviewer**

- Read: Part 7 (PR Review Standards)
- Reference: Part 3 (Migration), Part 4 (Import/Export)
- Check: Checklists in Part 7

**DevOps / SRE**

- Read: Part 6 (Failure Patterns)
- Reference: Recovery procedures in Part 6
- Monitor: Metrics in Part 6

**QA / Tester**

- Read: Testing requirements in Part 7
- Reference: Part 3 (Migration testing)
- Test: Scenarios in Part 6

---

## By Task

### "I need to add a new property"

1. Read: [Part 2 - Built-in Properties System](schema-integrity-02-schema-system.md#built-in-properties-system)
2. Read: [Part 3 - Property Migration](schema-integrity-03-migration-system.md#type-1-properties-migration)
3. Check: [Part 7 - Schema Change Checklist](schema-integrity-07-pr-review-standards.md#schema-change-checklist)
4. Example: [Part 7 - Example 1: Property Addition](schema-integrity-07-pr-review-standards.md#example-1-property-addition-good)

### "I need to change a property type"

1. Read: [Part 7 - Example 2: Property Type Change](schema-integrity-07-pr-review-standards.md#example-2-property-type-change-bad--fixed)
2. Check: Major version required
3. Implement: Data transformation migration
4. Test: Old data compatibility

### "Migration failed, users reporting issues"

1. Read: [Part 6 - Pattern 1: Migration Cascade Failure](schema-integrity-06-failure-patterns-recommendations.md#pattern-1-migration-cascade-failure)
2. Follow: [Part 6 - Procedure 1: Recover from Failed Migration](schema-integrity-06-failure-patterns-recommendations.md#procedure-1-recover-from-failed-migration)
3. Monitor: Metrics in Part 6

### "Users can't import old backups"

1. Read: [Part 4 - Schema Version Compatibility](schema-integrity-04-import-export-validation.md#schema-version-compatibility)
2. Check: [Part 4 - Import Compatibility Matrix](schema-integrity-04-import-export-validation.md#import-compatibility-matrix)
3. Implement: [Part 6 - Strategy 3: Import with Transform](schema-integrity-06-failure-patterns-recommendations.md#strategy-3-import-with-transform)

### "RTC sync broken after upgrade"

1. Read: [Part 5 - Schema Version Branching](schema-integrity-05-rtc-synchronization.md#schema-version-branching)
2. Check: [Part 5 - Decision Matrix](schema-integrity-05-rtc-synchronization.md#decision-matrix)
3. Follow: [Part 6 - Procedure 4: Merge Diverged RTC Branches](schema-integrity-06-failure-patterns-recommendations.md#procedure-4-merge-diverged-rtc-branches)

### "Reviewing a PR with schema changes"

1. Use: [Part 7 - Schema Change Checklist](schema-integrity-07-pr-review-standards.md#schema-change-checklist)
2. Check: [Part 7 - Quick Reference Card](schema-integrity-07-pr-review-standards.md#quick-reference-card)
3. Compare: [Part 7 - Code Review Examples](schema-integrity-07-pr-review-standards.md#code-review-examples)

---

## Key Files Reference

### Schema Management

| File                                                                                                          | Purpose             | Key Functions                       |
| ------------------------------------------------------------------------------------------------------------- | ------------------- | ----------------------------------- |
| [deps/db/src/logseq/db/frontend/schema.cljs](../../../deps/db/src/logseq/db/frontend/schema.cljs)             | Version management  | `version`, `compare-schema-version` |
| [deps/db/src/logseq/db/frontend/malli_schema.cljs](../../../deps/db/src/logseq/db/frontend/malli_schema.cljs) | Validation schema   | Entity schemas, property types      |
| [deps/db/src/logseq/db/frontend/property.cljs](../../../deps/db/src/logseq/db/frontend/property.cljs)         | Built-in properties | `built-in-properties` registry      |

> **Removed:** Reference to `deps/db/src/logseq/db/schema.cljs` (file no longer exists).

### Migration System

| File                                                                                          | Purpose                 | Key Functions                        |
| --------------------------------------------------------------------------------------------- | ----------------------- | ------------------------------------ |
| [src/main/frontend/worker/db/migrate.cljs](../../../src/main/frontend/worker/db/migrate.cljs) | Migration orchestration | `migrate`, `schema-version->updates` |

> **Removed:** Reference to `deps/db/src/logseq/db/frontend/migrate_65_7_through_65_16.cljs` (file no longer exists).

### Validation System

| File                                                                                                  | Purpose              | Key Functions                                             |
| ----------------------------------------------------------------------------------------------------- | -------------------- | --------------------------------------------------------- |
| [deps/db/src/logseq/db/frontend/validate.cljs](../../../deps/db/src/logseq/db/frontend/validate.cljs) | Validation logic     | `validate-db`, `validate-tx-report`, `validate-local-db!` |
| [deps/db/src/logseq/db.cljs](../../../deps/db/src/logseq/db.cljs)                                     | Transaction handling | `transact!` (with validation)                             |

### Import/Export

| File                                                                                                      | Purpose             | Key Functions                                                                |
| --------------------------------------------------------------------------------------------------------- | ------------------- | ---------------------------------------------------------------------------- |
| [deps/db/src/logseq/db/sqlite/export.cljs](../../../deps/db/src/logseq/db/sqlite/export.cljs)             | Export/import logic | `build-export`, `build-import`, `validate-export`                            |
| [src/main/frontend/handler/db_based/import.cljs](../../../src/main/frontend/handler/db_based/import.cljs) | UI import handlers  | `import-from-sqlite-db!`, `import-from-sqlite-zip!`, `import-from-edn-file!` |

### RTC / DB Sync

| File                                                                                                  | Purpose             | Key Functions                           |
| ----------------------------------------------------------------------------------------------------- | ------------------- | --------------------------------------- |
| [src/main/frontend/worker/sync/upload.cljs](../../../src/main/frontend/worker/sync/upload.cljs)       | Graph upload/create | `create-remote-graph!`, `upload-graph!` |
| [src/main/frontend/worker/sync/download.cljs](../../../src/main/frontend/worker/sync/download.cljs)   | Snapshot download   | `download-graph-by-id!`                 |
| [src/main/frontend/worker/sync/apply_txs.cljs](../../../src/main/frontend/worker/sync/apply_txs.cljs) | Remote tx apply     | `apply-remote-txs!`                     |

> **Removed:** References to `src/main/frontend/worker/rtc/branch_graph.cljs` and `src/main/frontend/worker/rtc/migrate.cljs` (directory no longer exists).

---

## Critical Findings Summary

### Top 5 Risks (from Part 1)

1. **No Backward Compatibility** (🔴 CRITICAL)
   - Older clients cannot open newer graphs
   - Forces all-or-nothing upgrades

2. **Strict Validation Enforcement** (🔴 HIGH)
   - All transactions validated
   - Bugs in validation block legitimate operations

3. **Sequential Migration Risk** (🔴 HIGH)
   - No rollback on partial failure
   - Failed migration corrupts graph

4. **Export as Last Resort** (🟠 MEDIUM)
   - Invalid graphs cannot be exported
   - Blocks backup when most needed

5. **RTC Version Branching** (🟡 MEDIUM)
   - Different versions create separate branches
   - Teams can become unsynchronized

### Risk Score (from Part 6)

**Overall Risk: 7.55/10** (🔴 HIGH)

| Factor               | Score (1-10) |
| -------------------- | ------------ |
| Data Loss Potential  | 9            |
| User Impact          | 8            |
| Frequency            | 4            |
| Recovery Difficulty  | 8            |
| Detection Difficulty | 6            |

---

## Common Patterns Reference

### Schema Change Types

```
Additive (Minor Version)
├─ New property (optional)
├─ New entity type
├─ New validation (non-breaking)
└─ Relaxed validation

Breaking (Major Version)
├─ Remove property
├─ Rename property
├─ Change property type
├─ Change cardinality
├─ Stricter validation
└─ Remove entity type
```

### Migration Types

```
:properties     → Add new properties
:classes        → Add new tags/categories
:rename-db-idents → Rename attributes
:fix            → Transform/repair data
```

### Validation Modes

```
Transaction-time → Every write (mandatory)
Full-graph      → Manual scan (optional)
Pre-export      → Before backup (blocks invalid)
```

### RTC Actions

```
upload-graph!         → Create/upload graph snapshot and metadata
download-graph-by-id! → Download graph snapshot by graph id
apply-remote-txs!     → Apply and reconcile remote transactions
```

> **Removed:** Prior action keywords `:compatible`, `:create-branch`, and `:download` were tied to removed `worker/rtc` files and are no longer directly traceable in current `src/main/frontend/worker/sync` implementation.

---

## Glossary

### Schema Terms

- **Schema Version:** Major.minor version (e.g., 65.33)
- **Major Version:** Breaking changes, incompatible with previous
- **Minor Version:** Additions, backward compatible
- **DataScript Schema:** Structural constraints (types, cardinality)
- **Malli Schema:** Business logic validation

### Migration Terms

- **Migration:** Code that transforms data between schema versions
- **Migration Registry:** Map of version → migration operations
- **Idempotent:** Safe to run multiple times
- **Checkpoint:** Backup before migration for rollback

### Entity Terms

- **Entity:** A record in the graph (block, property, class, etc.)
- **Attribute:** A property of an entity (e.g., `:block/title`)
- **Cardinality:** One value or many (`:one` or `:many`)
- **Reference:** Attribute that points to another entity

### Export/Import Terms

- **EDN:** Extensible Data Notation (Clojure data format)
- **Transit:** Serialization format for EDN
- **Export:** Graph backup in EDN format
- **Import:** Load exported graph into database

### RTC Terms

- **RTC:** Real-Time Collaboration (sync system)
- **Branch:** Server-side graph for specific major version
- **Operation:** Sync unit (create, update, delete)
- **Conflict:** Two clients modify same data simultaneously

---

## Testing Quick Reference

### Migration Testing Checklist

- [ ] Empty graph
- [ ] Populated graph (100, 1k, 10k blocks)
- [ ] Idempotency (run twice)
- [ ] Missing data handling
- [ ] Malformed data handling
- [ ] Performance benchmarks

### Import/Export Testing Checklist

- [ ] Same version export/import
- [ ] Old version import (3+ versions back)
- [ ] Invalid graph export (force option)
- [ ] Large graph (10k+ blocks)
- [ ] Migration during import

### RTC Testing Checklist

- [ ] Same version sync
- [ ] Mixed minor version sync
- [ ] Major version branching
- [ ] Migration sync
- [ ] Conflict resolution

---

## Metrics to Monitor

### Migration Metrics

- Success rate (target: >99%)
- Average duration (target: <5s for 10k blocks)
- Failure count (alert: >5/day)

### Validation Metrics

- Error rate (alert: >5% spike)
- Common error types
- Validation duration (target: <10s for 100k blocks)

### Export/Import Metrics

- Export failures (alert: >5/day)
- Import failures (alert: >5/day)
- Invalid graph blocks (track trend)

### RTC Metrics

- Active branches (alert: >5)
- Sync conflicts (alert: >10/hour)
- Average sync lag (target: <1s)

---

## Emergency Contacts & Resources

### Internal Resources

- Schema system owner: [Check team structure]
- Migration system owner: [Check team structure]
- On-call engineer: [Check PagerDuty]

### External Resources

- User support forum: https://discuss.logseq.com
- Bug tracker: https://github.com/logseq/logseq/issues
- Documentation: https://docs.logseq.com

---

## Document Maintenance

### Update Triggers

- New schema version released
- Migration bug discovered
- New failure pattern identified
- Testing requirements changed
- Team structure changed

### Review Schedule

- Quarterly review of all parts
- Update after major incidents
- Refresh examples with real cases

### Contribution

- Submit PRs for corrections
- Add new examples from real issues
- Update metrics and thresholds
- Add new failure patterns

---

## Investigation Metadata

**Completed:** December 2024  
**Total Documents:** 8  
**Total Pages:** ~50  
**Files Analyzed:** 25+  
**Code Lines Reviewed:** ~3,000  
**Test Cases Created:** 20+

**Contributors:** Development Team  
**Review Status:** Revised against current source (src/, deps/, packages/)  
**Next Review Date:** August 2026

---

## Quick Links

- [Part 1: Executive Summary](schema-integrity-01-executive-summary.md)
- [Part 2: Schema System](schema-integrity-02-schema-system.md)
- [Part 3: Migration System](schema-integrity-03-migration-system.md)
- [Part 4: Import/Export & Validation](schema-integrity-04-import-export-validation.md)
- [Part 5: RTC Synchronization](schema-integrity-05-rtc-synchronization.md)
- [Part 6: Failure Patterns & Recommendations](schema-integrity-06-failure-patterns-recommendations.md)
- [Part 7: PR Review Standards](schema-integrity-07-pr-review-standards.md)
- [Part 8: Investigation Index](schema-integrity-08-index.md) ← You are here

---

**End of Investigation Series**
