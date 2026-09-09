# Schema Evolution and Data Migration Investigation

**Investigation Date:** December 28, 2025  
**Investigation Type:** Best Practices Development  
**Status:** ✅ Complete

> **Last revised:** 2026-05-30

---

## Executive Summary

This investigation examines the correlation among schema changes, data validation, import/export, and synchronization in Logseq to establish best practices that prevent data migration issues during application upgrades.

### Key Findings

1. **Dual Validation System**: Logseq uses both DataScript schema validation (structural) and Malli validation (business logic), requiring coordination when making changes

2. **Versioned Migration Required**: Every property addition requires a migration entry in `schema-version->updates` and a version bump, but this is not automatically enforced

3. **Bootstrap Order Fragility**: Graph initialization depends on precise transaction order to resolve circular dependencies between properties and classes

4. **RTC Configuration Critical**: Properties need explicit RTC sync configuration to avoid synchronization issues between devices

5. **Import/Export Compatibility**: Schema changes can break import/export compatibility if not handled carefully, requiring version-aware format handling

6. **No Automated Rollback**: Failed migrations have no automated rollback mechanism, making testing critical before release

## Findings: New Capabilities

1. **Schema Progression to 65.33**: Current schema version in `db-schema/version` is now `65.33`, with frontend DB migrations extended through `65.33`

2. **Migration Supports Property Deletions**: Frontend migration updates now include `:delete-properties` entries (for example in `65.25`) in addition to property/class/fix migrations

3. **Built-in Data Repair During Migration Validation**: Migration flow includes `ensure-built-in-data-exists!`, and validation code calls it to restore missing built-in entities when needed

4. **Client Ops SQLite Binary Export API**: Worker thread APIs include `:thread-api/export-client-ops-db-binary`, with coverage in worker and db-worker-node tests

> **Removed:** The old migration file path `src/main/frontend/worker/db_migrate.cljs` is no longer valid. Use `src/main/frontend/worker/db/migrate.cljs`.

> **Removed:** The migration runner reference `migrate-db-graph` is no longer current. Use `migrate` in `frontend.worker.db.migrate`.

### Critical Risks Identified

🔴 **High Risk**

- Property additions without migration entries → validation failures on existing graphs
- Schema version changes without coordinated RTC handling → sync conflicts
- Type changes on existing properties → data loss during migration

🟡 **Medium Risk**

- Bootstrap order changes → graph creation failures
- Missing validation rules → invalid data acceptance
- Export format changes without version tracking → import failures

### Investigation Scope

This investigation covered:

- Schema definition and versioning system ([deps/db/src/logseq/db/schema.cljs](deps/db/src/logseq/db/schema.cljs), [deps/db/src/logseq/db/frontend/schema.cljs](deps/db/src/logseq/db/frontend/schema.cljs))
- Migration patterns ([src/main/frontend/worker/db/migrate.cljs](src/main/frontend/worker/db/migrate.cljs))
- Property system ([deps/db/src/logseq/db/frontend/property/build.cljc](deps/db/src/logseq/db/frontend/property/build.cljc))
- Validation mechanisms ([deps/db/src/logseq/db/frontend/validate.cljs](deps/db/src/logseq/db/frontend/validate.cljs))
- Import/export systems ([deps/db/src/logseq/db/sqlite/build.cljs](deps/db/src/logseq/db/sqlite/build.cljs), [deps/db/src/logseq/db/sqlite/export.cljs](deps/db/src/logseq/db/sqlite/export.cljs))
- RTC synchronization ([src/main/frontend/worker/rtc/](src/main/frontend/worker/rtc/))

---

## Navigation to Detailed Guides

This investigation has produced comprehensive documentation across multiple areas:

### 1. [Schema Migration Best Practices](../08-processes/schema-migration-best-practices.md)

**When to read:** Before adding/modifying properties, changing schema structure, or bumping schema versions

**Covers:**

- Property addition workflow with code templates
- Migration pattern examples
- Schema version management
- Bootstrap order considerations
- Property type changes and data transformations

### 2. [Validation Guidelines](../08-processes/validation-best-practices.md)

**When to read:** When adding validation rules, troubleshooting validation errors, or optimizing validation performance

**Covers:**

- DataScript vs Malli validation usage
- Adding custom validation rules
- Entity type dispatch system
- Performance optimization techniques
- Error handling and debugging

### 3. [Import/Export Best Practices](../08-processes/import-export-best-practices.md)

**When to read:** When modifying export formats, troubleshooting import failures, or ensuring cross-version compatibility

**Covers:**

- sqlite.build EDN format specifications
- Circular reference handling
- UUID consistency requirements
- Transit serialization patterns
- Cross-version compatibility strategies

### 4. [RTC/Sync Considerations](../08-processes/rtc-sync-schema-changes.md)

**When to read:** When adding properties that should sync, troubleshooting sync conflicts, or handling multi-device scenarios

**Covers:**

- Property RTC configuration options
- Conflict resolution patterns
- Schema version synchronization
- Multi-device compatibility testing
- RTC-aware property design

### 5. [PR Review Checklist](../08-processes/schema-change-pr-checklist.md) ⭐ **START HERE**

**When to read:** Before submitting PRs with schema changes, or when reviewing such PRs

**Covers:**

- Mandatory checks for property additions
- Schema version bump requirements
- Migration entry validation
- RTC configuration verification
- Testing requirements
- Import/export compatibility checks

### 6. [Testing Strategies](../08-processes/schema-change-testing.md)

**When to read:** When writing tests for schema changes or planning migration testing

**Covers:**

- Migration testing across versions
- Validation testing with invalid data
- Import/export round-trip tests
- RTC sync testing patterns
- Performance testing strategies
- Rollback testing procedures

---

## Quick Reference: Common Scenarios

### Scenario 1: Adding a New Built-in Property

**Required Actions:**

1. ✅ Add property definition to `built-in-properties` in [deps/db/src/logseq/db/frontend/property/build.cljc](deps/db/src/logseq/db/frontend/property/build.cljc)
2. ✅ Add migration entry to `schema-version->updates` in [src/main/frontend/worker/db/migrate.cljs](src/main/frontend/worker/db/migrate.cljs)
3. ✅ Bump `db-schema/version` in [deps/db/src/logseq/db/frontend/schema.cljs](deps/db/src/logseq/db/frontend/schema.cljs)
4. ✅ Add validation rules if needed in [deps/db/src/logseq/db/frontend/validate.cljs](deps/db/src/logseq/db/frontend/validate.cljs)
5. ✅ Configure RTC sync behavior in property definition
6. ✅ Test migration on sample graphs with previous schema version

**See:** [Schema Migration Best Practices](../08-processes/schema-migration-best-practices.md#adding-new-properties)

### Scenario 2: Changing Property Type

**Required Actions:**

1. ⚠️ **DO NOT** change type of existing property with user data
2. ✅ Create new property with new type
3. ✅ Add migration function to transform old values to new property
4. ✅ Deprecate old property (add `deprecated` flag)
5. ✅ Update all UI/query references
6. ✅ Test data transformation on real graphs

**See:** [Schema Migration Best Practices](../08-processes/schema-migration-best-practices.md#changing-property-types)

### Scenario 3: Modifying Validation Rules

**Required Actions:**

1. ✅ Add/modify validation rule in appropriate dispatch function
2. ✅ Add tests with valid and invalid data
3. ✅ Test against existing graphs (may contain invalid data!)
4. ✅ Consider adding fix function for existing invalid data
5. ✅ Document validation error messages clearly

**See:** [Validation Guidelines](../08-processes/validation-best-practices.md#adding-validation-rules)

### Scenario 4: Changing Export Format

**Required Actions:**

1. ✅ Maintain backward compatibility with previous format version
2. ✅ Add format version tracking to exports
3. ✅ Update import parser to handle both old and new formats
4. ✅ Test import of old format files
5. ✅ Update CLI export documentation

**See:** [Import/Export Best Practices](../08-processes/import-export-best-practices.md#format-versioning)

### Scenario 5: Adding RTC-Synced Property

**Required Actions:**

1. ✅ Configure `:rtc` settings in property definition
2. ✅ Decide on sync behavior (upload/download/sync)
3. ✅ Add property to RTC constants if needed
4. ✅ Test multi-device sync scenarios
5. ✅ Test conflict resolution behavior

**See:** [RTC/Sync Considerations](../08-processes/rtc-sync-schema-changes.md#configuring-property-rtc-behavior)

---

## Key Architectural Patterns

### 1. Multi-Dispatch Validation

**Pattern:** Entity type determines validation schema via Malli multi-method dispatch

**Location:** [deps/db/src/logseq/db/frontend/validate.cljs](deps/db/src/logseq/db/frontend/validate.cljs)

**Why it matters:** Adding new entity types or modifying existing ones requires understanding dispatch order

**Best Practice:** Always check entity dispatch precedence when adding validation rules

### 2. Reference-Based Property Values

**Pattern:** Property values stored as separate entities with references, not direct scalars

**Location:** [deps/db/src/logseq/db/frontend/property.cljs](deps/db/src/logseq/db/frontend/property.cljs)

**Why it matters:** Property value changes create new entities; type changes affect entity structure

**Best Practice:** Understand reference vs scalar types before choosing property type

### 3. Bootstrap Staging

**Pattern:** Multi-stage initialization resolves circular dependencies (Property class needs properties, properties need Property class)

**Location:** [deps/db/src/logseq/db/frontend/create_graph.cljs](deps/db/src/logseq/db/frontend/create_graph.cljs)

**Why it matters:** Changing bootstrap order breaks graph creation

**Best Practice:** Never modify bootstrap order without extensive testing

### 4. Dual Schema Validation

**Pattern:** DataScript enforces structural constraints, Malli enforces business logic

**Location:** [deps/db/src/logseq/db/schema.cljs](deps/db/src/logseq/db/schema.cljs) + [deps/db/src/logseq/db/frontend/validate.cljs](deps/db/src/logseq/db/frontend/validate.cljs)

**Why it matters:** Schema changes may require updates to both systems

**Best Practice:** Consider both validation layers when making changes

### 5. Versioned Migration System

**Pattern:** Schema versions use major.minor format; migrations apply sequentially

**Location:** [src/main/frontend/worker/db/migrate.cljs](src/main/frontend/worker/db/migrate.cljs)

**Why it matters:** Missing migration entries cause validation failures

**Best Practice:** Always add migration entry when bumping schema version

---

## Critical Code Locations

### Schema Definition

- **DataScript Schema:** [deps/db/src/logseq/db/schema.cljs](deps/db/src/logseq/db/schema.cljs)
- **Schema Version:** `db-schema/version` in [deps/db/src/logseq/db/frontend/schema.cljs](deps/db/src/logseq/db/frontend/schema.cljs) (currently "65.33")
- **Schema Attributes:** `ref-type-attributes`, `card-many-attributes`

### Property System

- **Built-in Properties:** [deps/db/src/logseq/db/frontend/property/build.cljc](deps/db/src/logseq/db/frontend/property/build.cljc)
- **Property Types:** [deps/db/src/logseq/db/frontend/property/type.cljs](deps/db/src/logseq/db/frontend/property/type.cljs)
- **Property Operations:** [deps/db/src/logseq/db/frontend/property.cljs](deps/db/src/logseq/db/frontend/property.cljs)

### Migration System

- **DB Graph Migration:** [src/main/frontend/worker/db/migrate.cljs](src/main/frontend/worker/db/migrate.cljs)
- **Migration Definitions:** `schema-version->updates` vector
- **Migration Runner:** `migrate` function

### Validation System

- **Malli Validation:** [deps/db/src/logseq/db/frontend/validate.cljs](deps/db/src/logseq/db/frontend/validate.cljs)
- **Entity Dispatch:** `entity-schema` multi-method
- **Validation Functions:** `validate-tx-report`, `validate-db!`

### Import/Export

- **SQLite Export:** [deps/db/src/logseq/db/sqlite/export.cljs](deps/db/src/logseq/db/sqlite/export.cljs)
- **Transit Utils:** [deps/db/src/logseq/db/sqlite/util.cljs](deps/db/src/logseq/db/sqlite/util.cljs)
- **Import Builder:** [deps/db/src/logseq/db/sqlite/build.cljs](deps/db/src/logseq/db/sqlite/build.cljs)

### RTC/Sync

- **RTC Coordination:** [src/main/frontend/worker/rtc/core.cljs](src/main/frontend/worker/rtc/core.cljs)
- **Apply Remote:** [src/main/frontend/worker/rtc/op_mem_layer.cljs](src/main/frontend/worker/rtc/op_mem_layer.cljs)
- **Client Ops:** [src/main/frontend/worker/rtc/client_op.cljs](src/main/frontend/worker/rtc/client_op.cljs)

### Initialization

- **Graph Creation:** [deps/db/src/logseq/db/frontend/create_graph.cljs](deps/db/src/logseq/db/frontend/create_graph.cljs)
- **Bootstrap Functions:** `create-db-graph-from-scratch!`

---

## Recommendations for Development Workflow

### 1. Pre-Development Checklist

Before making schema changes:

- [ ] Review [PR Review Checklist](../08-processes/schema-change-pr-checklist.md)
- [ ] Check current schema version in [deps/db/src/logseq/db/frontend/schema.cljs](deps/db/src/logseq/db/frontend/schema.cljs)
- [ ] Review recent migrations in [src/main/frontend/worker/db/migrate.cljs](src/main/frontend/worker/db/migrate.cljs)
- [ ] Identify affected areas (validation, import/export, RTC)
- [ ] Plan migration strategy

### 2. Development Process

During implementation:

- [ ] Follow patterns from [Schema Migration Best Practices](../08-processes/schema-migration-best-practices.md)
- [ ] Add migration entry immediately when adding property
- [ ] Update validation rules if needed
- [ ] Configure RTC behavior explicitly
- [ ] Write tests following [Testing Strategies](../08-processes/schema-change-testing.md)

### 3. Pre-PR Submission

Before submitting PR:

- [ ] Run through [PR Review Checklist](../08-processes/schema-change-pr-checklist.md)
- [ ] Verify schema version bumped
- [ ] Test migration on sample graphs
- [ ] Test import/export round-trip
- [ ] Test RTC sync if applicable
- [ ] Update documentation

### 4. Code Review Focus

When reviewing schema change PRs:

- [ ] Migration entry exists and matches property addition
- [ ] Schema version properly incremented
- [ ] Validation rules updated or explicitly not needed
- [ ] RTC configuration specified
- [ ] Tests cover migration, validation, import/export
- [ ] Documentation updated

### 5. Post-Merge Monitoring

After merging:

- [ ] Monitor issue tracker for migration errors
- [ ] Check for validation failures in error logs
- [ ] Watch for sync conflict reports
- [ ] Be ready to hotfix if critical issues found

---

## Integration with Existing Processes

### GitHub PR Review Process

**Update [prompts/review.md](../../prompts/review.md):**

```clojure
;; Schema Change Detection
- [ ] Check if built-in-properties modified
- [ ] Verify migration entry added to schema-version->updates
- [ ] Confirm schema version bumped in db-schema/version
- [ ] Validate RTC configuration present
- [ ] Ensure tests cover migration path
```

### Automated CI Checks (Recommended)

**Potential automation to add:**

1. **Migration Entry Check:** Detect changes to `built-in-properties` and verify corresponding entry in `schema-version->updates`
2. **Schema Version Validation:** Ensure version increments are valid (major or minor bump)
3. **Export Format Test:** Run import/export round-trip test on every schema change
4. **Migration Test:** Test migration from previous 3 versions to current

### Documentation Requirements

**For each schema change PR, include:**

1. **Migration Strategy:** Describe how existing graphs will be migrated
2. **Breaking Changes:** List any backward-incompatible changes
3. **Testing Performed:** Detail migration testing across versions
4. **RTC Impact:** Explain sync behavior changes if applicable
5. **Rollback Plan:** Document how to revert if issues found

---

## Common Pitfalls and How to Avoid Them

### Pitfall 1: Adding Property Without Migration

**Symptom:** Validation failures on existing graphs after upgrade

**Root Cause:** Property doesn't exist in older schema versions, causing validation errors

**Prevention:** Always add migration entry in `schema-version->updates`

**Detection:** Automated check for `built-in-properties` changes without migration

**Fix:** Add migration entry, bump version, release hotfix

### Pitfall 2: Changing Property Type

**Symptom:** Data loss or type errors when accessing property values

**Root Cause:** Existing property values don't match new type

**Prevention:** Never change type; create new property and migrate data

**Detection:** Manual code review (difficult to automate)

**Fix:** Revert type change, add proper migration with data transformation

### Pitfall 3: Missing RTC Configuration

**Symptom:** Property doesn't sync between devices or causes conflicts

**Root Cause:** Property lacks `:rtc` configuration in definition

**Prevention:** Always specify RTC behavior explicitly in property definition

**Detection:** Code review check for `:rtc` key in property map

**Fix:** Add RTC configuration, test multi-device sync

### Pitfall 4: Breaking Bootstrap Order

**Symptom:** New graphs fail to create; errors during initialization

**Root Cause:** Changed transaction order breaks circular dependency resolution

**Prevention:** Never modify bootstrap-related code without deep understanding

**Detection:** Integration tests for graph creation (should exist)

**Fix:** Revert changes, carefully analyze bootstrap dependencies

### Pitfall 5: Export Format Changes

**Symptom:** Cannot import graphs exported from previous versions

**Root Cause:** Changed export format without backward compatibility

**Prevention:** Always maintain format version tracking and parser compatibility

**Detection:** Import test with files from previous versions

**Fix:** Add format version detection, support both old and new formats

### Pitfall 6: Missing Validation Rules

**Symptom:** Invalid data enters database, causing errors later

**Root Cause:** New property/type added without validation rules

**Prevention:** Add Malli validation rules for all new properties

**Detection:** Code review verification of validation coverage

**Fix:** Add validation rules, run fix function on existing data

---

## Testing Strategy Summary

Comprehensive testing requires coverage across multiple dimensions:

### 1. Migration Testing

- Test upgrade from previous 3 schema versions to current
- Test with graphs of varying sizes (small, medium, large)
- Test with graphs containing edge cases (circular refs, missing data)

### 2. Validation Testing

- Test with valid data (should pass)
- Test with invalid data (should fail with clear errors)
- Test fix functions on invalid data
- Test performance with large datasets

### 3. Import/Export Testing

- Round-trip test (export then import, verify data identical)
- Cross-version test (export from old version, import to new)
- Large graph test (performance and memory)
- Error handling test (corrupted files)

### 4. RTC Sync Testing

- Multi-device concurrent edits
- Conflict resolution scenarios
- Schema version mismatch handling
- Network interruption recovery

### 5. Performance Testing

- Migration time on large graphs
- Validation overhead on transactions
- Export/import speed
- Memory usage during operations

**See:** [Testing Strategies](../08-processes/schema-change-testing.md) for detailed test scenarios

---

## Future Improvements

### Short-term (1-3 months)

1. **Automated Migration Check:** CI job to detect property additions without migration entries
2. **Migration Testing Suite:** Automated tests for upgrading from previous versions
3. **RTC Configuration Linter:** Verify all properties have explicit RTC config
4. **Documentation Templates:** PR templates for schema changes with required sections

### Medium-term (3-6 months)

1. **Schema Change Impact Tool:** Analyze property usage across graphs to estimate migration impact
2. **Validation Performance Profiling:** Identify slow validation rules and optimize
3. **Export Format Versioning:** Explicit version tracking in export files
4. **Migration Dry-run Mode:** Test migrations without committing changes

### Long-term (6-12 months)

1. **Automated Rollback System:** Safe rollback for failed migrations
2. **Schema Evolution Dashboard:** Visualize schema changes over time
3. **Property Usage Analytics:** Track which properties are actively used
4. **Migration Testing in Staging:** Test migrations on production-like data before release

---

## Related Documentation

### Investigation Documents

- This document: Schema evolution investigation and overview
- [CMD+R Reload Investigation](./cmd-r-reload-data-persistence.md): Related investigation on data persistence

### Process Documentation

- [Schema Migration Best Practices](../08-processes/schema-migration-best-practices.md)
- [Validation Guidelines](../08-processes/validation-best-practices.md)
- [Import/Export Best Practices](../08-processes/import-export-best-practices.md)
- [RTC/Sync Considerations](../08-processes/rtc-sync-schema-changes.md)
- [PR Review Checklist](../08-processes/schema-change-pr-checklist.md)
- [Testing Strategies](../08-processes/schema-change-testing.md)

### Developer Guides

- [Database Schema Guide](../03-features/database-graphs/schema-guide.md): Comprehensive schema reference
- [Property Development](../05-developer-guides/property-development/): Property system documentation (to be created)

### Existing Code Review

- [prompts/review.md](../../prompts/review.md): Code review checklist
- [AGENTS.md](../../AGENTS.md): Repository agent guidelines

---

## Glossary

**Bootstrap:** Multi-stage initialization process that resolves circular dependencies when creating a new graph

**Built-in Property:** Core property defined in Logseq source code (vs user-defined or plugin-defined)

**Cardinality:** Whether a property accepts single value (`:one`) or multiple values (`:many`)

**Closed Values:** Predefined set of allowed values for a property (like enum)

**DataScript:** In-memory database library used by Logseq (Clojure port of Datomic)

**EDN:** Extensible Data Notation, Clojure's data serialization format

**Malli:** Schema validation library for Clojure/ClojureScript

**Migration:** Code that transforms data from one schema version to another

**Property Value Entity:** Separate database entity that stores a property value (reference-based approach)

**Reference Type:** Property type where values are stored as entity references, not directly

**RTC:** Real-Time Collaboration, Logseq's multi-device sync system

**Scalar Type:** Property type where values are stored directly on the entity

**Schema Version:** Major.minor version number tracking database schema evolution

**sqlite.build:** EDN export format for DB graphs, graph-agnostic representation

**Transit:** Serialization format for Clojure data structures

**Validation:** Checking data conforms to schema rules (structural and business logic)

---

## Changelog

| Date       | Version | Changes                                                                                                 |
| ---------- | ------- | ------------------------------------------------------------------------------------------------------- |
| 2025-12-28 | 1.0     | Initial investigation and documentation                                                                 |
| 2026-05-30 | 1.1     | Validated against current source; updated schema/migration references and added new capability findings |

---

## Contributing

This investigation and associated documentation should be updated when:

- New schema change patterns are discovered
- Migration issues are encountered and resolved
- Validation strategies evolve
- RTC behavior changes
- Import/export formats are modified

To contribute updates:

1. Review the relevant specific guide document
2. Add new patterns or corrections
3. Update this overview if major changes
4. Submit PR with clear description of additions

---

## License

This documentation is part of the Logseq project and follows the same license.
