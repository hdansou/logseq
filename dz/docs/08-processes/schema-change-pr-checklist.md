# Schema Change PR Review Checklist

**Document Type:** Process Checklist  
**Last Updated:** December 28, 2025  
**Related Investigation:** [Schema Evolution and Data Migration](../07-investigations/schema-evolution-and-data-migration.md)

---

## Overview

This checklist ensures all schema changes are properly reviewed before merging. Use it for every PR that modifies properties, schema structure, validation, or sync behavior.

**Usage:** Copy relevant sections into PR description or use as review guide.

---

## Quick Detection: Does This PR Need Schema Review?

✅ **YES - Use this checklist if PR modifies:**

- [ ] Files in `deps/db/src/logseq/db/frontend/property/`
- [ ] `deps/db/src/logseq/db/schema.cljs`
- [ ] `src/main/frontend/worker/db_migrate.cljs`
- [ ] `deps/db/src/logseq/db/frontend/validate.cljs`
- [ ] `deps/db/src/logseq/db/sqlite/build.cljs`
- [ ] Any file with `built-in-properties` or `built-in-classes`
- [ ] RTC sync configuration in `src/main/frontend/worker/rtc/`

❌ **NO - Skip if only:**

- UI changes without schema impact
- Non-schema bug fixes
- Documentation updates
- Test-only changes

---

## Section 1: Property Changes

### 1.1 New Property Added

- [ ] **Property definition** added to `built-in-properties` in [deps/db/src/logseq/db/frontend/property/build.cljc](deps/db/src/logseq/db/frontend/property/build.cljc)
- [ ] **Migration entry** added to `schema-version->updates` in [src/main/frontend/worker/db_migrate.cljs](src/main/frontend/worker/db_migrate.cljs)
- [ ] **Schema version** bumped in [deps/db/src/logseq/db/schema.cljs](deps/db/src/logseq/db/schema.cljs)
- [ ] **Version numbers match** between migration entry and schema version
- [ ] **RTC configuration** explicitly specified (`:rtc` map present)
- [ ] **Property namespace** follows conventions (`logseq.property.*`, `user.property.*`, etc.)
- [ ] **Property type** appropriate for use case (`:default`, `:number`, `:date`, etc.)
- [ ] **Cardinality** correctly set (`:one` or `:many`)
- [ ] **Validation rules** added if needed in [deps/db/src/logseq/db/frontend/validate.cljs](deps/db/src/logseq/db/frontend/validate.cljs)
- [ ] **Tests** cover property creation, usage, and migration
- [ ] **Documentation** updated (if user-facing property)

**Property Definition Template Check:**

```clojure
{:db/ident :logseq.property.NAMESPACE/name
 :title "Display Name"
 :schema {:type :default  ; ✓ Type specified
          :cardinality :one  ; ✓ Cardinality specified
          :public? true  ; ✓ Visibility specified
          :hide? false
          :view-context :page
          :ui-position :properties}
 :queryable? true
 :rtc {:rtc/ignore-attr-when-init-upload false  ; ✓ RTC config present
       :rtc/ignore-attr-when-init-download false
       :rtc/ignore-attr-when-syncing false}}
```

### 1.2 Property Type Changed

⚠️ **CRITICAL: Property type changes require special handling!**

- [ ] **New property created** with new type (versioned name like `-v2`)
- [ ] **Migration function** transforms old values to new property
- [ ] **Old property** marked as deprecated
- [ ] **All code references** updated to use new property
- [ ] **Data transformation** handles parsing errors gracefully
- [ ] **Tests** cover data migration from old to new type
- [ ] **Breaking change** documented in PR description
- [ ] **Release notes** include migration instructions

❌ **NEVER:**

- Directly change type of existing property with user data
- Change cardinality without migration
- Remove required field without migration

### 1.3 Property Renamed

- [ ] **New property definition** with new ident
- [ ] **Migration entry** uses `:rename-db-idents` map
- [ ] **Old property** removed from `built-in-properties`
- [ ] **All code references** updated to new ident (search codebase)
- [ ] **Tests** verify rename migration works
- [ ] **Import/export** handles both old and new idents (if needed)

### 1.4 Property Modified

- [ ] **Changes are backward compatible** (additive only)
- [ ] **Existing graphs won't break** after update
- [ ] **Migration entry added** if structure changed
- [ ] **Tests** cover modification impact

---

## Section 2: Schema Version Management

### 2.1 Version Bump

- [ ] **Version incremented** in [deps/db/src/logseq/db/schema.cljs](deps/db/src/logseq/db/schema.cljs)
- [ ] **Bump type appropriate:**
  - Minor bump (65.16 → 65.17) for additive changes
  - Major bump (65.x → 66.0) for breaking changes
- [ ] **All migrations match version:**
  - Migration entries use same version as schema
  - No version gaps in migration sequence
- [ ] **CHANGELOG.md updated** with schema version change

### 2.2 Migration Entry

- [ ] **Migration entry format correct:**
  ```clojure
  ["VERSION" {:properties [...]        ; Or
              :classes [...]           ; Or
              :rename-db-idents {...}  ; Or
              :fix function-name}]     ; Or combination
  ```
- [ ] **Migration properties list complete** (all new properties included)
- [ ] **Fix function exists** if `:fix` specified
- [ ] **Fix function signature correct:**
  ```clojure
  (defn fix-fn [db] ; Takes db, returns tx-data
    [{:db/id ... :attr ...}])
  ```
- [ ] **Fix function is idempotent** (safe to run multiple times)
- [ ] **Fix function handles edge cases** (nil, missing data, etc.)

---

## Section 3: Validation Changes

### 3.1 Validation Rules Added/Modified

- [ ] **Entity schema updated** in [deps/db/src/logseq/db/frontend/validate.cljs](deps/db/src/logseq/db/frontend/validate.cljs)
- [ ] **Dispatch order considered** if adding new entity type
- [ ] **Required fields** clearly marked (vs optional with `{:optional true}`)
- [ ] **Error messages** clear and actionable
- [ ] **Validation works** on existing data (won't break current graphs)
- [ ] **Fix function provided** for existing invalid data (if applicable)
- [ ] **Tests cover:**
  - Valid data passes
  - Invalid data fails with correct error
  - Edge cases handled
  - Fix function works

### 3.2 Property Value Validation

- [ ] **Property value schema added** if new type:
  ```clojure
  (defmethod property-value-schema :logseq.property/my-prop
    [property db property-value]
    [:and
     [:string {:error/message "Must be string"}]
     [:fn {:error/message "Custom validation"} predicate]])
  ```
- [ ] **Validation appropriate for property type**
- [ ] **Error messages helpful** for users
- [ ] **Tests cover valid and invalid values**

### 3.3 Performance Impact

- [ ] **Validation performance acceptable:**
  - Complex validation avoided in hot paths
  - Expensive checks marked with comments
  - Consider disabling during bulk import
- [ ] **Large dataset testing** if validation is complex

---

## Section 4: Import/Export Compatibility

### 4.1 Export Format Changes

- [ ] **Export includes new properties** in [deps/db/src/logseq/db/sqlite/build.cljs](deps/db/src/logseq/db/sqlite/build.cljs)
- [ ] **Export handles references** correctly (converts to UUIDs)
- [ ] **Export format version** updated if breaking change
- [ ] **Backward compatibility** maintained (old exports still import)
- [ ] **Tests include round-trip:** export → import → export

### 4.2 Import Format Changes

- [ ] **Import handles new properties** in build import functions
- [ ] **Import resolves references** correctly (UUIDs to entities)
- [ ] **Import handles missing properties** gracefully (uses defaults)
- [ ] **Import validates data** after import
- [ ] **Tests cover:**
  - Current format import
  - Previous format import (backward compatibility)
  - Invalid data handling

### 4.3 Cross-Version Compatibility

- [ ] **Format versioning** considered for breaking changes
- [ ] **Migration path** exists from previous formats
- [ ] **Clear error messages** if format unsupported
- [ ] **Documentation** updated with format changes

---

## Section 5: RTC/Sync Configuration

### 5.1 RTC Configuration Present

- [ ] **Every property** has explicit `:rtc` configuration
- [ ] **RTC behavior appropriate** for property type:
  - User content: sync enabled
  - Device-local: sync disabled
  - Creation metadata: sync initial only
- [ ] **RTC constants updated** if special handling needed
- [ ] **Sync behavior documented** in property description

### 5.2 Multi-Device Compatibility

- [ ] **Tested with two devices** (same version)
- [ ] **Tested with version mismatch** (new property on old version)
- [ ] **Conflict resolution strategy** defined for complex properties
- [ ] **Schema version compatibility** considered:
  - New version can read old graphs
  - Old version gracefully handles new properties
- [ ] **Gradual rollout** planned if sync behavior changes

### 5.3 Conflict Handling

- [ ] **Conflict resolution strategy** documented
- [ ] **Last-write-wins acceptable** or custom merge provided
- [ ] **Order conflicts handled** if property affects block ordering
- [ ] **Tests cover concurrent edits**

---

## Section 6: Testing Requirements

### 6.1 Unit Tests

- [ ] **Property creation** tested
- [ ] **Property usage** tested (setting, getting values)
- [ ] **Validation rules** tested (valid + invalid cases)
- [ ] **Migration function** tested (transforms data correctly)
- [ ] **Edge cases** covered (nil, empty, special characters)

### 6.2 Migration Tests

- [ ] **Migration from previous version** tested:
  ```clojure
  (deftest test-migration-65-16-to-65-17
    (let [db-v16 (create-test-db-with-schema "65.16")
          migrated-db (migrate-db-graph db-v16 "65.17")]
      (is (= "65.17" (get-schema-version migrated-db)))
      (is (validate-db! migrated-db))))
  ```
- [ ] **Data preservation** verified (no data loss)
- [ ] **Invalid data fix** tested (if fix function provided)
- [ ] **Performance acceptable** on large datasets

### 6.3 Import/Export Tests

- [ ] **Round-trip test** passes:
  ```clojure
  (let [exported-1 (export-db db)
        imported (import-db exported-1)
        exported-2 (export-db imported)]
    (is (= exported-1 exported-2)))
  ```
- [ ] **Cross-version import** tested (old format → new app)
- [ ] **Large graph** tested (performance + memory)

### 6.4 RTC Tests

- [ ] **Multi-device sync** tested
- [ ] **Conflict resolution** tested
- [ ] **Version mismatch** tested
- [ ] **Network interruption recovery** tested (if applicable)

### 6.5 Manual Testing

- [ ] **Create new graph** with new schema
- [ ] **Open existing graph** (auto-migration)
- [ ] **Export graph** after migration
- [ ] **Import exported graph** in clean environment
- [ ] **Test RTC sync** between two devices (if applicable)
- [ ] **Check for console errors** during all operations

---

## Section 7: Code Quality

### 7.1 Code Review

- [ ] **Code follows existing patterns** in schema management
- [ ] **No hardcoded values** (use constants/config)
- [ ] **Error handling** comprehensive
- [ ] **Logging** appropriate (warnings for issues, info for progress)
- [ ] **Comments explain** complex logic, especially:
  - Migration reasoning
  - Validation rules
  - RTC behavior

### 7.2 Bootstrap Safety

⚠️ **CRITICAL: Bootstrap order must not change!**

- [ ] **Bootstrap order unchanged** in [deps/db/src/logseq/db/frontend/create_graph.cljs](deps/db/src/logseq/db/frontend/create_graph.cljs)
- [ ] **New properties added to correct stage** (bootstrap vs regular)
- [ ] **Circular dependencies considered**
- [ ] **Graph creation tested** if bootstrap touched

### 7.3 Performance

- [ ] **Migration completes quickly** (<30s for large graphs)
- [ ] **Validation overhead acceptable** (<5ms per transaction)
- [ ] **Export/import performance** reasonable
- [ ] **Memory usage** acceptable for large graphs

---

## Section 8: Documentation

### 8.1 Code Documentation

- [ ] **Property description** clear (what it stores, how it's used)
- [ ] **Migration reasoning** documented in comments
- [ ] **RTC behavior** explained in property definition
- [ ] **Breaking changes** clearly marked with warnings

### 8.2 User Documentation

If user-facing change:

- [ ] **User guide** updated (if applicable)
- [ ] **Migration instructions** provided (if breaking change)
- [ ] **Release notes** include schema change notes
- [ ] **CHANGELOG.md** updated with details

### 8.3 Developer Documentation

- [ ] **Schema guide** updated if schema structure changed
- [ ] **API documentation** updated if property exposed to plugins
- [ ] **Examples** provided for new property usage

---

## Section 9: Deployment Considerations

### 9.1 Rollout Plan

For significant schema changes:

- [ ] **Staged rollout** planned (beta → stable)
- [ ] **Rollback plan** documented
- [ ] **Monitoring** strategy defined (track migration errors)
- [ ] **Support preparation** (anticipated user issues documented)

### 9.2 Backward Compatibility

- [ ] **Old graphs work** with new schema
- [ ] **Export from old version** imports to new version
- [ ] **Deprecation timeline** clear (if removing features)
- [ ] **Migration path** exists for at least 3 previous versions

### 9.3 Breaking Changes

If breaking change:

- [ ] **Major version bump** (65.x → 66.0)
- [ ] **Clear communication** in release notes
- [ ] **Migration tool** provided (if needed)
- [ ] **Grace period** before removal (3+ releases)

---

## Section 10: PR Description Requirements

Every schema change PR must include:

### 10.1 Summary

```markdown
## Schema Change Summary

**Type:** [Property Addition / Property Modification / Schema Structure Change]
**Schema Version:** [Old] → [New]
**Breaking Change:** [Yes / No]

### Changes Made

- Added property `:logseq.property/...`
- Migration entry added for version X.Y
- Validation rules updated
- RTC configuration specified

### Migration Strategy

[Describe how existing graphs will migrate]

### Testing Performed

- [x] Unit tests
- [x] Migration tests
- [x] Import/export round-trip
- [x] Manual testing on sample graph
- [x] RTC sync testing (if applicable)
```

### 10.2 Reviewer Guidance

```markdown
## Review Focus Areas

Please pay special attention to:

1. Migration entry matches schema version bump
2. RTC configuration appropriate for property type
3. Validation rules don't break existing graphs
4. Tests cover migration path
5. Documentation complete
```

---

## Quick Checklist Summary

Copy this into PR description:

```markdown
## Schema Change Checklist

### Property Changes

- [ ] Property definition added/modified
- [ ] Migration entry added
- [ ] Schema version bumped
- [ ] Versions match
- [ ] RTC configuration specified

### Validation

- [ ] Validation rules added/updated
- [ ] Error messages clear
- [ ] Tests cover valid/invalid cases
- [ ] Works on existing data

### Import/Export

- [ ] Export includes new properties
- [ ] Import handles new properties
- [ ] Round-trip test passes
- [ ] Backward compatible

### RTC/Sync

- [ ] RTC configuration appropriate
- [ ] Multi-device tested
- [ ] Conflict resolution defined
- [ ] Version mismatch handled

### Testing

- [ ] Unit tests
- [ ] Migration tests
- [ ] Import/export tests
- [ ] RTC tests (if applicable)
- [ ] Manual testing

### Documentation

- [ ] Code comments
- [ ] User documentation (if needed)
- [ ] CHANGELOG.md updated
- [ ] Breaking changes documented
```

---

## Automated Checks (Future Enhancement)

**Recommended CI checks to add:**

1. **Migration Entry Validator**

   - Detect changes to `built-in-properties`
   - Verify corresponding entry in `schema-version->updates`
   - Verify schema version bumped

2. **RTC Configuration Linter**

   - Check all properties have `:rtc` key
   - Warn if RTC config missing or suspicious

3. **Version Consistency Check**

   - Ensure migration version matches schema version
   - Check no version gaps in migration sequence

4. **Round-Trip Test**

   - Automated export → import → export test
   - Verify output identical

5. **Validation Test**
   - Run validation on test graphs
   - Ensure no new errors introduced

---

## Review Process

### For PR Author

1. **Before opening PR:**

   - Run through relevant checklist sections
   - Include checklist in PR description
   - Document any deviations

2. **During review:**

   - Address reviewer questions promptly
   - Update checklist as changes made
   - Add additional tests if requested

3. **Before merging:**
   - Ensure all checks passed
   - Update documentation
   - Plan rollout strategy (if needed)

### For PR Reviewer

1. **Initial review:**

   - Check PR includes schema change checklist
   - Verify all required items checked
   - Spot check critical items

2. **Deep review:**

   - Review migration logic carefully
   - Check validation rules make sense
   - Verify tests comprehensive
   - Consider edge cases

3. **Before approving:**
   - Confirm tests pass
   - Verify documentation complete
   - Check backward compatibility
   - Consider user impact

---

## Common Review Issues

### Issue 1: Missing Migration Entry

```
❌ Property added but no migration entry
✅ Add migration entry in db_migrate.cljs
```

### Issue 2: Version Mismatch

```
❌ Schema version "65.17" but migration entry "65.16"
✅ Update migration entry to match schema version
```

### Issue 3: Missing RTC Configuration

```
❌ Property definition has no :rtc key
✅ Add explicit :rtc configuration
```

### Issue 4: No Tests

```
❌ Schema change without tests
✅ Add unit, migration, and integration tests
```

### Issue 5: Breaking Backward Compatibility

```
❌ Changed property type without migration
✅ Create new property, add migration function
```

---

## Related Documentation

- [Investigation Overview](../07-investigations/schema-evolution-and-data-migration.md)
- [Schema Migration Best Practices](./schema-migration-best-practices.md)
- [Validation Guidelines](./validation-best-practices.md)
- [Import/Export Best Practices](./import-export-best-practices.md)
- [RTC/Sync Considerations](./rtc-sync-schema-changes.md)
- [Testing Strategies](./schema-change-testing.md)

---

## Changelog

| Date       | Changes                                         |
| ---------- | ----------------------------------------------- |
| 2025-12-28 | Initial version based on investigation findings |
