# Logseq Database: Root Tag Guide

**Last revised:** 2026-05-29
**Status**: Complete Investigation

## Executive Summary

**Root Tag** (`:logseq.class/Root`) is a built-in class in Logseq's database system that serves as the **default parent class for new user-created tag/class entities when no explicit parent is supplied**. It acts as the root of the class hierarchy for the tag system, enabling class inheritance and ensuring user-created tags have at least one parent in the hierarchy.

---

## Table of Contents

1. [What is Root Tag?](#what-is-root-tag)
2. [Purpose and Behavior](#purpose-and-behavior)
3. [Data Structure and Storage](#data-structure-and-storage)
4. [Key Operations](#key-operations)
5. [Visibility and Filtering](#visibility-and-filtering)
6. [Bootstrap and Creation](#bootstrap-and-creation)
7. [Real-World Usage Scenarios](#real-world-usage-scenarios)
8. [Important Characteristics](#important-characteristics)
9. [Code References](#code-references)
10. [Findings: New Capabilities](#findings-new-capabilities)
11. [Developer Guidelines](#developer-guidelines)

---

## What is Root Tag?

### Definition

- **DB Ident**: `:logseq.class/Root`
- **Title**: "Root Tag"
- **Type**: Built-in class (one of Logseq's core classes)
- **Category**: Internal tag (hidden from UI in certain contexts)
- **UUID**: `#uuid "00000002-2737-8382-7000-000000000000"`

### Location in Codebase

**File**: `deps/db/src/logseq/db/frontend/class.cljs:16`

```clojure
(def ^:large-vars/data-var built-in-classes
  "Map of built-in classes for db graphs with their :db/ident as keys"
  (apply
   ordered-map
   (defkeywords
     :logseq.class/Root {:title "Root Tag"}
     ;; ... other built-in classes
```

---

## Purpose and Behavior

### Primary Function

Root Tag serves as the **implicit parent class** for all user-created tags/classes. When a user creates a new tag (class) without specifying a parent, it automatically extends `:logseq.class/Root`.

### Class Hierarchy Role

1. **Top of Hierarchy**: Root Tag sits at the top of the tag class hierarchy
2. **Default Parent**: Automatically assigned to newly created tag/class entities via the `:logseq.property.class/extends` property when they have no explicit parent
3. **Inheritance Enabler**: Allows the tag inheritance system to function properly

### Analogy

Think of Root Tag as similar to `Object` in Java or `object` in Python - it's the ultimate parent class that all other classes inherit from if they don't specify another parent.

### Example from Tests

**File**: `deps/outliner/test/logseq/outliner/page_test.cljs:54`

```clojure
(is (= #{"Root Tag" "c1"}
       (set (map :block/title (ldb/get-classes-parents [child-page3]))))
    "Child class with new parent has correct parents")
```

This test shows that a child class `child-page3` has both "Root Tag" and "c1" as parents, demonstrating the inheritance chain.

---

## Data Structure and Storage

### Database Schema

Root Tag is stored with these attributes:

- `:db/ident` - `:logseq.class/Root`
- `:block/title` - "Root Tag"
- `:block/name` - "root tag" (lowercase)
- `:logseq.property/built-in?` - `true`
- `:block/uuid` - `#uuid "00000002-2737-8382-7000-000000000000"`
- `:block/type` - `:class` (implicitly)

### Transit Export Format

From test fixtures (e.g., `src/test/migration/65.2.transit`):

```clojure
["^70",[1,"^4H",1750770383791,536870913]],
["^70",[1,"^6K","root tag",536870913]],
["^70",[1,"^5E",2,536870913]],
["^70",[1,"^5F","Root Tag",536870913]],
```

---

## Key Operations

### 4.1 Automatic Assignment to New Tags

**File**: `src/main/frontend/worker/pipeline.cljs:118-143`

When a new tag is created without a `:db/ident`, it's automatically assigned Root Tag as parent:

```clojure
(cond
  ;; add missing :db/ident and :logseq.property.class/extends for new tag
  (and (= (:v datom) (:db/id tag))
       (not (ldb/inline-tag? (:block/raw-title entity) tag))
       (not (:db/ident entity)))
  (let [eid (:db/id entity)]
    [[:db/add eid :db/ident (db-class/create-user-class-ident-from-name db-after (:block/title entity))]
     [:db/add eid :logseq.property.class/extends :logseq.class/Root]
     [:db/retract eid :block/tags :logseq.class/Page]]))
```

**When this happens**:

- User creates a tag `#MyNewTag` in their content
- System detects it's a new tag (has `:block/tags` pointing to `:logseq.class/Tag`)
- System detects it has no `:db/ident` yet
- System automatically:
  1. Creates a unique `:db/ident` (e.g., `:user.class/MyNewTag-abc123`)
  2. Adds `:logseq.property.class/extends :logseq.class/Root`
  3. Removes the `:logseq.class/Page` tag

### 4.2 Backfill Behavior for Legacy Tags

**File**: `src/main/frontend/worker/pipeline.cljs:118-143`

When a tag-class entity is observed without `:db/ident`, pipeline normalization adds a generated user class ident and sets `:logseq.property.class/extends` to `:logseq.class/Root`.

**Purpose**: Ensures class hierarchy invariants remain intact for tag entities that are missing modern class attributes.

### 4.3 Property Management

**File**: `deps/outliner/src/logseq/outliner/property.cljs:625-646`

When removing the `:logseq.property.class/extends` property entirely, it reverts to Root Tag:

```clojure
(and (ldb/class? block) (= property-id :logseq.property.class/extends))
(ldb/transact! conn
               [[:db/retract (:db/id block) :logseq.property.class/extends]
                [:db/add (:db/id block) :logseq.property.class/extends :logseq.class/Root]]
               {:outliner-op :save-block})

;; When setting a new extends value:
(let [tx-data (cond->
               [{:db/id (:db/id block) property-id v'}]
                (= property-id :logseq.property.class/extends)
                (conj [:db/retract (:db/id block) :logseq.property.class/extends :logseq.class/Root]))]
  (ldb/transact! conn tx-data {:outliner-op :save-block}))
```

**Key behavior**: Root Tag acts as a "fallback" - you can't have a tag without any parent, so removing all parents automatically assigns Root Tag.

### 4.4 Class Hierarchy Traversal

**File**: `deps/db/src/logseq/db/frontend/db.cljs:84-98`

Used when getting parent classes for inheritance checks:

```clojure
(defn get-classes-parents
  "Returns all parents of all classes. Like get-class-extends but for multiple classes"
  [tags]
  (let [tags' (filter entity-util/class? tags)
        result (mapcat db-class/get-class-extends tags')]
    (set result)))

(defn class-instance?
  "Whether `object` is an instance of `class`"
  [class object]
  (let [tags (:block/tags object)
        tags-ids (set (map :db/id tags))]
    (or
     (contains? tags-ids (:db/id class))
     (let [class-parent-ids (set (map :db/id (get-classes-parents tags)))]
       (contains? (set/union class-parent-ids tags-ids) (:db/id class))))))
```

**Usage**: Determines whether an object is an instance of a class, considering the full inheritance chain including Root Tag.

---

## Visibility and Filtering

### Hidden Contexts

Root Tag is marked as **internal** and **hidden** in certain contexts:

**File**: `deps/db/src/logseq/db/frontend/class.cljs:109-140`

```clojure
(def internal-tags
  "Built-in classes that are hidden on a node and all pages view"
  #{:logseq.class/Page :logseq.class/Property :logseq.class/Tag :logseq.class/Root
    :logseq.class/Asset})

(def private-tags
  "Built-in classes that are private and should not be used by a user directly.
  These used to be in block/type"
  (set/union (disj internal-tags :logseq.class/Root)  ; Note: Root is NOT private!
             #{:logseq.class/Journal :logseq.class/Whiteboard}))

(def hidden-tags
  "Built-in classes that are hidden in a few contexts like property values"
  #{:logseq.class/Page :logseq.class/Root :logseq.class/Asset})
```

**Important distinction**:

- Root Tag is in `internal-tags` and `hidden-tags`
- Root Tag is **NOT** in `private-tags` (unlike Page, Property, Tag, Asset)
- This means it CAN be referenced in some contexts where other built-in classes cannot

### UI Filtering Examples

Root Tag is filtered out in various UI contexts:

**File**: `src/main/frontend/components/property/value.cljs:783-888`

```clojure
classes' (remove (fn [class] (= :logseq.class/Root (:db/ident class))) classes)

;; And:
non-root-classes (cond-> (remove (fn [c] (= (:db/ident c) :logseq.class/Root)) classes)
```

**File**: `src/main/frontend/db/model.cljs:478`

```clojure
(keep (fn [e] (when-not (= :logseq.class/Root (:db/ident e)) e)) classes)
```

**Rationale**: Root Tag is implementation detail that shouldn't clutter the UI. Users shouldn't need to know it exists.

---

## Bootstrap and Creation

### Graph Creation

**File**: `deps/db/src/logseq/db/sqlite/create_graph.cljs:263`

Root Tag is one of the bootstrap classes created when initializing a new database graph:

```clojure
(defn bootstrap-class?
  [c]
  (contains? #{:logseq.class/Root :logseq.class/Property :logseq.class/Tag :logseq.class/Page :logseq.class/Template}
             (:db/ident c)))
```

**Bootstrap process**:

1. New graph is created
2. Bootstrap classes are created first (Root, Property, Tag, Page, Template)
3. Other built-in classes are created
4. User content is then loaded/created

> **Removed:** Bootstrap classes are created first (Root, Property, Tag, Template).

The current bootstrap set includes `Root`, `Property`, `Tag`, `Page`, and `Template`; the graph builder still splits them out so Root, Tag, and Property can bootstrap later class/property txs.

---

## Real-World Usage Scenarios

### Scenario 1: Creating a New Tag

```
User Action:
- User types: "This is about #AI and #MachineLearning"

System Behavior:
1. System detects two new tags: #AI, #MachineLearning
2. For each tag, system:
   - Creates block entity with :block/title "AI" / "MachineLearning"
   - Adds :block/tags [:logseq.class/Tag]
   - Pipeline detects missing :db/ident
   - Assigns :db/ident :user.class/AI-<random>
   - Assigns :logseq.property.class/extends :logseq.class/Root

Result:
- Both #AI and #MachineLearning now extend Root Tag
- They are part of the tag hierarchy
```

### Scenario 2: Tag Inheritance Chain

```
User Actions:
1. Creates tag #Animal (automatically extends Root Tag)
2. Creates tag #Mammal, sets "extends" property to #Animal
3. Creates tag #Dog, sets "extends" property to #Mammal

Inheritance Chain:
#Dog → #Mammal → #Animal → #Root Tag

Query Result:
get-classes-parents([#Dog]) → [#Mammal, #Animal, #Root Tag]

Practical Effect:
- A block tagged with #Dog is also considered tagged with #Mammal, #Animal, and #Root Tag
- Queries for #Animal will include blocks tagged with #Dog or #Mammal
```

### Scenario 3: Removing Extends Property

```
User Actions:
1. User creates #CustomTag extending #BaseTag
2. Later, user removes the "extends" property from #CustomTag

System Behavior:
1. Property removal triggers remove-block-property!
2. System detects this is a class and property is :logseq.property.class/extends
3. Instead of leaving no parent, system:
   - Retracts old extends value
   - Adds :logseq.property.class/extends :logseq.class/Root

Result:
- #CustomTag no longer inherits from #BaseTag
- #CustomTag now only extends Root Tag
- This prevents orphaned tags in the hierarchy
```

### Scenario 4: Legacy Tag Normalization

```
Scenario: Existing tag entity lacks class metadata (:db/ident)

Observed Data:
- Entity is tagged as :logseq.class/Tag
- Entity does not yet have :db/ident

Pipeline Behavior:
1. Detect missing :db/ident for tag-class entity
2. Generate user class ident from title
3. Add :logseq.property.class/extends :logseq.class/Root
4. Retract :logseq.class/Page where applicable

Result:
- Tag entity becomes a valid class entity
- Class hierarchy remains connected to Root Tag
```

---

## Important Characteristics

### Uniqueness

- Root Tag is **NOT excluded** from `private-tags` (unlike Page, Property, Tag)
- It IS included in `internal-tags` and `hidden-tags`
- This suggests it can be used in some contexts where other built-in classes cannot

> **Removed:** Root Tag itself **cannot be deleted** or modified by users.

### Immutability

- Root Tag is a built-in class with a **fixed UUID**
- It is always present in database graphs
- It is part of the core schema
- It is hidden from most class-selection UI so users do not usually interact with it directly

### Implicit Behavior

- Users **never manually assign** Root Tag
- Assignment happens automatically via:
  - Pipeline processing (new or legacy tag entities missing class metadata)
  - Property fallback logic when extends is removed
- **Hidden from most UI contexts** to avoid confusion

### Inheritance Rules

- Every user-created tag MUST have at least one parent
- Root Tag serves as the ultimate fallback parent
- Cannot create circular inheritance (Root Tag is at the top)
- Multiple inheritance is supported (tag can extend multiple classes, all eventually lead to Root Tag)

---

## Code References

| File                                                | Line     | Purpose                                  |
| --------------------------------------------------- | -------- | ---------------------------------------- |
| `deps/db/src/logseq/db/frontend/class.cljs`         | 17       | Definition of Root Tag                   |
| `deps/db/src/logseq/db/frontend/class.cljs`         | 109-140  | Visibility categorization                |
| `deps/db/src/logseq/db/frontend/class.cljs`         | 144-150  | Helper functions (class traversal start) |
| `src/main/frontend/worker/pipeline.cljs`            | 118-143  | Auto-assignment and backfill for tags    |
| `deps/outliner/src/logseq/outliner/property.cljs`   | 625-646  | Property management (set/remove)         |
| `deps/db/src/logseq/db/frontend/db.cljs`            | 84-98    | Class hierarchy traversal                |
| `src/main/frontend/components/property/value.cljs`  | 859, 970 | UI filtering                             |
| `src/main/frontend/db/model.cljs`                   | 357      | Model-level filtering                    |
| `deps/db/src/logseq/db/sqlite/create_graph.cljs`    | 263      | Bootstrap class identification           |
| `deps/outliner/test/logseq/outliner/page_test.cljs` | 52       | Test demonstrating inheritance           |

## Findings: New Capabilities

- Class pickers can now exclude Root explicitly via `get-all-classes`' `except-root-class?` option, and some UI flows use it to keep Root out of class suggestions.
- Class filtering is now split between Root-only hiding and broader hidden/private-class filtering, which gives callers finer control over which built-ins are surfaced.

---

## Developer Guidelines

### When Adding New Features

#### ✅ DO:

- Filter out Root Tag when showing tag lists to users
- Use `get-classes-parents` to check full inheritance chain
- Ensure new tags get Root Tag as default parent
- Consider Root Tag in class hierarchy queries
- Test migration scenarios with Root Tag

#### ❌ DON'T:

- Allow users to delete Root Tag
- Show Root Tag in UI unless debugging
- Create tags without any parent (must at least have Root Tag)
- Modify Root Tag's core properties
- Assume a tag has no parents (it always has at least Root Tag)

### Common Patterns

#### Filtering Root Tag from Results

```clojure
;; Pattern 1: Using remove
(remove (fn [class] (= :logseq.class/Root (:db/ident class))) classes)

;; Pattern 2: Using keep
(keep (fn [e] (when-not (= :logseq.class/Root (:db/ident e)) e)) classes)

;; Pattern 3: Using filter
(filter (fn [c] (not= (:db/ident c) :logseq.class/Root)) classes)
```

#### Checking Class Hierarchy

```clojure
;; Get all parents including Root Tag
(get-classes-parents [my-tag])

;; Check if object is instance of class (considers parents)
(class-instance? target-class my-object)

;; Get direct extends
(:logseq.property.class/extends my-tag)
```

#### Setting Extends Property

```clojure
;; Always retract Root Tag when setting explicit parent
(let [tx-data [[:db/add tag-id :logseq.property.class/extends parent-class-id]
               [:db/retract tag-id :logseq.property.class/extends :logseq.class/Root]]]
  (d/transact! conn tx-data))
```

### Testing Considerations

When writing tests:

1. **Expect Root Tag in parent chains** - Don't be surprised when it appears
2. **Test tag creation** - Verify Root Tag is automatically assigned
3. **Test normalization scenarios** - Ensure legacy tag entities get Root Tag
4. **Test property removal** - Verify fallback to Root Tag
5. **Test UI filtering** - Ensure Root Tag is hidden appropriately

Example test pattern:

```clojure
(deftest tag-inheritance-test
  (let [new-tag (create-tag! "MyTag")]
    (is (contains? (set (get-classes-parents [new-tag]))
                   (d/entity [:db/ident :logseq.class/Root]))
        "New tag should extend Root Tag")))
```

### Debugging Tips

If you see unexpected behavior with tags:

1. **Check if Root Tag exists** - Query for `:logseq.class/Root`
2. **Inspect extends chain** - Use `get-classes-parents` to see full chain
3. **Verify bootstrap** - Check if graph was properly initialized
4. **Check migrations** - Ensure migrations ran successfully
5. **Look for orphaned tags** - Tags without Root Tag may cause issues

Debug query:

```clojure
;; Find tags without Root Tag (should be empty!)
(d/q '[:find [?t ...]
       :where
       [?t :block/tags :logseq.class/Tag]
       (not [?t :logseq.property.class/extends :logseq.class/Root])]
     db)
```

---

## Conclusion

**Root Tag** is a fundamental infrastructure component of Logseq's class/tag system that:

- Provides a universal parent for all user-created tags
- Enables class inheritance without requiring users to manage it explicitly
- Ensures consistency and completeness in the tag hierarchy
- Operates transparently behind the scenes
- Prevents orphaned or disconnected tags in the class system

It represents the "top" of the tag inheritance tree, similar to `Object` in Java or `object` in Python, ensuring every tag has at least one parent and enabling the inheritance chain to function properly.

Understanding Root Tag is essential for:

- **Database developers** working on the class/tag system
- **Migration developers** handling schema upgrades
- **Feature developers** building tag-related functionality
- **Test writers** creating tests for tag behavior
- **Debuggers** investigating tag hierarchy issues

---

## Related Documentation

- [Database Schema Guide](../../../docs/03-features/database-graphs/schema-guide.md)
- [Tagging and Classification System](../../../docs/03-features/tagging-classification/system-guide.md)
- [Advanced Queries](../ddocs/how-to/queries/advanced-queries.md)

## Questions or Issues?

If you encounter issues related to Root Tag or have questions about the class hierarchy system:

1. Check this guide first
2. Search for `:logseq.class/Root` in the codebase
3. Look at test files for examples
4. Check recent migrations for related changes
5. Consult with the database team

---

**Document Status**: ✅ Complete and Verified  
**Investigation Date**: 2026-05-25  
**Maintained By**: Database Team
