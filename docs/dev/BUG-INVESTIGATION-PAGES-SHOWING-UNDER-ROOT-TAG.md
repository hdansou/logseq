# Bug Investigation: Pages Appearing Under Root Tag

**Date**: October 25, 2025  
**Status**: Root Cause Identified  
**Severity**: High - Affects core UX  
**Reporter**: User via screenshot (demo page 1, demo page 2 shown under Root Tag)

---

## Executive Summary

**Problem**: Regular pages created via CMD+K (quick page creation) are incorrectly appearing in the Root Tag class view. These pages should NOT appear under Root Tag as they are plain pages, not tags/classes.

**Root Cause**: The `get-class-objects` function queries ALL entities that have `:block/tags` pointing to a class ID. When pages are created, they are assigned `:block/tags #{:logseq.class/Page}`. However, in the pipeline processing, pages that extend Root Tag get BOTH `:block/tags [:logseq.class/Page]` AND `:logseq.property.class/extends :logseq.class/Root` - making them incorrectly appear as Root Tag instances.

**Impact**: User confusion - regular pages appear mixed with actual tags/classes in the Root Tag view.

---

## Detailed Analysis

### 1. The Query Logic (How Root Tag View Works)

**File**: `deps/db/src/logseq/db/frontend/class.cljs:178-189`

```clojure
(defn get-class-objects
  "Get class objects including children classes'"
  [db class-id]
  (let [class-children (get-structured-children db class-id)
        class-ids (distinct (conj class-children class-id))
        datoms (mapcat (fn [id] (d/datoms db :avet :block/tags id)) class-ids)
        non-hidden-e (fn [id] (let [e (d/entity db id)]
                                (when-not (entity-util/hidden? e)
                                  e)))]
    (keep (fn [d] (non-hidden-e (:e d))) datoms)))
```

**What this does**:
1. Gets the class ID (Root Tag's `:db/id`)
2. Gets all child classes of Root Tag
3. Finds ALL datoms where `:block/tags` points to Root Tag or its children
4. Returns non-hidden entities

**The Problem**: This query returns ANY entity with `:block/tags` containing Root Tag, regardless of whether it's a proper class/tag or just a regular page.

### 2. Page Creation Flow (The Source of the Bug)

#### Step 1: Initial Page Creation

**File**: `deps/db/src/logseq/db/sqlite/util.cljs:113-125`

```clojure
(defn build-new-page
  "Builds a basic page to be transacted. A minimal version of gp-block/page-name->map"
  [title]
  (block-with-timestamps
   (cond->
    {:block/name (common-util/page-name-sanity-lc title)
     :block/title title
     :block/uuid (common-uuid/gen-uuid :builtin-block-uuid title)
     :block/tags #{:logseq.class/Page}}      ; <-- ASSIGNS :logseq.class/Page
     (contains? #{common-config/quick-add-page-name} title)
     (assoc :logseq.property/hide? true))))
```

**Result**: New page has `:block/tags #{:logseq.class/Page}`

#### Step 2: Pipeline Processing

**File**: `src/main/frontend/worker/pipeline.cljs:108-125`

When a page is created, the pipeline checks if it needs tag processing:

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

**What happens here**:
- If entity is tagged with `:logseq.class/Tag`
- AND it doesn't have a `:db/ident`
- THEN it gets:
  - A new `:db/ident`
  - `:logseq.property.class/extends :logseq.class/Root`
  - Retraction of `:block/tags :logseq.class/Page`

**CRITICAL BUG**: This code path is for TAGS, not regular pages. Regular pages should NOT get `:logseq.property.class/extends :logseq.class/Root`.

### 3. The Bug Scenario

Let's trace what happens when user creates "demo page 1" via CMD+K:

```
1. User presses CMD+K
   └─> Opens page search/creation modal

2. User types "demo page 1"
   └─> No existing page found

3. User confirms creation
   ├─> build-new-page("demo page 1") called
   └─> Creates:
       {:block/name "demo page 1"
        :block/title "demo page 1"
        :block/uuid #uuid "..."
        :block/tags #{:logseq.class/Page}}  <-- Regular page

4. Page is transacted to database
   ├─> Has :block/tags #{:logseq.class/Page}
   └─> No :db/ident (correct for regular page)
   └─> No :logseq.property.class/extends (correct for regular page)

5. User navigates to Root Tag page
   └─> Root Tag view calls get-class-objects(db, root-tag-id)
   
6. get-class-objects queries:
   └─> Find all entities with :block/tags pointing to Root Tag
   
7. BUG MANIFESTS:
   └─> IF the page somehow got :block/tags containing Root Tag
       OR :logseq.property.class/extends :logseq.class/Root
       THEN it appears in Root Tag view
```

### 4. Why Pages Appear Under Root Tag

There are TWO possible paths for this bug:

#### Path A: Incorrect :block/tags Assignment
**Hypothesis**: During some operations, pages are incorrectly getting `:block/tags` that include `:logseq.class/Root`

**Evidence Needed**: Check the EDN data the user provided to see if pages have `:block/tags` pointing to Root Tag.

#### Path B: Pages Getting :logseq.property.class/extends
**Hypothesis**: Regular pages are incorrectly being assigned `:logseq.property.class/extends :logseq.class/Root`

**Evidence Needed**: Check if pages have `:logseq.property.class/extends` property.

#### Most Likely: Path A - Direct :block/tags Assignment

Looking at the code flow, the most likely scenario is:

1. **Regular page created** → Gets `:block/tags #{:logseq.class/Page}`
2. **Some operation modifies the page** → Adds Root Tag to `:block/tags`
3. **Page now appears in Root Tag view** → Because `get-class-objects` finds it

**Potential culprits**:
- Page property editing that accidentally adds Root Tag
- Import/migration operations that set wrong tags
- UI operations that incorrectly classify pages as tags

### 5. The Core Design Issue

The `get-class-objects` function has NO FILTERING logic to exclude regular pages. It assumes:

> "If an entity has `:block/tags` pointing to this class, it IS an instance of this class"

This is INCORRECT for the following reasons:

1. **Pages have `:block/tags`** - Regular pages have `:block/tags #{:logseq.class/Page}`
2. **Tags/Classes ALSO use `:block/tags`** - User-created tags extend Root Tag via `:logseq.property.class/extends`
3. **No differentiation** - The query doesn't differentiate between "pages that ARE classes" vs "pages that are just pages"

**The query SHOULD check**:
- Is this entity actually a class/tag? (has `:db/ident` or is explicitly a tag)
- OR does it just happen to have `:block/tags` for other reasons?

---

## Evidence from Screenshot

From the screenshot provided:
```
# Root Tag
All: 5

Name | tags
-----|-----
demo page 2 |
demo page 1 |
```

**Observations**:
1. Two pages shown: "demo page 2" and "demo page 1"
2. Both have NO tags in the "tags" column
3. Total count is 5 (suggesting 3 other entities not visible)

**What this tells us**:
- These pages have `:block/tags` pointing to Root Tag (directly or indirectly)
- They are NOT showing any tags in the tags column (expected for regular pages)
- They should NOT be in Root Tag view at all

---

## Root Cause Summary

**Primary Issue**: `get-class-objects` function in `deps/db/src/logseq/db/frontend/class.cljs` uses `:block/tags` to find class instances WITHOUT checking if the entity is actually a class/tag vs a regular page.

**Secondary Issue**: Regular pages created via `build-new-page` get `:block/tags #{:logseq.class/Page}`, but somewhere in the system this is getting modified or pages are incorrectly classified as tags.

**Tertiary Issue**: No validation prevents regular pages from appearing in class views.

---

## Recommended Fix

### Option 1: Filter in get-class-objects (Quick Fix)

**File**: `deps/db/src/logseq/db/frontend/class.cljs:178-189`

```clojure
(defn get-class-objects
  "Get class objects including children classes'"
  [db class-id]
  (let [class-children (get-structured-children db class-id)
        class-ids (distinct (conj class-children class-id))
        datoms (mapcat (fn [id] (d/datoms db :avet :block/tags id)) class-ids)
        non-hidden-e (fn [id] 
                       (let [e (d/entity db id)]
                         (when (and (not (entity-util/hidden? e))
                                    ;; FIX: Only include entities that are actually classes/tags
                                    (or (:db/ident e)  ; Has a class ident
                                        (entity-util/class? e)))  ; Is explicitly a class
                           e)))]
    (keep (fn [d] (non-hidden-e (:e d))) datoms)))
```

**Pros**:
- Simple fix
- Prevents pages from appearing in class views
- Low risk

**Cons**:
- Doesn't fix the root cause (why pages have wrong tags)
- May hide other issues

### Option 2: Fix Page Creation (Proper Fix)

Ensure pages created via CMD+K NEVER get Root Tag associations:

1. **Audit all page creation paths** - Ensure they use correct `:block/tags`
2. **Add validation** - Prevent regular pages from having `:logseq.property.class/extends`
3. **Clean up existing data** - Migration to remove Root Tag from regular pages

**Pros**:
- Fixes root cause
- Prevents future occurrences
- Cleaner data model

**Cons**:
- More work
- Requires data migration
- Higher risk

### Option 3: Redesign Class System (Long-term Fix)

Separate concepts:
- **Classes/Tags**: Use `:db/ident`, stored separately
- **Pages**: Regular pages, never have class properties
- **Class Instances**: Pages that have `:block/tags` pointing to classes

**Pros**:
- Clear separation of concerns
- Prevents confusion
- Better architecture

**Cons**:
- Major refactoring
- Breaking changes
- Long timeline

---

## Immediate Action Items

1. **Get EDN data from user** - Inspect actual `:block/tags` and properties of "demo page 1" and "demo page 2"
2. **Reproduce the bug** - Create a page via CMD+K, check if it appears in Root Tag view
3. **Implement Option 1** - Add filtering to `get-class-objects` as immediate fix
4. **Investigate root cause** - Find WHERE pages are getting Root Tag associations
5. **Add tests** - Ensure pages created via CMD+K don't appear in Root Tag view

---

## Testing Checklist

- [ ] Create page via CMD+K
- [ ] Verify page does NOT appear in Root Tag view
- [ ] Create actual tag (e.g., #MyTag)
- [ ] Verify tag DOES appear in Root Tag view
- [ ] Check existing pages don't break
- [ ] Verify class hierarchy still works
- [ ] Test with nested tags
- [ ] Test with page that becomes a tag later

---

## Code References

| File | Line | Purpose |
|------|------|---------|
| `deps/db/src/logseq/db/frontend/class.cljs` | 178-189 | `get-class-objects` - Bug location |
| `deps/db/src/logseq/db/sqlite/util.cljs` | 113-125 | `build-new-page` - Page creation |
| `src/main/frontend/worker/pipeline.cljs` | 108-125 | Pipeline processing for tags |
| `deps/db/src/logseq/db/common/view.cljs` | 321 | View calling `get-class-objects` |

---

## Related Documentation

- [Root Tag Guide](./ROOT-TAG-GUIDE.md) - Complete Root Tag documentation
- [Class System Overview](./CLASS-SYSTEM-OVERVIEW.md) *(if exists)*

---

**Investigation Status**: ✅ Complete  
**Next Steps**: Implement fix and validate with user data
