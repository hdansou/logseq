# Logseq Database Graph Schema Reference

**Complete enumeration of all built-in properties, classes, property types, and entity types**

**Schema Version**: 65.12
**Last Updated**: 2025-10-15
**Source**: `deps/db/src/logseq/db/frontend/`

---

## Table of Contents

1. [Built-in Properties (100+)](#built-in-properties)
   - [Schema Meta Properties](#schema-meta-properties)
   - [Core Block/Page Attributes](#core-blockpage-attributes)
   - [Task Management Properties](#task-management-properties)
   - [Temporal Properties](#temporal-properties)
   - [Property System Properties](#property-system-properties)
   - [Class System Properties](#class-system-properties)
   - [View Properties](#view-properties)
   - [Asset Properties](#asset-properties)
   - [PDF Annotation Properties](#pdf-annotation-properties)
   - [Whiteboard Properties](#whiteboard-properties)
   - [Linked References Properties](#linked-references-properties)
   - [Journal Properties](#journal-properties)
   - [FSRS (Spaced Repetition) Properties](#fsrs-spaced-repetition-properties)
   - [User Properties](#user-properties)
   - [History Properties](#history-properties)
   - [Template Properties](#template-properties)
   - [Embedding Properties](#embedding-properties)

2. [Built-in Classes (13)](#built-in-classes)

3. [Property Types (14)](#property-types)
   - [User-Facing Types (7)](#user-facing-types)
   - [Internal Types (10)](#internal-types)

4. [Entity Types (13)](#entity-types)
   - [Page Entity Types (5)](#page-entity-types)
   - [Block Entity Types (6)](#block-entity-types)
   - [Special Entity Types (2)](#special-entity-types)

---

## Built-in Properties

**Total Count**: 107 properties

**Location**: `deps/db/src/logseq/db/frontend/property.cljs:21-576`

---

### Schema Meta Properties

Properties that define how properties work (meta-properties).

| Property | Type | Cardinality | Public? | Hide? | Description |
|----------|------|-------------|---------|-------|-------------|
| **`:logseq.property/type`** | `:keyword` | one | ❌ | ✅ | The type of a property (e.g., :default, :number, :date) |
| **`:logseq.property/hide?`** | `:checkbox` | one | ❌ | ✅ | Whether property is hidden from UI |
| **`:logseq.property/public?`** | `:checkbox` | one | ❌ | ✅ | Whether property is user-visible and editable |
| **`:logseq.property/view-context`** | `:keyword` | one | ❌ | ✅ | Where property can be viewed (:page, :block, :class, :property, :never) |
| **`:logseq.property/ui-position`** | `:keyword` | one | ❌ | ✅ | UI position (:properties, :block-left, :block-right, :block-below) |
| **`:logseq.property/classes`** | `:entity` | many | ❌ | ✅ | Classes that have this property |
| **`:logseq.property/value`** | `:any` | one | ❌ | ✅ | Value of a property value block |

**Usage**: These properties are used to define the schema of other properties. For example, every property must have `:logseq.property/type` set.

---

### Core Block/Page Attributes

Properties that are also DataScript attributes (stored directly, not in `:block/properties`).

| Property | DataScript Attr | Type | Cardinality | Public? | Queryable? | Description |
|----------|----------------|------|-------------|---------|------------|-------------|
| **`:block/alias`** | `:block/alias` | `:page` | many | ✅ | ✅ | Page aliases |
| **`:block/tags`** | `:block/tags` | `:class` | many | ✅ | ✅ | Tags/classes on a block or page |
| **`:block/parent`** | `:block/parent` | `:entity` | one | ❌ | ❌ | Parent block/page |
| **`:block/order`** | `:block/order` | `:string` | one | ❌ | ❌ | Fractional index for ordering |
| **`:block/collapsed?`** | `:block/collapsed?` | `:checkbox` | one | ❌ | ❌ | Whether block is collapsed |
| **`:block/page`** | `:block/page` | `:entity` | one | ❌ | ❌ | Page that contains this block |
| **`:block/refs`** | `:block/refs` | `:entity` | many | ❌ | ❌ | All entities referenced in block |
| **`:block/link`** | `:block/link` | `:entity` | one | ❌ | ❌ | Entity this block links to (for embeds, tags) |
| **`:block/title`** | `:block/title` | `:string` | one | ❌ | ❌ | Block/page title (content) |
| **`:block/closed-value-property`** | `:block/closed-value-property` | `:entity` | many | ❌ | ❌ | Properties for which this is a closed value |
| **`:block/journal-day`** | `:block/journal-day` | `:raw-number` | one | ❌ | ✅ | Journal date (YYYYMMDD format) |
| **`:block/created-at`** | `:block/created-at` | `:datetime` | one | ❌ | ❌ | Block creation timestamp |
| **`:block/updated-at`** | `:block/updated-at` | `:datetime` | one | ❌ | ❌ | Block last update timestamp |

**Note**: These are stored as DataScript attributes (`:db/valueType :db.type/ref` or scalar), not as key-value pairs in `:block/properties`.

---

### Task Management Properties

Properties for task/todo management.

| Property | Type | Cardinality | Public? | UI Position | Queryable? | Closed Values | Description |
|----------|------|-------------|---------|-------------|------------|---------------|-------------|
| **`:logseq.property/status`** | `:default` | one | ✅ | block-left | ✅ | ✅ Yes (6) | Task status |
| **`:logseq.property/priority`** | `:default` | one | ✅ | block-left | ❌ | ✅ Yes (4) | Task priority |
| **`:logseq.property/deadline`** | `:datetime` | one | ✅ | block-below | ✅ | ❌ | Task deadline date/time |
| **`:logseq.property/scheduled`** | `:datetime` | one | ✅ | block-below | ✅ | ❌ | Task scheduled date/time |

**Status Closed Values**:
1. `:logseq.property/status.backlog` - "Backlog" (icon: "Backlog")
2. `:logseq.property/status.todo` - "Todo" (icon: "Todo", checkbox: false)
3. `:logseq.property/status.doing` - "Doing" (icon: "InProgress50")
4. `:logseq.property/status.in-review` - "In Review" (icon: "InReview")
5. `:logseq.property/status.done` - "Done" (icon: "Done", checkbox: true)
6. `:logseq.property/status.canceled` - "Canceled" (icon: "Cancelled")

**Priority Closed Values**:
1. `:logseq.property/priority.low` - "Low" (icon: "priorityLvlLow")
2. `:logseq.property/priority.medium` - "Medium" (icon: "priorityLvlMedium")
3. `:logseq.property/priority.high` - "High" (icon: "priorityLvlHigh")
4. `:logseq.property/priority.urgent` - "Urgent" (icon: "priorityLvlUrgent")

**Properties on Properties**:
- `:logseq.property/status`:
  - `:logseq.property/hide-empty-value`: `true`
  - `:logseq.property/default-value`: `:logseq.property/status.todo`
  - `:logseq.property/enable-history?`: `true`
- `:logseq.property/priority`:
  - `:logseq.property/hide-empty-value`: `true`
  - `:logseq.property/enable-history?`: `true`
- `:logseq.property/deadline`:
  - `:logseq.property/hide-empty-value`: `true`
  - `:logseq.property/description`: "Use it to finish something at a specific date(time)."
- `:logseq.property/scheduled`:
  - `:logseq.property/hide-empty-value`: `true`
  - `:logseq.property/description`: "Use it to plan something to start at a specific date(time)."

---

### Temporal Properties

Properties for repeating/recurring tasks.

| Property | Type | Cardinality | Public? | Queryable? | Closed Values | Description |
|----------|------|-------------|---------|------------|---------------|-------------|
| **`:logseq.property.repeat/recur-frequency`** | `:number` | one | ❌ | ✅ | ❌ | How often task repeats (e.g., 2 for "every 2 days") |
| **`:logseq.property.repeat/recur-unit`** | `:default` | one | ❌ | ✅ | ✅ Yes (6) | Time unit for recurrence |
| **`:logseq.property.repeat/repeated?`** | `:checkbox` | one | ❌ | ✅ | ❌ | Whether block is a repeating task |
| **`:logseq.property.repeat/temporal-property`** | `:property` | one | ❌ | ❌ | ❌ | Which temporal property triggers repeat (deadline/scheduled) |
| **`:logseq.property.repeat/checked-property`** | `:property` | one | ❌ | ❌ | ❌ | Property that tracks checked state |

**Recur Unit Closed Values**:
1. `:logseq.property.repeat/recur-unit.minute` - "Minute"
2. `:logseq.property.repeat/recur-unit.hour` - "Hour"
3. `:logseq.property.repeat/recur-unit.day` - "Day"
4. `:logseq.property.repeat/recur-unit.week` - "Week"
5. `:logseq.property.repeat/recur-unit.month` - "Month"
6. `:logseq.property.repeat/recur-unit.year` - "Year"

**Properties on Properties**:
- `:logseq.property.repeat/recur-frequency`:
  - `:logseq.property/hide-empty-value`: `true`
  - `:logseq.property/default-value`: `1`
- `:logseq.property.repeat/recur-unit`:
  - `:logseq.property/hide-empty-value`: `true`
  - `:logseq.property/default-value`: `:logseq.property.repeat/recur-unit.day`

---

### Property System Properties

Properties related to property definitions and behavior.

| Property | Type | Cardinality | Public? | View Context | Description |
|----------|------|-------------|---------|--------------|-------------|
| **`:logseq.property/description`** | `:default` | one | ✅ | any | Description of a property or class |
| **`:logseq.property/default-value`** | `:entity` | one | ❌ | property | Default ref value for property |
| **`:logseq.property/scalar-default-value`** | `:any` | one | ❌ | property | Default scalar value for property |
| **`:logseq.property/hide-empty-value`** | `:checkbox` | one | ✅ | property | Hide property when value is empty |
| **`:logseq.property/created-from-property`** | `:entity` | one | ❌ | - | Property that created this property value block |
| **`:logseq.property/built-in?`** | `:checkbox` | one | ❌ | - | Whether entity is built-in (created by Logseq) |
| **`:logseq.property/enable-history?`** | `:checkbox` | one | ✅ | property | Record history when property value changes |
| **`:logseq.property/choice-checkbox-state`** | `:checkbox` | one | ❌ | - | Checkbox state for choice values (used by status) |
| **`:logseq.property/checkbox-display-properties`** | `:property` | many | ❌ | - | Properties to display as checkboxes |

**Properties on Properties**:
- `:logseq.property/description`:
  - `:logseq.property/description`: (none - it doesn't describe itself)
- `:logseq.property/hide-empty-value`:
  - `:logseq.property/description`: "Hides a property's value on any node when empty e.g. when a property appears on a node through a tag."
- `:logseq.property/enable-history?`:
  - `:logseq.property/description`: "Records history anytime a property's value changes on a node."

---

### Class System Properties

Properties for class/tag definitions and behavior.

| Property | Type | Cardinality | Public? | View Context | Queryable? | Description |
|----------|------|-------------|---------|--------------|------------|-------------|
| **`:logseq.property.class/extends`** | `:class` | many | ✅ | class | ✅ | Parent classes (inheritance) |
| **`:logseq.property.class/properties`** | `:property` | many | ✅ | never | ❌ | Properties defined by this class |
| **`:logseq.property.class/hide-from-node`** | `:checkbox` | one | ✅ | class | ❌ | Hide class tag from node display |

**Properties on Properties**:
- `:logseq.property.class/extends`:
  - `:logseq.property/description`: "This enables tags to inherit properties from other tags"

---

### View Properties

Properties for configuring table/gallery views.

| Property | Type | Cardinality | Public? | Queryable? | Closed Values | RTC Ignore? | Description |
|----------|------|-------------|---------|------------|---------------|-------------|-------------|
| **`:logseq.property.view/type`** | `:default` | one | ❌ | ✅ | ✅ Yes (3) | ❌ | View type (table, list, gallery) |
| **`:logseq.property.view/feature-type`** | `:keyword` | one | ❌ | ❌ | ❌ | ❌ | Feature type for view |
| **`:logseq.property.view/group-by-property`** | `:property` | one | ❌ | ✅ | ❌ | ❌ | Property to group by |
| **`:logseq.property.view/sort-groups-by-property`** | `:property` | one | ❌ | ❌ | ❌ | ✅ | Property to sort groups by |
| **`:logseq.property.view/sort-groups-desc?`** | `:checkbox` | one | ❌ | ❌ | ❌ | ✅ | Sort groups descending? |
| **`:logseq.property.table/sorting`** | `:coll` | one | ❌ | ❌ | ❌ | ✅ | Table sorting configuration |
| **`:logseq.property.table/filters`** | `:map` | one | ❌ | ❌ | ❌ | ❌ | Table filter configuration |
| **`:logseq.property.table/hidden-columns`** | `:keyword` | many | ❌ | ❌ | ❌ | ❌ | Hidden columns in table |
| **`:logseq.property.table/ordered-columns`** | `:coll` | one | ❌ | ❌ | ❌ | ❌ | Column order in table |
| **`:logseq.property.table/sized-columns`** | `:map` | one | ❌ | ❌ | ❌ | ❌ | Column width settings |
| **`:logseq.property.table/pinned-columns`** | `:property` | many | ❌ | ❌ | ❌ | ❌ | Pinned columns in table |
| **`:logseq.property/view-for`** | `:node` | one | ❌ | ❌ | ❌ | ❌ | Query/page this view belongs to |

**View Type Closed Values**:
1. `:logseq.property.view/type.table` - "Table View" (icon: "table")
2. `:logseq.property.view/type.list` - "List View" (icon: "list")
3. `:logseq.property.view/type.gallery` - "Gallery View" (icon: "layout-grid")

**Properties on Properties**:
- `:logseq.property.view/type`:
  - `:logseq.property/default-value`: `:logseq.property.view/type.table`
- `:logseq.property.view/sort-groups-desc?`:
  - `:logseq.property/scalar-default-value`: `true`

**Note**: RTC Ignore means these properties are not synced during real-time collaboration.

---

### Asset Properties

Properties for file/asset management.

| Property | Type | Cardinality | Public? | Queryable? | RTC Ignore? | Description |
|----------|------|-------------|---------|------------|-------------|-------------|
| **`:logseq.property/asset`** | `:entity` | one | ❌ | ❌ | ❌ | Reference to asset entity |
| **`:logseq.property.asset/type`** | `:string` | one | ❌ | ✅ | ❌ | File MIME type (e.g., "image/png") |
| **`:logseq.property.asset/size`** | `:raw-number` | one | ❌ | ✅ | ❌ | File size in bytes |
| **`:logseq.property.asset/checksum`** | `:string` | one | ❌ | ❌ | ❌ | File checksum (hash) |
| **`:logseq.property.asset/last-visit-page`** | `:raw-number` | one | ❌ | ❌ | ✅ | Last visited page number (for PDFs) |
| **`:logseq.property.asset/remote-metadata`** | `:map` | one | ❌ | ❌ | ✅ | Cloud storage metadata |
| **`:logseq.property.asset/resize-metadata`** | `:map` | one | ❌ | ❌ | ❌ | Image resize metadata |

**Note**: `:type`, `:size`, and `:checksum` are **required** for blocks tagged with `#Asset`.

---

### PDF Annotation Properties

Properties for PDF highlights and annotations.

| Property | Type | Cardinality | Public? | Closed Values | Description |
|----------|------|-------------|---------|---------------|-------------|
| **`:logseq.property.pdf/hl-type`** | `:keyword` | one | ❌ | ❌ | Annotation type (highlight, underline, etc) |
| **`:logseq.property.pdf/hl-color`** | `:default` | one | ❌ | ✅ Yes (5) | Highlight color |
| **`:logseq.property.pdf/hl-page`** | `:raw-number` | one | ❌ | ❌ | Page number of annotation |
| **`:logseq.property.pdf/hl-image`** | `:entity` | one | ❌ | ❌ | Screenshot image of annotation |
| **`:logseq.property.pdf/hl-value`** | `:map` | one | ❌ | ❌ | Annotation position/bounds data |

**Highlight Color Closed Values**:
1. `:logseq.property/color.yellow` - "yellow"
2. `:logseq.property/color.red` - "red"
3. `:logseq.property/color.green` - "green"
4. `:logseq.property/color.blue` - "blue"
5. `:logseq.property/color.purple` - "purple"

---

### Whiteboard Properties

Properties for Tldraw whiteboard functionality.

| Property | Type | Cardinality | Public? | Description |
|----------|------|-------------|---------|-------------|
| **`:logseq.property.tldraw/page`** | `:map` | one | ❌ | Tldraw page data (canvas) |
| **`:logseq.property.tldraw/shape`** | `:map` | one | ❌ | Tldraw shape data (for blocks on whiteboard) |

---

### Linked References Properties

Properties for filtering linked references.

| Property | Type | Cardinality | Public? | Description |
|----------|------|-------------|---------|-------------|
| **`:logseq.property.linked-references/includes`** | `:node` | many | ❌ | References to include |
| **`:logseq.property.linked-references/excludes`** | `:node` | many | ❌ | References to exclude |

---

### Journal Properties

Properties for journal page configuration.

| Property | Type | Cardinality | Public? | Description |
|----------|------|-------------|---------|-------------|
| **`:logseq.property.journal/title-format`** | `:string` | one | ❌ | Date format for journal titles (e.g., "MMM do, yyyy") |

---

### FSRS (Spaced Repetition) Properties

Properties for flashcard/spaced repetition system.

| Property | Type | Cardinality | Public? | Description |
|----------|------|-------------|---------|-------------|
| **`:logseq.property.fsrs/due`** | `:datetime` | one | ❌ | When card is next due for review |
| **`:logseq.property.fsrs/state`** | `:map` | one | ❌ | FSRS algorithm state (difficulty, stability, etc) |

---

### User Properties

Properties for user profiles.

| Property | Type | Cardinality | Public? | Description |
|----------|------|-------------|---------|-------------|
| **`:logseq.property.user/name`** | `:string` | one | ✅ | User's display name |
| **`:logseq.property.user/email`** | `:string` | one | ✅ | User's email address |
| **`:logseq.property.user/avatar`** | `:string` | one | ✅ | URL to user's avatar image |

---

### History Properties

Properties for tracking property value changes.

| Property | Type | Cardinality | Public? | Description |
|----------|------|-------------|---------|-------------|
| **`:logseq.property.history/block`** | `:entity` | one | ❌ | Block whose property changed |
| **`:logseq.property.history/property`** | `:property` | one | ❌ | Property that changed |
| **`:logseq.property.history/ref-value`** | `:entity` | one | ❌ | New ref value (for ref-type properties) |
| **`:logseq.property.history/scalar-value`** | `:any` | one | ❌ | New scalar value (for scalar properties) |

**Note**: History blocks must have **either** `:ref-value` or `:scalar-value` (at least one required).

---

### Template Properties

Properties for templates.

| Property | Type | Cardinality | Public? | Queryable? | Description |
|----------|------|-------------|---------|------------|-------------|
| **`:logseq.property/used-template`** | `:node` | one | ❌ | ❌ | Template that was used to create this block |
| **`:logseq.property/template-applied-to`** | `:class` | many | ✅ | ✅ | Classes/tags to apply template to |

---

### Embedding Properties

Properties for semantic search embeddings.

| Property | Type | Cardinality | Public? | Queryable? | RTC Ignore? | Description |
|----------|------|-------------|---------|------------|-------------|-------------|
| **`:logseq.property.embedding/hnsw-label-updated-at`** | `:datetime` | one | ❌ | ❌ | ✅ | When HNSW label was last updated |

---

### Miscellaneous Properties

Other system properties.

| Property | Type | Cardinality | Public? | Queryable? | Description |
|----------|------|-------------|---------|------------|-------------|
| **`:logseq.property.node/display-type`** | `:keyword` | one | ❌ | ✅ | Display type for block (code, math, quote) |
| **`:logseq.property.code/lang`** | `:string` | one | ❌ | ✅ | Programming language for code blocks |
| **`:logseq.property/query`** | `:default` | one | ✅ | ❌ | Query text for query blocks |
| **`:logseq.property/page-tags`** | `:page` | many | ✅ | ❌ | Page tags (backward compatible tagging) |
| **`:logseq.property/background-color`** | `:default` | one | ❌ | ❌ | Block background color |
| **`:logseq.property/heading`** | `:any` | one | ❌ | ✅ | Heading level (1-6) or auto |
| **`:logseq.property/ls-type`** | `:keyword` | one | ❌ | ❌ | Legacy type field (PDF, whiteboard) |
| **`:logseq.property/order-list-type`** | `:default` | one | ❌ | ❌ | List type (ordered/unordered) |
| **`:logseq.property/icon`** | `:map` | one | ❌ | ❌ | Icon configuration |
| **`:logseq.property/publishing-public?`** | `:checkbox` | one | ✅ | ❌ | Include in published graph |
| **`:logseq.property/exclude-from-graph-view`** | `:checkbox` | one | ✅ | ❌ | Hide from graph visualization |
| **`:logseq.property/created-by-ref`** | `:entity` | one | ❌ | ✅ | User who created this block |

---

## Built-in Classes

**Total Count**: 13 classes

**Location**: `deps/db/src/logseq/db/frontend/class.cljs:16-93`

| Class | Extends | Title | Required Properties | Schema Properties | Special Behaviors |
|-------|---------|-------|-------------------|------------------|-------------------|
| **`:logseq.class/Root`** | - | "Root Tag" | - | - | Base of all class hierarchy; stable `:db/id 1` |
| **`:logseq.class/Tag`** | `Root` | "Tag" | - | - | All user classes extend Tag; stable `:db/id 2` |
| **`:logseq.class/Property`** | `Root` | "Property" | - | - | All properties are tagged with this; stable `:db/id 3` |
| **`:logseq.class/Page`** | `Root` | "Page" | - | - | Standard pages |
| **`:logseq.class/Journal`** | `Page` | "Journal" | - | - | Daily notes; has `:logseq.property.journal/title-format` = "MMM do, yyyy" |
| **`:logseq.class/Whiteboard`** | `Page` | "Whiteboard" | - | - | Canvas/whiteboard pages |
| **`:logseq.class/Task`** | - | "Task" | - | `:status`, `:priority`, `:deadline`, `:scheduled` | Task management |
| **`:logseq.class/Query`** | - | "Query" | - | `:query` | Executable query blocks; has icon "search" |
| **`:logseq.class/Card`** | - | "Card" | - | `:fsrs/state`, `:fsrs/due` | Flashcards for spaced repetition |
| **`:logseq.class/Cards`** | `Query` | "Cards" | - | - | Query blocks for flashcards; has icon "search" |
| **`:logseq.class/Asset`** | - | "Asset" | `:asset/type`, `:asset/size`, `:asset/checksum` | Same as required | File/image management; hidden from node; default view: gallery |
| **`:logseq.class/Code-block`** | - | "Code" | - | `:node/display-type`, `:code/lang` | Code blocks with syntax highlighting; hidden from node |
| **`:logseq.class/Quote-block`** | - | "Quote" | - | `:node/display-type` | Quote blocks; hidden from node |
| **`:logseq.class/Math-block`** | - | "Math" | - | `:node/display-type` | LaTeX math blocks; hidden from node |
| **`:logseq.class/Pdf-annotation`** | - | "PDF Annotation" | `:ls-type`, `:pdf/hl-color`, `:asset`, `:pdf/hl-page`, `:pdf/hl-value` | Same + `:pdf/hl-type`, `:pdf/hl-image` | PDF highlights; hidden from node |
| **`:logseq.class/Template`** | - | "Template" | - | `:template-applied-to` | Block templates; stable `:db/id 4` |

**Note**: "Schema Properties" are properties that appear on blocks tagged with this class. "Required Properties" are enforced by Malli validation.

---

### Class Categories

**Location**: `deps/db/src/logseq/db/frontend/class.cljs:95-120`

| Category | Classes | Purpose |
|----------|---------|---------|
| **Page Classes** | `Root`, `Tag`, `Property`, `Page`, `Journal`, `Whiteboard` | Classes that behave like pages (have `:block/name`) |
| **Internal Tags** | `Root`, `Tag`, `Property`, `Page`, `Asset` | Hidden from node display and all-pages view |
| **Private Tags** | `Tag`, `Property`, `Page`, `Asset`, `Journal`, `Whiteboard` | Should not be used by users directly |
| **Hidden Tags** | `Root`, `Page`, `Asset` | Hidden in some contexts like property values |
| **Page Children Classes** | `Journal`, `Whiteboard` | Classes that extend `Page` |

---

## Property Types

**Total Count**: 14 types (7 user-facing + 10 internal, with 3 shared)

**Location**: `deps/db/src/logseq/db/frontend/property/type.cljs:15-78`

---

### User-Facing Types

Types available to users in the UI when creating properties.

| Type | Storage | Validation | Supports Cardinality? | Supports Closed Values? | Example Use Case |
|------|---------|------------|---------------------|----------------------|------------------|
| **`:default`** | Ref (`:block/title`) | Text entity | ✅ Yes | ✅ Yes | Free text, tags, labels, choice fields |
| **`:number`** | Ref (`:logseq.property/value`) | Number entity | ✅ Yes | ✅ Yes | Ratings, scores, counts, numeric choices |
| **`:date`** | Ref (`:block/title`) | Journal page | ✅ Yes | ❌ No | Birth dates, event dates, milestones |
| **`:datetime`** | Scalar (`:number`) | Number (timestamp) | ❌ No | ❌ No | Created-at, updated-at, precise timestamps |
| **`:checkbox`** | Scalar (`:boolean`) | Boolean | ❌ No | ✅ Yes | Flags, toggles, yes/no fields |
| **`:url`** | Ref (`:block/title`) | URL format | ✅ Yes | ✅ Yes | Links, web references, external resources |
| **`:node`** | Ref (`:block/title`) | Page/block with tags | ✅ Yes | ❌ No | References to tagged objects |

**Order in UI**: As listed above (default, number, date, datetime, checkbox, url, node)

---

### Internal Types

Types only used by built-in properties (not available to users in UI).

| Type | Storage | Validation | Supports Cardinality? | User Allowed? | Example Built-in Properties |
|------|---------|------------|---------------------|---------------|---------------------------|
| **`:string`** | Scalar | String | ❌ No | ❌ No | `:logseq.property.asset/type`, `:logseq.property.code/lang` |
| **`:raw-number`** | Scalar | Number | ❌ No | ❌ No | `:block/journal-day`, `:logseq.property.asset/size` |
| **`:keyword`** | Scalar | Keyword | ❌ No | ❌ No | `:logseq.property/type`, `:logseq.property.node/display-type` |
| **`:map`** | Scalar | Map | ❌ No | ⚠️ Advanced | `:logseq.property.tldraw/page`, `:logseq.property.fsrs/state` |
| **`:coll`** | Scalar | Collection | ❌ No | ❌ No | `:logseq.property.table/sorting`, `:logseq.property.table/ordered-columns` |
| **`:any`** | Scalar | Any value | ❌ No | ❌ No | `:logseq.property/value`, `:logseq.property.history/scalar-value` |
| **`:entity`** | Ref | Entity exists | ❌ No | ❌ No | `:block/parent`, `:block/page`, `:logseq.property/asset` |
| **`:class`** | Ref | Class entity | ✅ Yes | ❌ No | `:block/tags`, `:logseq.property.class/extends` |
| **`:page`** | Ref | Page entity | ✅ Yes | ❌ No | `:block/alias` |
| **`:property`** | Ref | Property entity | ✅ Yes | ❌ No | `:logseq.property.class/properties`, `:logseq.property.history/property` |

**Note**: `:map` is the only internal type users can create through EDN or API (advanced use only).

---

### Property Type Categories

| Category | Types | Description |
|----------|-------|-------------|
| **Scalar Types** | `:string`, `:raw-number`, `:checkbox`, `:datetime`, `:keyword`, `:map`, `:coll`, `:any` | Values stored directly on entity |
| **Reference Types** | `:default`, `:number`, `:url`, `:date`, `:node`, `:entity`, `:class`, `:page`, `:property` | Values stored as references to other entities |
| **Value Ref Types** | `:default`, `:number`, `:url` | Ref types where value is in `:block/title` or `:logseq.property/value` |
| **Original Value Ref Types** | `:number` | Ref types storing value in `:logseq.property/value` (as native type) |
| **User Ref Types** | `:default`, `:number`, `:url`, `:date`, `:node` | Ref types available to users |
| **Closed Value Types** | `:default`, `:number`, `:url` | Types that support closed/allowed values |
| **Cardinality Types** | `:default`, `:number`, `:url`, `:date`, `:node`, `:class`, `:page`, `:property` | Types that support cardinality many |
| **Default Value Ref Types** | `:default`, `:number`, `:checkbox` | Types that support default values |
| **Text Ref Types** | `:default`, `:url`, `:entity` | Ref types that store text in `:block/title` |

---

### Property Type Validation Functions

**Location**: `deps/db/src/logseq/db/frontend/property/type.cljs:150-193`

| Type | Validation Function | Error Message |
|------|-------------------|---------------|
| `:default` | `text-entity?` | "should be a text block" |
| `:number` | `number-entity?` | "should be a number" |
| `:date` | `date?` | "should be a journal date" |
| `:datetime` | `number?` | "should be a datetime" |
| `:checkbox` | `boolean?` | (Malli built-in) |
| `:url` | `url-entity?` | "should be a URL" |
| `:node` | `node-entity?` | "should be a page/block with tags" |
| `:string` | `string?` | (Malli built-in) |
| `:raw-number` | `number?` | (Malli built-in) |
| `:entity` | `entity?` | "should be an Entity" |
| `:class` | `class-entity?` | "should be a Class" |
| `:property` | `property-entity?` | "should be a Property" |
| `:page` | `page-entity?` | "should be a Page" |
| `:keyword` | `keyword?` | (Malli built-in) |
| `:map` | `map?` | (Malli built-in) |
| `:coll` | `coll?` | (Malli built-in) |
| `:any` | `some?` | (Malli built-in) |

---

## Entity Types

**Total Count**: 13 entity types

**Location**: `deps/db/src/logseq/db/frontend/malli_schema.cljs:509-555`

---

### Page Entity Types

Entity types that represent pages (have `:block/name`).

#### 1. Normal Page

**Dispatch Key**: `:normal-page`

**Schema**: `normal-page`

**Required Attributes**:
- `:block/uuid` - UUID
- `:block/name` - String (lowercase page name)
- `:block/title` - String (display name)
- `:block/created-at` - Integer (timestamp)
- `:block/updated-at` - Integer (timestamp)

**Optional Attributes**:
- `:block/journal-day` - Integer (YYYYMMDD for journal pages)
- `:block/parent` - Integer (page can have parent page)
- `:block/order` - String (fractional index)
- `:block/properties` - Vector of property tuples
- `:block/tags` - Vector [property-map tags options]
- `:block/refs` - Set of integers
- `:block/tx-id` - Integer
- `:block/collapsed?` - Boolean

**Identification**: Has `:block/name` and tagged with `#Page` (or no special tag)

---

#### 2. Class Page

**Dispatch Key**: `:class`

**Schema**: `class-page`

**Required Attributes**:
- `:db/ident` - Qualified keyword (class namespace)
- `:block/uuid` - UUID
- `:block/name` - String
- `:block/title` - String
- `:block/created-at` - Integer
- `:block/updated-at` - Integer
- `:logseq.property.class/extends` - Set of integers (parent classes)

**Optional Attributes**: Same as normal page

**Special Case**: `:logseq.class/Root` doesn't require `:extends`

**Identification**: Tagged with `#Tag`

---

#### 3. Property Page

**Dispatch Key**: `:property`

**Schema**: `property-page` (multi-schema: internal, plugin, or user)

**Required Attributes**:
- `:db/ident` - Qualified keyword
- `:block/uuid` - UUID
- `:block/name` - String
- `:block/title` - String
- `:block/created-at` - Integer
- `:block/updated-at` - Integer
- `:logseq.property/type` - Keyword (property type)

**Optional Attributes**:
- `:db/index` - Boolean
- `:db/valueType` - `:db.type/ref` (for ref types)
- `:db/cardinality` - `:db.cardinality/many` or `:db.cardinality/one`
- `:block/order` - String
- `:logseq.property/classes` - Set of integers
- `:logseq.property/view-context` - Keyword
- Plus standard page/block attributes

**Sub-types**:
- **Internal Property**: `:db/ident` in `logseq.property.*` namespace or is db-attribute
- **Plugin Property**: `:db/ident` starts with `plugin.property.`
- **User Property**: All other qualified keywords with `.property` namespace

**Identification**: Tagged with `#Property`

---

#### 4. Hidden Page

**Dispatch Key**: `:hidden`

**Schema**: `hidden-page`

**Required Attributes**:
- `:block/uuid` - UUID
- `:block/name` - String
- `:block/title` - String
- `:block/created-at` - Integer
- `:block/updated-at` - Integer
- `:logseq.property/hide?` - Must be `true`

**Optional Attributes**:
- `:block/order` - String
- Plus standard page/block attributes

**Identification**: Has `:logseq.property/hide?` = `true`

---

#### 5. Whiteboard Page

**Dispatch Key**: `:normal-page` (same as normal page)

**Schema**: Same as `normal-page`

**Identification**: Tagged with `#Whiteboard` but validates as normal-page

---

### Block Entity Types

Entity types that represent blocks (have `:block/parent` and `:block/page`).

#### 6. Normal Block

**Dispatch Key**: `:block`

**Schema**: `normal-block` or `whiteboard-block`

**Required Attributes**:
- `:block/uuid` - UUID
- `:block/title` - String
- `:block/parent` - Integer
- `:block/order` - String
- `:block/page` - Integer
- `:block/created-at` - Integer
- `:block/updated-at` - Integer

**Optional Attributes**:
- `:block/collapsed?` - Boolean
- `:block/refs` - Set of integers
- `:block/link` - Integer
- `:block/properties` - Vector of property tuples
- `:block/tags` - Vector
- `:block/tx-id` - Integer
- `:logseq.property/created-from-property` - Integer

**Identification**: Has `:block/uuid` but no special attributes/tags

---

#### 7. Whiteboard Block

**Dispatch Key**: `:block`

**Schema**: `whiteboard-block`

**Required Attributes**:
- `:block/uuid` - UUID
- `:block/title` - String
- `:block/parent` - Integer
- `:block/page` - Integer (must be whiteboard page)
- `:block/created-at` - Integer
- `:block/updated-at` - Integer

**Optional Attributes**: Same as normal block

**Note**: Whiteboard blocks don't require `:block/order`

**Identification**: Has `:block/page` that is a whiteboard page

---

#### 8. Property Value Block

**Dispatch Key**: `:property-value-block`

**Schema**: `property-value-block`

**Required Attributes**:
- `:block/uuid` - UUID
- `:logseq.property/value` - String, double, or boolean
- `:logseq.property/created-from-property` - Integer
- `:block/parent` - Integer
- `:block/order` - String
- `:block/page` - Integer
- `:block/created-at` - Integer
- `:block/updated-at` - Integer

**Optional Attributes**: Same as normal block (except `:block/title` is excluded)

**Identification**: Has both `:logseq.property/created-from-property` and `:logseq.property/value`

---

#### 9. Closed Value Block

**Dispatch Key**: `:closed-value-block`

**Schema**: `closed-value-block`

**Required Attributes**:
- `:block/uuid` - UUID
- `:logseq.property/created-from-property` - Integer
- **One of**: `:block/title` OR `:logseq.property/value`
- `:block/parent` - Integer
- `:block/order` - String
- `:block/page` - Integer
- `:block/created-at` - Integer
- `:block/updated-at` - Integer

**Optional Attributes**:
- `:db/ident` - Keyword (for built-in closed values)
- `:block/closed-value-property` - Set of integers
- Plus standard block attributes

**Identification**: Has `:block/closed-value-property`

---

#### 10. Asset Block

**Dispatch Key**: `:asset-block`

**Schema**: `asset-block`

**Required Attributes**:
- `:block/uuid` - UUID
- `:block/title` - String
- `:block/parent` - Integer
- `:block/order` - String
- `:block/page` - Integer
- `:block/created-at` - Integer
- `:block/updated-at` - Integer
- `:logseq.property.asset/type` - String
- `:logseq.property.asset/checksum` - String
- `:logseq.property.asset/size` - Integer

**Optional Attributes**: Same as normal block

**Identification**: Tagged with `#Asset` or has `:logseq.property.asset/type`

---

#### 11. Property History Block

**Dispatch Key**: `:property-history-block`

**Schema**: `property-history-block`

**Required Attributes**:
- `:block/uuid` - UUID
- `:block/created-at` - Integer
- `:logseq.property.history/block` - Integer
- `:logseq.property.history/property` - Integer
- **One of**: `:logseq.property.history/ref-value` OR `:logseq.property.history/scalar-value`

**Optional Attributes**:
- `:block/updated-at` - Integer
- `:block/tx-id` - Integer

**Identification**: Has `:logseq.property.history/block`

---

### Special Entity Types

#### 12. File Block

**Dispatch Key**: `:file-block`

**Schema**: `file-block`

**Required Attributes**:
- `:block/uuid` - UUID
- `:file/content` - String
- `:file/path` - String
- `:file/created-at` - inst? (JavaScript Date)
- `:file/last-modified-at` - inst? (JavaScript Date)

**Optional Attributes**:
- `:file/size` - Integer
- `:block/tx-id` - Integer
- `:block/created-at` - Integer
- `:block/updated-at` - Integer

**Identification**: Has `:file/path`

---

#### 13. Key-Value Entity

**Dispatch Key**: `:db-ident-key-value`

**Schema**: `db-ident-key-val`

**Required Attributes**:
- `:db/ident` - Qualified keyword (in `logseq.*` namespace)
- `:kv/value` - Any value

**Optional Attributes**:
- `:block/tx-id` - Integer

**Identification**: Has `:db/ident` + `:kv/value` (and no `:block/uuid`)

---

#### 14. Empty Placeholder Entity

**Dispatch Key**: `:property-value-placeholder`

**Schema**: `property-value-placeholder`

**Required Attributes**:
- `:db/ident` - Must be `:logseq.property/empty-placeholder`
- `:block/uuid` - UUID

**Optional Attributes**:
- `:block/tx-id` - Integer
- `:block/created-at` - Integer
- `:block/updated-at` - Integer

**Identification**: `:db/ident` = `:logseq.property/empty-placeholder`

---

### Entity Type Dispatch Order

**Location**: `deps/db/src/logseq/db/frontend/malli_schema.cljs:509-539`

The dispatch function checks types in this order (order matters!):

1. `entity-util/property?` → `:property`
2. `entity-util/class?` → `:class`
3. `entity-util/hidden?` → `:hidden`
4. `entity-util/whiteboard?` → `:normal-page`
5. `entity-util/page?` → `:normal-page`
6. `entity-util/asset?` → `:asset-block`
7. `:file/path` present → `:file-block`
8. `:logseq.property.history/block` present → `:property-history-block`
9. `:block/closed-value-property` present → `:closed-value-block`
10. `:logseq.property/created-from-property` + `:logseq.property/value` → `:property-value-block`
11. `:db/ident` = `:logseq.property/empty-placeholder` → `:property-value-placeholder`
12. `:block/uuid` present → `:block`
13. `:db/ident` present → `:db-ident-key-value`

**Critical**: More specific types must be checked before more general types.

---

## Summary Statistics

| Category | Count |
|----------|-------|
| **Total Built-in Properties** | 107 |
| **Public Properties** | 21 |
| **DB Attribute Properties** | 13 |
| **Properties with Closed Values** | 4 (status, priority, pdf/hl-color, repeat/recur-unit) |
| **Total Built-in Classes** | 13 |
| **Page Classes** | 6 (Root, Tag, Property, Page, Journal, Whiteboard) |
| **Block Classes** | 7 (Task, Query, Card, Cards, Asset, Code, Quote, Math, PDF, Template) |
| **Property Types** | 14 (7 user + 10 internal, 3 shared) |
| **User-Facing Property Types** | 7 |
| **Ref Property Types** | 9 |
| **Scalar Property Types** | 8 |
| **Entity Types** | 13 |
| **Page Entity Types** | 5 |
| **Block Entity Types** | 6 |
| **Special Entity Types** | 2 |

---

## Quick Reference Tables

### Property Type Decision Matrix

| Need | Recommended Type |
|------|-----------------|
| Free text | `:default` |
| Number with choices | `:number` + closed values |
| Date only | `:date` |
| Date + time | `:datetime` |
| Yes/No | `:checkbox` |
| Web link | `:url` |
| Reference to tagged object | `:node` |
| Multiple values allowed | Use `:default`, `:number`, `:url`, `:date`, or `:node` + cardinality many |
| Predefined choices | Use `:default`, `:number`, `:url`, or `:checkbox` + closed values |

---

### Class Usage Guide

| Use Case | Recommended Class |
|----------|------------------|
| Custom tag | Create user class extending `:logseq.class/Tag` |
| Task variants | Tag with `:logseq.class/Task` |
| Special page type | Create user class extending `:logseq.class/Page` |
| File attachment | Tag with `:logseq.class/Asset` |
| Flashcard | Tag with `:logseq.class/Card` |
| Query collection | Tag with `:logseq.class/Query` or `:logseq.class/Cards` |
| Template | Tag with `:logseq.class/Template` |

---

### Entity Type Identification Cheat Sheet

```
Has :file/path? → File Block
Has :logseq.property.history/block? → Property History Block
Has :block/closed-value-property? → Closed Value Block
Has :logseq.property/value + :created-from-property? → Property Value Block
:db/ident = :logseq.property/empty-placeholder? → Empty Placeholder

Tagged #Property? → Property Page
Tagged #Tag? → Class Page
Tagged #Asset? → Asset Block
:logseq.property/hide? = true? → Hidden Page

Has :block/uuid?
  → Has :block/name? → Page (Normal/Whiteboard/Journal)
  → Has :block/parent? → Block (Normal/Whiteboard)

Has :db/ident + :kv/value? → Key-Value Entity
```

---

**End of Reference Document**

*For detailed explanations, architectural diagrams, and debugging guidance, see `db-graph-schema-developer-guide.md`*
