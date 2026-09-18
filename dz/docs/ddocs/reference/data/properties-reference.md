---
title: 'Properties Reference'
status: 🟢 Stable
quadrant: 'Reference'
last-verified: '2026-09-18'
verified-against: '5824434a74'
review-status: audited
platform: 'Desktop + Mobile'
related-docs:
  - '[Database Graph Schema Reference](../database/schema-overview.md)'
  - '[Stop Task Time Tracking in DB Graphs](../../how-to/tasks/disable-task-time-tracking.md)'
tags: ['properties', 'db-graphs', 'schema', 'reference']
---

> **Line references relocated 2026-09-18:** every source line reference on this page was
> moved from `be800f1711` to `5824434a74` by locating the same source text in the current
> file. No claim on this page was re-read against the new revision.

This page documents the **118** built-in DB-graph properties currently declared in
`built-in-properties` at `deps/db/src/logseq/db/frontend/property.cljs:20-704` (entries span
lines 43-704). Schema version at the audited commit is `65.33`
(`deps/db/src/logseq/db/frontend/schema.cljs:33`).

## Scope

- This is a DB-graph reference. It describes the built-in property definitions from the DB schema, not the older file-graph property drawer syntax.
- `Type` comes from `:schema :type`.
- `Card.` is `many` only when `:cardinality :many` is set; otherwise it is `one`.
- `Visibility` is derived from `:public?` and `:hide?`.
- `Definition` only includes code-backed descriptions, backing attributes, defaults, closed values, restricted classes, queryability (`:queryable?`), and RTC sync exclusion (`:rtc {:rtc/ignore-attr-when-syncing true}`, `deps/db/src/logseq/db/frontend/property.cljs:14-15`).

### How the count of 118 was derived

Two independent counts over `git show origin/master:deps/db/src/logseq/db/frontend/property.cljs`:

1. Top-level ident keys inside the `defkeywords` form (lines 43-704), matched at their
   fixed 5-space indentation: **118**.
2. The same keys grouped by namespace, summed:
   `logseq.property` 39, `block` 13, `logseq.property.asset` 11, `logseq.property.view` 10,
   `logseq.property.table` 6, `logseq.property.repeat` 6, `logseq.property.pdf` 5,
   `logseq.property.class` 5, `logseq.property.history` 4, `logseq.property.user` 3,
   `logseq.property.recycle` 3, `logseq.property.reaction` 2,
   `logseq.property.linked-references` 2, `logseq.property.fsrs` 2, and one each for
   `logseq.property.sync`, `logseq.property.publish`, `logseq.property.node`,
   `logseq.property.journal`, `logseq.property.comments`, `logseq.property.code`,
   `logseq.property.agent` = **118**.

The tables below list all 118. The previous revision of this page listed 109 and predated
the nine properties added in schema versions 65.26-65.33
(`src/main/frontend/worker/db/migrate.cljs:152-164`).

## Property Definition Metadata

| Ident                                          | Title                            | Type       | Card.  | Visibility       | Definition                                                                                            |
| ---------------------------------------------- | -------------------------------- | ---------- | ------ | ---------------- | ----------------------------------------------------------------------------------------------------- |
| `:logseq.property/type`                        | Property type                    | `keyword`  | `one`  | internal, hidden | -                                                                                                     |
| `:logseq.property/hide?`                       | Hide this property or page       | `checkbox` | `one`  | internal, hidden | -                                                                                                     |
| `:logseq.property/public?`                     | Property public?                 | `checkbox` | `one`  | internal, hidden | -                                                                                                     |
| `:logseq.property/view-context`                | Property view context            | `keyword`  | `one`  | internal, hidden | -                                                                                                     |
| `:logseq.property/ui-position`                 | Property position                | `keyword`  | `one`  | internal, hidden | -                                                                                                     |
| `:logseq.property/classes`                     | Property classes                 | `entity`   | `many` | internal, hidden | -                                                                                                     |
| `:logseq.property/value`                       | Property value                   | `any`      | `one`  | internal, hidden | -                                                                                                     |
| `:logseq.property/description`                 | Description                      | `default`  | `one`  | public           | -                                                                                                     |
| `:logseq.property/default-value`               | Default value                    | `entity`   | `one`  | internal, hidden | -                                                                                                     |
| `:logseq.property/scalar-default-value`        | Non ref type default value       | `any`      | `one`  | internal, hidden | -                                                                                                     |
| `:logseq.property/hide-empty-value`            | Hide empty value                 | `checkbox` | `one`  | public           | Hides a property's value on any node when empty e.g. when a property appears on a node through a tag. |
| `:logseq.property/query`                       | Query                            | `default`  | `one`  | public, hidden   | -                                                                                                     |
| `:logseq.property/created-from-property`       | Created from property            | `entity`   | `one`  | internal, hidden | -                                                                                                     |
| `:logseq.property/built-in?`                   | Built in?                        | `checkbox` | `one`  | internal, hidden | -                                                                                                     |
| `:logseq.property/asset`                       | Asset                            | `entity`   | `one`  | internal, hidden | -                                                                                                     |
| `:logseq.property/ls-type`                     | -                                | `keyword`  | `one`  | internal, hidden | -                                                                                                     |
| `:logseq.property/choice-checkbox-state`       | Choice checkbox state            | `checkbox` | `one`  | internal, hidden | Not queryable.                                                                                        |
| `:logseq.property/choice-classes`              | Choice classes                   | `class`    | `many` | internal, hidden | Not queryable.                                                                                        |
| `:logseq.property/choice-exclusions`           | Choice exclusions                | `node`     | `many` | internal, hidden | Not queryable.                                                                                        |
| `:logseq.property/checkbox-display-properties` | Properties displayed as checkbox | `property` | `many` | internal, hidden | Not queryable.                                                                                        |
| `:logseq.property/enable-history?`             | Enable property history          | `checkbox` | `one`  | public           | Records history anytime a property's value changes on a node.                                         |

## Block-backed Core Properties

| Ident                          | Title                 | Type         | Card.  | Visibility       | Definition                                |
| ------------------------------ | --------------------- | ------------ | ------ | ---------------- | ----------------------------------------- |
| `:block/alias`                 | Alias                 | `page`       | `many` | public           | Backed by `:block/alias`. Queryable.      |
| `:block/tags`                  | Tags                  | `class`      | `many` | public           | Backed by `:block/tags`. Queryable.       |
| `:block/parent`                | Node parent           | `entity`     | `one`  | internal, hidden | Backed by `:block/parent`.                |
| `:block/order`                 | Node order            | `string`     | `one`  | internal, hidden | Backed by `:block/order`.                 |
| `:block/collapsed?`            | Node collapsed?       | `checkbox`   | `one`  | internal, hidden | Backed by `:block/collapsed?`.            |
| `:block/page`                  | Node page             | `entity`     | `one`  | internal, hidden | Backed by `:block/page`.                  |
| `:block/refs`                  | Node references       | `entity`     | `many` | internal, hidden | Backed by `:block/refs`.                  |
| `:block/link`                  | Node links to         | `entity`     | `one`  | internal, hidden | Backed by `:block/link`.                  |
| `:block/title`                 | Node title            | `string`     | `one`  | internal, hidden | Backed by `:block/title`.                 |
| `:block/closed-value-property` | Closed value property | `entity`     | `one`  | internal, hidden | Backed by `:block/closed-value-property`. |
| `:block/journal-day`           | Journal date          | `raw-number` | `one`  | internal, hidden | Backed by `:block/journal-day`. Queryable. |
| `:block/created-at`            | Node created at       | `datetime`   | `one`  | internal, hidden | Backed by `:block/created-at`.            |
| `:block/updated-at`            | Node updated at       | `datetime`   | `one`  | internal, hidden | Backed by `:block/updated-at`.            |

## Class and Tag Properties

| Ident                                                 | Title                           | Type       | Card.  | Visibility | Definition                                                                                   |
| ----------------------------------------------------- | ------------------------------- | ---------- | ------ | ---------- | -------------------------------------------------------------------------------------------- |
| `:logseq.property.class/extends`                      | Extends                         | `class`    | `many` | public     | This enables tags to inherit properties from other tags. Queryable.                          |
| `:logseq.property.class/properties`                   | Tag Properties                  | `property` | `many` | public     | -                                                                                            |
| `:logseq.property.class/bidirectional-property-title` | Bidirectional property title    | `string`   | `one`  | public     | -                                                                                            |
| `:logseq.property.class/enable-bidirectional?`        | Enable bidirectional properties | `checkbox` | `one`  | public     | When enabled, this tag will show reverse nodes that link to the current node via properties. |
| `:logseq.property.class/hide-from-node`               | Hide from Node                  | `checkbox` | `one`  | public     | -                                                                                            |

## Page, Node, and Presentation Properties

| Ident                                      | Title                     | Type       | Card.  | Visibility       | Definition                                                                               |
| ------------------------------------------ | ------------------------- | ---------- | ------ | ---------------- | ---------------------------------------------------------------------------------------- |
| `:logseq.property.node/display-type`       | Node Display Type         | `keyword`  | `one`  | internal, hidden | Queryable.                                                                               |
| `:logseq.property.code/lang`               | Code Mode                 | `string`   | `one`  | internal, hidden | Queryable.                                                                               |
| `:logseq.property/page-tags`               | Page Tags                 | `page`     | `many` | public           | Provides a way for a page to associate to another page i.e. backward compatible tagging. |
| `:logseq.property/background-color`        | Background color          | `default`  | `one`  | internal, hidden | -                                                                                        |
| `:logseq.property/heading`                 | Heading                   | `any`      | `one`  | internal, hidden | Queryable.                                                                               |
| `:logseq.property/order-list-type`         | List type                 | `default`  | `one`  | internal, hidden | -                                                                                        |
| `:logseq.property.journal/title-format`    | Title Format              | `string`   | `one`  | internal         | -                                                                                        |
| `:logseq.property/icon`                    | Icon                      | `map`      | `one`  | internal         | -                                                                                        |
| `:logseq.property/publishing-public?`      | Publishing Public?        | `checkbox` | `one`  | public, hidden   | -                                                                                        |
| `:logseq.property.publish/published-url`   | Published URL             | `url`      | `one`  | public           | -                                                                                        |
| `:logseq.property/exclude-from-graph-view` | Excluded from Graph view? | `checkbox` | `one`  | public, hidden   | -                                                                                        |
| `:logseq.property/used-template`           | Used template             | `node`     | `one`  | internal, hidden | Restricted classes: `#{:logseq.class/Template}`.                                         |
| `:logseq.property/template-applied-to`     | Apply template to tags    | `class`    | `many` | public           | Queryable.                                                                               |

## PDF Annotation Properties

| Ident                           | Title            | Type         | Card. | Visibility       | Definition                                                 |
| ------------------------------- | ---------------- | ------------ | ----- | ---------------- | ---------------------------------------------------------- |
| `:logseq.property.pdf/hl-type`  | Annotation type  | `keyword`    | `one` | internal, hidden | -                                                          |
| `:logseq.property.pdf/hl-color` | Annotation color | `default`    | `one` | internal, hidden | Closed values: `yellow`, `red`, `green`, `blue`, `purple`. |
| `:logseq.property.pdf/hl-page`  | Annotation page  | `raw-number` | `one` | internal, hidden | -                                                          |
| `:logseq.property.pdf/hl-image` | Annotation image | `entity`     | `one` | internal, hidden | -                                                          |
| `:logseq.property.pdf/hl-value` | Annotation data  | `map`        | `one` | internal, hidden | -                                                          |

## Task Properties

| Ident                        | Title     | Type       | Card. | Visibility | Definition                                                                                                           |
| ---------------------------- | --------- | ---------- | ----- | ---------- | -------------------------------------------------------------------------------------------------------------------- |
| `:logseq.property/status`    | Status    | `default`  | `one`  | public     | Default: `:logseq.property/status.todo`. Closed values: `Backlog`, `Todo`, `Doing`, `In Review`, `Done`, `Canceled`. Queryable. |
| `:logseq.property/priority`  | Priority  | `default`  | `one` | public     | Closed values: `Low`, `Medium`, `High`, `Urgent`.                                                                    |
| `:logseq.property/deadline`  | Deadline  | `datetime` | `one`  | public     | Use it to finish something at a specific date(time). Queryable.                                                     |
| `:logseq.property/scheduled` | Scheduled | `datetime` | `one`  | public     | Use it to plan something to start at a specific date(time). Queryable.                                              |
| `:logseq.property/assignee`  | Assignee  | `node`     | `many` | public     | Restricted classes: `#{:logseq.class/Page}`. Queryable. Added in schema `65.30`.                                    |

## Repeating Task Properties

| Ident                                       | Title                       | Type       | Card. | Visibility       | Definition                                                                                                          |
| ------------------------------------------- | --------------------------- | ---------- | ----- | ---------------- | ------------------------------------------------------------------------------------------------------------------- |
| `:logseq.property.repeat/recur-frequency`   | Repeating recur frequency   | `number`   | `one` | internal         | Default: `1`. Queryable.                                                                                            |
| `:logseq.property.repeat/recur-unit`        | Repeating recur unit        | `default`  | `one` | internal         | Default: `:logseq.property.repeat/recur-unit.day`. Closed values: `Minute`, `Hour`, `Day`, `Week`, `Month`, `Year`. Queryable. |
| `:logseq.property.repeat/repeat-type`       | Repeating type              | `default`  | `one` | internal         | Default: `:logseq.property.repeat/repeat-type.double-plus`. Closed values: `Advance from completion`, `Advance from scheduled`, `Advance from scheduled, skip to future`. Queryable. Added in schema `65.26`. |
| `:logseq.property.repeat/repeated?`         | Node Repeats?               | `checkbox` | `one` | internal, hidden | Queryable.                                                                                                          |
| `:logseq.property.repeat/temporal-property` | Repeating Temporal Property | `property` | `one` | internal, hidden | -                                                                                                                   |
| `:logseq.property.repeat/checked-property`  | Repeating Checked Property  | `property` | `one` | internal, hidden | -                                                                                                                   |

## View and Linked Reference Properties

| Ident                                           | Title                     | Type       | Card.  | Visibility       | Definition                                                                                             |
| ----------------------------------------------- | ------------------------- | ---------- | ------ | ---------------- | ------------------------------------------------------------------------------------------------------ |
| `:logseq.property.linked-references/includes`   | Included references       | `node`     | `many` | internal, hidden | -                                                                                                      |
| `:logseq.property.linked-references/excludes`   | Excluded references       | `node`     | `many` | internal, hidden | -                                                                                                      |
| `:logseq.property.view/type`                    | View Type                 | `default`  | `one`  | internal, hidden | Default: `:logseq.property.view/type.table`. Closed values: `Table View`, `List View`, `Gallery View`. Queryable. |
| `:logseq.property.view/feature-type`            | View Feature Type         | `keyword`  | `one`  | internal, hidden | Not queryable.                                                                                         |
| `:logseq.property.view/group-by-property`       | View group by property    | `property` | `one`  | internal, hidden | Queryable.                                                                                             |
| `:logseq.property.view/gallery-asset-property`  | Gallery asset property    | `property` | `one`  | internal, hidden | Added in schema `65.33`.                                                                               |
| `:logseq.property.view/gallery-display-properties` | Gallery display properties | `property` | `many` | internal, hidden | Added in schema `65.33`.                                                                            |
| `:logseq.property.view/gallery-card-size`       | Gallery card size         | `keyword`  | `one`  | internal, hidden | Scalar default: `:default`. Not synced over RTC. Added in schema `65.33`.                              |
| `:logseq.property.view/gallery-card-width`      | Gallery card width        | `raw-number` | `one` | internal, hidden | Not synced over RTC. Added in schema `65.33`.                                                        |
| `:logseq.property.view/gallery-card-height`     | Gallery card height       | `raw-number` | `one` | internal, hidden | Not synced over RTC. Added in schema `65.33`.                                                        |
| `:logseq.property.view/sort-groups-by-property` | View sort groups by       | `property` | `one`  | internal, hidden | Not synced over RTC.                                                                                   |
| `:logseq.property.view/sort-groups-desc?`       | View sort groups DESC     | `checkbox` | `one`  | internal, hidden | Scalar default: `true`. Not synced over RTC.                                                           |
| `:logseq.property.table/sorting`                | View sorting              | `coll`     | `one`  | internal, hidden | Not synced over RTC.                                                                                   |
| `:logseq.property.table/filters`                | View filters              | `map`      | `one`  | internal, hidden | -                                                                                                      |
| `:logseq.property.table/hidden-columns`         | View hidden columns       | `keyword`  | `many` | internal, hidden | -                                                                                                      |
| `:logseq.property.table/ordered-columns`        | View ordered columns      | `coll`     | `one`  | internal, hidden | -                                                                                                      |
| `:logseq.property.table/sized-columns`          | View columns settings     | `map`      | `one`  | internal, hidden | -                                                                                                      |
| `:logseq.property.table/pinned-columns`         | Table view pinned columns | `property` | `many` | internal, hidden | -                                                                                                      |
| `:logseq.property/view-for`                     | This view belongs to      | `node`     | `one`  | internal, hidden | -                                                                                                      |

## Asset Properties

| Ident                                       | Title                 | Type         | Card. | Visibility       | Definition                          |
| ------------------------------------------- | --------------------- | ------------ | ----- | ---------------- | ----------------------------------- |
| `:logseq.property.asset/type`               | File Type             | `string`     | `one` | internal, hidden | Queryable.                          |
| `:logseq.property.asset/external-url`       | External URL          | `string`     | `one` | public           | Queryable.                          |
| `:logseq.property.asset/external-file-name` | External file name    | `string`     | `one` | internal, hidden | Not queryable.                      |
| `:logseq.property.asset/size`               | File Size             | `raw-number` | `one` | internal, hidden | Queryable.                          |
| `:logseq.property.asset/width`              | Image width           | `raw-number` | `one` | internal, hidden | Queryable.                          |
| `:logseq.property.asset/height`             | Image height          | `raw-number` | `one` | internal, hidden | Queryable.                          |
| `:logseq.property.asset/checksum`           | File checksum         | `string`     | `one` | internal, hidden | -                                   |
| `:logseq.property.asset/last-visit-page`    | Last visit page       | `raw-number` | `one` | internal, hidden | Not synced over RTC.                |
| `:logseq.property.asset/remote-metadata`    | File remote metadata  | `map`        | `one` | internal, hidden | Metadata of asset in remote storage |
| `:logseq.property.asset/resize-metadata`    | Asset resize metadata | `map`        | `one` | internal, hidden | -                                   |
| `:logseq.property.asset/align`              | Asset alignment       | `keyword`    | `one` | internal, hidden | Not queryable.                      |

## FSRS Properties

| Ident                         | Title | Type       | Card. | Visibility | Definition |
| ----------------------------- | ----- | ---------- | ----- | ---------- | ---------- |
| `:logseq.property.fsrs/due`   | Due   | `datetime` | `one` | internal   | -          |
| `:logseq.property.fsrs/state` | State | `map`      | `one` | internal   | -          |

## User Profile Properties

| Ident                          | Title       | Type     | Card. | Visibility | Definition |
| ------------------------------ | ----------- | -------- | ----- | ---------- | ---------- |
| `:logseq.property.user/name`   | User Name   | `string` | `one` | public     | -          |
| `:logseq.property.user/email`  | User Email  | `string` | `one` | public     | -          |
| `:logseq.property.user/avatar` | User Avatar | `string` | `one` | public     | -          |

## History, Lifecycle, Recycle, Reaction, and Sync Properties

| Ident                                      | Title                                                          | Type       | Card. | Visibility       | Definition |
| ------------------------------------------ | -------------------------------------------------------------- | ---------- | ----- | ---------------- | ---------- |
| `:logseq.property.history/block`           | History block                                                  | `entity`   | `one` | internal, hidden | -          |
| `:logseq.property.history/property`        | History property                                               | `property` | `one` | internal, hidden | -          |
| `:logseq.property.history/ref-value`       | History value                                                  | `entity`   | `one` | internal, hidden | -          |
| `:logseq.property.history/scalar-value`    | History scalar value                                           | `any`      | `one` | internal, hidden | -          |
| `:logseq.property/created-by-ref`          | Node created by                                                | `entity`   | `one` | internal, hidden | Queryable. |
| `:logseq.property/deleted-at`              | Deleted at                                                     | `datetime` | `one` | internal, hidden | -          |
| `:logseq.property/deleted-by-ref`          | Deleted by                                                     | `entity`   | `one` | internal, hidden | -          |
| `:logseq.property.recycle/original-parent` | Recycle original parent                                        | `node`     | `one` | internal, hidden | -          |
| `:logseq.property.recycle/original-page`   | Recycle original page                                          | `node`     | `one` | internal, hidden | -          |
| `:logseq.property.recycle/original-order`  | Recycle original order                                         | `string`   | `one` | internal, hidden | -          |
| `:logseq.property.reaction/emoji-id`       | Reaction emoji                                                 | `string`   | `one` | internal, hidden | -          |
| `:logseq.property.reaction/target`         | Reaction target                                                | `node`     | `one` | internal, hidden | -          |
| `:logseq.property.sync/large-title-object` | Reference to large block title stored in remote object storage | `map`      | `one` | internal, hidden | -          |

## Comment and Agent Properties

| Ident                              | Title             | Type     | Card.  | Visibility       | Definition                                                                       |
| ---------------------------------- | ----------------- | -------- | ------ | ---------------- | -------------------------------------------------------------------------------- |
| `:logseq.property.comments/blocks` | Commented blocks  | `node`   | `many` | internal, hidden | Backs `:logseq.class/Comments`. Added in schema `65.27`.                          |
| `:logseq.property.agent/session-id`| Agent Session ID  | `string` | `one`  | public, hidden   | Stores the AgentBridge session ID for a routed task. Added in schema `65.31`.     |

## Findings: New Capabilities

Nine built-in properties existed in `origin/master` (`be800f1711`) but were absent from the
previous revision of this page. All nine were introduced between schema `65.26` and `65.33`
(`src/main/frontend/worker/db/migrate.cljs:152-164`) and are now listed in the tables above.

| Ident                                              | Source                                | Schema | Notes                                                                               |
| -------------------------------------------------- | ------------------------------------- | ------ | ----------------------------------------------------------------------------------- |
| `:logseq.property.repeat/repeat-type`              | `property.cljs:381-393`               | 65.26  | Closed-value repeat strategy; defaults to `repeat-type.double-plus`.                  |
| `:logseq.property.comments/blocks`                 | `property.cljs:262-266`               | 65.27  | Ships with the new `:logseq.class/Comments` class (`class.cljs:43-47`).               |
| `:logseq.property/assignee`                        | `property.cljs:401-408`               | 65.30  | First public, many-cardinality task property scoped to `:logseq.class/Page`.          |
| `:logseq.property.agent/session-id`                | `property.cljs:685-690`               | 65.31  | AgentBridge session routing.                                                          |
| `:logseq.property.view/gallery-asset-property`     | `property.cljs:463-467`               | 65.33  | Gallery view configuration.                                                           |
| `:logseq.property.view/gallery-display-properties` | `property.cljs:469-474`               | 65.33  | Gallery view configuration.                                                           |
| `:logseq.property.view/gallery-card-size`          | `property.cljs:476-482`               | 65.33  | Scalar default `:default`; RTC-ignored.                                               |
| `:logseq.property.view/gallery-card-width`         | `property.cljs:484-489`               | 65.33  | RTC-ignored.                                                                          |
| `:logseq.property.view/gallery-card-height`        | `property.cljs:491-496`               | 65.33  | RTC-ignored.                                                                          |

Two related classes also arrived alongside these properties and are not property definitions,
but affect which properties appear on a node: `:logseq.class/Comments` and
`:logseq.class/Comment` (`deps/db/src/logseq/db/frontend/class.cljs:43-51`), bringing the
built-in class count to 18.

### Properties removed from the schema

> **Removed:** `:block/pre-block?`, `:logseq.property.embedding/hnsw-label` and
> `:logseq.property.embedding/hnsw-label-updated-at` were deleted in schema `65.25`
> (`src/main/frontend/worker/db/migrate.cljs:149-151`, a `:delete-properties` migration).
> They were never listed on this page; they are noted here so the removal is not
> re-discovered as a gap. `:logseq.property.asset/external-src` (added in `65.14`,
> `migrate.cljs:134`) no longer exists in `built-in-properties` either; the current asset URL property is
> `:logseq.property.asset/external-url` (`property.cljs:559-563`).
