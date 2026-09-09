---
title: 'Properties Reference'
status: 🟢 Stable
quadrant: 'Reference'
last-verified: '2026-04-28'
platform: 'Desktop + Mobile'
related-docs:
  - '[Database Graph Schema Reference](../database/schema-overview.md)'
  - '[Stop Task Time Tracking in DB Graphs](../../how-to/tasks/disable-task-time-tracking.md)'
tags: ['properties', 'db-graphs', 'schema', 'reference']
---

This page documents the 109 built-in DB-graph properties currently declared in `deps/db/src/logseq/db/frontend/property.cljs`.

## Scope

- This is a DB-graph reference. It describes the built-in property definitions from the DB schema, not the older file-graph property drawer syntax.
- `Type` comes from `:schema :type`.
- `Card.` is `many` only when `:cardinality :many` is set; otherwise it is `one`.
- `Visibility` is derived from `:public?` and `:hide?`.
- `Definition` only includes code-backed descriptions, backing attributes, defaults, closed values, restricted classes, or queryability.

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
| `:block/alias`                 | Alias                 | `page`       | `many` | public           | Backed by `:block/alias`.                 |
| `:block/tags`                  | Tags                  | `class`      | `many` | public           | Backed by `:block/tags`.                  |
| `:block/parent`                | Node parent           | `entity`     | `one`  | internal, hidden | Backed by `:block/parent`.                |
| `:block/order`                 | Node order            | `string`     | `one`  | internal, hidden | Backed by `:block/order`.                 |
| `:block/collapsed?`            | Node collapsed?       | `checkbox`   | `one`  | internal, hidden | Backed by `:block/collapsed?`.            |
| `:block/page`                  | Node page             | `entity`     | `one`  | internal, hidden | Backed by `:block/page`.                  |
| `:block/refs`                  | Node references       | `entity`     | `many` | internal, hidden | Backed by `:block/refs`.                  |
| `:block/link`                  | Node links to         | `entity`     | `one`  | internal, hidden | Backed by `:block/link`.                  |
| `:block/title`                 | Node title            | `string`     | `one`  | internal, hidden | Backed by `:block/title`.                 |
| `:block/closed-value-property` | Closed value property | `entity`     | `one`  | internal, hidden | Backed by `:block/closed-value-property`. |
| `:block/journal-day`           | Journal date          | `raw-number` | `one`  | internal, hidden | Backed by `:block/journal-day`.           |
| `:block/created-at`            | Node created at       | `datetime`   | `one`  | internal, hidden | Backed by `:block/created-at`.            |
| `:block/updated-at`            | Node updated at       | `datetime`   | `one`  | internal, hidden | Backed by `:block/updated-at`.            |

## Class and Tag Properties

| Ident                                                 | Title                           | Type       | Card.  | Visibility | Definition                                                                                   |
| ----------------------------------------------------- | ------------------------------- | ---------- | ------ | ---------- | -------------------------------------------------------------------------------------------- |
| `:logseq.property.class/extends`                      | Extends                         | `class`    | `many` | public     | This enables tags to inherit properties from other tags                                      |
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
| `:logseq.property/status`    | Status    | `default`  | `one` | public     | Default: `:logseq.property/status.todo`. Closed values: `Backlog`, `Todo`, `Doing`, `In Review`, `Done`, `Canceled`. |
| `:logseq.property/priority`  | Priority  | `default`  | `one` | public     | Closed values: `Low`, `Medium`, `High`, `Urgent`.                                                                    |
| `:logseq.property/deadline`  | Deadline  | `datetime` | `one` | public     | Use it to finish something at a specific date(time).                                                                 |
| `:logseq.property/scheduled` | Scheduled | `datetime` | `one` | public     | Use it to plan something to start at a specific date(time).                                                          |

## Repeating Task Properties

| Ident                                       | Title                       | Type       | Card. | Visibility       | Definition                                                                                                          |
| ------------------------------------------- | --------------------------- | ---------- | ----- | ---------------- | ------------------------------------------------------------------------------------------------------------------- |
| `:logseq.property.repeat/recur-frequency`   | Repeating recur frequency   | `number`   | `one` | internal         | Default: `1`.                                                                                                       |
| `:logseq.property.repeat/recur-unit`        | Repeating recur unit        | `default`  | `one` | internal         | Default: `:logseq.property.repeat/recur-unit.day`. Closed values: `Minute`, `Hour`, `Day`, `Week`, `Month`, `Year`. |
| `:logseq.property.repeat/repeated?`         | Node Repeats?               | `checkbox` | `one` | internal, hidden | Queryable.                                                                                                          |
| `:logseq.property.repeat/temporal-property` | Repeating Temporal Property | `property` | `one` | internal, hidden | -                                                                                                                   |
| `:logseq.property.repeat/checked-property`  | Repeating Checked Property  | `property` | `one` | internal, hidden | -                                                                                                                   |

## View and Linked Reference Properties

| Ident                                           | Title                     | Type       | Card.  | Visibility       | Definition                                                                                             |
| ----------------------------------------------- | ------------------------- | ---------- | ------ | ---------------- | ------------------------------------------------------------------------------------------------------ |
| `:logseq.property.linked-references/includes`   | Included references       | `node`     | `many` | internal, hidden | -                                                                                                      |
| `:logseq.property.linked-references/excludes`   | Excluded references       | `node`     | `many` | internal, hidden | -                                                                                                      |
| `:logseq.property.view/type`                    | View Type                 | `default`  | `one`  | internal, hidden | Default: `:logseq.property.view/type.table`. Closed values: `Table View`, `List View`, `Gallery View`. |
| `:logseq.property.view/feature-type`            | View Feature Type         | `keyword`  | `one`  | internal, hidden | Not queryable.                                                                                         |
| `:logseq.property.view/group-by-property`       | View group by property    | `property` | `one`  | internal, hidden | Queryable.                                                                                             |
| `:logseq.property.view/sort-groups-by-property` | View sort groups by       | `property` | `one`  | internal, hidden | -                                                                                                      |
| `:logseq.property.view/sort-groups-desc?`       | View sort groups DESC     | `checkbox` | `one`  | internal, hidden | Scalar default: `true`.                                                                                |
| `:logseq.property.table/sorting`                | View sorting              | `coll`     | `one`  | internal, hidden | -                                                                                                      |
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
| `:logseq.property.asset/last-visit-page`    | Last visit page       | `raw-number` | `one` | internal, hidden | -                                   |
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
