---
title: '[Reference Subject] Reference'
description: '[Brief description of what this reference covers]'
status: draft # draft | review | stable
lastVerified: '2026-01-29'
logseqVersion: '0.11.x'
platforms: [desktop, web, mobile] # or subset
graphType: [file, db] # or both
sinceVersion: '0.10.0' # When this feature was introduced
relatedDocs:
  tutorials:
    - '[../tutorials/relevant-tutorial.md](../tutorials/relevant-tutorial.md)'
  howTo:
    - '[../how-to/relevant-guide.md](../how-to/relevant-guide.md)'
  explanation:
    - '[../explanation/relevant-concept.md](../explanation/relevant-concept.md)'
---

# [Reference Subject] Reference

> Quick reference for [subject]. For conceptual understanding, see [Explanation](../explanation/concepts/related.md).

## Overview

[1-2 sentences describing what this reference covers]

## Quick Reference

| Item     | Description | Default | Since   |
| -------- | ----------- | ------- | ------- |
| `item-1` | Description | `value` | v0.10.0 |
| `item-2` | Description | `value` | v0.10.0 |

## Detailed Reference

### [Category 1]

#### `item-name`

**Type**: `string | number | boolean`

**Default**: `default-value`

**Since**: v0.10.0

**Description**: [Detailed description of what this item does]

**Example**:

```clojure
;; Example usage
{:item-name "value"}
```

**Notes**:

- [Important note 1]
- [Important note 2]

---

#### `another-item`

[Continue pattern...]

### [Category 2]

[Continue with more categories...]

## Complete Example

```clojure
;; A complete example showing multiple items together
{:item-1 "value"
 :item-2 42
 :item-3 true}
```

## Compatibility Notes

### Platform Differences

| Feature   | Desktop | Web | Mobile |
| --------- | ------- | --- | ------ |
| Feature A | ✅      | ✅  | ❌     |
| Feature B | ✅      | ✅  | ✅     |

### Graph Type Differences

| Feature   | File Graph | DB Graph |
| --------- | ---------- | -------- |
| Feature A | ✅         | ✅       |
| Feature B | ❌         | ✅       |

## Deprecated Items

| Item       | Deprecated In | Replacement | Removal |
| ---------- | ------------- | ----------- | ------- |
| `old-item` | v0.10.0       | `new-item`  | v0.12.0 |

## See Also

- **Learn to use this**: [Related Tutorial](../tutorials/related.md)
- **Solve a problem**: [Related How-To](../how-to/related.md)
- **Understand why**: [Related Explanation](../explanation/related.md)

## Changelog

| Version | Changes          |
| ------- | ---------------- |
| v0.11.0 | Added `new-item` |
| v0.10.0 | Initial release  |

---

_Last updated: 2026-01-29 | [Report an issue](https://github.com/logseq/logseq/issues)_
