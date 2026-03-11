# Logseq Plugin API Changes (October 2024 - October 2025)

> **Last revised:** 2026-03-10

> **For Plugin Developers**: A comprehensive list of all API changes, new features, and improvements made to the Logseq Plugin API over the past 12 months.

---

## Table of Contents

- [Latest Version: 0.0.17 (October 2025)](#latest-version-0017-october-2025)
- [Version 0.0.16](#version-0016)
- [Version 0.0.15](#version-0015)
- [Database Graph API Improvements](#database-graph-api-improvements)
- [Property System APIs](#property-system-apis)
- [Bug Fixes & Improvements](#bug-fixes--improvements)
- [Breaking Changes](#breaking-changes)
- [Migration Guide](#migration-guide)

---

## Latest Version: 0.0.17 (October 2025)

### 🆕 New Database Graph APIs

Added comprehensive APIs for working with tags and properties in database-based graphs:

#### `getAllTags()`

Get all tags (classes) in the current database graph.

```typescript
const tags = await logseq.Editor.getAllTags()
console.log('Available tags:', tags.map(tag => tag.name))
```

- **Added**: October 20, 2025
- **Compatibility**: Database graphs only
- **Returns**: `Promise<PageEntity[] | null>`

#### `getAllProperties()`

Get all properties in the current database graph, including both built-in and user-defined properties.

```typescript
const properties = await logseq.Editor.getAllProperties()
console.log('Total properties:', properties.length)

// Filter for custom properties only
const customProps = properties.filter(p => !p.ident?.startsWith('logseq.property/'))
```

- **Added**: October 20, 2025
- **Compatibility**: Database graphs only
- **Returns**: `Promise<PageEntity[] | null>`

#### `getTagObjects(tag)`

Get all blocks tagged with a specific tag/class.

```typescript
// Get all blocks tagged as "Task"
const taskBlocks = await logseq.Editor.getTagObjects("logseq.class/Task")
console.log(`Found ${taskBlocks.length} task blocks`)

// Get blocks by custom tag name
const articleBlocks = await logseq.Editor.getTagObjects("Article")

// Get blocks by UUID
const blocks = await logseq.Editor.getTagObjects("650f28c4-d3ab-4164-9d1f-d49ec3ba9e56")
```

- **Added**: October 20, 2025 (Enhanced October 9, 2025)
- **Compatibility**: Database graphs only
- **Parameters**: 
  - `tag` can be: Block UUID, DB ident (e.g., "logseq.class/Task"), or tag title
- **Returns**: `Promise<BlockEntity[] | null>`
- **Throws**: Error if the identifier is not a valid tag or doesn't exist

### 🔧 ClojureScript SDK Support

Added comprehensive ClojureScript SDK generation from TypeScript definitions (October 21, 2025):

- New `extract-sdk-schema.js` script to generate SDK schema
- Automatic CLJS wrapper generation for all plugin APIs
- Bean conversion between JavaScript and ClojureScript data structures

### 📖 Documentation Improvements

- Added comprehensive JSDoc documentation for all new database graph APIs
- Improved type definitions across all interfaces
- Added usage examples for complex APIs

---

## Version 0.0.16

### 🆕 New UI & Assets APIs

#### UI APIs

```typescript
// Query element dimensions
const rect = await logseq.UI.queryElementRect('.my-element')
console.log(rect.width, rect.height)

// Query element by ID
const exists = await logseq.UI.queryElementById('my-component')

// Check if UI slot is valid
const isValid = await logseq.UI.checkSlotValid('toolbar')

// Resolve theme CSS property values
const colors = await logseq.UI.resolveThemeCssPropsVals(['--ls-primary-background-color'])
```

**Added APIs:**

- `logseq.UI.queryElementRect(selector: string)` - Get element bounding rectangle
- `logseq.UI.queryElementById(id: string)` - Check if element exists
- `logseq.UI.checkSlotValid(slot: string)` - Validate UI slot
- `logseq.UI.resolveThemeCssPropsVals(props: string | Array<string>)` - Get theme CSS values

#### Assets API

```typescript
// Open asset file in Logseq app
const opened = await logseq.Assets.builtInOpen('/path/to/file.pdf')
```

**Added API:**

- `logseq.Assets.builtInOpen(path: string)` - Open asset files in Logseq

### 🐛 Bug Fixes

- **Fixed**: Plugin shortcuts not working in editing mode ([#10392](https://github.com/logseq/logseq/issues/10392))
- **Fixed**: Commands without keybindings not appearing in keymap ([#10466](https://github.com/logseq/logseq/issues/10466))
- **Fixed**: Plugins breaking global `config.edn` ([#10465](https://github.com/logseq/logseq/issues/10465))

---

## Version 0.0.15

### 🆕 New Features

#### Graph Configuration

```typescript
// Set graph configurations
await logseq.App.setCurrentGraphConfigs({
  'preferred-format': 'markdown',
  'journal/page-title-format': 'yyyy-MM-dd'
})
```

**Added API:**

- `logseq.App.setCurrentGraphConfigs(configs: {})` - Update current graph settings

#### Journal Creation Hook

```typescript
// React when today's journal is created
logseq.App.onTodayJournalCreated(({ title }) => {
  console.log(`Today's journal created: ${title}`)
})
```

**Added Hook:**

- `logseq.App.onTodayJournalCreated` - Hook for daily note creation events

#### Template APIs

Expanded template-related functionality:

- `logseq.App.getTemplate(name)` - Get template by name
- `logseq.App.existTemplate(name)` - Check if template exists
- `logseq.App.createTemplate(blockUUID, name, opts)` - Create new template
- `logseq.App.removeTemplate(name)` - Delete template
- `logseq.App.insertTemplate(blockUUID, name)` - Insert template into block

#### Plugin Marketplace Updates

- Auto-check for plugin updates from the marketplace
- Plugin flag for slash commands

### 🐛 Bug Fixes

- **Fixed**: Select/Input elements in `onMacroRendererSlotted` not functioning ([#8374](https://github.com/logseq/logseq/issues/8374))
- **Fixed**: `getPageBlocksTree` not working with page UUID ([#4920](https://github.com/logseq/logseq/issues/4920))

---

## Database Graph API Improvements

### Property System APIs

Comprehensive property management for database graphs:

#### Property Entity Management

```typescript
// Get a property
const property = await logseq.Editor.getProperty('my-property')

// Create or update property
const propertyId = await logseq.Editor.upsertProperty('my-property', {
  type: 'number',
  cardinality: 'one',
  hide: false,
  public: true
}, { name: 'My Custom Property' })

// Remove property
await logseq.Editor.removeProperty('my-property')
```

**Added APIs (January 2025):**

- `logseq.Editor.getProperty(key)` - Get property entity
- `logseq.Editor.upsertProperty(key, schema, opts)` - Create/update property
- `logseq.Editor.removeProperty(key)` - Delete property

#### Block Property Management

```typescript
// Set block property
await logseq.Editor.upsertBlockProperty(block, 'status', 'In Progress')

// Get specific property value
const status = await logseq.Editor.getBlockProperty(block, 'status')

// Get all block properties
const allProps = await logseq.Editor.getBlockProperties(block)

// Get all page properties
const pageProps = await logseq.Editor.getPageProperties(page)

// Remove block property
await logseq.Editor.removeBlockProperty(block, 'status')
```

**Added APIs (September 2025):**

- `logseq.Editor.upsertBlockProperty(block, key, value)` - Set block property
- `logseq.Editor.getBlockProperty(block, key)` - Get property value
- `logseq.Editor.getBlockProperties(block)` - Get all block properties
- `logseq.Editor.getPageProperties(page)` - Get all page properties (September 5, 2025)
- `logseq.Editor.removeBlockProperty(block, key)` - Remove property

#### Property Type System

Supported property types:

- `default` - Default property
- `number` - Numeric values
- `node` - References to other blocks/pages
- `date` - Date values
- `datetime` - Date and time values
- `checkbox` - Boolean values (auto-inferred, September 27, 2025)
- `url` - URL values (auto-inferred, September 27, 2025)
- `json` - JSON data
- `string` - Text values

**Cardinality:**
- `one` - Single value
- `many` - Multiple values

### File Graph vs Database Graph Separation

**October 7, 2025**: Major refactoring separated file-based and database-specific plugin APIs for better type safety and clarity.

**Important for Plugin Developers:**

- Check graph type before calling DB-specific APIs: `logseq.App.checkCurrentIsDbGraph()`
- DB-only APIs now clearly marked in documentation
- File-based graphs continue to work with traditional APIs

---

## Bug Fixes & Improvements

### October 2025

- **Fixed**: Invalid DB when uploading new plugin (October 24)
- **Fixed**: DB API functions using file graph attributes (October 22)
- **Fixed**: `get_all_pages` API issues (October 16)
- **Fixed**: Plugin API E2E tests (October 13)
- **Fixed**: `get_property` issues (October 9)
- **Fixed**: `prependBlock` functionality (October 7)
- **Fixed**: `get_page_linked_references` (October 7)
- **Fixed**: `insert_batch_block` now supports properties (October 6)
- **Fixed**: `insert_batch_block` target support for page UUID (October 6)
- **Fixed**: `insertBlock` API with properties (October 6)

### September 2025

- **Enhanced**: Auto-infer `:checkbox` and `:url` property types and cardinality (September 27)
- **Enhanced**: Improved error messages for plugin APIs (September 26)
- **Fixed**: API types (September 25)
- **Enhanced**: Added `fullTitle` to block entities, removed temporary properties (September 24)
- **Fixed**: Reuse core.async promise-chan for transact operations (September 24)
- **Fixed**: API tests on file graphs (September 24)

### August 2025

- **Enhanced**: `append_block_in_page` handles current page context better (August 26)
- **Fixed**: Unexpected API failures now return proper 500 errors (August 26)
- **Enhanced**: Added improvements to append operations based on feedback (August 26)
- **Fixed**: Renamed `api-query-token` options to `api-server-token` (August 25)

### Earlier 2025

- **June**: Fixed API types and incorrect property keys for plugin callers (June 20)
- **March**: Enhanced plugin type definitions (March 20)
- **January**: 
  - Added Utils APIs (January 9)
  - Enhanced types across the board (January 9)
  - Added App proxy call improvements (January 6)

---

## Breaking Changes

### ⚠️ None Currently

The Logseq team has maintained backward compatibility throughout all updates. However, be aware:

1. **Database-only APIs**: New database graph APIs (getAllTags, getAllProperties, getTagObjects) will return errors if called on file-based graphs.

2. **Type Changes**: Some property types were refined (e.g., `map` → `json`). Check your property definitions if using advanced features.

3. **API Server Token**: The option `api-query-token` was renamed to `api-server-token` in August 2025.

---

## Migration Guide

### Adopting Database Graph APIs

If your plugin needs to work with both file-based and database graphs:

```typescript
async function getTags() {
  // Check graph type first
  const isDbGraph = await logseq.App.checkCurrentIsDbGraph()
  
  if (isDbGraph) {
    // Use new database API
    return await logseq.Editor.getAllTags()
  } else {
    // Use file-based approach
    // (e.g., query for pages with specific patterns)
    return await logseq.Editor.getAllPages()
      .then(pages => pages.filter(p => p.type === 'class'))
  }
}
```

### Using Property APIs Safely

```typescript
async function setBlockStatus(block, status) {
  try {
    const isDbGraph = await logseq.App.checkCurrentIsDbGraph()
    
    if (isDbGraph) {
      // Database graph: use property API
      await logseq.Editor.upsertBlockProperty(block, 'status', status)
    } else {
      // File graph: update block content with property syntax
      const content = await logseq.Editor.getBlock(block).then(b => b.content)
      const newContent = updatePropertyInContent(content, 'status', status)
      await logseq.Editor.updateBlock(block, newContent)
    }
  } catch (error) {
    console.error('Failed to set status:', error)
  }
}
```

### Updating to Latest UI APIs

```typescript
// Old approach: manual DOM queries (still works)
const element = parent.document.querySelector('.my-element')

// New approach: use UI APIs for better compatibility
const rect = await logseq.UI.queryElementRect('.my-element')
if (rect) {
  console.log('Element found at:', rect.x, rect.y)
}
```

---

## Best Practices for Plugin Development

### 1. Check Graph Type

Always check if the user is on a database or file-based graph before using graph-specific APIs:

```typescript
const isDbGraph = await logseq.App.checkCurrentIsDbGraph()
```

### 2. Handle Errors Gracefully

```typescript
try {
  const tags = await logseq.Editor.getAllTags()
  // Process tags...
} catch (error) {
  if (error.message.includes('database')) {
    logseq.UI.showMsg('This feature requires a database graph', 'warning')
  } else {
    logseq.UI.showMsg('Failed to fetch tags', 'error')
  }
}
```

### 3. Use Type Checking

Take advantage of TypeScript for better development experience:

```typescript
import '@logseq/libs'

// TypeScript will now provide autocomplete and type checking
const tags: PageEntity[] | null = await logseq.Editor.getAllTags()
```

### 4. Stay Updated

Check the [CHANGELOG](./CHANGELOG.md) regularly for new features and updates.

---

## Resources

### Documentation

- **Plugin API Docs**: https://plugins-doc.logseq.com/
- **Alternative Docs**: https://logseq.github.io/plugins/
- **Plugin Samples**: https://github.com/logseq/logseq-plugin-samples

### Package Information

- **npm Package**: `@logseq/libs`
- **Current Version**: 0.2.12
- **Latest API Version**: 0.0.17

### Community

- **Discord**: Join the Logseq Discord for plugin development discussions
- **Forum**: [https://discuss.logseq.com/](https://discuss.logseq.com/)
- **GitHub Issues**: Report bugs or request features

---

## Version History Summary

| Version | Release Date | Key Features |
|---------|-------------|--------------|
| **0.0.17** | October 2025 | Database graph APIs (getAllTags, getAllProperties, getTagObjects), CLJS SDK |
| **0.0.16** | - | UI query APIs, Assets.builtInOpen, keymap fixes |
| **0.0.15** | - | Graph configs API, template APIs, journal creation hook |
| **0.0.14** | - | DataScript query fixes |
| **0.0.13** | - | Block renderer slots, plugin-to-plugin calls, code editor API |
| **0.0.12** | - | Graph configs, favorites, search service hooks |

---

**Last Updated**: October 26, 2025  
**Maintained By**: Logseq Core Team  
**Feedback**: Please report issues or suggestions on [GitHub](https://github.com/logseq/logseq/issues)
