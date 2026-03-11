# Plugin API Documentation Task List

> **Last revised:** 2026-03-10

> **Goal**: Improve TypeDoc documentation coverage for Logseq Plugin APIs to enhance developer experience and ensure all methods appear properly on https://plugins-doc.logseq.com

## Current Status (Updated: October 26, 2025)

| Interface    | Documented | Total | Coverage | Priority  | Status      |
| ------------ | ---------- | ----- | -------- | --------- | ----------- |
| IEditorProxy | 54         | 54    | 100%     | 🔴 HIGH   | ✅ COMPLETE |
| IAppProxy    | 51         | 51    | 100%     | 🔴 HIGH   | ✅ COMPLETE |
| IUIProxy     | 6          | 6     | 100%     | 🟡 MEDIUM | ✅ COMPLETE |
| IGitProxy    | 3          | 3     | 100%     | 🟢 LOW    | ✅ COMPLETE |
| IUtilsProxy  | 2          | 2     | 100%     | 🟢 LOW    | ✅ COMPLETE |
| IDBProxy     | 4          | 4     | 100%     | ✅ DONE   | ✅ COMPLETE |
| IAssetsProxy | 4          | 4     | 100%     | ✅ DONE   | ✅ COMPLETE |

**Total Progress**: 124/124 methods documented (100%) ✅

---

## Documentation Completed

All phases have been successfully completed:

### ✅ Phase 1: Critical Database & Property APIs (COMPLETE)

All database graph and property APIs fully documented with comprehensive examples.

### ✅ Phase 2: Core Block & Page Operations (COMPLETE)

All block manipulation, page operations, and navigation methods fully documented.

### ✅ Phase 3: App State & Configuration (COMPLETE)

All app state, configuration, command registration, and UI control methods fully documented.

### ✅ Phase 4: UI & User Interface (COMPLETE)

All UI proxy methods fully documented with examples.

### ✅ Phase 5: Git Integration (COMPLETE)

All Git-related methods fully documented.

### ✅ Phase 6: Utilities (COMPLETE)

All utility methods fully documented.

---

## Documentation Summary

### What Was Documented

1. **IEditorProxy** - 38 previously undocumented methods:

   - Block editing state methods (checkEditing, insertAtEditingCursor, etc.)
   - Block CRUD operations (insertBlock, updateBlock, removeBlock, getBlock)
   - Page operations (getPage, createPage, deletePage, renamePage, getAllPages)
   - Block navigation (getPreviousSiblingBlock, getNextSiblingBlock, moveBlock)
   - Property system (getProperty, upsertProperty, upsertBlockProperty, etc.)
   - Page/block content methods (prependBlockInPage, appendBlockInPage)
   - Namespace operations (getPagesFromNamespace, getPagesTreeFromNamespace)
   - Utility methods (newBlockUUID, isPageBlock, editBlock, selectBlock)

2. **IAppProxy** - 42 previously undocumented methods:

   - App information (getInfo, getUserInfo, getUserConfigs)
   - Search service registration (registerSearchService, unregisterSearchService)
   - Command registration (registerCommand, registerCommandPalette, registerCommandShortcut)
   - External commands (invokeExternalCommand, invokeExternalPlugin, getExternalPlugin)
   - State management (getStateFromStore, setStateFromStore)
   - Native operations (relaunch, quit, openExternalLink)
   - Graph methods (getCurrentGraph, checkCurrentIsDbGraph, getCurrentGraphConfigs, etc.)
   - Navigation (pushState, replaceState)
   - Templates (getTemplate, createTemplate, insertTemplate, etc.)
   - UI controls (setZoomFactor, setFullScreen, sidebar controls)
   - UI registration (registerUIItem, registerPageMenuItem)
   - Event hooks (onCurrentGraphChanged, onGraphAfterIndexed, onThemeModeChanged, etc.)

3. **IUIProxy** - 6 methods:

   - showMsg - Display notifications
   - closeMsg - Dismiss notifications
   - queryElementRect - Get element dimensions
   - queryElementById - Check element existence
   - checkSlotValid - Validate UI slots
   - resolveThemeCssPropsVals - Get theme CSS variables

4. **IGitProxy** - 2 previously undocumented methods:

   - loadIgnoreFile - Load .gitignore content
   - saveIgnoreFile - Save .gitignore content

5. **IUtilsProxy** - 1 method:
   - escape - String escaping utility

---

## Documentation Standards Applied

Each documented method includes:

✅ Clear, concise description of functionality  
✅ Database vs file-based compatibility notes where applicable  
✅ `@param` tags with types and descriptions  
✅ `@returns` tag with return type details  
✅ `@added 0.0.17` version tag for newly documented APIs  
✅ `@example` blocks (minimum 1-2 per method)  
✅ `@throws` tags where applicable  
✅ `@link` tags to external resources when relevant  
✅ Practical, real-world usage examples  
✅ Edge case documentation

---

## Next Steps

### Immediate Actions

- [ ] Review generated documentation locally
- [ ] Coordinate with team to publish to <https://plugins-doc.logseq.com>
- [ ] Update `libs/CHANGELOG.md` for version 0.0.17
- [ ] Consider updating `libs/package.json` version when ready to publish

### Future Improvements

- [ ] Add more advanced examples for complex workflows
- [ ] Create tutorial documents combining multiple APIs
- [ ] Add troubleshooting section for common issues
- [ ] Consider video tutorials for key API patterns

---

## Notes

- **File Updated**: `/libs/src/LSPlugin.ts` (~3,900 lines)
- **Documentation Added**: ~2,500 lines of JSDoc comments
- **TypeScript Compilation**: ✅ All checks pass, no errors
- **Coverage Achieved**: 100% of public API methods
- **Estimated Total Effort**: ~8 hours

---

**Completed**: October 26, 2025  
**Status**: ✅ All phases complete, ready for publication

### IEditorProxy - Block Manipulation

- [ ] `checkEditing()` - Check if a block is being edited
- [ ] `insertAtEditingCursor(content)` - Insert content at cursor position
- [ ] `restoreEditingCursor()` - Restore editing cursor position
- [ ] `exitEditingMode(selectBlock?)` - Exit block editing mode
- [ ] `getEditingCursorPosition()` - Get current cursor position in editing block
- [ ] `getEditingBlockContent()` - Get content of currently editing block
- [ ] `getCurrentPage()` - Get current page entity
- [ ] `getCurrentBlock()` - Get current block entity
- [ ] `getSelectedBlocks()` - Get currently selected blocks
- [ ] `clearSelectedBlocks()` - Clear block selection
- [ ] `insertBlock(srcBlock, content, opts)` - Insert a new block
- [ ] `insertBatchBlock(srcBlock, batch, opts)` - Insert multiple blocks at once
- [ ] `updateBlock(srcBlock, content, opts)` - Update block content
- [ ] `removeBlock(srcBlock)` - Delete a block
- [ ] `getBlock(srcBlock, opts)` - Get block entity by identity
- [ ] `setBlockCollapsed(uuid, opts)` - Collapse/expand a block
- [ ] `moveBlock(srcBlock, targetBlock, opts)` - Move block to new location
- [ ] `editBlock(srcBlock, opts)` - Enter edit mode for a block
- [ ] `selectBlock(srcBlock)` - Select a block
- [ ] `getPreviousSiblingBlock(srcBlock)` - Get previous sibling block
- [ ] `getNextSiblingBlock(srcBlock)` - Get next sibling block

### IEditorProxy - Page Operations

- [ ] `getPage(srcPage, opts)` - Get page entity
- [ ] `createPage(pageName, properties, opts)` - Create a new page
- [ ] `createJournalPage(date)` - Create a journal page
- [ ] `deletePage(pageName)` - Delete a page
- [ ] `renamePage(oldName, newName)` - Rename a page
- [ ] `getAllPages(repo?)` - Get all pages in graph
- [ ] `prependBlockInPage(page, content, opts)` - Add block at start of page
- [ ] `appendBlockInPage(page, content, opts)` - Add block at end of page
- [ ] `scrollToBlockInPage(pageName, blockId, opts)` - Scroll to specific block
- [ ] `openInRightSidebar(id)` - Open block/page in right sidebar

### IEditorProxy - Namespace Operations

- [ ] `getPagesFromNamespace(namespace)` - Get pages in namespace
- [ ] `getPagesTreeFromNamespace(namespace)` - Get namespace pages as tree

### IEditorProxy - Utility Methods

- [ ] `newBlockUUID()` - Generate new block UUID
- [ ] `isPageBlock(block)` - Check if entity is a page block
- [ ] `saveFocusedCodeEditorContent()` - Save code editor content

**Estimated effort**: 8-10 hours

---

## Phase 3: App State & Configuration (Priority: HIGH)

### IAppProxy - App Information & State

- [ ] `getUserInfo()` - Get current user information
- [ ] `getUserConfigs()` - Get user configurations
- [ ] `getCurrentGraph()` - Get current graph info
- [ ] `getCurrentGraphConfigs()` - Get current graph configs
- [ ] `setCurrentGraphConfigs(configs)` - Update graph configs
- [ ] `getCurrentGraphFavorites()` - Get favorite pages list
- [ ] `getCurrentGraphRecent()` - Get recent pages list
- [ ] `getStateFromStore(path)` - Get state value from store
- [ ] `setStateFromStore(path, value)` - Set state value in store

### IAppProxy - Commands & Shortcuts

- [ ] `registerCommand(type, opts, action)` - Register a command
- [ ] `registerCommandPalette(opts, action)` - Register command palette item
- [ ] `registerCommandShortcut(keybinding, action, opts)` - Register keyboard shortcut
- [ ] `unregisterCommandShortcut(keybinding)` - Unregister keyboard shortcut

### IAppProxy - UI & Layout

- [ ] `setLeftSidebarVisible(visible)` - Show/hide left sidebar
- [ ] `setRightSidebarVisible(visible)` - Show/hide right sidebar
- [ ] `clearRightSidebarBlocks(opts)` - Clear right sidebar content
- [ ] `pushState(state, opts)` - Navigate to state
- [ ] `replaceState(state, opts)` - Replace current state

### IAppProxy - Search

- [ ] `registerSearchService(service)` - Register custom search service
- [ ] `unregisterSearchService()` - Unregister search service

### IAppProxy - Theme & Display

- [ ] `setZoomFactor(factor)` - Set app zoom level
- [ ] `setFullScreen(fullscreen)` - Toggle fullscreen mode
- [ ] `relaunch()` - Restart the app
- [ ] `quit()` - Quit the app

### IAppProxy - Plugin Interaction

- [ ] `invokeExternalPlugin(pluginId, method)` - Call another plugin's method
- [ ] `queryElementById(id)` - Query DOM element by ID
- [ ] `queryElementRect(selector)` - Get element dimensions

### IAppProxy - Hooks

- [ ] `onCurrentGraphChanged(callback)` - Hook for graph changes
- [ ] `onTodayJournalCreated(callback)` - Hook for daily note creation
- [ ] `onRouteChanged(callback)` - Hook for route/navigation changes
- [ ] `onSidebarVisibleChanged(callback)` - Hook for sidebar visibility
- [ ] `onBlockRendererSlotted(callback)` - Hook for block renderer slots
- [ ] `onMacroRendererSlotted(callback)` - Hook for macro renderer slots

**Estimated effort**: 10-12 hours

---

## Phase 4: UI & User Interface (Priority: MEDIUM)

### IUIProxy - All Methods

- [ ] `showMsg(content, status, opts)` - Show notification message
- [ ] `closeMsg(key)` - Close a notification message
- [ ] `queryElementById(id)` - Query element by ID
- [ ] `queryElementRect(selector)` - Get element bounding rect
- [ ] `checkSlotValid(slot)` - Check if UI slot is valid
- [ ] `resolveThemeCssPropsVals(props)` - Resolve theme CSS property values

**Estimated effort**: 2-3 hours

---

## Phase 5: Git Integration (Priority: LOW)

### IGitProxy - Version Control

- [ ] `execCommand(args)` - Execute git command
- [ ] `loadIgnoreFile()` - Load .gitignore file

**Estimated effort**: 1-2 hours

---

## Phase 6: Utilities (Priority: LOW)

### IUtilsProxy - Utility Methods

- [ ] `escape(str)` - Escape string for safe use

**Estimated effort**: 30 minutes

---

## Documentation Standards

Each method should include:

1. **Description** - Clear, concise explanation of what the method does
2. **Compatibility Notes** - Specify if DB-only, file-based-only, or universal
3. **`@param` tags** - Document each parameter with type and description
4. **`@returns` tag** - Document return type and what it represents
5. **`@throws` tag** - Document any errors the method might throw
6. **`@example` blocks** - Provide at least 1-2 practical usage examples
7. **`@added` tag** - Version when the API was introduced (if known)
8. **`@link` tags** - Link to related samples or documentation when applicable

### Example Template:

````typescript
/**
 * Brief one-line description
 *
 * Longer description explaining the method's purpose, behavior,
 * and any important details developers should know.
 *
 * Only works for database-based graphs. (if applicable)
 *
 * @added 0.0.X
 * @param paramName - Description of parameter
 * @param optionalParam - Description of optional parameter
 * @returns Description of return value
 * @throws Error description if applicable
 *
 * @example
 * ```ts
 * // Example usage
 * const result = await logseq.Editor.methodName(arg1, arg2)
 * console.log(result)
 * ```
 *
 * @example
 * ```ts
 * // Advanced example
 * const result = await logseq.Editor.methodName(arg1, { option: true })
 * ```
 *
 * @link https://github.com/logseq/logseq-plugin-samples/tree/master/example (if available)
 */
methodName: (param: Type) => Promise<ReturnType>
````

---

## Testing Checklist

After documentation updates:

- [ ] Run `npm run build:docs` in `libs/` directory
- [ ] Verify TypeScript compiles without errors
- [ ] Check generated documentation in `libs/docs/` (if output locally)
- [ ] Verify JSDoc renders correctly in IDE tooltips
- [ ] Review for consistency in tone and formatting
- [ ] Check all code examples are syntactically correct
- [ ] Ensure `@param` types match actual TypeScript types

---

## Deliverables

- [ ] Update `libs/src/LSPlugin.ts` with comprehensive JSDoc
- [ ] Update `libs/CHANGELOG.md` for version 0.0.17
- [ ] Update `libs/package.json` version to 0.0.17 (when ready to publish)
- [ ] Generate new TypeDoc documentation
- [ ] Coordinate with team to deploy updated docs to https://plugins-doc.logseq.com

---

## Notes

- **Current file**: `/libs/src/LSPlugin.ts` (~3,900 lines including JSDoc)
- **Reference implementations**: Check `/src/main/logseq/api/` for ClojureScript implementations
- **Test files**: `/clj-e2e/test/logseq/e2e/plugins_basic_test.clj` for usage examples
- **Existing samples**: https://github.com/logseq/logseq-plugin-samples

---

**Last Updated**: October 26, 2025
**Status**: Planning Phase
**Total Estimated Effort**: 25-32 hours
