# Changelog (2025-07-04 to 2025-07-30)

## New Feature

### Quick Add System
- Add quick add functionality with auto-editing and notifications ([e259be0c](https://github.com/logseq/logseq/commit/e259be0c5))
- Enable quick add on mobile platforms ([f2bf7e3](https://github.com/logseq/logseq/commit/f2bf7e36e))

### Mobile Application  
- Launch new mobile app with Capacitor framework ([9c83237](https://github.com/logseq/logseq/commit/9c83237ba))
- Add mobile support for RTC graph creation ([053ff9b](https://github.com/logseq/logseq/commit/053ff9b28))

### Graph Management
- Enable uploading local graphs to server ([c86885a](https://github.com/logseq/logseq/commit/c86885a96))

### CLI Tools
- Introduce CLI with graph listing and querying capabilities ([1e64fed](https://github.com/logseq/logseq/commit/1e64fedac))
- Add graph display and version commands ([57b8d72](https://github.com/logseq/logseq/commit/57b8d728f))
- Implement EDN export functionality ([903f59b](https://github.com/logseq/logseq/commit/903f59b71))
- Add search command with highlighting and result limits ([2ad9e29](https://github.com/logseq/logseq/commit/2ad9e29d8))

### Search and Embedding
- Implement hybrid search with semantic capabilities ([0722688](https://github.com/logseq/logseq/commit/0722688fd))
- Add local embedding models with download progress ([133b5e4](https://github.com/logseq/logseq/commit/133b5e497))
- Integrate semantic search results in block search ([b23f9e2](https://github.com/logseq/logseq/commit/b23f9e28a))

### PDF Annotations
- Support importing basic PDF text annotations ([536f39c](https://github.com/logseq/logseq/commit/536f39cbb))
- Add PDF area annotation import ([4c9d063](https://github.com/logseq/logseq/commit/4c9d06332))
- Enable markdown annotation import with child blocks ([3f21c29](https://github.com/logseq/logseq/commit/3f21c297b))

### RTC Support
- Add migration support for RTC graphs ([025156558](https://github.com/logseq/logseq/commit/025156558))

## Enhancement

### Mobile User Experience
- Improve mobile layout with Silk UI components ([8599a1d](https://github.com/logseq/logseq/commit/8599a1d5c))
- Polish mobile colors and visual design ([000ea6f](https://github.com/logseq/logseq/commit/000ea6f59))
- Enhance multiple block dragging on mobile ([01ee6a1](https://github.com/logseq/logseq/commit/01ee6a1c6))
- Add sync button to restart RTC without app restart ([563b185](https://github.com/logseq/logseq/commit/563b185a4))

### User Interface
- Replace loading indicators with skeleton loaders ([791bfe5](https://github.com/logseq/logseq/commit/791bfe51d))
- Show download progress for graph operations ([21bdbce](https://github.com/logseq/logseq/commit/21bdbce6f))
- Improve settings organization by moving actions to "More Actions" ([48da90f](https://github.com/logseq/logseq/commit/48da90f20))
- Disable journal blocks virtualization for better scrolling ([2c47b92](https://github.com/logseq/logseq/commit/2c47b9267))
- Only show page parents in header breadcrumbs ([44f9d8a](https://github.com/logseq/logseq/commit/44f9d8aa0))

### File Handling
- Use current timestamp as default name for pasted files ([015f7a3](https://github.com/logseq/logseq/commit/015f7a3f5))

### RTC Improvements  
- Auto-restart RTC on WebSocket timeout ([dbb20de](https://github.com/logseq/logseq/commit/dbb20dea4))
- Ensure asset synchronization with push-asset-block-updates ([bbe8f89](https://github.com/logseq/logseq/commit/bbe8f890e))

### Search Performance
- Optimize embedding distance threshold for relevant results ([f1d8e52](https://github.com/logseq/logseq/commit/f1d8e526f))
- Adjust batch sizes to reduce resource usage during embedding ([690b92c](https://github.com/logseq/logseq/commit/690b92c97))

## Bug Fix

### Mobile Fixes
- Fix block controls not appearing when collapsed ([d2827ed](https://github.com/logseq/logseq/commit/d2827edcf))
- Resolve editing content not saving when keyboard closes quickly ([4f47856](https://github.com/logseq/logseq/commit/4f4785614))
- Fix floating cursor issues in quick add mode ([879c3ce](https://github.com/logseq/logseq/commit/879c3ce42))
- Correct missing background colors in dark theme ([3146cc7](https://github.com/logseq/logseq/commit/3146cc71b))
- Fix property value popup not closing properly ([33b5da8](https://github.com/logseq/logseq/commit/33b5da8a6))
- Resolve sync indicator not responding to clicks ([6d91359](https://github.com/logseq/logseq/commit/6d9135941))
- Fix tag display issues on mobile ([e98e993](https://github.com/logseq/logseq/commit/e98e99396))
- Correct blank graph name display ([c012941](https://github.com/logseq/logseq/commit/c01294152))

### Graph and Data Issues
- Fix local file path handling in MD and DB graphs ([37736741](https://github.com/logseq/logseq/commit/37736741e))
- Resolve math equation rendering problems ([2c13bce](https://github.com/logseq/logseq/commit/2c13bce35))
- Fix number property deletion in table view ([45709f9](https://github.com/logseq/logseq/commit/45709f943))
- Correct icon property handling in file graphs ([4caa79a](https://github.com/logseq/logseq/commit/4caa79a22))
- Fix backlinks context for file graphs ([4455dfc](https://github.com/logseq/logseq/commit/4455dfca8))

### Editor and Block Management
- Fix cursor position bugs in code blocks ([6e9864a](https://github.com/logseq/logseq/commit/6e9864a3b))
- Resolve editor cursor positioning issues ([585eb41](https://github.com/logseq/logseq/commit/585eb41d4))
- Fix reference preview display in editing mode ([9d7856d](https://github.com/logseq/logseq/commit/9d7856d34))
- Prevent empty block brackets from showing ([5f7213a](https://github.com/logseq/logseq/commit/5f7213a47))
- Fix search functionality for quick-added blocks ([57f29d2](https://github.com/logseq/logseq/commit/57f29d2c6))
- Resolve multiple empty blocks issue in quick add ([d088e3e](https://github.com/logseq/logseq/commit/d088e3e91))

### Property and Table Fixes
- Fix number property value persistence after block changes ([0885de9](https://github.com/logseq/logseq/commit/0885de9c7))
- Correct scrolling in multi-value property choices ([2390f81](https://github.com/logseq/logseq/commit/2390f81da))
- Fix extends value deletion not reverting to Root ([09e60ae](https://github.com/logseq/logseq/commit/09e60ae29))

### Publishing and Graph Loading
- Fix publishing app not loading after mobile changes ([379eb4d](https://github.com/logseq/logseq/commit/379eb4d8f))
- Resolve repo switcher appearing in publishing mode ([adbac92](https://github.com/logseq/logseq/commit/adbac92f4))
- Fix published file graph loading errors ([e0632f0](https://github.com/logseq/logseq/commit/e0632f0e2))
- Correct login button functionality in publishing ([af1283a](https://github.com/logseq/logseq/commit/af1283a5d))

### RTC and Sync Issues
- Fix RTC indicator status updates ([a9a6bce](https://github.com/logseq/logseq/commit/a9a6bcee3))
- Resolve class-block schema issues in RTC ([b4b375f](https://github.com/logseq/logseq/commit/b4b375fe0))
- Fix RTC graph detection logic ([8c1930e](https://github.com/logseq/logseq/commit/8c1930ec7))

### PDF Annotation Fixes
- Fix PDF annotation import file association ([5de565d](https://github.com/logseq/logseq/commit/5de565dcc))
- Resolve annotation images not displaying in PDF viewer ([6b3240f](https://github.com/logseq/logseq/commit/6b3240f4b))
- Fix block references to annotations ([f4a8b94](https://github.com/logseq/logseq/commit/f4a8b9438))

### Search and UI Fixes  
- Fix search result and icon alignment ([14855ed](https://github.com/logseq/logseq/commit/14855ed54))
- Correct search query wrapping with special characters ([eb2e4a7](https://github.com/logseq/logseq/commit/eb2e4a7d2))
- Fix downloading status styling ([4a4b7b7](https://github.com/logseq/logseq/commit/4a4b7b7b6))

### API and Integration Fixes
- Fix API query method and error handling ([98746156](https://github.com/logseq/logseq/commit/98746156e))
- Correct property key case handling in API calls ([e489e93](https://github.com/logseq/logseq/commit/e489e9361))
- Fix database namespace issues in HTTP API ([2d1436a](https://github.com/logseq/logseq/commit/2d1436a89))

## Performance

### Embedding Optimizations
- Skip UI refresh for embedding data transactions ([bf3af0f](https://github.com/logseq/logseq/commit/bf3af0fed))
- Optimize batch embedding sizes for resource efficiency ([690b92c](https://github.com/logseq/logseq/commit/690b92c97))

## Refactoring

### Code Organization
- Move database-specific functions to dedicated namespace ([eabee3f](https://github.com/logseq/logseq/commit/eabee3fe1))
- Refactor journal page lookup for better organization ([bb6ed70](https://github.com/logseq/logseq/commit/bb6ed709e))
- Reorganize sanitize-filename functionality ([6ef1c62](https://github.com/logseq/logseq/commit/6ef1c62e9))

## Chore

### Dependency Management
- Update brace-expansion dependency to 1.1.12 ([3f62b66](https://github.com/logseq/logseq/commit/3f62b6668))
- Update pbkdf2 dependency to 3.1.3 ([18da9d6](https://github.com/logseq/logseq/commit/18da9d6e9))
- Update form-data dependency to 4.0.4 ([9129aac](https://github.com/logseq/logseq/commit/9129aac9c))

### Development Tools
- Add linter configuration for mobile development ([cd65517](https://github.com/logseq/logseq/commit/cd655174a))
- Update GitHub workflow dependencies ([6924223](https://github.com/logseq/logseq/commit/6924223c8))
- Add CLI workflow for linters ([19484b4](https://github.com/logseq/logseq/commit/19484b460))
- Remove unused SSL configuration for iOS development ([73b5cd1](https://github.com/logseq/logseq/commit/73b5cd182))

### Documentation  
- Update database version documentation for mobile and RTC ([8f31892](https://github.com/logseq/logseq/commit/8f318921c))

### Cleanup
- Remove references to nonexistent static files ([623618a](https://github.com/logseq/logseq/commit/623618a04))
- Remove unused e2e test namespace ([f736895](https://github.com/logseq/logseq/commit/f736895b1))
- Remove deprecated RTC events ([654e6d1](https://github.com/logseq/logseq/commit/654e6d1b7))

## Thanks to Contributors

- @charlie
- @dependabot[bot]
- @Gabriel Horner
- @rcmerci
- @Tienson Qin
