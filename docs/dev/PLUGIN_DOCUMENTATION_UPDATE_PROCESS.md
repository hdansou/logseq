# Plugin Documentation Update Process

> **Purpose**: This guide documents the complete process for updating and publishing Logseq Plugin API documentation to https://plugins-doc.logseq.com

---

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Documentation Workflow](#documentation-workflow)
4. [Adding JSDoc to APIs](#adding-jsdoc-to-apis)
5. [Building Documentation](#building-documentation)
6. [Testing Documentation](#testing-documentation)
7. [Publishing Documentation](#publishing-documentation)
8. [Troubleshooting](#troubleshooting)

---

## Overview

The Logseq plugin documentation is generated from TypeScript definition files using **TypeDoc**. The documentation source is in `/libs/src/LSPlugin.ts` and gets published to https://plugins-doc.logseq.com.

### Key Components

- **Source**: `/libs/src/LSPlugin.ts` - TypeScript interface definitions with JSDoc comments
- **Build Tool**: TypeDoc with `typedoc-plugin-lsp-docs` plugin
- **Output**: Generated documentation site
- **Package**: `@logseq/libs` npm package
- **Version**: Currently at v0.2.3 (next: v0.0.17 for new DB APIs)

---

## Prerequisites

### Required Tools

```bash
# Node.js and npm
node --version  # Should be 14.x or higher
npm --version   # Should be 6.x or higher

# Install dependencies
cd libs/
npm install
```

### Understanding the Architecture

```
libs/
├── src/
│   ├── LSPlugin.ts          # Main API definitions (THIS IS WHERE YOU ADD DOCS)
│   ├── LSPlugin.user.ts     # User-facing plugin wrapper
│   ├── LSPlugin.core.ts     # Core implementation
│   └── modules/             # Plugin modules
├── scripts/
│   └── extract-sdk-schema.js # Schema extraction for CLJS SDK
├── package.json             # Package configuration
├── CHANGELOG.md             # Version history
└── tsconfig.json            # TypeScript configuration
```

---

## Documentation Workflow

### Step 1: Identify APIs to Document

Check current documentation coverage:

```bash
cd /Users/dzu/Projects/src/github.com/logseq/libs/src

# Run coverage analysis
awk '/^export interface IEditorProxy/,/^}/' LSPlugin.ts | grep -E "^  [a-zA-Z_]" | wc -l  # Total methods
awk '/^export interface IEditorProxy/,/^}/' LSPlugin.ts | grep -c "  /\*\*"              # Documented methods
```

Or refer to the task list: [`PLUGIN_API_DOCUMENTATION_TODO.md`](./PLUGIN_API_DOCUMENTATION_TODO.md)

### Step 2: Research the Implementation

Before documenting an API, understand what it does:

```bash
# Find the ClojureScript implementation
cd /Users/dzu/Projects/src/github.com/logseq
grep -r "get-all-tags" src/main/logseq/api/

# Check usage in tests
grep -r "getAllTags" clj-e2e/test/
```

Key files to check:
- `/src/main/logseq/api.cljs` - Main API exports
- `/src/main/logseq/api/db_based.cljs` - Database graph APIs
- `/src/main/logseq/api/file_based.cljs` - File-based graph APIs
- `/src/main/logseq/api/editor.cljs` - Editor APIs
- `/clj-e2e/test/logseq/e2e/plugins_basic_test.clj` - E2E tests

### Step 3: Add JSDoc Documentation

Edit `/libs/src/LSPlugin.ts` and add comprehensive JSDoc comments.

---

## Adding JSDoc to APIs

### JSDoc Template

```typescript
/**
 * Brief one-line description of what the method does
 * 
 * More detailed explanation of the method's behavior, edge cases,
 * and any important information developers need to know.
 * 
 * Only works for database-based graphs. (if applicable)
 * Only works for file-based graphs. (if applicable)
 * 
 * @added 0.0.X - Version when this API was introduced
 * @param paramName - Description of what this parameter does
 * @param optionalParam - Description of optional parameter (optional)
 * @returns Description of what the method returns and what it means
 * @throws Error description if the method can throw errors
 * 
 * @example
 * ```ts
 * // Simple usage example
 * const result = await logseq.Editor.methodName(arg1)
 * console.log(result)
 * ```
 * 
 * @example
 * ```ts
 * // Advanced usage with options
 * const result = await logseq.Editor.methodName(arg1, {
 *   option1: true,
 *   option2: 'value'
 * })
 * ```
 * 
 * @link https://github.com/logseq/logseq-plugin-samples/tree/master/example
 */
methodName: (param: Type, optionalParam?: Type) => Promise<ReturnType>
```

### Real Example: getAllTags

```typescript
/**
 * Get all tags (classes) in the current database graph
 * 
 * Only works for database-based graphs. Returns all tag entities excluding the root class.
 * 
 * @added 0.0.17
 * @returns Promise that resolves to an array of tag/class page entities, or null if the operation fails
 * 
 * @example
 * ```ts
 * const tags = await logseq.Editor.getAllTags()
 * console.log('Available tags:', tags.map(tag => tag.name))
 * ```
 */
getAllTags: () => Promise<PageEntity[] | null>
```

### JSDoc Best Practices

1. **Be Clear and Concise**
   - Start with a brief one-liner
   - Expand with details only if needed
   - Use simple language

2. **Specify Graph Compatibility**
   - "Only works for database-based graphs"
   - "Only works for file-based graphs"
   - "Works for both graph types"

3. **Document Parameters Thoroughly**
   - Explain what the parameter accepts
   - Give examples of valid values
   - Note if it's optional

4. **Provide Practical Examples**
   - Show real-world usage
   - Include common patterns
   - Demonstrate edge cases when relevant

5. **Link to Resources**
   - Plugin samples repository
   - Related documentation
   - Issue discussions

6. **Version Tags**
   - Use `@added X.X.X` for new APIs
   - Use `@deprecated` for deprecated APIs with alternatives

---

## Building Documentation

### Local Build

Generate TypeScript declarations and JSON schema:

```bash
cd /Users/dzu/Projects/src/github.com/logseq/libs

# Build TypeScript declarations
npm run build
# Output: dist/LSPlugin.d.ts, dist/LSPlugin.user.d.ts

# Generate SDK schema (for ClojureScript SDK)
npm run generate:schema
# Output: dist/logseq-sdk-schema.json

# Generate documentation (if configured)
npm run build:docs
# Output: docs/ directory (may be gitignored)
```

### Verify TypeScript Compilation

```bash
cd /Users/dzu/Projects/src/github.com/logseq/libs

# Check for TypeScript errors
npx tsc --noEmit

# Should output: No errors
```

### Check Generated Schema

```bash
cd /Users/dzu/Projects/src/github.com/logseq/libs

# View generated schema
cat dist/logseq-sdk-schema.json | jq '.interfaces.IEditorProxy.methods[] | select(.name == "getAllTags")'
```

Expected output should include your JSDoc documentation.

---

## Testing Documentation

### 1. Verify in IDE

Open a plugin project and check IntelliSense:

```typescript
// In a plugin project
logseq.Editor.getAllTags() // Hover should show your JSDoc
```

### 2. Test in VS Code

1. Open a plugin project in VS Code
2. Type `logseq.Editor.`
3. Autocomplete should show methods with descriptions
4. Hover over methods to see full JSDoc

### 3. Verify Code Examples

Ensure all code examples are syntactically correct:

```bash
# Extract and test examples
cd /Users/dzu/Projects/src/github.com/logseq/libs

# Create test file from examples
cat src/LSPlugin.ts | grep -A 20 "@example" > /tmp/test-examples.ts

# Check syntax (may need adjustment)
npx tsc --noEmit /tmp/test-examples.ts
```

### 4. Test with TypeDoc Locally

If TypeDoc is configured to output locally:

```bash
cd /Users/dzu/Projects/src/github.com/logseq/libs

npm run build:docs

# Open the generated docs
open docs/index.html  # macOS
# or
xdg-open docs/index.html  # Linux
```

---

## Publishing Documentation

### Update Version Numbers

#### 1. Update CHANGELOG.md

```bash
cd /Users/dzu/Projects/src/github.com/logseq/libs
```

Edit `CHANGELOG.md`:

```markdown
## [0.0.17]
### Added
- Support database graph APIs for querying tags and properties:
  - `logseq.Editor.getAllTags()` - Get all tags in database graph
  - `logseq.Editor.getAllProperties()` - Get all properties
  - `logseq.Editor.getTagObjects(tag)` - Get blocks tagged with specific tag
- Added comprehensive JSDoc documentation for new database APIs

## [0.0.16]
...
```

#### 2. Update package.json Version

```bash
cd /Users/dzu/Projects/src/github.com/logseq/libs
```

Edit `package.json`:

```json
{
  "name": "@logseq/libs",
  "version": "0.0.17",  // Increment version
  ...
}
```

Or use npm version command:

```bash
npm version minor  # 0.2.3 -> 0.3.0
# or
npm version patch  # 0.2.3 -> 0.2.4
```

### Build for Production

```bash
cd /Users/dzu/Projects/src/github.com/logseq/libs

# Clean previous builds
rm -rf dist/

# Build everything
npm run build

# Generate schema
npm run generate:schema

# Build documentation
npm run build:docs
```

### Commit Changes

```bash
cd /Users/dzu/Projects/src/github.com/logseq

git add libs/src/LSPlugin.ts
git add libs/CHANGELOG.md
git add libs/package.json

git commit -m "feat(libs): add JSDoc for database graph APIs (v0.0.17)

- Add documentation for getAllTags()
- Add documentation for getAllProperties()
- Add documentation for getTagObjects()
- Update CHANGELOG for v0.0.17"
```

### Create Pull Request

1. Push to your branch:
   ```bash
   git push origin your-branch-name
   ```

2. Create PR with description:
   ```markdown
   ## Summary
   Adds comprehensive JSDoc documentation for new database graph APIs introduced in plugin version 0.0.17.
   
   ## Changes
   - Added JSDoc for `getAllTags()`, `getAllProperties()`, `getTagObjects()`
   - Updated CHANGELOG.md
   - Version bump to 0.0.17
   
   ## Documentation
   - All methods include descriptions, examples, and parameter documentation
   - Follows TypeDoc standards for proper documentation generation
   
   ## Testing
   - [x] TypeScript compiles without errors
   - [x] JSDoc renders correctly in IDE
   - [x] Code examples are syntactically correct
   ```

### Publish to npm (Maintainers Only)

After PR is merged:

```bash
cd /Users/dzu/Projects/src/github.com/logseq/libs

# Login to npm (if not already)
npm login

# Publish package
npm publish

# Tag the release
git tag v0.0.17
git push origin v0.0.17
```

### Deploy Documentation Site

**Note**: This step is typically handled by Logseq maintainers.

The documentation site deployment process may involve:

1. Running TypeDoc to generate static site
2. Deploying to hosting (GitHub Pages, Vercel, etc.)
3. Updating DNS if needed

Contact the Logseq team for specifics on documentation deployment pipeline.

---

## Troubleshooting

### TypeDoc Not Generating Docs

**Problem**: `npm run build:docs` doesn't output documentation.

**Solution**:
```bash
# Check if typedoc is installed
npm list typedoc

# Reinstall if needed
npm install --save-dev typedoc typedoc-plugin-lsp-docs

# Run manually
npx typedoc --plugin typedoc-plugin-lsp-docs src/LSPlugin.user.ts
```

### JSDoc Not Showing in IDE

**Problem**: IntelliSense doesn't show JSDoc.

**Solution**:
1. Rebuild TypeScript declarations: `npm run build`
2. Restart IDE/VS Code
3. Check that JSDoc is directly above the method (no blank lines)
4. Ensure JSDoc uses `/**` not `/*`

### TypeScript Compilation Errors

**Problem**: `tsc` shows errors after adding JSDoc.

**Solution**:
```bash
# Check the specific error
npx tsc --noEmit

# Common issues:
# - Type mismatch in @param tags (docs vs actual types)
# - Invalid TypeScript in @example blocks
# - Missing closing tags in JSDoc
```

Fix by ensuring:
- `@param` types match actual TypeScript types
- Code examples are wrapped in backticks with `ts` language tag
- JSDoc comment is properly closed with `*/`

### Version Number Confusion

The package has two versioning schemes:

- **npm package version**: `0.2.3` (in `package.json`)
  - Standard semantic versioning
  - Incremented for npm releases

- **API feature version**: `0.0.17` (in `@added` tags)
  - Tracks when specific APIs were introduced
  - Used in CHANGELOG sections
  - Helps plugin developers target specific features

When updating:
- Increment npm version for releases
- Use feature version in `@added` tags for new APIs
- Document both in CHANGELOG

### Missing Examples in Documentation

**Problem**: Code examples don't appear in generated docs.

**Solution**:
- Ensure examples use triple backticks: ` ```ts `
- Check examples are within JSDoc block
- Verify TypeDoc configuration includes examples
- Use multiple `@example` tags for multiple examples

---

## Quick Reference

### Common Commands

```bash
# Build everything
cd libs/ && npm run build

# Generate schema only
cd libs/ && npm run generate:schema

# Check TypeScript
cd libs/ && npx tsc --noEmit

# Build docs (if configured)
cd libs/ && npm run build:docs

# Version bump
cd libs/ && npm version [major|minor|patch]

# Publish (maintainers only)
cd libs/ && npm publish
```

### File Locations

| File | Purpose |
|------|---------|
| `/libs/src/LSPlugin.ts` | **Main source** - Add JSDoc here |
| `/libs/package.json` | Package configuration and version |
| `/libs/CHANGELOG.md` | Version history |
| `/libs/scripts/extract-sdk-schema.js` | Schema extraction script |
| `/libs/dist/` | Build output (gitignored) |
| `/libs/PLUGIN_API_DOCUMENTATION_TODO.md` | Documentation task list |

### JSDoc Tags Reference

| Tag | Purpose | Example |
|-----|---------|---------|
| `@param` | Document parameter | `@param name - The user's name` |
| `@returns` | Document return value | `@returns User object or null` |
| `@throws` | Document exceptions | `@throws Error if user not found` |
| `@example` | Show usage example | See template above |
| `@added` | Version introduced | `@added 0.0.17` |
| `@deprecated` | Mark as deprecated | `@deprecated Use newMethod instead` |
| `@link` | Link to resources | `@link https://example.com` |

---

## Additional Resources

- **Plugin Samples**: https://github.com/logseq/logseq-plugin-samples
- **TypeDoc Documentation**: https://typedoc.org/
- **JSDoc Guide**: https://jsdoc.app/
- **Current Plugin Docs**: https://plugins-doc.logseq.com/
- **Alternative Docs**: https://logseq.github.io/plugins/

---

## Contributing

When contributing documentation:

1. ✅ Follow the JSDoc template
2. ✅ Include practical examples
3. ✅ Test TypeScript compilation
4. ✅ Verify in IDE
5. ✅ Update CHANGELOG
6. ✅ Submit PR with clear description

---

**Last Updated**: October 26, 2025  
**Maintainer**: Logseq Core Team  
**Status**: Active Development
