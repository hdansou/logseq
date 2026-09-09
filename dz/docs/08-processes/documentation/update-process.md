# Plugin Documentation Update Process

> **Last revised:** 2026-05-30

> **Purpose**: This guide documents the current process for updating and publishing Logseq Plugin API documentation to https://plugins-doc.logseq.com.

---

## Table of Contents

1. [Overview](#overview)
2. [Findings: New Capabilities](#findings-new-capabilities)
3. [Prerequisites](#prerequisites)
4. [Documentation Workflow](#documentation-workflow)
5. [Adding JSDoc to APIs](#adding-jsdoc-to-apis)
6. [Building Documentation](#building-documentation)
7. [Testing Documentation](#testing-documentation)
8. [Publishing Documentation](#publishing-documentation)
9. [Troubleshooting](#troubleshooting)

---

## Overview

The Logseq plugin documentation is generated from TypeScript definitions in `libs/src/LSPlugin.ts` using TypeDoc.

### Key Components

- **Source**: `/libs/src/LSPlugin.ts` - TypeScript interface definitions with JSDoc comments
- **Build Tool**: TypeDoc (`typedoc`)
- **Output**: Generated documentation artifacts (including `docs/out.json` when using `build:docs:json`)
- **Package**: `@logseq/libs`
- **Current package version**: `0.3.3`
- **API feature tags**: `@added` tags (for example `0.0.17`) track feature introduction history independently from npm package versioning

> **Removed:** `typedoc-plugin-lsp-docs` is not in the current `libs/package.json`.

---

## Findings: New Capabilities

Current runtime plugin API exports include additional database graph capabilities that should be considered in docs updates. They are exported in `/src/main/logseq/api.cljs` and implemented in `/src/main/logseq/api/db_based.cljs`:

- `createTag`, `getTag`, `getTagsByName`
- `addTagExtends`, `removeTagExtends`
- `addBlockTag`, `removeBlockTag`
- `addTagProperty`, `removeTagProperty`
- `setBlockIcon`, `removeBlockIcon`
- `addPropertyValueChoices`, `setPropertyNodeTags`

These are in addition to `getAllTags`, `getAllProperties`, and `getTagObjects`.

Validation scope note:

- `src/` contains the authoritative runtime API exports and DB API implementations used in this process.
- `deps/` and `packages/` do not contain alternate plugin API export surfaces for this workflow, so this guide now points to the canonical `src/main/logseq/api*` sources.

---

## Prerequisites

### Required Tools

```bash
# Node.js and pnpm
node --version
pnpm --version

# Install dependencies
cd libs/
pnpm install
```

---

## Documentation Workflow

### Step 1: Identify APIs to Document

Check current API definitions and JSDoc coverage in `LSPlugin.ts`:

```bash
cd /Users/dzu/Projects/src/github.com/logseq-eng-documentation/libs/src

# Example rough counts for IEditorProxy
awk '/^export interface IEditorProxy/,/^}/' LSPlugin.ts | grep -E "^  [a-zA-Z_]" | wc -l
awk '/^export interface IEditorProxy/,/^}/' LSPlugin.ts | grep -c "  /\*\*"
```

> **Removed:** `./PLUGIN_API_DOCUMENTATION_TODO.md` from this directory is not the active maintained checklist.

Use `/dz/docs/dev/PLUGIN_API_DOCUMENTATION_TODO.md` if you need the backlog context.

### Step 2: Research the Implementation

Before documenting an API, verify runtime exports and implementation:

```bash
cd /Users/dzu/Projects/src/github.com/logseq-eng-documentation

# Runtime exports
rg -n "get_all_tags|get_all_properties|get_tag_objects|create_tag|get_tag|add_block_tag" src/main/logseq/api.cljs

# DB API implementations
rg -n "defn .*get-all-tags|defn .*create-tag|defn .*add-block-tag" src/main/logseq/api/db_based.cljs

# E2E references
rg -n "getAllTags|getAllProperties|getTagObjects" clj-e2e/test/
```

Key files to check:

- `/src/main/logseq/api.cljs` - Main plugin API exports
- `/src/main/logseq/api/db_based.cljs` - Database graph plugin APIs
- `/src/main/logseq/api/editor.cljs` - Editor APIs
- `/src/main/logseq/api/plugin.cljs` - Plugin API plumbing
- `/clj-e2e/test/logseq/e2e/plugins_basic_test.clj` - Plugin E2E coverage

> **Removed:** `/src/main/logseq/api/file_based.cljs` is not present in current source.

### Step 3: Add JSDoc Documentation

Edit `/libs/src/LSPlugin.ts` and add or update JSDoc comments.

---

## Adding JSDoc to APIs

### JSDoc Template

````typescript
/**
 * Brief one-line description.
 *
 * Add behavior details, compatibility notes, and constraints.
 *
 * @added 0.0.X
 * @param paramName - Parameter description.
 * @returns Return value semantics.
 *
 * @example
 * ```ts
 * const result = await logseq.Editor.methodName(arg1)
 * console.log(result)
 * ```
 */
methodName: (param: Type) => Promise<ReturnType>
````

### Best Practices

1. State graph compatibility (database-only, file-based-only, or both).
2. Document all params and return behavior clearly.
3. Include realistic examples and edge cases where helpful.
4. Keep version tags accurate (`@added`, `@deprecated`).

---

## Building Documentation

### Local Build

```bash
cd /Users/dzu/Projects/src/github.com/logseq-eng-documentation/libs

# Build declarations + bundles
pnpm run build

# Generate schema for CLJS SDK tooling
pnpm run generate:schema

# Generate docs
pnpm run build:docs
# or JSON output used by docs pipeline
pnpm run build:docs:json
```

### Verify TypeScript Compilation

```bash
cd /Users/dzu/Projects/src/github.com/logseq-eng-documentation/libs
pnpm exec tsc --noEmit
```

### Check Generated Schema

```bash
cd /Users/dzu/Projects/src/github.com/logseq-eng-documentation/libs
jq '.interfaces.IEditorProxy.methods[] | select(.name == "getAllTags")' dist/logseq-sdk-schema.json
```

---

## Testing Documentation

### 1. Verify in IDE

In a plugin project:

```typescript
logseq.Editor.getAllTags() // Hover should show JSDoc
```

### 2. Verify docs build output

```bash
cd /Users/dzu/Projects/src/github.com/logseq-eng-documentation/libs
pnpm run build:docs
```

### 3. Validate example snippets (optional)

```bash
cd /Users/dzu/Projects/src/github.com/logseq-eng-documentation/libs
rg -n "@example" src/LSPlugin.ts
```

---

## Publishing Documentation

### Update Version Entries

```bash
cd /Users/dzu/Projects/src/github.com/logseq-eng-documentation/libs
```

1. Update `CHANGELOG.md` for the current `0.3.x` release line.
2. Update `package.json` from `0.3.3` to the next intended version.

Example:

```json
{
  "name": "@logseq/libs",
  "version": "0.3.4"
}
```

Version bump helper:

```bash
pnpm version patch
# or
pnpm version minor
```

### Build and Commit

```bash
cd /Users/dzu/Projects/src/github.com/logseq-eng-documentation/libs
rm -rf dist/
pnpm run build
pnpm run generate:schema
pnpm run build:docs
```

```bash
cd /Users/dzu/Projects/src/github.com/logseq-eng-documentation
git add libs/src/LSPlugin.ts libs/CHANGELOG.md libs/package.json
git commit -m "docs(libs): update plugin API docs and version metadata"
```

### Publish (Maintainers Only)

```bash
cd /Users/dzu/Projects/src/github.com/logseq-eng-documentation/libs
npm login
pnpm publish
```

> **Removed:** Hardcoded release tag examples like `v0.0.17` are stale for current package versioning.

---

## Troubleshooting

### TypeDoc Not Generating Docs

```bash
cd /Users/dzu/Projects/src/github.com/logseq-eng-documentation/libs
pnpm list typedoc
pnpm add -D typedoc
pnpm exec typedoc src/LSPlugin.user.ts
```

### JSDoc Not Showing in IDE

1. Rebuild declarations with `pnpm run build`.
2. Restart IDE TypeScript service.
3. Ensure comments use `/** ... */` directly above declarations.

### TypeScript Compilation Errors After Doc Edits

```bash
cd /Users/dzu/Projects/src/github.com/logseq-eng-documentation/libs
pnpm exec tsc --noEmit
```

---

## Quick Reference

### Common Commands

```bash
cd libs/ && pnpm run build
cd libs/ && pnpm run generate:schema
cd libs/ && pnpm exec tsc --noEmit
cd libs/ && pnpm run build:docs
cd libs/ && pnpm version [major|minor|patch]
cd libs/ && pnpm publish
```

### File Locations

| File                                  | Purpose                             |
| ------------------------------------- | ----------------------------------- |
| `/libs/src/LSPlugin.ts`               | Main source for JSDoc               |
| `/libs/package.json`                  | Package config/version              |
| `/libs/CHANGELOG.md`                  | Release notes                       |
| `/libs/scripts/extract-sdk-schema.js` | SDK schema generation               |
| `/libs/dist/`                         | Build output                        |

> **Removed:** `/libs/PLUGIN_API_DOCUMENTATION_TODO.md` is not the active checklist path.

Active checklist path: `/dz/docs/dev/PLUGIN_API_DOCUMENTATION_TODO.md`.

---

## Additional Resources

- Plugin Samples: https://github.com/logseq/logseq-plugin-samples
- TypeDoc: https://typedoc.org/
- JSDoc: https://jsdoc.app/
- Current Plugin Docs: https://plugins-doc.logseq.com/
- Alternative Docs: https://logseq.github.io/plugins/

---

**Last Updated**: May 30, 2026
**Maintainer**: Logseq Core Team
**Status**: Active Development
