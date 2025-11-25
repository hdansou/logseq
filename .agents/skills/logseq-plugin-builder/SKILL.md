---
name: logseq-plugin-builder
description: Build production-ready Logseq DB plugins using TypeScript. Use when creating new Logseq DB plugins, adding features, or helping users go from idea to prototype to production. These plugins are ONLY for Logseq DB (the new database version), not file-based graphs. Follows professional programming principles (DRY, YAGNI, SOLID, KISS, SoC).
allowed-tools: Read, Write, Edit, Glob, Grep, Bash, WebFetch, WebSearch
---

# Logseq DB Plugin Builder

Build professional Logseq DB plugins using TypeScript. This skill helps users go from idea to prototype to production plugin fast, focusing exclusively on **Logseq DB** (the new database version of Logseq).

## Important: DB-Only Plugins

These plugins are built for **Logseq DB only** - the new database-based version of Logseq. They use:
- `@logseq/libs@0.2.8` (the DB version SDK)
- Structured properties with types and schemas
- First-class tags/classes with metadata
- Full Datascript/Datalog queries
- Real-time collaboration ready

## Core Principles

All plugins MUST follow:

- **DRY**: Extract reusable logic into functions/modules
- **YAGNI**: Build only what's needed now, not speculative features
- **SOLID**: Single responsibility, open/closed, Liskov substitution, interface segregation, dependency inversion
- **KISS**: Prefer simple, clear solutions over complex ones
- **SoC**: Separate UI, business logic, data access, and configuration

## Development Workflow

### Phase 1: Understand the Idea

1. **Clarify Requirements**
   - What problem does this plugin solve?
   - What are the core features? (apply YAGNI - no extras)
   - Does it need UI components?
   - What Logseq DB features will it use? (properties, tags, queries)
   - What user settings are needed?

2. **Plan Architecture**
   - Sketch module structure (SoC)
   - Define responsibilities (SOLID)
   - Plan TypeScript types/interfaces
   - Design property schemas
   - Plan tag/class structure (if needed)

### Phase 2: Project Setup

**Directory Structure:**
```
logseq-plugin-[name]/
├── src/
│   ├── index.ts           # Main entry point
│   ├── types.ts           # TypeScript type definitions
│   ├── config.ts          # Configuration constants
│   ├── services/          # Business logic (SoC)
│   │   ├── plugin-service.ts
│   │   ├── property-service.ts
│   │   ├── tag-service.ts
│   │   └── query-service.ts
│   ├── ui/                # UI components (if needed)
│   └── utils/             # Reusable utilities (DRY)
├── package.json
├── tsconfig.json
├── webpack.config.js
├── .gitignore
├── icon.svg
├── LICENSE
└── README.md
```

**package.json:**
```json
{
  "name": "logseq-plugin-[name]",
  "version": "0.1.0",
  "description": "Brief description",
  "main": "dist/index.js",
  "scripts": {
    "build": "webpack --mode production",
    "dev": "webpack --mode development --watch"
  },
  "dependencies": {
    "@logseq/libs": "^0.2.8"
  },
  "devDependencies": {
    "typescript": "^5.0.0",
    "webpack": "^5.0.0",
    "webpack-cli": "^5.0.0",
    "ts-loader": "^9.0.0"
  },
  "logseq": {
    "id": "logseq-plugin-[name]",
    "title": "Plugin Name",
    "icon": "./icon.svg",
    "main": "dist/index.js"
  }
}
```

**tsconfig.json:**
```json
{
  "compilerOptions": {
    "target": "ES2020",
    "module": "ESNext",
    "lib": ["ES2020", "DOM"],
    "moduleResolution": "node",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "outDir": "./dist",
    "rootDir": "./src",
    "declaration": true,
    "sourceMap": true
  },
  "include": ["src/**/*"],
  "exclude": ["node_modules", "dist"]
}
```

**webpack.config.js:**
```javascript
const path = require('path');

module.exports = {
  entry: './src/index.ts',
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        use: 'ts-loader',
        exclude: /node_modules/
      }
    ]
  },
  resolve: {
    extensions: ['.tsx', '.ts', '.js']
  },
  output: {
    filename: 'index.js',
    path: path.resolve(__dirname, 'dist'),
    library: { type: 'commonjs2' }
  },
  target: 'electron-renderer',
  externals: {
    '@logseq/libs': 'commonjs2 @logseq/libs'
  }
};
```

### Phase 3: Core Implementation

**src/index.ts:**
```typescript
import '@logseq/libs';
import type { SettingSchemaDesc } from '@logseq/libs/dist/LSPlugin';
import { PluginService } from './services/plugin-service';
import { CONFIG } from './config';

async function main() {
  console.log(`${CONFIG.PLUGIN_NAME} loaded`);

  // Verify DB graph (required)
  const isDbGraph = await logseq.App.checkCurrentIsDbGraph();
  if (!isDbGraph) {
    logseq.UI.showMsg(
      `${CONFIG.PLUGIN_NAME} requires Logseq DB`,
      'warning'
    );
    return;
  }

  // Initialize plugin
  const pluginService = new PluginService();
  logseq.useSettingsSchema(getSettingsSchema());
  await pluginService.initialize();

  console.log(`${CONFIG.PLUGIN_NAME} initialized`);
}

function getSettingsSchema(): SettingSchemaDesc[] {
  return [
    {
      key: 'enableFeatureX',
      type: 'boolean',
      default: true,
      title: 'Enable Feature X',
      description: 'Description of what this does'
    }
  ];
}

logseq.ready(main).catch(console.error);
```

**src/config.ts:**
```typescript
export const CONFIG = {
  PLUGIN_NAME: 'My Plugin',
  PLUGIN_ID: 'logseq-plugin-my-plugin',
  VERSION: '0.1.0',
  DEFAULTS: {
    enableFeatureX: true,
    maxItems: 10
  }
} as const;
```

**src/types.ts:**
```typescript
export interface PluginConfig {
  enableFeatureX: boolean;
  maxItems: number;
}

export type PropertyType =
  | 'default'
  | 'number'
  | 'date'
  | 'datetime'
  | 'checkbox'
  | 'url'
  | 'node'
  | 'json'
  | 'string';

export type PropertyCardinality = 'one' | 'many';

export interface PropertySchema {
  type: PropertyType;
  cardinality?: PropertyCardinality;
  hide?: boolean;
  public?: boolean;
}

export interface PluginService {
  initialize(): Promise<void>;
  cleanup(): void;
}
```

**src/services/plugin-service.ts:**
```typescript
import { PropertyService } from './property-service';
import { TagService } from './tag-service';
import { CONFIG } from '../config';

export class PluginService {
  private propertyService: PropertyService;
  private tagService: TagService;
  private subscriptions: Array<() => void> = [];

  constructor() {
    this.propertyService = new PropertyService();
    this.tagService = new TagService();
  }

  async initialize(): Promise<void> {
    await this.setupSchema();
    this.registerCommands();
    this.registerEventHandlers();
  }

  private async setupSchema(): Promise<void> {
    // Define required properties
    await this.propertyService.ensureProperty('status', {
      type: 'default'
    });
    await this.propertyService.ensureProperty('priority', {
      type: 'number'
    });

    // Ensure required tags exist
    await this.tagService.ensureTag('Project');
  }

  private registerCommands(): void {
    logseq.Editor.registerSlashCommand('my-command', async () => {
      const block = await logseq.Editor.getCurrentBlock();
      if (!block) {
        logseq.UI.showMsg('No block selected', 'warning');
        return;
      }
      await this.handleCommand(block.uuid);
    });
  }

  private registerEventHandlers(): void {
    const unsub = logseq.DB.onChanged(({ blocks }) => {
      console.log(`${blocks.length} blocks changed`);
    });
    this.subscriptions.push(unsub);
  }

  private async handleCommand(uuid: string): Promise<void> {
    try {
      await this.propertyService.setBlockProperty(
        uuid,
        'status',
        'processed'
      );
      logseq.UI.showMsg('Block processed', 'success');
    } catch (error) {
      console.error('Command failed:', error);
      logseq.UI.showMsg('Command failed', 'error');
    }
  }

  cleanup(): void {
    this.subscriptions.forEach(unsub => unsub());
    this.subscriptions = [];
  }
}

// Cleanup on unload
logseq.beforeunload(() => {
  // Plugin cleanup
});
```

### Phase 4: Service Layer Implementations

**src/services/property-service.ts:**
```typescript
import type { PropertySchema } from '../types';

export class PropertyService {
  async ensureProperty(
    name: string,
    schema: PropertySchema
  ): Promise<boolean> {
    try {
      const existing = await logseq.Editor.getProperty(name);
      if (!existing) {
        await logseq.Editor.upsertProperty(name, schema);
        console.log(`Created property: ${name}`);
      }
      return true;
    } catch (error) {
      console.error(`Failed to ensure property ${name}:`, error);
      return false;
    }
  }

  async setBlockProperty(
    blockUuid: string,
    key: string,
    value: unknown
  ): Promise<void> {
    await logseq.Editor.upsertBlockProperty(blockUuid, key, value);
  }

  async getBlockProperty<T = unknown>(
    blockUuid: string,
    key: string
  ): Promise<T | null> {
    return await logseq.Editor.getBlockProperty(blockUuid, key);
  }

  async getAllBlockProperties(
    blockUuid: string
  ): Promise<Record<string, unknown>> {
    return await logseq.Editor.getBlockProperties(blockUuid);
  }

  async removeBlockProperty(
    blockUuid: string,
    key: string
  ): Promise<void> {
    await logseq.Editor.removeBlockProperty(blockUuid, key);
  }

  async getAllProperties(): Promise<any[]> {
    return await logseq.Editor.getAllProperties() || [];
  }
}
```

**src/services/tag-service.ts:**
```typescript
export class TagService {
  async ensureTag(tagName: string): Promise<string | null> {
    let tag = await logseq.Editor.getTag(tagName);
    if (!tag) {
      tag = await logseq.Editor.createTag(tagName);
      console.log(`Created tag: ${tagName}`);
    }
    return tag?.uuid || null;
  }

  async addTagToBlock(
    blockUuid: string,
    tagName: string
  ): Promise<boolean> {
    try {
      const tagUuid = await this.ensureTag(tagName);
      if (tagUuid) {
        await logseq.Editor.addBlockTag(blockUuid, tagUuid);
        return true;
      }
      return false;
    } catch (error) {
      console.error(`Failed to add tag:`, error);
      return false;
    }
  }

  async removeTagFromBlock(
    blockUuid: string,
    tagName: string
  ): Promise<boolean> {
    try {
      const tag = await logseq.Editor.getTag(tagName);
      if (tag) {
        await logseq.Editor.removeBlockTag(blockUuid, tag.uuid);
        return true;
      }
      return false;
    } catch (error) {
      console.error(`Failed to remove tag:`, error);
      return false;
    }
  }

  async getBlocksByTag(tagName: string): Promise<any[]> {
    try {
      return await logseq.Editor.getTagObjects(tagName) || [];
    } catch (error) {
      console.error(`Failed to get blocks:`, error);
      return [];
    }
  }

  async getAllTags(): Promise<any[]> {
    return await logseq.Editor.getAllTags() || [];
  }

  async addPropertyToTag(
    tagName: string,
    propertyName: string
  ): Promise<boolean> {
    try {
      const tag = await logseq.Editor.getTag(tagName);
      if (tag) {
        await logseq.Editor.addTagProperty(tag.uuid, propertyName);
        return true;
      }
      return false;
    } catch (error) {
      console.error(`Failed to add property to tag:`, error);
      return false;
    }
  }
}
```

**src/services/query-service.ts:**
```typescript
export class QueryService {
  async queryByProperty(
    propertyKey: string,
    value?: unknown
  ): Promise<any[]> {
    const valueClause = value !== undefined
      ? `[(= ?v ${JSON.stringify(value)})]`
      : '';

    return await logseq.DB.datascriptQuery(`
      [:find (pull ?b [*])
       :where
       [?b :block/properties ?props]
       [(get ?props :${propertyKey}) ?v]
       ${valueClause}]
    `);
  }

  async queryByPropertyRange(
    propertyKey: string,
    min: number,
    max: number
  ): Promise<any[]> {
    return await logseq.DB.datascriptQuery(`
      [:find (pull ?b [*])
       :in $ ?min ?max
       :where
       [?b :block/properties ?props]
       [(get ?props :${propertyKey}) ?v]
       [(>= ?v ?min)]
       [(<= ?v ?max)]]
    `, min, max);
  }

  async queryByTag(tagName: string): Promise<any[]> {
    return await logseq.DB.datascriptQuery(`
      [:find (pull ?b [*])
       :where
       [?b :block/tags ?t]
       [?t :block/original-name "${tagName}"]]
    `);
  }

  async queryByTagAndProperty(
    tagName: string,
    propertyKey: string,
    value?: unknown
  ): Promise<any[]> {
    const valueClause = value !== undefined
      ? `[(= ?v ${JSON.stringify(value)})]`
      : '';

    return await logseq.DB.datascriptQuery(`
      [:find (pull ?b [*])
       :where
       [?b :block/tags ?t]
       [?t :block/original-name "${tagName}"]
       [?b :block/properties ?props]
       [(get ?props :${propertyKey}) ?v]
       ${valueClause}]
    `);
  }
}
```

### Phase 5: Utility Functions

**src/utils/block-utils.ts:**
```typescript
export async function getCurrentBlock() {
  return await logseq.Editor.getCurrentBlock();
}

export async function insertBlock(
  parentUuid: string,
  content: string,
  options?: {
    sibling?: boolean;
    properties?: Record<string, unknown>;
  }
): Promise<any> {
  return await logseq.Editor.insertBlock(
    parentUuid,
    content,
    options
  );
}

export async function updateBlock(
  uuid: string,
  content: string,
  options?: {
    properties?: Record<string, unknown>;
  }
): Promise<void> {
  await logseq.Editor.updateBlock(uuid, content, options);
}

export async function getBlock(uuid: string): Promise<any> {
  return await logseq.Editor.getBlock(uuid);
}
```

**src/utils/error-handler.ts:**
```typescript
export function handleError(error: unknown, context: string): void {
  console.error(`Error in ${context}:`, error);
  const message = error instanceof Error
    ? error.message
    : 'Unknown error';
  logseq.UI.showMsg(`${context}: ${message}`, 'error');
}

export async function safeExecute<T>(
  fn: () => Promise<T>,
  context: string,
  fallback?: T
): Promise<T | undefined> {
  try {
    return await fn();
  } catch (error) {
    handleError(error, context);
    return fallback;
  }
}
```

### Phase 6: Testing & Production

**Testing Checklist:**
- [ ] Install: `npm install`
- [ ] Build: `npm run dev`
- [ ] Load in Logseq DB graph
- [ ] Test all commands work
- [ ] Test properties are created
- [ ] Test tags work correctly
- [ ] Test queries return expected results
- [ ] Test settings panel
- [ ] No console errors

**Production Checklist:**
- [ ] Build production: `npm run build`
- [ ] All TypeScript errors fixed
- [ ] README.md complete
- [ ] icon.svg included
- [ ] LICENSE file added
- [ ] Version number set
- [ ] Screenshots for README
- [ ] Tested thoroughly in Logseq DB

## Key Logseq DB APIs

### Property Operations
```typescript
// Create property schema
await logseq.Editor.upsertProperty('priority', {
  type: 'number',
  cardinality: 'one'
});

// Set property on block
await logseq.Editor.upsertBlockProperty(uuid, 'priority', 5);

// Get property value
const val = await logseq.Editor.getBlockProperty(uuid, 'priority');

// Get all block properties
const props = await logseq.Editor.getBlockProperties(uuid);

// Get all properties in graph
const all = await logseq.Editor.getAllProperties();

// Remove property from block
await logseq.Editor.removeBlockProperty(uuid, 'priority');
```

### Tag/Class Operations
```typescript
// Create tag
const tag = await logseq.Editor.createTag('Project');

// Get tag
const tag = await logseq.Editor.getTag('Project');

// Add tag to block
await logseq.Editor.addBlockTag(blockUuid, tagUuid);

// Remove tag from block
await logseq.Editor.removeBlockTag(blockUuid, tagUuid);

// Get all blocks with tag
const blocks = await logseq.Editor.getTagObjects('Project');

// Get all tags
const tags = await logseq.Editor.getAllTags();

// Add property to tag
await logseq.Editor.addTagProperty(tagUuid, 'priority');

// Remove property from tag
await logseq.Editor.removeTagProperty(tagUuid, 'priority');
```

### Block Operations
```typescript
// Current block
const block = await logseq.Editor.getCurrentBlock();

// Insert block
await logseq.Editor.insertBlock(parentUuid, 'Content', {
  sibling: false,
  properties: { status: 'new' }
});

// Update block
await logseq.Editor.updateBlock(uuid, 'New content', {
  properties: { priority: 5 }
});

// Get block
const block = await logseq.Editor.getBlock(uuid);

// Remove block
await logseq.Editor.removeBlock(uuid);
```

### Query Operations
```typescript
// Simple DSL query
const results = await logseq.DB.q('(property priority 5)');

// Datascript query
const blocks = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :where
   [?b :block/properties ?props]
   [(get ?props :priority) ?p]
   [(> ?p 3)]]
`);

// Query with inputs
const tagged = await logseq.DB.datascriptQuery(`
  [:find (pull ?b [*])
   :in $ ?tag-name
   :where
   [?b :block/tags ?t]
   [?t :block/original-name ?tag-name]]
`, 'Project');
```

### Event Subscriptions
```typescript
// All DB changes
const unsub = logseq.DB.onChanged(({ blocks, txMeta }) => {
  console.log(`${blocks.length} blocks changed`);
});

// Specific block changes
const unsub = logseq.DB.onBlockChanged(uuid, (block) => {
  console.log('Block updated:', block.content);
});

// Cleanup
logseq.beforeunload(() => {
  unsub();
});
```

## Property Types

| Type | JS Type | Example | Auto-Inferred |
|------|---------|---------|---------------|
| `default` | string | `"text"` | ✅ string |
| `number` | number | `42`, `3.14` | ✅ number |
| `checkbox` | boolean | `true`, `false` | ✅ boolean |
| `date` | Date | `new Date()` | ✅ Date |
| `datetime` | number | timestamp | - |
| `url` | string | `"https://"` | ✅ URL |
| `json` | object | `{k: "v"}` | ✅ object |
| `node` | string | Block UUID | - |
| `string` | string | `"text"` | - |

**Cardinality:**
- `one` - Single value (default)
- `many` - Array (auto-inferred from arrays)

## Anti-Patterns

1. ❌ Skip DB check → ✅ Always verify `checkCurrentIsDbGraph()`
2. ❌ Hardcode settings → ✅ Use settings schema
3. ❌ Monolithic code → ✅ SoC with services
4. ❌ Ignore types → ✅ Strict TypeScript
5. ❌ Skip errors → ✅ try-catch all API calls
6. ❌ Pull `[*]` → ✅ Specific pull patterns
7. ❌ Create properties without checking → ✅ Use getProperty first

## Claude Instructions

When building a plugin:

1. **Understand** - Ask questions to clarify requirements
2. **Design** - Plan architecture (share with user)
3. **Setup** - Create project with proper structure
4. **Implement** - Build using patterns above
5. **Test** - Provide testing instructions
6. **Document** - Write comprehensive README
7. **Finalize** - Complete production checklist

Always:
- Use TypeScript strictly with proper types
- Follow DRY, YAGNI, SOLID, KISS, SoC principles
- Separate concerns into services
- Handle all errors gracefully
- Verify DB graph at startup
- Create modular, testable code
- Keep it simple - no over-engineering
- Explain architectural decisions

## References

- **API Docs**: https://logseq.github.io/plugins/
- **Samples**: https://github.com/logseq/logseq-plugin-samples
- **SDK**: @logseq/libs@0.2.8 (Logseq DB version)
- **Discord**: https://discord.gg/KpN4eHY (#plugin-dev)
- **Internal Docs**: /docs/05-developer-guides/plugin-development/

## Version Info

- **Target**: Logseq DB (new database version)
- **SDK**: @logseq/libs@0.2.8
- **API Version**: 0.0.17
- **Updated**: November 2025
