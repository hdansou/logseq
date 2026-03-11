# Tutorial: Build a Duplicate Block Plugin for Logseq DB Graphs

> **Last revised:** 2026-03-10

**What You'll Build:** A Logseq plugin that allows users to duplicate blocks/nodes with all their properties, children, and metadata.

**Time Required:** 30-45 minutes

**Prerequisites:**

- Basic JavaScript or ClojureScript knowledge
- Node.js and npm/yarn installed
- Logseq installed with a DB graph
- Code editor (VS Code recommended)

**What You'll Learn:**

- Setting up a Logseq plugin project
- Working with DB graph properties
- Block manipulation and traversal
- Error handling and user feedback
- Testing and debugging plugins

---

## Table of Contents

1. [Project Setup](#step-1-project-setup)
2. [Basic Plugin Structure](#step-2-basic-plugin-structure)
3. [Implementing Block Duplication](#step-3-implementing-block-duplication)
4. [Handling Properties](#step-4-handling-properties)
5. [Duplicating Child Blocks](#step-5-duplicating-child-blocks)
6. [Adding User Interface](#step-6-adding-user-interface)
7. [Error Handling](#step-7-error-handling)
8. [Testing the Plugin](#step-8-testing-the-plugin)
9. [Publishing](#step-9-publishing)

---

## Step 1: Project Setup

### 1.1 Create Project Directory

```bash
# Create project folder
mkdir logseq-duplicate-block
cd logseq-duplicate-block

# Initialize npm project
npm init -y
```

### 1.2 Install Dependencies

```bash
# Install Logseq SDK
npm install @logseq/libs

# Install development dependencies
npm install --save-dev typescript @types/node
```

### 1.3 Create Project Structure

```bash
# Create necessary directories and files
mkdir src
touch src/index.js
touch package.json
touch README.md
```

Your project structure should look like:

```
logseq-duplicate-block/
├── src/
│   └── index.js
├── package.json
├── README.md
└── node_modules/
```

### 1.4 Configure package.json

Edit `package.json`:

```json
{
  "name": "logseq-duplicate-block",
  "version": "1.0.0",
  "description": "Duplicate blocks with all properties and children",
  "main": "src/index.js",
  "logseq": {
    "id": "logseq-duplicate-block",
    "title": "Duplicate Block",
    "icon": "📋",
    "main": "src/index.js"
  },
  "author": "Your Name",
  "license": "MIT",
  "dependencies": {
    "@logseq/libs": "^0.2.3"
  }
}
```

**Key Points:**

- `logseq.id` - Unique identifier for your plugin
- `logseq.title` - Display name in Logseq
- `logseq.icon` - Icon shown in plugin list
- `logseq.main` - Entry point file

---

## Step 2: Basic Plugin Structure

### 2.1 Create Main Plugin File

Edit `src/index.js`:

```javascript
// Import Logseq SDK
import '@logseq/libs'

/**
 * Main plugin initialization
 */
async function main() {
  console.log('Duplicate Block plugin loaded!')

  // Check if we're in a DB graph
  const isDbGraph = await logseq.App.checkCurrentIsDbGraph()

  if (!isDbGraph) {
    logseq.UI.showMsg('Duplicate Block plugin requires a DB graph', 'warning', {
      timeout: 5000,
    })
    return
  }

  console.log('DB graph detected - initializing plugin features')

  // We'll add features here in next steps
  await registerCommands()
}

/**
 * Register plugin commands
 */
async function registerCommands() {
  // We'll implement this next
  console.log('Registering commands...')
}

// Initialize plugin when Logseq is ready
logseq.ready(main).catch(console.error)
```

**What This Does:**

- Imports the Logseq SDK
- Checks if current graph is DB-based (required for our plugin)
- Shows warning if not a DB graph
- Sets up the foundation for commands

### 2.2 Test Basic Setup

1. **Link Plugin to Logseq:**

   - Open Logseq
   - Go to Settings → Plugins → Load unpacked plugin
   - Select your `logseq-duplicate-block` folder
   - Click "Load"

2. **Check Console:**

   - Open Developer Tools (View → Toggle Developer Tools)
   - You should see: `Duplicate Block plugin loaded!`
   - And: `DB graph detected - initializing plugin features`

**Troubleshooting:**

- If plugin doesn't load, check `package.json` syntax
- Make sure `src/index.js` exists and has no syntax errors
- Check console for error messages

---

## Step 3: Implementing Block Duplication

### 3.1 Create Duplicate Function

Add to `src/index.js`:

```javascript
/**
 * Duplicate a single block with its content
 * @param {string} blockUuid - UUID of block to duplicate
 * @param {string} targetUuid - UUID of reference block
 * @param {object} options - Placement options
 */
async function duplicateBlock(blockUuid, targetUuid, options = {}) {
  try {
    console.log(`Duplicating block ${blockUuid}`)

    // Get the source block
    const sourceBlock = await logseq.Editor.getBlock(blockUuid)

    if (!sourceBlock) {
      throw new Error('Source block not found')
    }

    console.log('Source block:', sourceBlock)

    // Create new block with same content
    const newBlock = await logseq.Editor.insertBlock(
      targetUuid,
      sourceBlock.content,
      {
        sibling: options.sibling !== false, // Default: insert as sibling
        before: options.before || false,
      }
    )

    if (!newBlock) {
      throw new Error('Failed to create new block')
    }

    console.log('Created new block:', newBlock.uuid)

    return newBlock
  } catch (error) {
    console.error('Error duplicating block:', error)
    throw error
  }
}
```

**What This Does:**

- Gets the source block data
- Creates a new block with the same content
- Inserts it as a sibling (next to the original)
- Returns the new block for further processing

### 3.2 Register Duplicate Command

Update the `registerCommands` function:

```javascript
/**
 * Register plugin commands
 */
async function registerCommands() {
  // Register block context menu item
  logseq.Editor.registerBlockContextMenuItem(
    '📋 Duplicate Block',
    async (block) => {
      try {
        console.log('Duplicate command triggered for block:', block.uuid)

        // Duplicate the block
        const newBlock = await duplicateBlock(block.uuid, block.uuid)

        // Show success message
        logseq.UI.showMsg(`Block duplicated successfully!`, 'success')

        console.log('Duplication complete:', newBlock.uuid)
      } catch (error) {
        console.error('Duplication failed:', error)
        logseq.UI.showMsg(
          `Failed to duplicate block: ${error.message}`,
          'error'
        )
      }
    }
  )

  console.log('Duplicate command registered')
}
```

**What This Does:**

- Adds "📋 Duplicate Block" to the block context menu (right-click on bullet)
- When clicked, duplicates the block
- Shows success/error messages

### 3.3 Test Basic Duplication

1. **Reload Plugin:**

   - In Logseq: Settings → Plugins → Click reload icon next to your plugin

2. **Test:**
   - Create a block with some text: "Test block"
   - Right-click on the block bullet (•)
   - Click "📋 Duplicate Block"
   - You should see a duplicate block appear below

**Expected Result:**

```
- Test block
- Test block  ← duplicate
```

---

## Step 4: Handling Properties

Now let's make it duplicate properties too (the key feature for DB graphs!)

### 4.1 Add Property Duplication

Update `duplicateBlock` function:

```javascript
/**
 * Duplicate a block with all its properties
 */
async function duplicateBlock(blockUuid, targetUuid, options = {}) {
  try {
    console.log(`Duplicating block ${blockUuid}`)

    // Get the source block
    const sourceBlock = await logseq.Editor.getBlock(blockUuid)

    if (!sourceBlock) {
      throw new Error('Source block not found')
    }

    // Create new block with same content
    const newBlock = await logseq.Editor.insertBlock(
      targetUuid,
      sourceBlock.content,
      {
        sibling: options.sibling !== false,
        before: options.before || false,
      }
    )

    if (!newBlock) {
      throw new Error('Failed to create new block')
    }

    // --- NEW: Duplicate properties ---
    await duplicateProperties(sourceBlock, newBlock)

    console.log('Block duplicated with properties:', newBlock.uuid)

    return newBlock
  } catch (error) {
    console.error('Error duplicating block:', error)
    throw error
  }
}

/**
 * Duplicate all properties from source to target block
 */
async function duplicateProperties(sourceBlock, targetBlock) {
  try {
    // Get all properties from source block
    const properties = await logseq.Editor.getBlockProperties(sourceBlock.uuid)

    if (!properties || Object.keys(properties).length === 0) {
      console.log('No properties to duplicate')
      return
    }

    console.log('Duplicating properties:', properties)

    // Copy each property to the new block
    for (const [key, value] of Object.entries(properties)) {
      // Skip internal/system properties
      if (key.startsWith('logseq.')) {
        console.log(`Skipping system property: ${key}`)
        continue
      }

      try {
        await logseq.Editor.upsertBlockProperty(targetBlock.uuid, key, value)
        console.log(`Copied property ${key}:`, value)
      } catch (error) {
        console.warn(`Failed to copy property ${key}:`, error)
        // Continue with other properties even if one fails
      }
    }

    console.log('Properties duplicated successfully')
  } catch (error) {
    console.error('Error duplicating properties:', error)
    // Don't throw - we want the block to be created even if properties fail
  }
}
```

**What This Does:**

- Gets all properties from the source block
- Copies each property to the new block
- Skips system properties (those starting with `logseq.`)
- Handles errors gracefully - won't fail entire duplication if one property fails

### 4.2 Test Property Duplication

1. **Create a block with properties:**

   ```
   - Test block
     priority:: 5
     status:: active
     tags:: urgent, review
   ```

2. **Duplicate it:**

   - Right-click bullet → "📋 Duplicate Block"

3. **Verify:**
   - New block should have same content AND all properties
   - Check properties panel to confirm

**Expected Result:**

```
- Test block
  priority:: 5
  status:: active
  tags:: urgent, review

- Test block  ← duplicate with same properties
  priority:: 5
  status:: active
  tags:: urgent, review
```

---

## Step 5: Duplicating Child Blocks

Let's add support for duplicating children (nested blocks).

### 5.1 Add Recursive Duplication

Add new function:

```javascript
/**
 * Recursively duplicate a block and all its children
 */
async function duplicateBlockWithChildren(blockUuid, targetUuid, options = {}) {
  try {
    console.log(`Duplicating block with children: ${blockUuid}`)

    // Get source block with children
    const sourceBlock = await logseq.Editor.getBlock(blockUuid, {
      includeChildren: true,
    })

    if (!sourceBlock) {
      throw new Error('Source block not found')
    }

    // Duplicate the main block
    const newBlock = await duplicateBlock(blockUuid, targetUuid, options)

    // Duplicate children recursively
    if (sourceBlock.children && sourceBlock.children.length > 0) {
      console.log(`Duplicating ${sourceBlock.children.length} children`)

      for (const child of sourceBlock.children) {
        await duplicateBlockWithChildren(
          child.uuid,
          newBlock.uuid,
          { sibling: false } // Children should be nested, not siblings
        )
      }
    }

    return newBlock
  } catch (error) {
    console.error('Error duplicating block with children:', error)
    throw error
  }
}
```

### 5.2 Update Command to Use Recursive Duplication

Update `registerCommands`:

```javascript
async function registerCommands() {
  // Register block context menu item
  logseq.Editor.registerBlockContextMenuItem(
    '📋 Duplicate Block',
    async (block) => {
      try {
        console.log('Duplicate command triggered for block:', block.uuid)

        // Use recursive duplication to include children
        const newBlock = await duplicateBlockWithChildren(
          block.uuid,
          block.uuid
        )

        logseq.UI.showMsg(`Block duplicated successfully!`, 'success')

        console.log('Duplication complete:', newBlock.uuid)
      } catch (error) {
        console.error('Duplication failed:', error)
        logseq.UI.showMsg(
          `Failed to duplicate block: ${error.message}`,
          'error'
        )
      }
    }
  )

  console.log('Duplicate command registered')
}
```

### 5.3 Test Child Block Duplication

1. **Create a nested structure:**

   ```
   - Parent block
     priority:: high
     - Child 1
       status:: active
       - Nested child
     - Child 2
       tags:: urgent
   ```

2. **Duplicate parent:**

   - Right-click on "Parent block" → "📋 Duplicate Block"

3. **Verify:**
   - Entire tree should be duplicated
   - All properties preserved at every level

**Expected Result:**

```
- Parent block
  priority:: high
  - Child 1
    status:: active
    - Nested child
  - Child 2
    tags:: urgent

- Parent block  ← duplicate with entire tree
  priority:: high
  - Child 1
    status:: active
    - Nested child
  - Child 2
    tags:: urgent
```

---

## Step 6: Adding User Interface

Let's add options for how to duplicate (with/without children, position, etc.)

### 6.1 Add Configuration Options

Add at the top of `src/index.js`:

```javascript
/**
 * Plugin configuration
 */
const CONFIG = {
  duplicateChildren: true, // Include children by default
  duplicateProperties: true, // Include properties by default
  insertPosition: 'after', // 'after' or 'before'
}
```

### 6.2 Add Settings Schema

Add after the `main` function:

```javascript
/**
 * Setup plugin settings
 */
async function setupSettings() {
  logseq.useSettingsSchema([
    {
      key: 'duplicateChildren',
      type: 'boolean',
      title: 'Duplicate children blocks',
      description: 'Include child blocks when duplicating',
      default: true,
    },
    {
      key: 'duplicateProperties',
      type: 'boolean',
      title: 'Duplicate properties',
      description: 'Copy all properties to duplicated block',
      default: true,
    },
    {
      key: 'insertPosition',
      type: 'enum',
      title: 'Insert position',
      description: 'Where to place the duplicate',
      enumChoices: ['after', 'before'],
      enumPicker: 'radio',
      default: 'after',
    },
  ])
}
```

### 6.3 Update Main Function

Update `main` to call `setupSettings`:

```javascript
async function main() {
  console.log('Duplicate Block plugin loaded!')

  const isDbGraph = await logseq.App.checkCurrentIsDbGraph()

  if (!isDbGraph) {
    logseq.UI.showMsg('Duplicate Block plugin requires a DB graph', 'warning', {
      timeout: 5000,
    })
    return
  }

  console.log('DB graph detected - initializing plugin features')

  // Setup settings UI
  await setupSettings()

  // Register commands
  await registerCommands()
}
```

### 6.4 Use Settings in Duplication

Update `registerCommands` to use settings:

```javascript
async function registerCommands() {
  logseq.Editor.registerBlockContextMenuItem(
    '📋 Duplicate Block',
    async (block) => {
      try {
        console.log('Duplicate command triggered for block:', block.uuid)

        // Get user settings
        const settings = logseq.settings
        const includeChildren = settings.duplicateChildren !== false
        const includeProperties = settings.duplicateProperties !== false
        const position = settings.insertPosition || 'after'

        console.log('Settings:', {
          includeChildren,
          includeProperties,
          position,
        })

        // Duplicate based on settings
        let newBlock
        if (includeChildren) {
          newBlock = await duplicateBlockWithChildren(block.uuid, block.uuid, {
            sibling: true,
            before: position === 'before',
            includeProperties,
          })
        } else {
          newBlock = await duplicateBlock(block.uuid, block.uuid, {
            sibling: true,
            before: position === 'before',
            includeProperties,
          })
        }

        logseq.UI.showMsg(`Block duplicated successfully!`, 'success')

        console.log('Duplication complete:', newBlock.uuid)
      } catch (error) {
        console.error('Duplication failed:', error)
        logseq.UI.showMsg(
          `Failed to duplicate block: ${error.message}`,
          'error'
        )
      }
    }
  )

  console.log('Duplicate command registered')
}
```

### 6.5 Update Duplication Functions to Respect Settings

Update `duplicateProperties` to check settings:

```javascript
async function duplicateProperties(sourceBlock, targetBlock, options = {}) {
  // Check if property duplication is enabled
  if (options.includeProperties === false) {
    console.log('Property duplication disabled')
    return
  }

  try {
    const properties = await logseq.Editor.getBlockProperties(sourceBlock.uuid)

    if (!properties || Object.keys(properties).length === 0) {
      console.log('No properties to duplicate')
      return
    }

    console.log('Duplicating properties:', properties)

    for (const [key, value] of Object.entries(properties)) {
      if (key.startsWith('logseq.')) {
        console.log(`Skipping system property: ${key}`)
        continue
      }

      try {
        await logseq.Editor.upsertBlockProperty(targetBlock.uuid, key, value)
        console.log(`Copied property ${key}:`, value)
      } catch (error) {
        console.warn(`Failed to copy property ${key}:`, error)
      }
    }

    console.log('Properties duplicated successfully')
  } catch (error) {
    console.error('Error duplicating properties:', error)
  }
}
```

Update `duplicateBlock` to pass options:

```javascript
async function duplicateBlock(blockUuid, targetUuid, options = {}) {
  try {
    console.log(`Duplicating block ${blockUuid}`)

    const sourceBlock = await logseq.Editor.getBlock(blockUuid)

    if (!sourceBlock) {
      throw new Error('Source block not found')
    }

    const newBlock = await logseq.Editor.insertBlock(
      targetUuid,
      sourceBlock.content,
      {
        sibling: options.sibling !== false,
        before: options.before || false,
      }
    )

    if (!newBlock) {
      throw new Error('Failed to create new block')
    }

    // Duplicate properties (respecting options)
    await duplicateProperties(sourceBlock, newBlock, options)

    console.log('Block duplicated:', newBlock.uuid)

    return newBlock
  } catch (error) {
    console.error('Error duplicating block:', error)
    throw error
  }
}
```

### 6.6 Test Settings

1. **Access Settings:**

   - Settings → Plugins → Find "Duplicate Block"
   - Click settings icon

2. **Try Different Combinations:**
   - Uncheck "Duplicate children blocks"
   - Duplicate a block with children → Only parent duplicated
   - Uncheck "Duplicate properties"
   - Duplicate a block with properties → No properties copied

---

## Step 7: Error Handling

Let's add robust error handling and user feedback.

### 7.1 Add Error Handler Utility

Add to `src/index.js`:

```javascript
/**
 * Handle errors with user-friendly messages
 */
function handleError(error, context = 'Operation') {
  console.error(`${context} error:`, error)

  let message = error.message || 'Unknown error occurred'

  // Provide helpful messages for common errors
  if (message.includes('not found')) {
    message = 'Block not found. It may have been deleted.'
  } else if (message.includes('permission')) {
    message = 'Permission denied. Check plugin permissions.'
  } else if (message.includes('network')) {
    message = 'Network error. Check your connection.'
  }

  logseq.UI.showMsg(`${context} failed: ${message}`, 'error', { timeout: 5000 })
}
```

### 7.2 Add Validation

Add validation function:

```javascript
/**
 * Validate block before duplication
 */
async function validateBlock(blockUuid) {
  try {
    const block = await logseq.Editor.getBlock(blockUuid)

    if (!block) {
      throw new Error('Block not found')
    }

    if (!block.content) {
      console.warn('Block has no content')
    }

    return true
  } catch (error) {
    console.error('Block validation failed:', error)
    return false
  }
}
```

### 7.3 Update Commands with Validation

Update `registerCommands`:

```javascript
async function registerCommands() {
  logseq.Editor.registerBlockContextMenuItem(
    '📋 Duplicate Block',
    async (block) => {
      try {
        console.log('Duplicate command triggered for block:', block.uuid)

        // Validate block
        const isValid = await validateBlock(block.uuid)
        if (!isValid) {
          handleError(new Error('Invalid block'), 'Validation')
          return
        }

        // Show progress message
        const progressMsg = await logseq.UI.showMsg(
          'Duplicating block...',
          'info',
          { timeout: 0 } // Don't auto-close
        )

        // Get settings
        const settings = logseq.settings
        const includeChildren = settings.duplicateChildren !== false
        const includeProperties = settings.duplicateProperties !== false
        const position = settings.insertPosition || 'after'

        // Duplicate
        let newBlock
        if (includeChildren) {
          newBlock = await duplicateBlockWithChildren(block.uuid, block.uuid, {
            sibling: true,
            before: position === 'before',
            includeProperties,
          })
        } else {
          newBlock = await duplicateBlock(block.uuid, block.uuid, {
            sibling: true,
            before: position === 'before',
            includeProperties,
          })
        }

        // Close progress message
        logseq.UI.closeMsg(progressMsg)

        // Show success
        logseq.UI.showMsg(`Block duplicated successfully!`, 'success', {
          timeout: 3000,
        })

        console.log('Duplication complete:', newBlock.uuid)
      } catch (error) {
        handleError(error, 'Duplication')
      }
    }
  )

  console.log('Duplicate command registered')
}
```

---

## Step 8: Testing the Plugin

### 8.1 Test Cases

Create a test page in Logseq with these scenarios:

#### Test 1: Simple Block

```
- Simple block
```

**Expected:** Duplicate appears below

#### Test 2: Block with Properties

```
- Block with properties
  priority:: 5
  status:: active
  tags:: test, demo
```

**Expected:** Duplicate has all properties

#### Test 3: Nested Blocks

```
- Parent
  - Child 1
  - Child 2
    - Nested child
```

**Expected:** Entire tree duplicated

#### Test 4: Complex Properties

```
- Complex block
  priority:: 10
  due-date:: [[2025-12-31]]
  assigned-to:: [[John Doe]]
  metadata:: {"version": "1.0", "tags": ["urgent"]}
```

**Expected:** All property types preserved

#### Test 5: Settings Test

1. Disable "Duplicate children"
2. Duplicate parent with children
   **Expected:** Only parent duplicated

### 8.2 Manual Testing Checklist

- [ ] Plugin loads without errors
- [ ] DB graph check works
- [ ] Context menu item appears
- [ ] Simple duplication works
- [ ] Properties are duplicated
- [ ] Children are duplicated recursively
- [ ] Settings can be changed
- [ ] Settings affect behavior
- [ ] Error messages are clear
- [ ] Console logs are helpful

### 8.3 Debugging Tips

**If duplication fails:**

1. **Check Console:**

   ```javascript
   // Look for error messages
   console.log('Last error:', error)
   ```

2. **Verify Block UUID:**

   ```javascript
   console.log('Block UUID:', block.uuid)
   const retrieved = await logseq.Editor.getBlock(block.uuid)
   console.log('Retrieved:', retrieved)
   ```

3. **Check Properties:**

   ```javascript
   const props = await logseq.Editor.getBlockProperties(block.uuid)
   console.log('Properties:', props)
   ```

4. **Test Step by Step:**
   - Comment out property duplication
   - Test block creation only
   - Add features back one at a time

---

## Step 9: Publishing

### 9.1 Prepare for Release

#### Update README.md

````markdown
# Duplicate Block Plugin

Duplicate blocks with all their properties and children in Logseq DB graphs.

## Features

- 📋 Duplicate any block with a right-click
- 🏷️ Preserves all properties
- 🌳 Recursively duplicates children
- ⚙️ Configurable options

## Installation

### From Logseq Marketplace (Coming Soon)

1. Open Logseq
2. Go to Plugins → Marketplace
3. Search for "Duplicate Block"
4. Click Install

### Manual Installation

1. Download the latest release
2. Unzip the file
3. Open Logseq → Settings → Plugins → Load unpacked plugin
4. Select the unzipped folder

## Usage

1. Right-click on any block bullet (•)
2. Select "📋 Duplicate Block"
3. The block will be duplicated below with all properties and children

## Settings

- **Duplicate children blocks**: Include child blocks in duplication
- **Duplicate properties**: Copy all properties to duplicate
- **Insert position**: Place duplicate before or after original

## Requirements

- Logseq with DB graph (not file-based graphs)
- Logseq version 0.2.0 or higher

## Development

```bash
npm install
npm run dev
```
````

## License

MIT

````

### 9.2 Add Icon

Create `icon.svg`:

```svg
<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 32 32">
  <rect x="4" y="4" width="12" height="12" fill="#3b82f6" rx="2"/>
  <rect x="16" y="16" width="12" height="12" fill="#3b82f6" rx="2"/>
  <path d="M 10 16 L 10 20 L 14 20" stroke="#3b82f6" stroke-width="2" fill="none"/>
  <path d="M 16 10 L 20 10 L 20 14" stroke="#3b82f6" stroke-width="2" fill="none"/>
</svg>
````

### 9.3 Update package.json for Release

```json
{
  "name": "logseq-duplicate-block",
  "version": "1.0.0",
  "description": "Duplicate blocks with all properties and children",
  "main": "src/index.js",
  "logseq": {
    "id": "logseq-duplicate-block",
    "title": "Duplicate Block",
    "icon": "./icon.svg",
    "main": "src/index.js"
  },
  "author": "Your Name",
  "license": "MIT",
  "repository": {
    "type": "git",
    "url": "https://github.com/yourusername/logseq-duplicate-block"
  },
  "keywords": ["logseq", "plugin", "duplicate", "blocks", "db-graph"],
  "dependencies": {
    "@logseq/libs": "^0.2.3"
  }
}
```

### 9.4 Create Release Package

```bash
# Create release directory
mkdir release
cp -r src icon.svg package.json README.md LICENSE release/

# Create zip
cd release
zip -r logseq-duplicate-block-v1.0.0.zip *
```

### 9.5 Publish to GitHub

1. **Create Repository:**

   - Go to GitHub
   - Create new repository: `logseq-duplicate-block`

2. **Push Code:**

   ```bash
   git init
   git add .
   git commit -m "Initial release v1.0.0"
   git remote add origin https://github.com/yourusername/logseq-duplicate-block
   git push -u origin main
   ```

3. **Create Release:**
   - Go to Releases → Create new release
   - Tag: `v1.0.0`
   - Upload the zip file
   - Publish release

---

## Complete Source Code

Here's the final `src/index.js`:

```javascript
import '@logseq/libs'

/**
 * Main plugin initialization
 */
async function main() {
  console.log('Duplicate Block plugin loaded!')

  const isDbGraph = await logseq.App.checkCurrentIsDbGraph()

  if (!isDbGraph) {
    logseq.UI.showMsg('Duplicate Block plugin requires a DB graph', 'warning', {
      timeout: 5000,
    })
    return
  }

  console.log('DB graph detected - initializing plugin features')

  await setupSettings()
  await registerCommands()
}

/**
 * Setup plugin settings
 */
async function setupSettings() {
  logseq.useSettingsSchema([
    {
      key: 'duplicateChildren',
      type: 'boolean',
      title: 'Duplicate children blocks',
      description: 'Include child blocks when duplicating',
      default: true,
    },
    {
      key: 'duplicateProperties',
      type: 'boolean',
      title: 'Duplicate properties',
      description: 'Copy all properties to duplicated block',
      default: true,
    },
    {
      key: 'insertPosition',
      type: 'enum',
      title: 'Insert position',
      description: 'Where to place the duplicate',
      enumChoices: ['after', 'before'],
      enumPicker: 'radio',
      default: 'after',
    },
  ])
}

/**
 * Register plugin commands
 */
async function registerCommands() {
  logseq.Editor.registerBlockContextMenuItem(
    '📋 Duplicate Block',
    async (block) => {
      try {
        console.log('Duplicate command triggered for block:', block.uuid)

        const isValid = await validateBlock(block.uuid)
        if (!isValid) {
          handleError(new Error('Invalid block'), 'Validation')
          return
        }

        const progressMsg = await logseq.UI.showMsg(
          'Duplicating block...',
          'info',
          { timeout: 0 }
        )

        const settings = logseq.settings
        const includeChildren = settings.duplicateChildren !== false
        const includeProperties = settings.duplicateProperties !== false
        const position = settings.insertPosition || 'after'

        let newBlock
        if (includeChildren) {
          newBlock = await duplicateBlockWithChildren(block.uuid, block.uuid, {
            sibling: true,
            before: position === 'before',
            includeProperties,
          })
        } else {
          newBlock = await duplicateBlock(block.uuid, block.uuid, {
            sibling: true,
            before: position === 'before',
            includeProperties,
          })
        }

        logseq.UI.closeMsg(progressMsg)

        logseq.UI.showMsg(`Block duplicated successfully!`, 'success', {
          timeout: 3000,
        })

        console.log('Duplication complete:', newBlock.uuid)
      } catch (error) {
        handleError(error, 'Duplication')
      }
    }
  )

  console.log('Duplicate command registered')
}

/**
 * Duplicate a single block
 */
async function duplicateBlock(blockUuid, targetUuid, options = {}) {
  try {
    console.log(`Duplicating block ${blockUuid}`)

    const sourceBlock = await logseq.Editor.getBlock(blockUuid)

    if (!sourceBlock) {
      throw new Error('Source block not found')
    }

    const newBlock = await logseq.Editor.insertBlock(
      targetUuid,
      sourceBlock.content,
      {
        sibling: options.sibling !== false,
        before: options.before || false,
      }
    )

    if (!newBlock) {
      throw new Error('Failed to create new block')
    }

    await duplicateProperties(sourceBlock, newBlock, options)

    console.log('Block duplicated:', newBlock.uuid)

    return newBlock
  } catch (error) {
    console.error('Error duplicating block:', error)
    throw error
  }
}

/**
 * Recursively duplicate a block and all its children
 */
async function duplicateBlockWithChildren(blockUuid, targetUuid, options = {}) {
  try {
    console.log(`Duplicating block with children: ${blockUuid}`)

    const sourceBlock = await logseq.Editor.getBlock(blockUuid, {
      includeChildren: true,
    })

    if (!sourceBlock) {
      throw new Error('Source block not found')
    }

    const newBlock = await duplicateBlock(blockUuid, targetUuid, options)

    if (sourceBlock.children && sourceBlock.children.length > 0) {
      console.log(`Duplicating ${sourceBlock.children.length} children`)

      for (const child of sourceBlock.children) {
        await duplicateBlockWithChildren(child.uuid, newBlock.uuid, {
          sibling: false,
          includeProperties: options.includeProperties,
        })
      }
    }

    return newBlock
  } catch (error) {
    console.error('Error duplicating block with children:', error)
    throw error
  }
}

/**
 * Duplicate all properties from source to target block
 */
async function duplicateProperties(sourceBlock, targetBlock, options = {}) {
  if (options.includeProperties === false) {
    console.log('Property duplication disabled')
    return
  }

  try {
    const properties = await logseq.Editor.getBlockProperties(sourceBlock.uuid)

    if (!properties || Object.keys(properties).length === 0) {
      console.log('No properties to duplicate')
      return
    }

    console.log('Duplicating properties:', properties)

    for (const [key, value] of Object.entries(properties)) {
      if (key.startsWith('logseq.')) {
        console.log(`Skipping system property: ${key}`)
        continue
      }

      try {
        await logseq.Editor.upsertBlockProperty(targetBlock.uuid, key, value)
        console.log(`Copied property ${key}:`, value)
      } catch (error) {
        console.warn(`Failed to copy property ${key}:`, error)
      }
    }

    console.log('Properties duplicated successfully')
  } catch (error) {
    console.error('Error duplicating properties:', error)
  }
}

/**
 * Validate block before duplication
 */
async function validateBlock(blockUuid) {
  try {
    const block = await logseq.Editor.getBlock(blockUuid)

    if (!block) {
      throw new Error('Block not found')
    }

    if (!block.content) {
      console.warn('Block has no content')
    }

    return true
  } catch (error) {
    console.error('Block validation failed:', error)
    return false
  }
}

/**
 * Handle errors with user-friendly messages
 */
function handleError(error, context = 'Operation') {
  console.error(`${context} error:`, error)

  let message = error.message || 'Unknown error occurred'

  if (message.includes('not found')) {
    message = 'Block not found. It may have been deleted.'
  } else if (message.includes('permission')) {
    message = 'Permission denied. Check plugin permissions.'
  } else if (message.includes('network')) {
    message = 'Network error. Check your connection.'
  }

  logseq.UI.showMsg(`${context} failed: ${message}`, 'error', { timeout: 5000 })
}

// Initialize plugin
logseq.ready(main).catch(console.error)
```

---

## Next Steps & Enhancements

### Ideas for Improvement

1. **Keyboard Shortcut:**

   ```javascript
   logseq.App.registerCommandShortcut('mod+shift+d', async () => {
     const block = await logseq.Editor.getCurrentBlock()
     if (block) {
       // Duplicate current block
     }
   })
   ```

2. **Duplicate to Different Location:**

   - Add UI to select target page/block
   - Support duplicating to clipboard

3. **Advanced Options:**

   - Duplicate without properties but keep children
   - Duplicate and modify (e.g., add timestamp)
   - Batch duplicate multiple selected blocks

4. **Performance:**

   - Add progress indicator for large trees
   - Batch property updates
   - Optimize recursive duplication

5. **Integration:**
   - Add to slash command menu
   - Add to command palette
   - Support file-based graphs (with limitations)

---

## Troubleshooting

### Plugin doesn't load

- Check `package.json` syntax
- Verify file paths
- Check browser console for errors

### Properties not duplicated

- Ensure it's a DB graph
- Check property names (system properties are skipped)
- Verify property permissions

### Children not duplicated

- Check "Duplicate children" setting
- Verify recursive function isn't hitting errors
- Check console for error messages

### Performance issues

- Large trees may take time
- Consider adding progress indicator
- Test with smaller structures first

---

## Conclusion

Congratulations! You've built a complete Logseq plugin that:

✅ Duplicates blocks with all content
✅ Preserves all properties (DB graph feature)
✅ Recursively duplicates children
✅ Provides user configuration
✅ Handles errors gracefully
✅ Ready for publication

**What You Learned:**

- Logseq plugin project structure
- Working with DB graph properties
- Block manipulation and traversal
- User settings and configuration
- Error handling and user feedback
- Testing and debugging strategies

**Next Tutorial Ideas:**

- Build a property sync plugin
- Create a custom query interface
- Develop a task management system
- Build a graph analytics plugin

Happy coding! 🚀
