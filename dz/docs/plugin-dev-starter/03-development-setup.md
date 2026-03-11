# Development Setup

> **Last revised:** 2026-03-10

Complete guide to setting up your Logseq plugin development environment.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Environment Setup](#environment-setup)
- [Project Templates](#project-templates)
- [Build Tools](#build-tools)
- [Debugging](#debugging)
- [Hot Reload](#hot-reload)

## Prerequisites

### Required Software

1. **Node.js** (v14 or higher)

   ```bash
   # Check version
   node --version

   # Download from nodejs.org
   ```

2. **Package Manager**

   - npm (comes with Node.js)
   - Or yarn: `npm install -g yarn`
   - Or pnpm: `npm install -g pnpm`

3. **Logseq Desktop App**

   - Download from [logseq.com](https://logseq.com)
   - Enable Developer Mode in Settings

4. **Code Editor** (recommended: VS Code)
   - Install [VS Code](https://code.visualstudio.com/)
   - Install TypeScript extension

### Optional Tools

- **Git** - Version control
- **TypeScript** - Type safety
- **ESLint** - Code linting
- **Prettier** - Code formatting

## Environment Setup

### 1. Enable Developer Mode

In Logseq:

1. Open **Settings** (`Cmd/Ctrl + ,`)
2. Go to **Advanced**
3. Enable **Developer mode**
4. Restart Logseq

### 2. Create Plugin Directory

```bash
# Create and navigate to plugin directory
mkdir ~/logseq-plugins
cd ~/logseq-plugins

# Create your first plugin
mkdir my-first-plugin
cd my-first-plugin
```

### 3. Initialize Node Project

```bash
# Initialize package.json
npm init -y

# Install Logseq SDK
npm install @logseq/libs

# Install TypeScript dev dependencies
npm install -D typescript @types/node

# Create TypeScript config
npx tsc --init
```

### 4. Configure TypeScript

Update `tsconfig.json`:

```json
{
  "compilerOptions": {
    "target": "ES2017",
    "module": "ESNext",
    "moduleResolution": "node",
    "lib": ["ES2017", "DOM"],
    "outDir": "./dist",
    "rootDir": "./src",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "resolveJsonModule": true,
    "declaration": true,
    "sourceMap": true
  },
  "include": ["src/**/*"],
  "exclude": ["node_modules", "dist"]
}
```

### 5. Update package.json

```json
{
  "name": "my-first-plugin",
  "version": "0.0.1",
  "description": "My first Logseq plugin",
  "main": "dist/index.js",
  "scripts": {
    "build": "tsc",
    "dev": "tsc --watch",
    "clean": "rm -rf dist"
  },
  "logseq": {
    "id": "my-first-plugin",
    "title": "My First Plugin",
    "icon": "🚀"
  },
  "devDependencies": {
    "@types/node": "^18.0.0",
    "typescript": "^4.9.0"
  },
  "dependencies": {
    "@logseq/libs": "^0.2.12"
  }
}
```

### 6. Create Source Files

```bash
# Create src directory and main file
mkdir src
touch src/index.ts
```

**src/index.ts**:

```typescript
import '@logseq/libs'

const main = async () => {
  console.log('Plugin loaded!')

  logseq.UI.showMsg('Hello from my first plugin!', 'success')
}

logseq.ready(main).catch(console.error)
```

## Project Templates

### Basic Template Structure

```
my-plugin/
├── src/
│   ├── index.ts           # Main entry point
│   ├── commands.ts        # Command definitions
│   ├── ui.ts              # UI components
│   └── utils.ts           # Helper functions
├── dist/                  # Compiled output (generated)
├── node_modules/          # Dependencies (generated)
├── package.json           # Package manifest
├── tsconfig.json          # TypeScript config
├── .gitignore            # Git ignore rules
└── README.md             # Documentation
```

### Recommended .gitignore

```gitignore
# Dependencies
node_modules/
package-lock.json
yarn.lock
pnpm-lock.yaml

# Build output
dist/
*.js
*.js.map

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Logs
*.log
npm-debug.log*

# Environment
.env
.env.local
```

### Advanced Template with Bundler

For more complex plugins, use a bundler:

```bash
npm install -D esbuild

# Or webpack
npm install -D webpack webpack-cli ts-loader
```

**build.js** (esbuild):

```javascript
const esbuild = require('esbuild')

esbuild
  .build({
    entryPoints: ['src/index.ts'],
    bundle: true,
    outfile: 'dist/index.js',
    format: 'iife',
    platform: 'browser',
    target: 'es2017',
    minify: process.env.NODE_ENV === 'production',
    sourcemap: true,
  })
  .catch(() => process.exit(1))
```

Update package.json scripts:

```json
{
  "scripts": {
    "build": "node build.js",
    "dev": "node build.js && node build.js --watch"
  }
}
```

## Build Tools

### TypeScript Only

Simple compilation:

```bash
# One-time build
npm run build

# Watch mode
npm run dev
```

### With esbuild

Fast bundling and minification:

```bash
# Install
npm install -D esbuild

# Build script
node -e "require('esbuild').build({
  entryPoints: ['src/index.ts'],
  bundle: true,
  outfile: 'dist/index.js'
})"
```

### With Webpack

Advanced configuration:

**webpack.config.js**:

```javascript
const path = require('path')

module.exports = {
  entry: './src/index.ts',
  mode: 'development',
  devtool: 'source-map',
  module: {
    rules: [
      {
        test: /\.tsx?$/,
        use: 'ts-loader',
        exclude: /node_modules/,
      },
    ],
  },
  resolve: {
    extensions: ['.tsx', '.ts', '.js'],
  },
  output: {
    filename: 'index.js',
    path: path.resolve(__dirname, 'dist'),
  },
}
```

## Debugging

### Console Logging

Access Logseq DevTools:

- **View → Toggle Developer Tools** (or `Cmd/Ctrl + Shift + I`)

```typescript
// Basic logging
console.log('Debug info:', data)
console.error('Error occurred:', error)
console.warn('Warning:', message)

// With logseq helper
import '@logseq/libs'

console.log('[MyPlugin]', 'Starting...')
```

### Source Maps

Enable source maps in `tsconfig.json`:

```json
{
  "compilerOptions": {
    "sourceMap": true
  }
}
```

Now you can:

- Set breakpoints in original TypeScript
- Step through code
- Inspect variables

### Debug Configuration

**VS Code launch.json**:

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "type": "node",
      "request": "launch",
      "name": "Debug Plugin",
      "runtimeExecutable": "/path/to/Logseq.app",
      "console": "integratedTerminal"
    }
  ]
}
```

### Error Handling

```typescript
// Wrap in try-catch
try {
  const block = await logseq.Editor.getBlock(uuid)
  // ... process block
} catch (error) {
  console.error('Failed to get block:', error)
  logseq.UI.showMsg(`Error: ${error.message}`, 'error')
}

// Global error handler
window.addEventListener('error', (event) => {
  console.error('Unhandled error:', event.error)
})
```

## Hot Reload

### Manual Reload

After making changes:

1. Save files
2. Rebuild: `npm run build`
3. In Logseq: **Settings → Plugins → Reload plugin button**

### Auto Rebuild

Use watch mode:

```bash
npm run dev
```

Changes → Auto rebuild → Manual reload in Logseq

### Automatic Reload (Advanced)

Create a watch script with plugin reload:

**scripts/watch.js**:

```javascript
const chokidar = require('chokidar')
const { exec } = require('child_process')

console.log('Watching for changes...')

chokidar.watch('src/**/*.ts').on('change', (path) => {
  console.log(`File changed: ${path}`)
  exec('npm run build', (error, stdout) => {
    if (error) {
      console.error('Build failed:', error)
    } else {
      console.log('✅ Build complete')
      // Optionally trigger Logseq plugin reload via IPC
    }
  })
})
```

## VS Code Integration

### Recommended Extensions

Install these VS Code extensions:

- **TypeScript and JavaScript Language Features** (built-in)
- **ESLint** - Code linting
- **Prettier** - Code formatting
- **Error Lens** - Inline error display

### Settings

**.vscode/settings.json**:

```json
{
  "typescript.tsdk": "node_modules/typescript/lib",
  "editor.formatOnSave": true,
  "editor.defaultFormatter": "esbenp.prettier-vscode",
  "typescript.preferences.importModuleSpecifier": "relative"
}
```

### Tasks

**.vscode/tasks.json**:

```json
{
  "version": "2.0.0",
  "tasks": [
    {
      "label": "Build Plugin",
      "type": "npm",
      "script": "build",
      "group": {
        "kind": "build",
        "isDefault": true
      }
    },
    {
      "label": "Watch Plugin",
      "type": "npm",
      "script": "dev",
      "isBackground": true,
      "problemMatcher": "$tsc-watch"
    }
  ]
}
```

## Testing Your Setup

Create a simple test:

**src/index.ts**:

```typescript
import '@logseq/libs'

const main = async () => {
  console.log('🚀 Plugin starting...')

  // Test API access
  const graph = await logseq.App.getCurrentGraph()
  console.log('Current graph:', graph?.name)

  // Test UI
  logseq.UI.showMsg('Setup test successful!', 'success')

  // Test command registration
  logseq.Editor.registerSlashCommand('Test Command', () => {
    logseq.UI.showMsg('Command works!', 'info')
  })

  console.log('✅ Plugin loaded successfully')
}

logseq.ready(main).catch(console.error)
```

Build and test:

```bash
npm run build
# Load in Logseq and test
```

## Troubleshooting

### Common Issues

**Plugin not loading:**

- Check console for errors
- Verify `package.json` has `logseq` field
- Ensure `main` points to correct file
- Check file permissions

**TypeScript errors:**

- Run `npm install` to install dependencies
- Check `tsconfig.json` configuration
- Ensure `@logseq/libs` is imported

**Build failures:**

- Clear dist folder: `npm run clean`
- Reinstall: `rm -rf node_modules && npm install`
- Check Node.js version: `node --version`

### Getting Help

- Check [Logseq Discord](https://discord.gg/logseq) #plugin-dev channel
- Search [GitHub Issues](https://github.com/logseq/logseq/issues)
- Review [Plugin Samples](https://github.com/logseq/logseq-plugin-samples)

## Next Steps

- ✅ Environment is set up!
- 📝 Continue to [Blocks and Pages](05-blocks-and-pages.md)
- 🎨 Learn [UI Components](06-ui-components.md)
- ⌨️ Master [Commands](07-commands-shortcuts.md)

---

Your development environment is ready to go! 🎉
