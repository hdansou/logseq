# Setting Up Logseq CLI from Source on macOS

This guide provides step-by-step instructions for setting up the `logseq` command in your terminal from the source code. After following this guide, you'll be able to run `logseq` commands directly in your terminal, with changes to the source code automatically reflected.

## Table of Contents

- [Goal](#goal)
- [Prerequisites](#prerequisites)
- [Setup Steps (The Working Method)](#setup-steps-the-working-method)
- [Verification](#verification)
- [Usage](#usage)
- [What NOT to Do](#what-not-to-do)
- [Troubleshooting](#troubleshooting)
- [Development Workflow](#development-workflow)
- [Testing](#testing)

## Goal

By the end of this guide, you will have:

- ✅ The `logseq` command available in your terminal
- ✅ Source code linked (not copied) so any code changes are immediately available
- ✅ The ability to run commands like `logseq list`, `logseq import-edn`, etc.

## Prerequisites

You need these installed on your macOS system:

1. **Node.js** (version 22 or higher)

   - Check: `node --version`
   - If using nvm: `nvm install 22 && nvm use 22`

2. **Yarn**

   - Check: `yarn --version`
   - Install: `npm install -g yarn`

3. **Logseq Database Version desktop app** (to use the CLI with graphs)
   - Download from: https://github.com/logseq/logseq/#-database-version

## Setup Steps (The Working Method)

This is the exact method that works on macOS with nvm. Follow these steps in order.

### Step 1: Navigate to the CLI Source Directory

```bash
cd /Users/dzu/Projects/src/github.com/logseq/deps/cli
```

Replace with your actual path to the `deps/cli` directory.

### Step 2: Install Dependencies

```bash
yarn install
```

**Expected output:**

```
yarn install v1.22.22
[1/5] Validating package.json...
[2/5] Resolving packages...
success Already up-to-date.
Done in 0.04s.
```

### Step 3: Test from Source (Optional but Recommended)

Verify the CLI works before setting up the global command:

```bash
node cli.mjs -h
```

You should see the help output with all commands including `import-edn`.

### Step 4: Set Up the Global `logseq` Command

**Important:** `yarn link` does NOT work properly for this setup. Use the manual symlink method below.

#### Remove Any Existing Installation

```bash
npm uninstall -g @logseq/cli
```

#### Create Symlinks to Source Directory

**For nvm users (most common on macOS):**

```bash
# Create symlink to the source directory
ln -sf $(pwd) ~/.nvm/versions/node/$(node --version)/lib/node_modules/@logseq/cli

# Create the executable symlink
ln -sf ~/.nvm/versions/node/$(node --version)/lib/node_modules/@logseq/cli/cli.mjs \
       ~/.nvm/versions/node/$(node --version)/bin/logseq
```

**For system Node.js (if not using nvm):**

```bash
NPM_PREFIX=$(npm config get prefix)
ln -sf $(pwd) $NPM_PREFIX/lib/node_modules/@logseq/cli
ln -sf $NPM_PREFIX/lib/node_modules/@logseq/cli/cli.mjs $NPM_PREFIX/bin/logseq
```

## Verification

Verify everything is set up correctly:

### 1. Check the `logseq` command exists

```bash
which logseq
```

**Expected output (for nvm):**

```
/Users/dzu/.nvm/versions/node/v22.20.0/bin/logseq
```

### 2. Verify it's using the source code (symlinked, not copied)

```bash
ls -la $(which logseq)
```

**Should show:** A symlink arrow (`->`) pointing to your node_modules

```bash
readlink ~/.nvm/versions/node/$(node --version)/lib/node_modules/@logseq/cli
```

**Should output:** Your source directory path, e.g., `/Users/dzu/Projects/src/github.com/logseq/deps/cli`

### 3. Test the command

```bash
logseq -h
```

**Should show:** Full help including the `import-edn` command

```bash
logseq -v
```

**Should show:** `0.3.0` (or current version)

```bash
logseq list
```

**Should show:** Your DB graphs (requires Logseq Desktop app running)

## Usage

Now you can use the `logseq` command anywhere in your terminal!

### Common Commands

```bash
# List all available graphs
logseq list

# Show graph information
logseq show my-graph

# Export graph to EDN
logseq export-edn my-graph -f output.edn

# Import EDN into a graph
logseq import-edn -g my-graph -f input.edn

# Search in a graph
logseq search my-graph "search term"

# Query a graph
logseq query my-graph '[:find (pull ?b [*]) :where [?b :block/title]]'

# Get help for any command
logseq help import-edn
```

### Two Ways to Run

1. **Using the global command** (recommended):

   ```bash
   logseq <command> [options]
   ```

2. **Direct from source** (always works):
   ```bash
   cd /Users/dzu/Projects/src/github.com/logseq/deps/cli
   node cli.mjs <command> [options]
   ```

## What NOT to Do

❌ **Don't use `yarn link`**

- It doesn't create a proper symlink and causes version issues
- The global command won't reflect your source code changes

❌ **Don't use `npm install -g .`**

- It creates a **copy**, not a symlink
- You'll need to reinstall every time you change the source code
- The `logseq` command won't stay in sync with your development

✅ **DO use the manual symlink method** (shown in Step 4 above)

- Changes to source code are immediately reflected
- No need to reinstall after editing code

## Development Workflow

### Making Changes

1. Edit the ClojureScript source files in `src/logseq/cli/`
2. Test your changes:
   ```bash
   node cli.mjs <command>
   ```
3. If using symlink installation, the global `logseq` command automatically uses your changes
4. If using npm install, reinstall: `npm install -g .`

### Directory Structure

```
deps/cli/
├── cli.mjs              # Entry point
├── package.json         # npm package configuration
├── src/
│   └── logseq/
│       └── cli/
│           ├── cli.cljs           # Main CLI namespace
│           ├── spec.cljs          # Command specifications
│           ├── commands/          # Command implementations
│           │   ├── graph.cljs
│           │   ├── search.cljs
│           │   ├── query.cljs
│           │   ├── export.cljs
│           │   ├── export-edn.cljs
│           │   ├── import-edn.cljs
│           │   ├── append.cljs
│           │   └── mcp-server.cljs
│           └── common/            # Shared utilities
├── vendor/              # Vendored dependencies
└── test/               # Test files
```

### Key Files to Know

- **cli.mjs** (line 1-15): Entry point that bootstraps the ClojureScript runtime
- **src/logseq/cli.cljs** (line 70-110): Command table defining all available commands
- **src/logseq/cli/spec.cljs**: Command-line argument specifications
- **src/logseq/cli/commands/**: Individual command implementations

### Adding a New Command

1. Create a new file in `src/logseq/cli/commands/your-command.cljs`
2. Add the command to the table in `src/logseq/cli.cljs`:
   ```clojure
   {:cmds ["your-command"] :desc "Your command description"
    :fn (lazy-load-fn 'logseq.cli.commands.your-command/your-fn)
    :spec cli-spec/your-command}
   ```
3. Add the spec in `src/logseq/cli/spec.cljs`:
   ```clojure
   (def your-command
     {:option {:alias :o
               :desc "Option description"}})
   ```

## Testing

The CLI uses nbb-test-runner for testing.

### Run All Tests

```bash
yarn test
```

### Run Specific Tests

```bash
# Run tests with :focus metadata
yarn test -i focus
```

### Show Test Options

```bash
yarn test -H
```

## Troubleshooting

### `logseq` command not found

**Check:**

```bash
which logseq
echo $PATH
```

**Fix:**

- Ensure nvm is initialized in your shell profile (`~/.zshrc` or `~/.bash_profile`)
- Restart your terminal
- Run: `hash -r` to refresh command cache

### `import-edn` or other new commands are missing

**This is the exact problem we solved!**

**Cause:** The global `logseq` command is pointing to an old copied version, not the source.

**Fix:**

```bash
# 1. Verify the problem - compare outputs
node cli.mjs -h     # Should show import-edn
logseq -h           # Missing import-edn? Then you have this issue

# 2. Check if it's a symlink or a copy
readlink ~/.nvm/versions/node/$(node --version)/lib/node_modules/@logseq/cli

# If it returns nothing or an error, it's a copy, not a symlink

# 3. Fix it with the symlink method
npm uninstall -g @logseq/cli
ln -sf $(pwd) ~/.nvm/versions/node/$(node --version)/lib/node_modules/@logseq/cli
ln -sf ~/.nvm/versions/node/$(node --version)/lib/node_modules/@logseq/cli/cli.mjs \
       ~/.nvm/versions/node/$(node --version)/bin/logseq

# 4. Verify
logseq -h  # Should now show import-edn
```

### Changes to source code not reflected

**Cause:** Using a copy instead of symlink.

**Verify:**

```bash
readlink ~/.nvm/versions/node/$(node --version)/lib/node_modules/@logseq/cli
```

If this returns nothing, you have a copy. Follow the fix above.

### `Error: The database version's desktop app is not installed`

**Fix:** Install Logseq Desktop (Database Version):
https://github.com/logseq/logseq/#-database-version

### Permission errors during `yarn install`

**Fix:**

```bash
# Use nvm (recommended)
# Or fix npm permissions
sudo chown -R $(whoami) ~/.npm
```

## Quick Reference

### The Correct Setup Commands (Copy-Paste)

```bash
# Navigate to source
cd /Users/dzu/Projects/src/github.com/logseq/deps/cli

# Install dependencies
yarn install

# Remove old installation
npm uninstall -g @logseq/cli

# Create symlinks (for nvm)
ln -sf $(pwd) ~/.nvm/versions/node/$(node --version)/lib/node_modules/@logseq/cli
ln -sf ~/.nvm/versions/node/$(node --version)/lib/node_modules/@logseq/cli/cli.mjs \
       ~/.nvm/versions/node/$(node --version)/bin/logseq

# Verify
logseq -h
```

### Verify Your Setup is Correct

Both of these should show identical output with `import-edn` included:

```bash
node cli.mjs -h
logseq -h
```

This should output your source directory path:

```bash
readlink ~/.nvm/versions/node/$(node --version)/lib/node_modules/@logseq/cli
```

## Summary

✅ **What you now have:**

- The `logseq` command works globally in your terminal
- Source code is symlinked (any edits are immediately available)
- All commands including `import-edn` are available

✅ **Key takeaway:**

- Never use `yarn link` or `npm install -g .` for this project
- Always use the manual symlink method shown in Step 4

✅ **Next steps:**

- Use `logseq` commands from anywhere
- Edit source code in `src/logseq/cli/` and test immediately
- See [README.md](../README.md) for command usage examples

## Additional Resources

- **Usage Examples**: [README.md](../README.md)
- **Logseq DB Docs**: https://github.com/logseq/docs/blob/master/db-version.md
- **Issues**: https://github.com/logseq/logseq/issues
