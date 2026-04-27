# Tutorial: Build a Schedule Today Command for Logseq DB Graphs

> **Last revised:** 2026-04-26

**What You'll Build:** A Logseq plugin command that sets the current block's `Scheduled` property to today.

**Time Required:** 20-30 minutes

**Prerequisites:**

- Basic JavaScript knowledge
- Node.js and npm installed
- Logseq installed with a DB graph
- A code editor

**What You'll Learn:**

- How to create a minimal Logseq plugin
- How to register a command palette command
- How to write to a built-in DB property from a plugin
- How to avoid the `Plugins can only upsert its own properties` error

---

## Table of Contents

1. [Create the Plugin Project](#step-1-create-the-plugin-project)
2. [Load a Minimal Plugin](#step-2-load-a-minimal-plugin)
3. [Register the Command](#step-3-register-the-command)
4. [Schedule the Current Block for Today](#step-4-schedule-the-current-block-for-today)
5. [Test the Command](#step-5-test-the-command)
6. [Troubleshoot Common Mistakes](#step-6-troubleshoot-common-mistakes)

---

## Step 1: Create the Plugin Project

If you already have a plugin project, you can skip to [Step 2](#step-2-load-a-minimal-plugin).

### 1.1 Create the project directory

```bash
mkdir logseq-schedule-today
cd logseq-schedule-today
npm init -y
npm install @logseq/libs
mkdir src
touch src/index.js
```

Your directory should now look like this:

```text
logseq-schedule-today/
├── node_modules/
├── src/
│   └── index.js
├── package-lock.json
└── package.json
```

### 1.2 Add Logseq plugin metadata

Edit `package.json` so it includes a `logseq` section:

```json
{
  "name": "logseq-schedule-today",
  "version": "1.0.0",
  "description": "Schedule the current block for today in a DB graph",
  "main": "src/index.js",
  "logseq": {
    "id": "logseq-schedule-today",
    "title": "Schedule Today",
    "icon": "📅",
    "main": "src/index.js"
  },
  "dependencies": {
    "@logseq/libs": "^0.2.12"
  }
}
```

**Result:** You now have a plugin folder that Logseq can load.

---

## Step 2: Load a Minimal Plugin

Add this code to `src/index.js`:

```javascript
import '@logseq/libs'

async function main() {
  const isDbGraph = await logseq.App.checkCurrentIsDbGraph()

  if (!isDbGraph) {
    await logseq.UI.showMsg(
      'Schedule Today works only in DB graphs.',
      'warning'
    )
    return
  }

  await logseq.UI.showMsg('Schedule Today plugin loaded.', 'success')
}

logseq.ready(main).catch(console.error)
```

### 2.1 Load the plugin in Logseq

1. Open Logseq.
2. Go to `Settings -> Plugins`.
3. Choose `Load unpacked plugin`.
4. Select your `logseq-schedule-today` folder.

**Result:** You should see a `Schedule Today plugin loaded.` toast in a DB graph.

If you open the plugin in a file graph, you should see a warning instead.

---

## Step 3: Register the Command

Next, register a command palette entry so you can trigger your plugin.

Replace `src/index.js` with this version:

```javascript
import '@logseq/libs'

function registerCommands() {
  logseq.App.registerCommandPalette(
    {
      key: 'schedule-current-block-today',
      label: 'Schedule current block for today',
      keybinding: {
        binding: 'mod+shift+t',
      },
    },
    async () => {
      await logseq.UI.showMsg('Command reached.', 'success')
    }
  )
}

async function main() {
  const isDbGraph = await logseq.App.checkCurrentIsDbGraph()

  if (!isDbGraph) {
    await logseq.UI.showMsg(
      'Schedule Today works only in DB graphs.',
      'warning'
    )
    return
  }

  registerCommands()
  await logseq.UI.showMsg('Schedule Today plugin loaded.', 'success')
}

logseq.ready(main).catch(console.error)
```

### 3.1 Verify the command exists

1. Reload the plugin.
2. Open the command palette with `Cmd+Shift+P`.
3. Search for `Schedule current block for today`.
4. Run it.

**Result:** You should see a `Command reached.` toast.

At this point, your plugin command exists, but it does not change any block yet.

---

## Step 4: Schedule the Current Block for Today

Now replace the test toast with the real scheduling logic.

Replace `src/index.js` with this final version:

```javascript
import '@logseq/libs'

function getStartOfTodayMs() {
  const today = new Date()
  today.setHours(0, 0, 0, 0)
  return today.getTime()
}

async function scheduleCurrentBlockToday() {
  const block = await logseq.Editor.getCurrentBlock()

  if (!block) {
    await logseq.UI.showMsg('Place the cursor in a block first.', 'warning')
    return
  }

  await logseq.Editor.upsertBlockProperty(
    block.uuid,
    'logseq.property/scheduled',
    getStartOfTodayMs()
  )

  await logseq.UI.showMsg('Scheduled for today.', 'success')
}

function registerCommands() {
  logseq.App.registerCommandPalette(
    {
      key: 'schedule-current-block-today',
      label: 'Schedule current block for today',
      keybinding: {
        binding: 'mod+shift+t',
      },
    },
    scheduleCurrentBlockToday
  )
}

async function main() {
  const isDbGraph = await logseq.App.checkCurrentIsDbGraph()

  if (!isDbGraph) {
    await logseq.UI.showMsg(
      'Schedule Today works only in DB graphs.',
      'warning'
    )
    return
  }

  registerCommands()
  await logseq.UI.showMsg('Schedule Today plugin loaded.', 'success')
}

logseq.ready(main).catch(console.error)
```

### Why this version works

- It uses the built-in property ident `logseq.property/scheduled`.
- It writes a raw epoch timestamp in milliseconds.
- It does not try to create a new plugin-owned property.

Two details matter here:

1. Do **not** use `scheduled` as the property key.
   That creates a plugin property instead of targeting Logseq's built-in `Scheduled` property.

2. Do **not** pass a `Date` object directly.
   The built-in `Scheduled` property expects a millisecond timestamp, so use `today.getTime()`.

**Result:** Your command now updates the block under the cursor.

---

## Step 5: Test the Command

### 5.1 Try it on a block

1. Open a DB graph.
2. Create a block such as `Follow up with the team`.
3. Put the cursor inside that block.
4. Run `Schedule current block for today` from the command palette, or press `Cmd+Shift+T`.

### 5.2 Confirm the result

After running the command, you should see:

- A `Scheduled for today.` toast
- A `Scheduled` property below the block
- Today's journal date shown as the property value

If the block was not already treated as a task, Logseq will still attach the scheduled property correctly.

**Result:** You now have a working custom command that schedules the current block for today.

---

## Step 6: Troubleshoot Common Mistakes

### Error: `Plugins can only upsert its own properties`

You are probably using the wrong key.

Use this:

```javascript
'logseq.property/scheduled'
```

Not this:

```javascript
'scheduled'
```

### The command runs, but the date is wrong

Make sure you reset the time before calling `getTime()`:

```javascript
const today = new Date()
today.setHours(0, 0, 0, 0)
```

Without that step, you store the current time instead of the start of the day.

### Nothing happens when you run the command

Make sure:

- You are in a DB graph
- The plugin is loaded
- The cursor is inside a block

### The command is missing from the command palette

Reload the plugin after editing `src/index.js`, then open `Cmd+Shift+P` and search again.

---

## Next Steps

Once this version works, you can extend it in a few useful directions:

1. Add a second command for `Deadline today` using `logseq.property/deadline`.
2. Register a slash command that calls the same function.
3. Support selected blocks instead of only the current block.
4. Extract the date helper so you can add `Schedule tomorrow` and `Schedule next week` commands.

For more background on DB graph properties and plugin APIs, see the [DB Graph Plugin Guide](./db-graphs.md).
