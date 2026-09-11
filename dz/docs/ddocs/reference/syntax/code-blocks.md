---
title: 'Code Blocks Reference'
status: 🟢
since-version: 'N/A'
quadrant: 'Reference'
last-verified: '2026-09-11'
verified-against: 'be800f1711'
reviewStatus: audited
graphType: [db]
platform: 'All'
related-docs:
  - 'tutorials/getting-started/03-basic-editing.md'
  - 'reference/formatting/markdown-syntax.md'
  - 'reference/features/calculator.md'
---

> **Last revised:** 2026-09-11

# Code Blocks Reference

Complete reference for code blocks in Logseq DB graphs. The editor is CodeMirror 5, pinned at `5.65.18` (`package.json:161`). All source paths are at `be800f1711`.

## Basic Syntax

### Inline Code

Wrap code in single backticks:

```markdown
Use the `console.log()` function to output
```

**Renders as:** Use the `console.log()` function to output. Inline `Code` and `Verbatim` nodes render as `<code>` (`src/main/frontend/components/block.cljs:2016-2017`).

### Code Blocks Are Nodes

In a DB graph, a code block is a block whose `:logseq.property.node/display-type` is `:code`. Its language is stored in `:logseq.property.code/lang`, and its `:block/title` is the raw code with no fences. Such a block renders with the code editor (`block.cljs:2582-2583`). See [Creating Code Blocks](#creating-code-blocks).

A complete fenced block can also end up inside an ordinary block's text:

````markdown
```javascript
function greet(name) {
  return `Hello, ${name}!`
}
```
````

mldoc parses it as a `Src` node, and the block renders it with the same editor (`block.cljs:5116-5124`). The language picker does not change the language of such a fence. Code evaluation and plugin renderers apply only to fences like this (see [Limitations](#limitations)).

## Creating Code Blocks

### Method 1: Slash Command

Type `/` and pick **Code block** (`src/main/frontend/commands.cljs:246-249`, label `src/resources/dicts/en.edn:522-523`). This runs `code-block-steps`, which turns the current block into a code block and exits editing (`src/main/frontend/commands.cljs:134-138`).

The slash command has no language dropdown. The block gets the **last language you used**, read from the `:logseq.kv/latest-code-lang` key-value entry (`src/main/frontend/handler/events.cljs:374-378`, `:400-406`). To change it, use the language button on the rendered code block.

### Method 2: Triple Backticks

Type three backticks in an empty block. Backticks auto-pair, so three keystrokes produce six characters (`src/main/frontend/handler/editor.cljs:3405-3407`). When the block's whole content is exactly six backticks, Logseq converts it into a code block with an empty title (`editor.cljs:3559-3563`).

### Method 3: Calculator

**Calculator** turns the block into a code block with language `calc` and focuses the editor (`commands.cljs:119-123`, `:331-334`). See the [Calculator Reference](../features/calculator.md).

### Display Type and the Code Tag Stay in Sync

The built-in class for code blocks is `:logseq.class/Code-block`, titled "Code" (`deps/db/src/logseq/db/frontend/class.cljs:76-79`). The DB worker keeps the class and the display type in step:

- Setting display type `:code` adds that tag.
- Adding the tag sets display type `:code`. If the block has no language yet, it also applies the latest code language.

Source: `src/main/frontend/worker/pipeline.cljs:316-336`.

> **Removed:** this page previously listed a third method, a `Ctrl/Cmd + Shift + C` keyboard shortcut. In `src/main/frontend/modules/shortcut/config.cljs:229` that binding is `:editor/copy-text`; no shortcut in the config creates a code block.

## Code Block Toolbar

Hovering a code block shows two buttons:

- **Language button.** It shows the current language, or "Choose language" (`block.cljs:4950`, `en.edn:490`). It opens a picker listing every `CodeMirror.modeInfo` name. Choosing one does three things (`block.cljs:4882-4894`, `:4927-4948`):
  - switches the editor mode;
  - sets `:logseq.property.code/lang` to that name, for example `Python`;
  - stores the name as `:logseq.kv/latest-code-lang`.

  The picker acts only on blocks whose display type is `:code` (`block.cljs:4891-4893`).
- **Copy.** Copies the editor's contents to the clipboard and shows a "copied" notification (`block.cljs:4952-4961`).

## Supported Languages

A language string is resolved in two steps:

1. The code block component maps `edn`, `clj`, `cljc`, `cljs` and `clojurescript` to `clojure` (`block.cljs:4915`).
2. `text->cm-mode` lower-cases the string and calls `CodeMirror.findModeByName` (`src/main/frontend/extensions/code.cljs:374-389`). That function matches a mode's **name or alias**, case-insensitively (`codemirror/mode/meta.js:212-220` in CodeMirror 5.65.18). If it finds one, the editor uses that mode's MIME type. If not, the lower-cased string is passed through as a mode name.

So an identifier gets highlighting only if it is a CodeMirror name or alias, or exactly the name of a registered mode. `code.cljs` imports 122 `codemirror/mode/*` files.

The tables below were checked by running `findModeByName` from CodeMirror 5.65.18 on each identifier.

### Popular Languages

| Language      | Identifier            | Mode         | Not recognized |
| ------------- | --------------------- | ------------ | -------------- |
| JavaScript    | `javascript`, `js`    | `javascript` |                |
| TypeScript    | `typescript`, `ts`    | `javascript` |                |
| Python        | `python`              | `python`     | `py`           |
| Java          | `java`                | `clike`      |                |
| C/C++         | `c`, `cpp`, `c++`     | `clike`      |                |
| C#            | `csharp`, `cs`        | `clike`      |                |
| Go            | `go`                  | `go`         | `golang`       |
| Rust          | `rust`                | `rust`       |                |
| Ruby          | `ruby`, `rb`          | `ruby`       |                |
| PHP           | `php`                 | `php`        |                |
| Swift         | `swift`               | `swift`      |                |
| Kotlin        | `kotlin`              | `clike`      |                |
| R             | `r`                   | `r`          |                |
| MATLAB/Octave | `octave`              | `octave`     | `matlab`       |
| SQL           | `sql`                 | `sql`        |                |
| Bash/Shell    | `bash`, `shell`, `sh` | `shell`      |                |

### Web Technologies

| Language  | Identifier     | Mode           |
| --------- | -------------- | -------------- |
| HTML      | `html`         | `htmlmixed`    |
| CSS       | `css`          | `css`          |
| SCSS/Sass | `scss`, `sass` | `css` / `sass` |
| Less      | `less`         | `css`          |
| JSX       | `jsx`          | `jsx`          |
| Vue       | `vue`          | `vue` (not a name or alias; works because a mode named `vue` is registered) |
| XML       | `xml`          | `xml`          |
| JSON      | `json`         | `javascript`   |
| YAML      | `yaml`, `yml`  | `yaml`         |
| TOML      | `toml`         | `toml`         |

### Markup & Documentation

| Language         | Identifier       | Notes                                        |
| ---------------- | ---------------- | -------------------------------------------- |
| Markdown         | `markdown`       | `markdown` mode. `md` is not recognized      |
| reStructuredText | `rst`            | `rst` mode                                   |
| LaTeX            | `latex`, `tex`   | `stex` mode                                  |

Note: AsciiDoc and Org Mode have no imported CodeMirror mode, so they get no syntax highlighting.

### Functional & Academic

| Language    | Identifier                                  | Mode         |
| ----------- | ------------------------------------------- | ------------ |
| Haskell     | `haskell` (`hs` is not recognized)          | `haskell`    |
| Erlang      | `erlang`                                    | `erlang`     |
| Clojure     | `clojure`, `clj`, `cljs`, `cljc`, `edn`     | `clojure`    |
| Scheme      | `scheme`                                    | `scheme`     |
| Common Lisp | `commonlisp`, `lisp`                        | `commonlisp` |
| OCaml       | `ocaml`                                     | `mllike`     |
| F#          | `fsharp`                                    | `mllike`     |
| Elm         | `elm`                                       | `elm`        |
| Scala       | `scala`                                     | `clike`      |

Note: Elixir has no imported CodeMirror mode, so it gets no syntax highlighting.

### Data & Query Languages

| Language | Identifier                   | Notes            |
| -------- | ---------------------------- | ---------------- |
| SQL      | `sql`, `mysql`, `postgresql` | `sql` mode       |
| SPARQL   | `sparql`                     | `sparql` mode    |
| Cypher   | `cypher`                     | `cypher` mode    |

Note: GraphQL and Datalog have no imported CodeMirror mode, so they get no syntax highlighting.

### Configuration & DevOps

| Language   | Identifier            | Notes                                         |
| ---------- | --------------------- | --------------------------------------------- |
| Dockerfile | `dockerfile`          | `dockerfile` mode. `docker` is not recognized |
| Nginx      | `nginx`               | `nginx` mode                                  |
| Properties | `properties`, `ini`   | `properties` mode                             |

Note: Apache and Terraform have no imported CodeMirror mode, so they get no syntax highlighting.

### Specialized Languages

| Language  | Identifier  | Mode        |
| --------- | ----------- | ----------- |
| Verilog   | `verilog`   | `verilog`   |
| VHDL      | `vhdl`      | `vhdl`      |
| Assembly  | `gas`       | `gas`       |
| Fortran   | `fortran`   | `fortran`   |
| COBOL     | `cobol`     | `cobol`     |
| APL       | `apl`       | `apl`       |
| Brainfuck | `brainfuck` | `brainfuck` |

Note: Ada and NASM have no imported CodeMirror modes. The assembly mode is `gas` (GNU Assembler syntax).

## Code Block Features

### Syntax Highlighting

The editor highlights syntax according to the resolved language (see above):

```javascript
// Functions with proper syntax highlighting
function fibonacci(n) {
  if (n <= 1) return n;
  return fibonacci(n - 1) + fibonacci(n - 2);
}

const result = fibonacci(10);
console.log(result); // 55
```

### Line Numbers

Line numbers are on by default (`:lineNumbers true`, `src/main/frontend/extensions/code.cljs:427`). You can turn them off with `:editor/extra-codemirror-options` (see below).

### Bracket Matching

The editor always auto-closes brackets and quotes (`:autoCloseBrackets true`, `code.cljs:426`). Highlighting the matching bracket is a different option.

`:matchBrackets` is set to `lisp-like?` (`code.cljs:428`). `lisp-like?` is true only when `mode` equals `"scheme"`, `"lisp"`, `"clojure"` or `"edn"` (`code.cljs:422`). But `mode` holds the value `text->cm-mode` returns, and for all four of those languages that is a **MIME type**, such as `text/x-clojure` or `text/x-scheme` (`code.cljs:374-389`). The comparison therefore never matches, and bracket matching stays off in every language.

The `matchbrackets` addon is imported (`code.cljs:4`). You can turn it on graph-wide with `:editor/extra-codemirror-options {:matchBrackets true}`.

> **Unverified:** this is read from source and was not reproduced in a running app.

> **Removed:** this page previously said "CodeMirror highlights matching brackets/parentheses when cursor is near them" without qualification, and listed a **Code Folding** feature ("Long code blocks can be collapsed/expanded"). The only CodeMirror addons imported are `closebrackets`, `matchbrackets`, `show-hint` and `active-line` (`code.cljs:3-6`), so in-editor code folding is not available. The outliner's own block collapse still applies to the block containing the code.

### Active Line

`:styleActiveLine true` highlights the line the cursor is on (`code.cljs:429`).

### Theme

The editor theme is `solarized <theme>`, or `lsradix <theme>` when a Radix accent color is active (`code.cljs:402-406`). It follows the app's light/dark theme when that changes (`code.cljs:619-624`).

### Read-only in Published Graphs

When `config/publishing?` is true, the editor is created with `:readOnly true` and the cursor blink turned off (`code.cljs:443-445`).

### Custom CodeMirror Options

`:editor/extra-codemirror-options` in the graph config is merged into the editor options, so any CodeMirror 5 option can be set graph-wide. It is merged after the defaults, so it overrides them (`code.cljs:370-372`, `:430-433`). The one exception is the `logseq/config.edn` editor, where a user-set `:readOnly` is removed so the file stays editable.

## Editing and Leaving the Editor

| Key | Effect | Source |
| --- | ------ | ------ |
| `Esc` | Saves the code and returns to editing the block | `code.cljs:436-439`, `:391-400` |
| `←` at the very start / `→` at the very end | Moves to the adjacent block | `code.cljs:506-519` |
| `↑` at line 1, column 1 / `↓` at the end of the last line | Moves to the block above / below | `code.cljs:521-536` |
| `mod+[` / `mod+]` | Stopped, so they do not trigger browser back/forward | `code.cljs:537-543` |
| `Tab` | Not captured. `:tabIndex -1` leaves Tab bound globally | `code.cljs:435` |

## Code Block Properties

A code block is an ordinary block, so you can add your own properties with `/Add property` (`commands.cljs:354-358`) alongside the two that Logseq sets:

| Property | Type | Title | Schema | Source |
| -------- | ---- | ----- | ------ | ------ |
| `:logseq.property.node/display-type` | `:keyword` | "Node Display Type" | `:public? false`, `:hide? true`, `:queryable? true` | `deps/db/src/logseq/db/frontend/property.cljs:138-143` |
| `:logseq.property.code/lang` | `:string` | "Code Mode" | `:public? false`, `:hide? true`, `:queryable? true` | `property.cljs:148-153` |

Neither property appears in the properties area. `:string` is not a ref type (`deps/db/src/logseq/db/frontend/property/type.cljs:58-66`), so the language is stored as a plain string you can match directly in a query.

## Practical Examples

### JavaScript/TypeScript

```typescript
interface User {
  id: number;
  name: string;
  email: string;
}

async function fetchUser(id: number): Promise<User> {
  const response = await fetch(`/api/users/${id}`);
  return response.json();
}
```

### Python

```python
import numpy as np
import pandas as pd

def analyze_data(df: pd.DataFrame) -> dict:
    """Perform basic statistical analysis."""
    return {
        'mean': df.mean(),
        'median': df.median(),
        'std': df.std()
    }

# Load and analyze
data = pd.read_csv('data.csv')
results = analyze_data(data)
```

### SQL

```sql
SELECT
    u.name,
    COUNT(o.id) as order_count,
    SUM(o.total) as total_spent
FROM users u
LEFT JOIN orders o ON u.id = o.user_id
WHERE o.created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
GROUP BY u.id, u.name
HAVING total_spent > 100
ORDER BY total_spent DESC
LIMIT 10;
```

### Bash/Shell

```bash
#!/bin/bash

# Backup script
BACKUP_DIR="/backup/$(date +%Y%m%d)"
mkdir -p "$BACKUP_DIR"

for dir in /home/*/Documents; do
    user=$(basename $(dirname "$dir"))
    tar -czf "$BACKUP_DIR/${user}_docs.tar.gz" "$dir"
    echo "Backed up $user's documents"
done
```

### Docker

```dockerfile
FROM node:18-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci --only=production

COPY . .

EXPOSE 3000

CMD ["node", "server.js"]
```

### JSON/YAML

```json
{
  "name": "my-project",
  "version": "1.0.0",
  "dependencies": {
    "express": "^4.18.0",
    "dotenv": "^16.0.0"
  },
  "scripts": {
    "start": "node server.js",
    "dev": "nodemon server.js"
  }
}
```

```yaml
version: '3.8'
services:
  web:
    image: nginx:alpine
    ports:
      - "80:80"
    volumes:
      - ./html:/usr/share/nginx/html
  db:
    image: postgres:14
    environment:
      POSTGRES_PASSWORD: secret
    volumes:
      - db-data:/var/lib/postgresql/data

volumes:
  db-data:
```

## Code Block with Classes

Logseq tags code blocks with `:logseq.class/Code-block`. The class schema declares both properties (`class.cljs:76-79`):

- `:logseq.property.code/lang`: language identifier
- `:logseq.property.node/display-type`: display type

**Example block in database:**

```clojure
{:db/id 123
 :block/title "const x = 1;"          ; raw code, no ``` fences
 :block/tags [:logseq.class/Code-block]
 :logseq.property.code/lang "javascript"
 :logseq.property.node/display-type :code}
```

The value of `:logseq.property.code/lang` depends on where it came from:

- The language picker stores the CodeMirror mode name, such as `JavaScript` (`block.cljs:4882-4894`).
- The file-graph importer stores the language exactly as written on the fence.

The importer handles fences this way:

- A block whose text is a single fence and nothing else becomes a code block (`deps/graph-parser/src/logseq/graph_parser/exporter.cljs:2123-2130`).
- Fences mixed with other text are moved into child code blocks only when the `:extract-code-snippets?` import option is set (`exporter.cljs:2131-2137`, children built at `:2048-2063`).

> **Removed:** this example previously showed `:block/title "```javascript\ncode\n```"`. In a DB graph the fences are not stored; `:block/title` holds the raw code. The importer builds exactly that shape (see above), and the editor sets `:block/title ""` when converting a block (`src/main/frontend/handler/editor.cljs:3559-3563`).

The class and the display type are two views of the same fact. `:code` ↔ `:logseq.class/Code-block` is a fixed mapping (`deps/db/src/logseq/db/frontend/db.cljs:113-130`), and the worker pipeline keeps the two in sync (`src/main/frontend/worker/pipeline.cljs:316-336`).

## Querying Code Blocks

Find all Python code blocks. Create a block with **Advanced Query** (`commands.cljs:125-132`, `:326`) and enter:

```clojure
{:query [:find (pull ?b [*])
         :where
         [?b :block/tags ?class]
         [?class :db/ident :logseq.class/Code-block]
         (or [?b :logseq.property.code/lang "python"]
             [?b :logseq.property.code/lang "Python"])]}
```

Both spellings are matched because the picker stores `Python` while imported fences usually store `python` (see above). The attributes are checked against source; the query was not run in a live graph.

> **Removed:** a `#+BEGIN_QUERY … #+END_QUERY` wrapper around this query. In a DB graph that syntax renders a deprecation warning instead of running.

## Best Practices

### 1. Always Specify a Language

Pick a language from the code block's language button. A block with no language, or an identifier that is not recognized (see [Supported Languages](#supported-languages)), gets no syntax highlighting.

Good:

```python
print("Hello")
```

Avoid:

```
print("Hello")  # No syntax highlighting
```

### 2. Keep Code Blocks Focused

Put one concept in each code block. Avoid several unrelated functions in one block.

```python
def calculate_total(items):
    return sum(item.price for item in items)
```

### 3. Add Context with Properties

Typing `purpose:: API configuration` into a DB-graph block does not create a property. Instead, select the code block and run `/Add property` (or press `mod+p` on macOS, `ctrl+alt+p` elsewhere). Then add properties such as `purpose` or `source` with a page as the value.

### 4. Use Comments for Explanation

```python
# Calculate Fibonacci sequence using memoization
def fib(n, memo={}):
    if n in memo:
        return memo[n]  # Return cached value
    if n <= 1:
        return n
    memo[n] = fib(n-1, memo) + fib(n-2, memo)
    return memo[n]
```

## Limitations

### Execution

A code block created with **Code block** or triple backticks is not executed. The known exceptions:

- **Calculator blocks.** Blocks in `calc` mode compute a result for each line (`code.cljs:471`; see the [Calculator Reference](../features/calculator.md)).
- **Clojure fences with `:results`.** A fence written as ```` ```clojure :results ```` inside an ordinary block's text is evaluated with SCI. The value appears under "Results:" (`block.cljs:4963-4965`, `src/main/frontend/extensions/sci.cljs:64-73`, `en.edn:1925`). mldoc 1.5.9 (`package.json:176`) parses `:results` into the fence's `options`. Code-display blocks never pass options (`block.cljs:2583`), so this does not happen for them.

  > **Unverified:** the `:results` path is read from source and the mldoc parser output; it was not reproduced in a running app.

For anything else:

- Copy the code to an external IDE or REPL.
- Use a plugin. `logseq.Experiments.registerFencedCodeRenderer(lang, opts)` (`libs/src/modules/LSPlugin.Experiments.ts:105-120`) lets a plugin render its own UI for fences of a given language. It applies only to fences inside ordinary block text (`block.cljs:5120-5123`, `src/main/frontend/handler/plugin.cljs:593-596`). Plugins are off on native mobile (`src/main/frontend/util.cljc:131`).

### Limited Autocompletion

The editor offers only basic bracket and quote completion (`:autoCloseBrackets true`) and no language-specific autocompletion. The one exception is the `config.edn` file editor, which gets a hint-based completer bound to `:` and `Ctrl-Space` (`code.cljs:423`, `:440-442`, `:446-447`).

### Tab Is Not Captured

`:tabIndex -1` is set, so Tab stays bound globally and does not indent inside the code editor (`code.cljs:435`). Press `Esc` to leave the editor and save (`code.cljs:436-439`).

> **Removed:** a "No Live Editing" section ("Code blocks are static. Changes require manual editing."). It described nothing specific, and the editor is live.

## Alternative: Inline Code

For short snippets, use inline code:

```markdown
Use `Array.map()` to transform arrays, or `filter()` to select items.
```

## Findings: New Capabilities

Verified against `be800f1711`:

- **Code blocks remember your last language.** A block created with `/Code block` gets `:logseq.kv/latest-code-lang` instead of plain text (`src/main/frontend/handler/events.cljs:374-378`, `:400-406`). Choosing a language from the picker updates that entry (`block.cljs:4940-4948`).
- **Advanced queries are themselves code blocks.** The **Advanced Query** slash command sets `:logseq.property.node/display-type :code` and `:logseq.property.code/lang "clojure"` on the query property's value block. That is why an advanced query renders with Clojure highlighting (`src/main/frontend/commands.cljs:125-132`). The plain **Query** command runs the query command instead (`commands.cljs:110-117`).
- **`:editor/extra-codemirror-options` is a supported config escape hatch** for any CodeMirror 5 option (`src/main/frontend/extensions/code.cljs:370-372`, `:430-433`).
- **`calc` is a special mode.** It gets `:viewportMargin js/Infinity` (`code.cljs:449-450`) and has its own **Calculator** slash command (`commands.cljs:119-123`, `:331-334`).
- **Copy button and language picker** on every code block (`block.cljs:4927-4961`).

---

## Related Features

- [Calculator Reference](../features/calculator.md): the `calc` code block mode
- [Blockquotes](../formatting/markdown-syntax.md#blockquotes): for quoted text
- [Markdown Syntax](../formatting/markdown-syntax.md): complete formatting guide

## See Also

- [CodeMirror 5 Documentation](https://codemirror.net/5/)
- [CodeMirror 5 Modes](https://codemirror.net/5/mode/)
