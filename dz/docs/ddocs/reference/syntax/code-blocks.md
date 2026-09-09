---
title: 'Code Blocks Reference'
status: 🟢
since-version: 'N/A'
quadrant: 'Reference'
last-verified: '2026-09-09'
verified-against: 'be800f1711'
reviewStatus: merged-not-fully-audited
platform: 'All'
related-docs:
  - 'tutorials/getting-started/03-basic-editing.md'
  - 'reference/syntax/markdown.md'
---

> **Last revised:** 2026-09-09

# Code Blocks Reference

Complete reference for working with code blocks in Logseq, powered by CodeMirror 5 (pinned at `5.65.18` in `package.json:161`).

## Basic Syntax

### Inline Code

Wrap code in single backticks:

```markdown
Use the `console.log()` function to output
```

**Renders as:** Use the `console.log()` function to output

### Code Blocks

Use triple backticks with language identifier:

````markdown
```javascript
function greet(name) {
  return `Hello, ${name}!`
}
```
````

**Renders as a syntax-highlighted code block.**

## Supported Languages

Logseq imports 122 CodeMirror mode files (`src/main/frontend/extensions/code.cljs`), which collectively support many language names and MIME types. Language resolution goes through `text->cm-mode`, which lower-cases the language identifier and calls `CodeMirror.findModeByName` — falling back to the raw string when nothing matches (`src/main/frontend/extensions/code.cljs:374-389`). Some languages (e.g., TypeScript, Kotlin, Scala) are therefore handled by shared modes (`javascript`, `clike`) rather than dedicated ones.

### Popular Languages

| Language   | Identifier            | Example Use             | Mode          |
| ---------- | --------------------- | ----------------------- | ------------- |
| JavaScript | `javascript`, `js`    | Web development         | `javascript`  |
| TypeScript | `typescript`, `ts`    | Typed JavaScript        | `javascript`  |
| Python     | `python`, `py`        | Data science, scripting | `python`      |
| Java       | `java`                | Enterprise applications | `clike`       |
| C/C++      | `c`, `cpp`, `c++`     | Systems programming     | `clike`       |
| C#         | `csharp`, `cs`        | .NET development        | `clike`       |
| Go         | `go`, `golang`        | Cloud services          | `go`          |
| Rust       | `rust`                | Systems programming     | `rust`        |
| Ruby       | `ruby`, `rb`          | Web development         | `ruby`        |
| PHP        | `php`                 | Web development         | `php`         |
| Swift      | `swift`               | iOS development         | `swift`       |
| Kotlin     | `kotlin`              | Android development     | `clike`       |
| R          | `r`                   | Statistics              | `r`           |
| MATLAB     | `matlab`              | Scientific computing    | `octave`      |
| SQL        | `sql`                 | Database queries        | `sql`         |
| Bash/Shell | `bash`, `shell`, `sh` | Scripts                 | `shell`       |

### Web Technologies

| Language  | Identifier     | Mode           |
| --------- | -------------- | -------------- |
| HTML      | `html`         | `htmlmixed`    |
| CSS       | `css`          | `css`          |
| SCSS/Sass | `scss`, `sass` | `css` / `sass` |
| Less      | `less`         | `css`          |
| JSX       | `jsx`          | `jsx`          |
| Vue       | `vue`          | `vue`          |
| XML       | `xml`          | `xml`          |
| JSON      | `json`         | `javascript`   |
| YAML      | `yaml`, `yml`  | `yaml`         |
| TOML      | `toml`         | `toml`         |

### Markup & Documentation

| Language         | Identifier       | Notes                                       |
| ---------------- | ---------------- | ------------------------------------------- |
| Markdown         | `markdown`, `md` | Dedicated `markdown` mode (`gfm` is imported too) |
| reStructuredText | `rst`            | Dedicated `rst` mode                        |
| LaTeX            | `latex`, `tex`   | Via `stex` mode                             |

Note: AsciiDoc and Org Mode are not among the imported CodeMirror modes, so they will not receive syntax highlighting.

### Functional & Academic

| Language    | Identifier       | Mode        |
| ----------- | ---------------- | ----------- |
| Haskell     | `haskell`, `hs`  | `haskell`   |
| Erlang      | `erlang`         | `erlang`    |
| Clojure     | `clojure`, `clj` | `clojure`   |
| Scheme      | `scheme`         | `scheme`    |
| Common Lisp | `commonlisp`     | `commonlisp`|
| OCaml       | `ocaml`          | `mllike`    |
| F#          | `fsharp`         | `mllike`    |
| Elm         | `elm`            | `elm`       |
| Scala       | `scala`          | `clike`     |

Note: Elixir does not have an imported CodeMirror mode and will not receive syntax highlighting.

### Data & Query Languages

| Language | Identifier                   | Notes                          |
| -------- | ---------------------------- | ------------------------------ |
| SQL      | `sql`, `mysql`, `postgresql` | Dedicated `sql` mode           |
| SPARQL   | `sparql`                     | Dedicated `sparql` mode        |
| Cypher   | `cypher`                     | Dedicated `cypher` mode        |

Note: GraphQL and Datalog do not have imported CodeMirror modes and will not receive syntax highlighting.

### Configuration & DevOps

| Language   | Identifier             | Notes                 |
| ---------- | ---------------------- | --------------------- |
| Dockerfile | `dockerfile`, `docker` | Dedicated mode        |
| Nginx      | `nginx`                | Dedicated mode        |
| Properties | `properties`           | Dedicated mode        |

Note: Apache, INI, and Terraform do not have imported CodeMirror modes and will not receive syntax highlighting.

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

Note: Ada and NASM do not have imported CodeMirror modes. The assembly mode is `gas` (GNU Assembler syntax).

## Code Block Features

### Syntax Highlighting

Logseq automatically highlights syntax based on the language identifier:

````javascript
;```javascript
// Functions with proper syntax highlighting
function fibonacci(n) {
  if (n <= 1) return n;
  return fibonacci(n - 1) + fibonacci(n - 2);
}

const result = fibonacci(10);
console.log(result); // 55
```
````

### Line Numbers

Line numbers are displayed automatically for all code blocks.

### Bracket Matching

Bracket matching is **not** on for every language. `:matchBrackets` is set to `lisp-like?`, which is true only when the resolved mode is one of `scheme`, `lisp`, `clojure` or `edn` (`src/main/frontend/extensions/code.cljs:422` and `:428`). In every other language the editor still auto-closes brackets and quotes (`:autoCloseBrackets true`) but does not highlight the matching partner.

> **Removed:** this page previously said "CodeMirror highlights matching brackets/parentheses when cursor is near them" without qualification, and listed a **Code Folding** feature ("Long code blocks can be collapsed/expanded"). Neither CodeMirror's `addon/fold/*` nor a `foldGutter` option appears anywhere in the frontend source, so in-editor code folding is not available. The outliner's own block collapse still applies to the block containing the code.

### Active Line

`:styleActiveLine true` highlights the line the cursor is on (`src/main/frontend/extensions/code.cljs:429`).

### Read-only in Published Graphs

When `config/publishing?` is true, the editor is created with `:readOnly true` and a disabled cursor blink (`src/main/frontend/extensions/code.cljs:443-445`).

### Custom CodeMirror Options

`:editor/extra-codemirror-options` in `logseq/config.edn` is merged into the editor options, so any CodeMirror 5 option can be set graph-wide. The one exception is the `config.edn` editor itself, where a user-set `:readOnly` is stripped so the file stays editable (`src/main/frontend/extensions/code.cljs:370-372`, `430-433`).

## Creating Code Blocks

### Method 1: Slash Command

Type `/` and pick **Code block** (`src/main/frontend/commands.cljs:246-249`; label from `src/resources/dicts/en.edn:522-523`). This runs `code-block-steps`, which converts the current block into a code block (`src/main/frontend/commands.cljs:134-138`).

There is no language dropdown in the slash command. The new block is created with the **last language you used**, read from the `:logseq.kv/latest-code-lang` key-value entry (`src/main/frontend/handler/events.cljs:374-378`, `399-407`). Change it from the language selector on the rendered code block.

### Method 2: Triple Backticks

Type three backticks in an empty block. Backticks auto-pair, so three keystrokes produce six characters, and Logseq watches for exactly `` `````` `` to convert the block into a code block with an empty title (`src/main/frontend/handler/editor.cljs:3405-3407`, `3559-3564`).

> **Removed:** this page previously listed a third method, a `Ctrl/Cmd + Shift + C` keyboard shortcut. In `src/main/frontend/modules/shortcut/config.cljs:229` that binding is `:editor/copy-text`; no shortcut in the config creates a code block.

## Code Block Properties

A code block is an ordinary block, so you can add your own properties to it alongside the two the system sets:

- `:logseq.property.node/display-type` — `:code`
- `:logseq.property.code/lang` — the language identifier, titled **Code Mode** in the UI (`deps/db/src/logseq/db/frontend/property.cljs:148-153`)

Both are `:public? false` and `:hide? true`, so they do not show in the properties area; `:logseq.property.code/lang` is `:queryable? true`, which is what makes the query below work.

## Practical Examples

### JavaScript/TypeScript

````typescript
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
`````

`````

### Python

````python
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
`````

### SQL

````sql
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
````

### Bash/Shell

````bash
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
````

### Docker

````dockerfile
```dockerfile
FROM node:18-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci --only=production

COPY . .

EXPOSE 3000

CMD ["node", "server.js"]
```
````

### JSON/YAML

````json
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
````

````yaml
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
````

## Code Block with Classes

Logseq stores code blocks as `:logseq.class/Code-block` entities:

**Properties:**

- `:logseq.property.code/lang` — Language identifier
- `:logseq.property.node/display-type` — Display type

**Example block in database:**

````clojure
{:db/id 123
 :block/title "const x = 1;"          ; raw code, no ``` fences
 :block/tags [:logseq.class/Code-block]
 :logseq.property.code/lang "javascript"
 :logseq.property.node/display-type :code}
````

> **Removed:** this example previously showed `:block/title "```javascript\ncode\n```"`. In a DB graph the fences are not stored — `:block/title` holds the raw code text. See the importer, which builds exactly this shape from a parsed fence (`deps/graph-parser/src/logseq/graph_parser/exporter.cljs:2052-2061`), and the editor path, which sets `:block/title ""` when converting a block (`src/main/frontend/handler/editor.cljs:3559-3563`).

The class and the display type are two views of the same fact: `:code` ↔ `:logseq.class/Code-block` is a fixed mapping in `deps/db/src/logseq/db/frontend/db.cljs:113-129`. The class declares both properties in its schema (`deps/db/src/logseq/db/frontend/class.cljs:76-79`).

## Querying Code Blocks

Find all code blocks by language:

```clojure
#+BEGIN_QUERY
{:query [:find (pull ?b [*])
         :where
         [?b :block/tags ?class]
         [?class :db/ident :logseq.class/Code-block]
         [?b :logseq.property.code/lang "python"]]}
#+END_QUERY
```

## Best Practices

### 1. Always Specify Language

````markdown
✅ Good:

```python
print("Hello")
```
````

❌ Avoid:

```
print("Hello")  # No syntax highlighting
```

````

### 2. Keep Code Blocks Focused

```markdown
✅ Good: One concept per block
```python
def calculate_total(items):
    return sum(item.price for item in items)
````

❌ Avoid: Multiple unrelated functions in one block

`````

### 3. Add Context with Properties

````markdown
```javascript
const API_KEY = process.env.API_KEY;
`````

purpose:: API configuration
related:: [[Environment Variables]]
source:: [[Project Setup Guide]]

`````

### 4. Use Comments for Explanation

````python
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
`````

## Limitations

### No Execution

Logseq does not execute code blocks. For execution:

- Copy to external IDE/REPL
- Use Logseq plugins for code execution
- Link to external notebooks (Jupyter, Observable)

### No Live Editing

Code blocks are static. Changes require manual editing.

### Limited Autocompletion

Basic bracket/quote completion only (`:autoCloseBrackets true`). No language-specific autocompletion — with one exception: the `logseq/config.edn` editor gets a hint-based completer bound to `:` and `Ctrl-Space` (`src/main/frontend/extensions/code.cljs:440-442`, `446-447`).

### Tab Is Not Captured

`:tabIndex -1` is set so Tab stays bound globally rather than indenting inside the code editor (`src/main/frontend/extensions/code.cljs:435`). Press `Esc` to leave the editor and save (`src/main/frontend/extensions/code.cljs:436-439`).

## Alternative: Inline Code

For short snippets, use inline code:

```markdown
Use `Array.map()` to transform arrays, or `filter()` to select items.
```

## Findings: New Capabilities

Verified against `be800f1711`:

- **Code blocks remember your last language.** Creating one via `/Code block` applies `:logseq.kv/latest-code-lang` rather than defaulting to plain text (`src/main/frontend/handler/events.cljs:374-378`, `399-407`).
- **Advanced queries are themselves code blocks.** The `/Query` slash command sets `:logseq.property.node/display-type :code` and `:logseq.property.code/lang "clojure"` on the query property's value block, which is why an advanced query renders with Clojure highlighting (`src/main/frontend/commands.cljs:125-132`).
- **`:editor/extra-codemirror-options` is a supported config.edn escape hatch** for any CodeMirror 5 option (`src/main/frontend/extensions/code.cljs:370-372`, `430-433`).
- **`calc` is a special mode**, given `:viewportMargin js/Infinity` and reachable from its own **Calculator** slash command (`src/main/frontend/extensions/code.cljs:449-450`; `src/main/frontend/commands.cljs:119-123`, `331-333`).

---

## Related Features

- [Math & LaTeX Blocks](../features/math-latex.md) - For mathematical notation
- [Quote Blocks](../syntax/markdown.md#quotes) - For quoted text
- [Markdown Reference](../syntax/markdown.md) - Complete formatting guide

## See Also

- [CodeMirror Documentation](https://codemirror.net/)
- [Supported CodeMirror Modes](https://codemirror.net/mode/)
