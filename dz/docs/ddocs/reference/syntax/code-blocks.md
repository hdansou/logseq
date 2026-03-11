---
title: 'Code Blocks Reference'
status: 🟢
since-version: 'N/A'
quadrant: 'Reference'
last-verified: '2026-01-11'
platform: 'All'
related-docs:
  - 'tutorials/getting-started/03-basic-editing.md'
  - 'reference/syntax/markdown.md'
---

> **Last revised:** 2026-03-10

# Code Blocks Reference

Complete reference for working with code blocks in Logseq, powered by CodeMirror 5.

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

Logseq imports ~120 CodeMirror mode files, which collectively support many language names and MIME types. Language resolution uses `CodeMirror.findModeByName`, so some languages (e.g., TypeScript, Kotlin, Scala) are handled by shared modes (`javascript`, `clike`) rather than dedicated ones.

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
| Markdown         | `markdown`, `md` | Dedicated mode via `gfm` (GitHub Flavored)  |
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

CodeMirror highlights matching brackets/parentheses when cursor is near them.

### Code Folding

Long code blocks can be collapsed/expanded (varies by platform).

## Creating Code Blocks

### Method 1: Slash Command

1. Type `/code`
2. Select language from dropdown
3. Start typing code

### Method 2: Manual Triple Backticks

````markdown
```language-name
code here
```
````

### Method 3: Keyboard Shortcut

- **Windows/Linux:** `Ctrl + Shift + C`
- **macOS:** `Cmd + Shift + C`

## Code Block Properties

Add metadata to code blocks using properties:

````markdown
- ```python
  def hello():
      print("Hello, World!")
  ```
````

language:: python
purpose:: Example function
source:: [[Tutorial]]

`````

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
 :block/title "```javascript\ncode\n```"
 :block/tags [:logseq.class/Code-block]
 :logseq.property.code/lang "javascript"
 :logseq.property.node/display-type :code}
````

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

Basic bracket/quote completion only. No language-specific autocompletion.

## Alternative: Inline Code

For short snippets, use inline code:

```markdown
Use `Array.map()` to transform arrays, or `filter()` to select items.
```

## Related Features

- [Math & LaTeX Blocks](../features/math-latex.md) - For mathematical notation
- [Quote Blocks](../syntax/markdown.md#quotes) - For quoted text
- [Markdown Reference](../syntax/markdown.md) - Complete formatting guide

## See Also

- [CodeMirror Documentation](https://codemirror.net/)
- [Supported CodeMirror Modes](https://codemirror.net/mode/)
