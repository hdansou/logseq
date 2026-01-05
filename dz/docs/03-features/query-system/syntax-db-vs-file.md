# Query Syntax: DB vs File-Based Graphs

**Critical Differences in How Queries Are Written**

---

## 🚨 Important: Syntax is Different!

The way you write queries **fundamentally differs** between DB-based and file-based graphs.

---

## File-Based Graph Query Syntax

### Simple (DSL) Queries

**Syntax**: Use `{{query ...}}` macro wrapper

```clojure
{{query (and [[page-name]] (task TODO))}}
```

**Example with Template Variables**:

```clojure
{{query (and <% today %> (task TODO))}}
```

**Note**: File-based graphs use uppercase task statuses: `TODO`, `DOING`, `DONE`, `NOW`, `LATER`

**Renders**: Automatically executed and displays results inline

---

### Advanced Queries (File-Based)

**Syntax**: Multi-line code block with query map

````markdown
```query
{:title "My Query"
 :query [:find (pull ?b [*])
         :in $ ?today
         :where [?b :block/journal-day ?today]]
 :inputs [:today]}
```
````

**Alternative Single-Line**:

```clojure
{{query {:query [:find ...] :inputs [...]}}}
```

---

## DB-Based Graph Query Syntax

### ⚠️ NO `{{query}}` Wrapper!

In DB graphs, queries are **properties**, not macros. The `{{query}}` macro is **deprecated** and will show a warning.

### Simple (DSL) Queries - DB Version

**Syntax**: Use `/Query` command to create query property

**How to create**:

1. Type `/Query` in any block
2. Logseq creates the `:logseq.property/query` property
3. Enter your query in the property value

**Query value example**:

```clojure
(and [[page-name]] (task Todo))
```

**Example with Template Variables**:

```clojure
(and <% today %> (task Todo))
```

**Important**: DB-based graphs use **title-case task statuses** (NOT quoted):

- Use: `Todo`, `Doing`, `Done`, `Now`, `Later`
- NOT: `TODO`, `DOING`, `DONE`, `NOW`, `LATER` (file-based syntax)
- NOT: `"Todo"`, `"Doing"`, `"Done"` (quoted strings)

---

### Scheduled and Deadline Syntax

**File-Based Graphs**: Use org-mode style timestamp syntax

```markdown
- TODO Task description
  SCHEDULED: <2024-03-15 Fri>
- TODO Another task
  DEADLINE: <2024-03-20 Wed>
- TODO Combined
  SCHEDULED: <2024-03-15 Fri> DEADLINE: <2024-03-20 Wed>
```

**DB-Based Graphs**: Use properties (`:logseq.property/scheduled` and `:logseq.property/deadline`)

Properties are set via UI or shortcuts:

- `Cmd/Ctrl + P` then `D` for Deadline
- `Cmd/Ctrl + P` then `S` for Scheduled

Values are stored as datetime timestamps (milliseconds since epoch).

**Query Syntax Differences**:

| Aspect             | File-Based                                           | DB-Based                                                                       |
| ------------------ | ---------------------------------------------------- | ------------------------------------------------------------------------------ |
| **Property Names** | `scheduled`, `deadline` (lowercase)                  | `scheduled`, `deadline` (lowercase in queries)                                 |
| **Storage**        | `:block/scheduled`, `:block/deadline` (integer date) | `:logseq.property/scheduled`, `:logseq.property/deadline` (datetime timestamp) |
| **Display**        | `SCHEDULED: <date>`, `DEADLINE: <date>`              | Property UI with date picker                                                   |

**Query Examples**:

File-Based:

```clojure
{{query (and (task TODO) (property scheduled today))}}
{{query (and (task TODO) (between deadline today +7d))}}
```

DB-Based (using `/Query`):

```clojure
(and (task Todo) (property scheduled today))
(and (task Todo) (between deadline today +7d))
```

---

### Advanced Queries - DB Version

**Syntax**: Use `/Query` command

1. Type `/Query` in any block
2. Logseq automatically:
   - Sets `:block/tags` to `:logseq.class/Query`
   - Creates `:logseq.property/query` property with empty value
   - Sets property metadata:
     - `:logseq.property.node/display-type` to `:code`
     - `:logseq.property.code/lang` to `"clojure"`
3. Fill in the query value:

```clojure
{:title "My Query"
 :query [:find (pull ?b [*])
         :in $ ?today
         :where [?b :block/journal-day ?today]]
 :inputs [:today]}
```

**Note**: The property value is stored as a plain string containing the Clojure map

---

## Side-by-Side Comparison

### Example: Today's Tasks

| File-Based                                | DB-Based                                   |
| ----------------------------------------- | ------------------------------------------ |
| `{{query (and <% today %> (task TODO))}}` | `/Query` → `(and <% today %> (task Todo))` |

**Key Difference**: Task statuses in DB-based graphs use title-case (`Todo`, `Doing`, `Done`), while file-based graphs use uppercase keywords (`TODO`, `DOING`, `DONE`). DB graphs do NOT use quoted strings.

### Example: Scheduled Tasks

**File-Based**:

```clojure
{{query (and (task TODO) (property scheduled today))}}
```

**DB-Based** (using `/Query` command):

```clojure
(and (task Todo) (property scheduled today))
```

**Key Difference**: Query logic is the same, but task status format and wrapper syntax differ.

### Example: Advanced Query

**File-Based**:

````markdown
```query
{:title "Last Week"
 :query [:find (pull ?b [*])
         :in $ ?start ?end %
         :where (between ?b ?start ?end)]
 :inputs [:-7d :today]}
```
````

`````

**DB-Based** (using `/Query` command):
```clojure
{:title "Last Week"
 :query [:find (pull ?b [*])
         :in $ ?start ?end %
         :where (between ?b ?start ?end)]
 :inputs [:-7d :today]}
```

(This is the value you enter in the `:logseq.property/query` property created by `/Query`)

---

## Key Differences Summary

| Aspect | File-Based | DB-Based |
|--------|------------|----------|
| **Simple Query Syntax** | `{{query (and ...)}}` | `/Query` command creates property |
| **Task Statuses** | Uppercase keywords: `TODO`, `DOING`, `DONE` | Title-case: `Todo`, `Doing`, `Done` |
| **Scheduled/Deadline Display** | `SCHEDULED: <date>`, `DEADLINE: <date>` | Property UI with date picker |
| **Scheduled/Deadline Storage** | `:block/scheduled`, `:block/deadline` (integer) | `:logseq.property/scheduled`, `:logseq.property/deadline` (datetime) |
| **Advanced Query Syntax** | Code block with query map | Property value with query map |
| **Creation Method** | Type `{{query}}` macro manually | Use `/Query` command (creates `:logseq.property/query`) |
| **Storage** | Inline in markdown | As `:logseq.property/query` property value |
| **Display** | Inline macro expansion | Property-based rendering |
| **{{query}} Macro** | ✅ Supported | ⚠️ Deprecated (shows warning) |

---

## Template Variables Work the Same

**Both graphs support**: `<% today %>`, `<% current page %>`, etc.

**File-Based**:
```clojure
{{query (and <% today %>)}}
```

**DB-Based** (using `/Query` command):
```clojure
(and <% today %>)
```

**Resolution**: Template variables resolve **before** query execution in both

---

## Query Inputs Work the Same

**Both graphs support**: `:today`, `:current-page`, `:-7d`, etc.

**File-Based**:
````markdown
```query
{:query [:find ...]
 :inputs [:today :current-page]}
```
`````

**DB-Based** (using `/Query` command):

```clojure
{:query [:find ...]
 :inputs [:today :current-page]}
```

`````

**Resolution**: Query inputs resolve **during** query execution in both

---

## How to Create Queries in DB Graphs

### Using `/Query` Command (Recommended)

1. Type `/Query` in any block (note: capital Q)
2. Logseq automatically:
   - Adds `:logseq.class/Query` tag to the block
   - Creates `:logseq.property/query` property
   - Sets property to display as code with Clojure syntax highlighting
3. Enter your query in the property value field

**For Simple Queries**:

```clojure
(and [[page-name]] (task Todo))
```

**For Advanced Queries**:

```clojure
{:title "My Query"
 :query [:find (pull ?b [*])
         :where [?b :block/marker "Todo"]]
 :inputs [:today]}
```

**Note**: The old `{{query}}` macro is deprecated in DB graphs and will show a warning message

---

## Property-Based Query Features (DB Only)

### Query Display Type

In DB graphs, the `/Query` command automatically sets:

```clojure
:block/tags [:logseq.class/Query]
:logseq.property/query ""
:logseq.property.node/display-type :code
:logseq.property.code/lang "clojure"
```

This means:

- Block is tagged as a Query
- Query property displays as code editor
- Syntax highlighting for Clojure
- Better editing experience

### Query Block Structure

When you use `/Query`, the block structure is:

```
- [Query tag]
  query: (and [[page-name]] (task Todo))
  → Query results render below automatically
```

---

## Migration Notes

### File → DB Migration

**Old (File-based)**:

```clojure
{{query (and [[project]])}}
```

**New (DB-based)**:

1. Type `/Query` command
2. Enter query value: `(and [[project]])`

(Note: `{{query}}` macro is deprecated in DB graphs)

**Advanced Query Migration**:

**Old** (File-based):

````markdown
```query
{:title "My Query" ...}
```
`````

**New** (DB-based):

1. Type `/Query` command
2. Enter in the property value field:

```clojure
{:title "My Query" ...}
```

````

---

## Common Mistakes

### ❌ Don't: Use `{{query}}` in DB Graphs

```clojure
❌ {{query (and [[page]])}}
````

This won't work! DB graphs don't use macro syntax.

### ✅ Do: Use Property Syntax

```markdown
✅ query:: (and [[page]])
```

---

### ❌ Don't: Forget Property Name

```markdown
❌ (and [[page]])
```

Must have `query::` property name.

---

### ❌ Don't: Mix Syntaxes

```markdown
❌ query:: {{query (and [[page]])}}
```

Don't nest macros in properties.

---

## Testing Your Queries

### File-Based Testing

```clojure
{{query (and [[test-page]])}}
```

Should render immediately if `test-page` exists.

### DB-Based Testing

1. Type `/Query` in a block
2. Enter: `(and [[test-page]])`
3. Results should render below the block

---

## Debugging Tips

### File-Based Debugging

1. Check macro syntax: `{{query ...}}`
2. Verify query is valid DSL or datalog
3. Look for error messages in console

### DB-Based Debugging

1. Verify you used `/Query` command (capital Q)
2. Check block has `:logseq.class/Query` tag
3. Check `:logseq.property/query` property exists
4. Verify query value syntax is correct
5. For task queries, use title-case: `Todo`, `Doing`, `Done` (not quoted)
6. Check browser console for error messages

---

## Best Practices

### File-Based Best Practices

✅ Use `{{query}}` for inline queries
✅ Use code blocks for complex queries
✅ Keep queries in dedicated query pages
✅ Document queries with comments

### DB-Based Best Practices

✅ Use `/Query` command for creation (capital Q)
✅ Don't use deprecated `{{query}}` macro
✅ Use title-case for task statuses: `Todo`, `Doing`, `Done`
✅ Don't quote task status values in DSL queries
✅ Leverage property-based organization
✅ Take advantage of code syntax highlighting

---

## Quick Reference

### File-Based Cheat Sheet

````clojure
# Simple Query
{{query (and [[page]])}}

# With Template Variables
{{query (and <% today %>)}}

# Advanced Query
```query
{:query [:find ...]
 :inputs [:today]}
````

````

### DB-Based Cheat Sheet

```clojure
# Simple Query
# Type: /Query
# Enter: (and [[page]])

# With Template Variables
# Type: /Query
# Enter: (and <% today %>)

# With Task Statuses
# Type: /Query
# Enter: (and (task Todo Doing) [[project]])

# Advanced Query
# Type: /Query
# Enter:
{:query [:find ...]
 :inputs [:today]}
```

---

## Implementation Details

### Why Different?

**File-Based**:
- Uses markdown macro system
- Queries are text macros that expand
- Parser looks for `{{query ...}}` pattern

**DB-Based**:
- Uses property system
- Queries are structured data
- Properties have types and metadata
- Better query management and indexing

### Code Locations

**File-Based Query Handling**:
- `src/main/frontend/components/file_based/query.cljs`
- Macro-based rendering

**DB-Based Query Handling**:
- `/Query` command: `src/main/frontend/commands.cljs:195-203`
- Creates `:logseq.property/query` property with metadata
- Property-based rendering: `src/main/frontend/components/query/view.cljs`
- Query DSL parsing: `src/main/frontend/db/query_dsl.cljs:376-389` (`build-task` function)
- Task statuses: Title-cased via `common-util/capitalize-all`

---

## Conclusion

**Key Takeaway**:

| If you're using... | Create queries with... | Task status format |
|-------------------|---------------------|-------------------|
| **File-based graph** | `{{query ...}}` macro | `TODO`, `DOING`, `DONE` |
| **DB-based graph** | `/Query` command | `Todo`, `Doing`, `Done` |

Both syntaxes support:
- ✅ Template variables `<% %>`
- ✅ Query inputs `:keyword`
- ✅ DSL and advanced queries

The **resolution logic** is the same, only the **syntax wrapper** differs!

---

**Last Updated**: 2025-11-22
**Version**: 2.1.0
**Changes**:
- v2.1.0 (2025-11-22): Added scheduled/deadline syntax differences between file-based and DB-based graphs
- v2.0.0 (2025-11-21): Updated to reflect actual DB graph query syntax using `/Query` command instead of incorrect `query::` property syntax. Corrected task status format (title-case without quotes)---

*Always check which graph type you're using before writing queries!*
```
````
