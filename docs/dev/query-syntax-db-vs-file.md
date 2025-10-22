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

In DB graphs, queries are **properties**, not macros.

### Simple (DSL) Queries - DB Version

**Syntax**: Use `query::` property

```markdown
- query:: (and [[page-name]] (task "Todo"))
```

**Example with Template Variables**:

```markdown
- query:: (and <% today %> (task "Todo"))
```

**Important**: DB-based graphs use **quoted, title-case task statuses**:
- Use: `"Todo"`, `"Doing"`, `"Done"`, `"Now"`, `"Later"`
- NOT: `TODO`, `DOING`, `DONE`, `NOW`, `LATER` (file-based syntax)

**How It Works**:

1. Add `query::` property to any block
2. Property value contains the query code
3. Logseq automatically renders results below

---

### Advanced Queries - DB Version

**Syntax**: Use `query::` property with code block

````markdown
- query::
  ```clojure
  {:title "My Query"
   :query [:find (pull ?b [*])
           :in $ ?today
           :where [?b :block/journal-day ?today]]
   :inputs [:today]}
  ```
````

`````

**Alternative**: Use query builder UI
- Type `/query` command
- Logseq creates the property automatically
- Edit the property value

---

## Side-by-Side Comparison

### Example: Today's Tasks

| File-Based | DB-Based |
|------------|----------|
| `{{query (and <% today %> (task TODO))}}` | `query:: (and <% today %> (task "Todo"))` |

**Key Difference**: Task statuses in DB-based graphs must be quoted and title-cased (`"Todo"`), while file-based graphs use uppercase keywords (`TODO`).

### Example: Advanced Query

**File-Based**:
````markdown
```query
{:title "Last Week"
 :query [:find (pull ?b [*])
         :in $ ?start ?end %
         :where (between ?b ?start ?end)]
 :inputs [:-7d :today]}
`````

`````

**DB-Based**:
```markdown
- query::
  ```clojure
  {:title "Last Week"
   :query [:find (pull ?b [*])
           :in $ ?start ?end %
           :where (between ?b ?start ?end)]
   :inputs [:-7d :today]}
  ```
```

---

## Key Differences Summary

| Aspect | File-Based | DB-Based |
|--------|------------|----------|
| **Simple Query Syntax** | `{{query (and ...)}}` | `query:: (and ...)` |
| **Task Statuses** | Uppercase keywords: `TODO`, `DOING`, `DONE` | Quoted, title-case: `"Todo"`, `"Doing"`, `"Done"` |
| **Advanced Query Syntax** | Code block with query map | Property with code block value |
| **Creation Method** | Type macro manually | Use `/query` command or property |
| **Storage** | Inline in markdown | As property on block |
| **Display** | Inline macro expansion | Property-based rendering |

---

## Template Variables Work the Same

**Both graphs support**: `<% today %>`, `<% current page %>`, etc.

**File-Based**:
```clojure
{{query (and <% today %>)}}
```

**DB-Based**:
```markdown
query:: (and <% today %>)
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

**DB-Based**:

````markdown
- query::
  ```clojure
  {:query [:find ...]
   :inputs [:today :current-page]}
  ```
````

````

**Resolution**: Query inputs resolve **during** query execution in both

---

## How to Create Queries in DB Graphs

### Method 1: Using `/query` Command

1. Type `/query` in any block
2. Logseq automatically creates:
   ```markdown
   - query::
     ```clojure

     ```
````

3. Fill in your query

### Method 2: Manual Property

1. Add property: `query::`
2. Type your query as the value
3. For multi-line, press Shift+Enter

### Method 3: Code Block (Advanced)

1. Create block with `query::` property
2. Add code block:
   ````markdown
   - query::
     ```clojure
     {:query [...] :inputs [...]}
     ```
   ````

---

## Property-Based Query Features (DB Only)

### Query Display Type

In DB graphs, queries have additional metadata:

```clojure
;; Auto-set by Logseq when you use /query
:logseq.property.node/display-type :code
:logseq.property.code/lang "clojure"
```

This means:

- Query property displays as code
- Syntax highlighting for clojure
- Better editing experience

### Query Block Structure

````markdown
- Some parent block
  - query::
    ```clojure
    {:query [:find ...]
     :inputs [...]}
    ```
  - Results render here (auto-generated)
````

---

## Migration Notes

### File → DB Migration

**Old (File-based)**:

```clojure
{{query (and [[project]])}}
```

**New (DB-based)**:

```markdown
query:: (and [[project]])
```

**Advanced Query Migration**:

**Old**:

````markdown
```query
{:title "My Query" ...}
```
````

**New**:

````markdown
- query::
  ```clojure
  {:title "My Query" ...}
  ```
````

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

```markdown
- query:: (and [[test-page]])
```

Should render results below the property.

---

## Debugging Tips

### File-Based Debugging

1. Check macro syntax: `{{query ...}}`
2. Verify query is valid DSL or datalog
3. Look for error messages in console

### DB-Based Debugging

1. Check property exists: `query::`
2. Verify property value is valid
3. Use query builder UI to validate syntax
4. Check block property metadata

---

## Best Practices

### File-Based Best Practices

✅ Use `{{query}}` for inline queries
✅ Use code blocks for complex queries
✅ Keep queries in dedicated query pages
✅ Document queries with comments

### DB-Based Best Practices

✅ Use `/query` command for creation
✅ Use `query::` property consistently
✅ Leverage property-based organization
✅ Use query builder for validation
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

```markdown
# Simple Query
query:: (and [[page]])

# With Template Variables
query:: (and <% today %>)

# Advanced Query
- query::
  ```clojure
  {:query [:find ...]
   :inputs [:today]}
````

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
- `src/main/frontend/components/query.cljs:63-67`
- Property-based rendering
- Uses `:logseq.property/query` attribute

---

## Conclusion

**Key Takeaway**:

| If you're using... | Write queries as... |
|-------------------|---------------------|
| **File-based graph** | `{{query ...}}` macro |
| **DB-based graph** | `query::` property |

Both syntaxes support:
- ✅ Template variables `<% %>`
- ✅ Query inputs `:keyword`
- ✅ DSL and advanced queries

The **resolution logic** is the same, only the **syntax wrapper** differs!

---

**Last Updated**: 2025-10-16
**Version**: 1.0.0

---

*Always check which graph type you're using before writing queries!*
```
