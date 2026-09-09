# Logseq DB AST (Abstract Syntax Tree): Comprehensive Investigation

**Investigation Date:** November 21, 2025  
**Last Verified:** May 29, 2026
**Last revised:** 2026-05-29
**Version:** 1.1  
**Investigation Type:** Architecture Deep Dive

---

## Executive Summary

### Verification Status (2026-05-25)

- Core architecture findings remain valid: mldoc-backed AST flow, two-level AST hierarchy, and non-persistent AST behavior.
- Several historical file line ranges in this document have drifted as source files evolved.
- Treat symbol names and file paths as source of truth; resolve exact locations from current code before using line-specific references.
- This document is kept as a deep-dive reference, not a line-stable API contract.

### Key Findings

- **AST is the core parsing representation** - Logseq uses mldoc's AST to represent parsed markdown/org-mode content
- **Two-level hierarchy** - Block AST (paragraphs, headings, lists) contains Inline AST (links, emphasis, code)
- **JSON bridge with mldoc** - JavaScript library mldoc converts text → JSON → EDN AST in ClojureScript
- **Temporary AST storage** - AST is cached with `:block.temp/ast-title` and `:block.temp/ast-body` for UI rendering
- **No persistent AST** - AST is regenerated on-demand from text content, not stored in database
- **Multiple transformation pipelines** - Export (HTML, OPML, text), rendering (hiccup), and extraction (blocks/refs)

## Findings: New Capabilities

- **Inline macro recovery is now explicit** - `recover-inline-macros` and `normalize-macro-asts` rebuild fragmented macros, including nested page refs and script markup like `^{...}` / `_{...}`.
- **Export helpers are broader than the original guide implied** - `parseAndExportMarkdown`, `parseAndExportOPML`, and `export` now sit alongside `astExportMarkdown` and `getReferences` in the mldoc bridge.
- **Default config handling is more defensive** - `default-config-map` now filters nil options, capitalizes the format name, and converts `export-md-remove-options` into mldoc's `export_md_remove_options` shape.

### Critical Components

| Component            | Location                                                      | Purpose                                   |
| -------------------- | ------------------------------------------------------------- | ----------------------------------------- |
| **mldoc schema**     | `deps/graph-parser/src/logseq/graph_parser/schema/mldoc.cljc` | Defines AST structure with Malli schemas  |
| **mldoc wrapper**    | `deps/graph-parser/src/logseq/graph_parser/mldoc.cljc`        | Wraps mldoc JS library, converts JSON→EDN |
| **block extraction** | `deps/graph-parser/src/logseq/graph_parser/block.cljs`        | Extracts blocks from AST for database     |
| **AST rendering**    | `src/main/frontend/components/block.cljs`                     | Maps AST to hiccup for UI display         |
| **AST caching**      | `src/main/frontend/format/block.cljs`                         | Caches parsed AST with LRU cache          |

---

## 1. What is the Logseq DB AST?

### 1.1 Conceptual Overview

The **AST (Abstract Syntax Tree)** in Logseq is a structured data representation of markdown/org-mode content. It's not a "DB AST" in the sense of being stored in the database, but rather a **parsing intermediate representation** used throughout the application.

```
Text Content → mldoc Parser → JSON → EDN Converter → ClojureScript AST → Multiple Uses
    ↓                              ↓                          ↓                   ↓
"# Header"                    JSON array              Clojure vectors      UI/Export/DB
```

### 1.2 Why AST Exists

**Purpose:**

1. **Structured parsing** - Convert unstructured text into analyzable data structures
2. **Extract semantic information** - Identify blocks, references, links, tags, properties
3. **Enable transformations** - Support export formats (HTML, OPML), imports, rendering
4. **Facilitate analysis** - Query content, extract metadata, build graph connections

**Key Insight:** AST is the **lingua franca** between text storage and structured processing.

---

## 2. AST Structure and Schema

### 2.1 Two-Level Hierarchy

AST has two distinct levels:

#### **Block-Level AST** (Structural elements)

- Paragraph
- Heading
- List (ordered/unordered)
- Code blocks (Example, Src)
- Quote
- Table
- Properties
- Directives, Comments, etc.

#### **Inline-Level AST** (Text formatting within blocks)

- Plain text
- Emphasis (Bold, Italic, Underline, Strike-through, Highlight)
- Links (Page refs, Block refs, URLs)
- Code/Verbatim
- Tags
- Timestamps
- Macros
- LaTeX
- Entities

### 2.2 AST Format: EDN Vectors

**Location:** `deps/graph-parser/src/logseq/graph_parser/schema/mldoc.cljc`

All AST nodes follow this pattern:

```clojure
[ast-type ast-content]
```

**Examples:**

```clojure
;; Block AST
["Paragraph" [inline-elements]]
["Heading" {:title [...] :level 1 :marker "TODO" :priority "A" ...}]
["List" [{:content [...] :items [...] :ordered true ...}]]
["Src" {:language "clojure" :lines ["(def x 1)"] :pos_meta {...}}]

;; Inline AST
["Plain" "hello world"]
["Emphasis" [["Bold"] [["Plain" "important"]]]]
["Link" {:url ["Page_ref" "MyPage"] :label [["Plain" "link text"]] :full_text "[[MyPage]]"}]
["Code" "`code`"]
["Tag" [["Plain" "tagname"]]]
["Timestamp" ["Scheduled" {:date {...} :time {...} :active true}]]
```

### 2.3 Schema Definitions (Malli)

**File:** `deps/graph-parser/src/logseq/graph_parser/schema/mldoc.cljc` (line ranges may drift)

#### Inline AST Schema

```clojure
(def inline-ast-schema
  [:schema {:registry {::inline
    [:or
      [:tuple [:= "Emphasis"] ...]
      [:tuple [:= "Plain"] :string]
      [:tuple [:= "Link"] link-schema]
      [:tuple [:= "Code"] :string]
      [:tuple [:= "Tag"] [:sequential [:ref ::inline]]]
      [:tuple [:= "Timestamp"] ...]
      [:tuple [:= "Macro"] {:name :string :arguments [:sequential :string]}]
      ...
    ]}}
   ::inline])
```

#### Block AST Schema

```clojure
(def block-ast-schema
  [:schema {:registry {::block
    [:or
      [:tuple [:= "Paragraph"] [:sequential [:ref ::inline]]]
      [:tuple [:= "Heading"] heading-schema]
      [:tuple [:= "List"] [:sequential list-item-schema]]
      [:tuple [:= "Src"] {:lines [...] :language :string ...}]
      ...
    ]}}
   ::block])
```

**Position Metadata:**
AST nodes can have position metadata attached:

```clojure
[[ast-node] {:start_pos 0 :end_pos 42}]
```

---

## 3. AST Generation Pipeline

### 3.1 Text → AST Flow

```
┌─────────────────┐
│  User Content   │
│  (Markdown/Org) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  mldoc (JS lib) │  ← External library (OCaml → JS compiled)
│  parseJson()    │
└────────┬────────┘
         │ JSON string
         ▼
┌─────────────────┐
│ json->clj       │  ← cljs-bean conversion
│ (EDN conversion)│
└────────┬────────┘
         │ EDN vectors
         ▼
┌─────────────────┐
│ Post-processing │  ← Add position meta, extract properties
│ collect-page-   │
│ properties      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  AST (EDN)      │  ← Ready for consumption
└─────────────────┘
```

### 3.2 Key Entry Points

#### Primary Parsing Function

**Location:** `deps/graph-parser/src/logseq/graph_parser/mldoc.cljc:148-167`

```clojure
(defn ->edn
  "Parses content into AST"
  ([content config]
   (if (string? content)
     (try
       (if (string/blank? content)
         []
         (-> content
             (parse-json config)           ; Call mldoc JS
             (common-util/json->clj)       ; JSON → EDN
             (update-src-full-content content)  ; Fix code blocks
             (collect-page-properties config))) ; Add Properties block
       (catch :default e
         (log/error :unexpected-error e)
         []))
     (log/error :edn/wrong-content-type content)))
  ([repo content format]
   (->edn content (get-default-config repo format))))
```

#### mldoc JavaScript Interface

**Location:** `deps/graph-parser/src/logseq/graph_parser/mldoc.cljc:22-26`

```clojure
(defonce parseJson (gobj/get Mldoc "parseJson"))
(defonce parseInlineJson (gobj/get Mldoc "parseInlineJson"))
(defonce astExportMarkdown (gobj/get Mldoc "astExportMarkdown"))
(defonce getReferences (gobj/get Mldoc "getReferences"))
```

#### Configuration

**Location:** `deps/graph-parser/src/logseq/graph_parser/mldoc.cljc:53-74`

```clojure
(defn default-config-map
  [format opts]
  {:toc false
   :parse_outline_only false
   :heading_number false
   :keep_line_break true
   :format "Markdown"  ; or "Org"
   :heading_to_list false
   :exporting_keep_properties false
   :export_md_indent_style nil
   :export_md_remove_options []})
```

**DB Graph Differences:**

```clojure
(defn get-default-config [repo format]
  (let [db-based? (sqlite-util/db-based-graph? repo)]
    (cond-> (default-config-map format)
      db-based?
      (assoc :enable_drawers false    ; No drawers in DB graphs
             :parse_marker false       ; No TODO markers
             :parse_priority false)))) ; No priorities
```

### 3.3 Inline Parsing

**Location:** `deps/graph-parser/src/logseq/graph_parser/mldoc.cljc:177-185`

```clojure
(defn inline->edn
  "Parse inline text (for rendering individual strings)"
  [text config]
  (try
    (if (string/blank? text)
      {}
      (-> text
          (inline-parse-json config)
          (common-util/json->clj)))
    (catch :default _e
      [])))
```

**Usage:** Parsing page titles, property values, individual text snippets.

---

## 4. AST → Block Extraction

### 4.1 Purpose

Convert AST into database-ready block structures with:

- UUIDs
- Parent-child relationships
- Levels and indentation
- Extracted properties, timestamps, tags, references
- Content positions

### 4.2 Extraction Flow

**Location:** `deps/graph-parser/src/logseq/graph_parser/block.cljs:713-788`

```clojure
(defn extract-blocks
  "Extract headings from mldoc ast"
  [ast content format {:keys [user-config db-graph-mode? export-to-db-graph?] :as options}]
  {:pre [(seq ast) (string? content) (contains? #{:markdown :org} format)]}
  (let [encoded-content (utf8/encode content)
        all-blocks (vec (reverse ast))
        [blocks body pre-block-properties]
        (loop [headings []
               ast-blocks (reverse ast)
               block-idx 0
               timestamps {}
               properties {}
               body []]
          (if (seq ast-blocks)
            (let [[ast-block pos-meta] (first ast-blocks)]
              (cond
                (paragraph-timestamp-block? ast-block)
                ; Extract SCHEDULED/DEADLINE

                (gp-property/properties-ast? ast-block)
                ; Extract properties

                (heading-block? ast-block)
                ; Construct block with title, properties, body
                (let [block' (construct-block ast-block properties timestamps body ...)]
                  (recur (conj headings block'') ...))

                :else
                ; Add to body
                (recur headings ... (conj body ast-block))))
            [(reverse headings) body properties]))
        result (with-pre-block-if-exists blocks body pre-block-properties ...)]
    (map #(dissoc % :block/meta) result)))
```

### 4.3 Block Structure Output

**Example:**

```clojure
{:block/uuid #uuid "..."
 :block/content "TODO Write documentation #important"
 :block/title "TODO Write documentation #important"  ; Raw text
 :block/marker "TODO"
 :block/priority "A"
 :block/level 1
 :block/tags #{#:block{:name "important"}}
 :block/refs [{:block/name "important"}]  ; All references
 :block/scheduled 20251121
 :block/properties {:custom-prop "value"}
 :block/page {:block/name "page-name"}
 :block/parent [:block/uuid parent-uuid]
 :block/left [:block/uuid left-sibling-uuid]
 :block/format :markdown}
```

### 4.4 Reference Extraction

**From AST nodes:**

```clojure
(defn get-page-reference [block format]
  (cond
    (and (vector? block) (= "Link" (first block)))
    ; Extract page ref from Link node

    (and (vector? block) (= "Tag" (first block)))
    ; Extract tag name

    (and (vector? block) (= "Macro" (first block)))
    ; Check for {{embed [[PageName]]}}
    ...))
```

**All inline elements are walked to find:**

- Page references `[[Page Name]]`
- Block references `((block-uuid))`
- Tags `#tag`
- Macro embeds `{{embed ...}}`

---

## 5. AST Caching and Performance

### 5.1 AST is NOT Persisted

**Critical:** AST is **never stored in the database**. Only text content is persisted.

**Reasoning:**

- AST can be regenerated from text
- Storage overhead would be massive
- Schema changes would require migrations
- Text is source of truth

### 5.2 Temporary AST Attributes

**Location:** `deps/db/src/logseq/db/common/entity_plus.cljc:24-25`

```clojure
(def nil-db-ident-entities
  #{:block.temp/ast-title
    :block.temp/ast-body
    :block.temp/load-status
    :block.temp/has-children?
    ...})
```

**These are ephemeral attributes used during rendering:**

- `:block.temp/ast-title` - Parsed title AST (inline elements)
- `:block.temp/ast-body` - Parsed body AST (block elements)

**They exist only in-memory** for the current block being rendered.

### 5.3 AST Cache (LRU)

**Location:** `src/main/frontend/format/block.cljs:99-130`

```clojure
(defonce *blocks-ast-cache (volatile! (cache/lru-cache-factory {} :threshold 5000)))

(defn- parse-title-and-body-helper
  [format content]
  (let [parse-config (mldoc/get-default-config format)
        ast (->> (format/to-edn content format parse-config)
                 (map first))
        title (when (gp-block/heading-block? (first ast))
                (:title (second (first ast))))
        body (vec (if title (rest ast) ast))
        body (drop-while gp-property/properties-ast? body)]
    (cond->
     (if (seq body) {:block.temp/ast-body body} {})
      title
      (assoc :block.temp/ast-title title))))

(def ^:private cached-parse-title-and-body-helper
  (common.cache/cache-fn
   *blocks-ast-cache
   (fn [format content] [[format content] [format content]])
   parse-title-and-body-helper))
```

**Cache Key:** `[format content]`  
**Cache Size:** 5000 entries (LRU eviction)  
**Scope:** Per-session, in-memory only

### 5.4 Performance Characteristics

| Operation           | Time Complexity | Notes                             |
| ------------------- | --------------- | --------------------------------- |
| Parse text → AST    | O(n)            | n = content length, mldoc is fast |
| Cache lookup        | O(1)            | Hash map lookup                   |
| Block extraction    | O(m)            | m = # of AST nodes                |
| Render AST → hiccup | O(m)            | Walk all nodes                    |

**Bottlenecks:**

- Initial parsing (first render)
- Large documents (>10,000 blocks)
- Complex inline elements (many refs/tags)

---

## 6. AST Rendering Pipeline

### 6.1 AST → Hiccup Flow

```
┌──────────────┐
│ Block Entity │ {:block/title "..."}
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ parse-title- │  ← Cache lookup or parse
│ and-body     │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ :block.temp/ │  ← Temporary AST attributes
│ ast-title    │
│ ast-body     │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ map-inline   │  ← Convert inline AST → hiccup
│ (recursive)  │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ Hiccup       │  [:span [:b "text"] [:a ...]]
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ Rum/React    │  ← Render in browser
│ Component    │
└──────────────┘
```

### 6.2 map-inline Function

**Location:** `src/main/frontend/components/block.cljs` (extensive, ~1000+ lines)

**Core pattern:**

```clojure
(defn map-inline
  "Convert inline AST elements to hiccup"
  [config inline-list]
  (map (fn [inline-element]
         (let [[ast-type ast-content] inline-element]
           (case ast-type
             "Plain" ast-content
             "Emphasis" (inline-emphasis config ast-content)
             "Link" (inline-link config ast-content)
             "Tag" (inline-tag config ast-content)
             "Code" [:code ast-content]
             "Macro" (inline-macro config ast-content)
             ...)))
       inline-list))
```

**Handles:**

- Text formatting (bold, italic, etc.)
- Links (pages, blocks, URLs)
- Tags
- Timestamps
- Macros (queries, embeds, custom)
- LaTeX
- Code blocks
- Images/assets

### 6.3 Block Rendering

**Location:** `src/main/frontend/components/block.cljs:2343-2440`

```clojure
(rum/defc text-block-title
  [config block]
  (let [block (if-not (:block.temp/ast-title block)
                ; Parse if not cached
                (merge block (block/parse-title-and-body ...))
                block)
        block-ast-title (:block.temp/ast-title block)
        config (assoc config :block block)
        ...]
    (when (seq block-ast-title)
      (->elem
       elem
       {:data-hl-type ...}
       ; Render inline AST
       (map-inline config block-ast-title)))))
```

**Rendering cycle:**

1. Check for `:block.temp/ast-title`
2. If missing, call `parse-title-and-body` (hits cache)
3. Extract `block-ast-title`
4. Pass to `map-inline` for hiccup conversion
5. Rum renders hiccup to DOM

---

## 7. AST Export Pipelines

### 7.1 Export Formats

Logseq supports exporting AST to:

- **HTML** - Web pages, static sites
- **OPML** - Outliner format
- **Markdown/Text** - Plain text export

> **Removed:** JSON is not a supported export target in current source. The AST remains an internal parse result, not a user-facing export mode.

### 7.2 HTML Export

**Location:** `src/main/frontend/handler/export/html.cljs`

**Pipeline:**

```clojure
(defn export-helper
  [content format options]
  (let [ast (mldoc/->edn content format)
        ast (mapv common/remove-block-ast-pos ast)
        ast (removev common/Properties-block-ast? ast)
        ast* (common/replace-block&page-reference&embed ast)
        ast** (mapv common/replace-Heading-with-Paragraph ast*)
        hiccup (z/root (reduce block-ast->hiccup empty-ul-hiccup ast***))]
    (-> hiccup h/render-html utils/prettifyXml)))
```

**Transformations:**

1. Remove position metadata
2. Filter Properties blocks
3. Replace block/page references with expanded content
4. Convert headings to paragraphs (if needed)
5. Walk AST → hiccup
6. Render hiccup → HTML string
7. Prettify XML

### 7.3 OPML Export

**Location:** `src/main/frontend/handler/export/opml.cljs`

**OPML format:**

```xml
<opml version="2.0">
  <head><title>Page Title</title></head>
  <body>
    <outline text="First block">
      <outline text="Nested block" />
    </outline>
  </body>
</opml>
```

**AST → OPML:**

- Headings become nested `<outline>` elements
- Inline elements converted to plain text
- List structure preserved with nesting

### 7.4 Markdown Export

**Location:** `deps/cli/src/logseq/cli/common/export/text.cljs`

**Pipeline:**

```clojure
1. Parse AST from content
2. Walk AST with transformers:
   - remove-emphasis (if opted)
   - remove-page-ref-brackets
   - remove-tags
   - remove-properties
3. Convert AST → "simple AST" (intermediate format)
4. simple-asts→string (final text)
```

**Simple AST format:**

```clojure
{:type :raw-text :content "hello"}
{:type :space}
{:type :newline :line-count 2}
{:type :indent :level 1 :extra-space-count 0}
```

**Purpose:** Easier to manipulate whitespace, indentation, formatting.

---

## 8. AST Transformation Utilities

### 8.1 Walk Functions

**Location:** `deps/cli/src/logseq/cli/common/export/common.cljs:601-670`

```clojure
(defn walk-block-ast
  "Apply transformations to AST recursively"
  [{:keys [map-fns-on-inline-ast
           mapcat-fns-on-inline-ast
           fns-on-inline-coll]}
   block-ast]
  (let [[ast-type ast-content] block-ast]
    (case ast-type
      "Paragraph"
      (mk-paragraph-ast
       (walk-block-ast-helper ast-content map-fns ...))

      "Heading"
      ["Heading" (assoc ast-content :title (walk-inline ...))]

      "List"
      ["List" (walk-block-ast-for-list ast-content ...)]

      ...)))
```

**Transformation types:**

- `:map-fns-on-inline-ast` - Transform each inline element
- `:mapcat-fns-on-inline-ast` - Transform and flatten
- `:fns-on-inline-coll` - Transform entire inline collection

**Example transformers:**

```clojure
(defn remove-emphasis [inline-ast]
  (let [[ast-type ast-content] inline-ast]
    (case ast-type
      "Emphasis"
      (let [[_ inline-coll] ast-content]
        inline-coll)  ; Unwrap emphasis, return contents
      [inline-ast])))

(defn remove-page-ref-brackets [inline-ast]
  (let [[ast-type ast-content] inline-ast]
    (case ast-type
      "Link"
      (let [{:keys [url]} ast-content]
        (if (= "Page_ref" (first url))
          ["Plain" (second url)]  ; [[Page]] → Page
          inline-ast))
      inline-ast)))
```

### 8.2 AST Comparison and Diffing

**Location:** `src/main/frontend/fs/diff_merge.cljs`

**Used for:** Three-way merge during file sync

```clojure
(defn three-way-merge [base income current format]
  (let [->ast (fn [text] (gp-mldoc/->edn text (gp-mldoc/default-config format)))
        base-ast (->ast base)
        income-ast (->ast income)
        current-ast (->ast current)
        base-diffblocks (ast->diff-blocks base-ast ...)
        income-diffblocks (ast->diff-blocks income-ast ...)
        current-diffblocks (ast->diff-blocks current-ast ...)
        merged (.mergeBlocks merger base-diffblocks [current income])]
    (rebuild-content merged format)))
```

**Process:**

1. Parse all three versions to AST
2. Convert AST → diffable block structures
3. Use diff-merge algorithm (JavaScript library)
4. Rebuild content from merged diff

---

## 9. AST-Related Gotchas and Edge Cases

### 9.1 Position Metadata Handling

**Issue:** Position metadata `{:start_pos :end_pos}` can be fragile

```clojure
; AST with position
[["Heading" {...}] {:start_pos 0 :end_pos 20}]

; After transformation
(mapv remove-block-ast-pos ast)
; → ["Heading" {...}]
```

**Why remove?**

- Positions change after edits
- Not needed for most operations
- Can cause confusion in comparisons

### 9.2 Paragraph_line vs Paragraph

> **Removed:** the old `Paragraph_line` warning is stale. Current parser schema and tests no longer expose `Paragraph_line`, so treat `Paragraph` as the block-level unit in this guide.

### 9.3 Properties Block

**Special AST node added by Logseq:**

```clojure
["Properties" [[key val refs] [key2 val2 refs2] ...]]
```

**Origin:**

- **NOT from mldoc**
- Added by `collect-page-properties` function
- Combines all directive/property lines

**Filtering:**

```clojure
(defn Properties-block-ast? [[tp _]]
  (= tp "Properties"))

(removev Properties-block-ast? ast)
```

### 9.4 DB vs File Graph Differences

**DB Graphs:**

```clojure
{:enable_drawers false      ; No :LOGBOOK: drawers
 :parse_marker false        ; No TODO/DONE markers
 :parse_priority false}     ; No [#A] priorities
```

**File Graphs:**

```clojure
{:enable_drawers true
 :parse_marker true
 :parse_priority true}
```

**Impact:** AST structure differs between graph types!

### 9.5 Multiline Block Titles

**Challenge:** Deciding when to cut block title vs include in body

**For DB graphs:**

```clojure
(let [cut-multiline? (and export-to-db-graph?
                          (or (properties-in-prev-block?)
                              (logbook-in-prev-block?)
                              (deadline-scheduled-in-prev-block?)))]
  ...)
```

**Title should only contain first line if:**

- Previous block has properties
- Previous block is logbook drawer
- Previous block has timestamps

---

## 10. Common AST Patterns and Recipes

### 10.1 Extract All Page References

```clojure
(defn extract-page-refs [ast]
  (->> ast
       (mapcat (fn [[block _pos]]
                 (tree-seq vector? identity block)))
       (filter (fn [node]
                 (and (vector? node)
                      (= "Link" (first node))
                      (= "Page_ref" (first (:url (second node)))))))
       (map (fn [node] (second (:url (second node)))))))
```

### 10.2 Find All Tags

```clojure
(defn extract-tags [ast]
  (->> ast
       (mapcat (fn [[block _pos]]
                 (tree-seq vector? identity block)))
       (filter (fn [node]
                 (and (vector? node)
                      (= "Tag" (first node)))))
       (map (fn [node] (get-tag node)))))
```

### 10.3 Count Headings by Level

```clojure
(defn count-headings-by-level [ast]
  (->> ast
       (filter (fn [[block _pos]]
                 (= "Heading" (first block))))
       (map (fn [[_ content]] (:level content)))
       frequencies))
```

### 10.4 Extract Code Blocks

```clojure
(defn extract-code-blocks [ast]
  (->> ast
       (filter (fn [[block _pos]]
                 (= "Src" (first block))))
       (map (fn [[_ content]]
              {:language (:language content)
               :code (string/join "\n" (:lines content))}))))
```

### 10.5 Replace Inline Elements

```clojure
(defn replace-inline-element [inline-coll pred replacement-fn]
  (mapv (fn [inline]
          (if (pred inline)
            (replacement-fn inline)
            inline))
        inline-coll))

; Example: Replace all page refs with plain text
(replace-inline-element
  inline-coll
  (fn [[type _]] (= type "Link"))
  (fn [[_ {:keys [url label]}]]
    (if (= "Page_ref" (first url))
      ["Plain" (or (get-in label [0 1]) (second url))]
      ["Link" content])))
```

---

## 11. Performance Optimization Strategies

### 11.1 Caching Best Practices

**DO:**

- ✅ Cache parsed AST with `cached-parse-title-and-body-helper`
- ✅ Use LRU cache to limit memory growth
- ✅ Cache at appropriate granularity (per block, not per page)

**DON'T:**

- ❌ Don't cache AST in database
- ❌ Don't cache transformed AST (cache source AST)
- ❌ Don't cache without size limits

### 11.2 Lazy Evaluation

**For large documents:**

```clojure
; BAD: Eager evaluation
(mapv walk-block-ast ast)  ; Processes all blocks immediately

; GOOD: Lazy evaluation
(map walk-block-ast ast)  ; Processes on-demand
```

### 11.3 Avoid Re-parsing

**Check if AST already exists:**

```clojure
(let [block (if-not (:block.temp/ast-title block)
              (merge block (block/parse-title-and-body ...))
              block)]
  ; Use (:block.temp/ast-title block)
  ...)
```

### 11.4 Batch Operations

**For multiple blocks:**

```clojure
; BAD: Parse each block individually
(doseq [block blocks]
  (parse-and-process block))

; GOOD: Parse entire page once
(let [page-ast (mldoc/->edn page-content format)
      extracted-blocks (extract-blocks page-ast ...)]
  (process-blocks extracted-blocks))
```

---

## 12. Testing and Debugging AST

### 12.1 Inspecting AST

**In REPL:**

```clojure
(require '[logseq.graph-parser.mldoc :as mldoc])

(def ast (mldoc/->edn "test-repo" "# TODO Header\n- Item 1\n- Item 2" :markdown))

(clojure.pprint/pprint ast)
; [["Heading"
;   {:title [["Plain" "Header"]]
;    :marker "TODO"
;    :level 1
;    ...}]
;  {:start_pos 0 :end_pos 15}]
```

**Print schema:**

```clojure
(require '[logseq.graph-parser.schema.mldoc :as schema])
(require '[malli.core :as m])

(m/validate schema/block-ast-schema ast-node)
```

### 12.2 Common Debugging Techniques

**Add logging:**

```clojure
(defn walk-block-ast [fns block-ast]
  (prn "Processing:" (first block-ast))  ; DEBUG
  (let [[ast-type ast-content] block-ast]
    ...))
```

**Use metadata:**

```clojure
(defn construct-block [ast ...]
  (with-meta
    {:block/content ...}
    {:source-ast ast
     :debug-info {...}}))
```

### 12.3 Test Patterns

**Location:** `deps/graph-parser/test/logseq/graph_parser/block_test.cljs`

```clojure
(deftest test-extract-blocks
  (let [content "# Header\n- Item"
        ast (mldoc/->edn "test" content :markdown)
        blocks (block/extract-blocks ast content :markdown {...})]
    (is (= 2 (count blocks)))
    (is (= "Header" (:block/title (first blocks))))))
```

**Test AST transformations:**

```clojure
(deftest test-remove-emphasis
  (let [ast ["Emphasis" [["Bold"] [["Plain" "text"]]]]
        result (common/remove-emphasis ast)]
    (is (= [["Plain" "text"]] result))))
```

---

## 13. Architecture Diagrams

### 13.1 High-Level Data Flow

```
                        ┌──────────────────┐
                        │ User Edits Block │
                        └────────┬─────────┘
                                 │
                                 ▼
                   ┌─────────────────────────┐
                   │ Save :block/content     │ (Text string)
                   │ to Database             │
                   └────────┬────────────────┘
                            │
                            │
      ┌─────────────────────┴────────────────────┐
      │                                          │
      ▼                                          ▼
┌──────────────┐                        ┌──────────────┐
│  UI Render   │                        │   Export     │
│  Request     │                        │   Request    │
└──────┬───────┘                        └──────┬───────┘
       │                                       │
       ▼                                       ▼
┌──────────────┐                        ┌──────────────┐
│ Check Cache  │                        │ mldoc/->edn  │
│ for AST      │                        │ (Parse)      │
└──────┬───────┘                        └──────┬───────┘
       │                                       │
       │ Cache miss                            │
       ▼                                       ▼
┌──────────────┐                        ┌──────────────┐
│ mldoc/->edn  │                        │ Transform    │
│ (Parse)      │                        │ AST (walk)   │
└──────┬───────┘                        └──────┬───────┘
       │                                       │
       ▼                                       ▼
┌──────────────┐                        ┌──────────────┐
│ Store in     │                        │ AST→HTML     │
│ :block.temp/ │                        │ AST→OPML     │
│ ast-*        │                        │ AST→Text     │
└──────┬───────┘                        └──────────────┘
       │
       ▼
┌──────────────┐
│ map-inline   │
│ (AST→hiccup) │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│ Rum Render   │
│ (hiccup→DOM) │
└──────────────┘
```

### 13.2 AST Structure Hierarchy

```
Block AST (Collection of blocks)
│
├─ ["Heading" {...}]
│   │
│   ├─ :title → [inline-ast ...]
│   ├─ :marker → "TODO"
│   ├─ :priority → "A"
│   └─ :level → 1
│
├─ ["Paragraph" [inline-ast ...]]
│   │
│   └─ Inline collection
│
├─ ["List" [{:content [...] :items [...]}]]
│   │
│   ├─ :content → [block-ast ...]
│   └─ :items → [list-item ...]
│       │
│       ├─ :content → [block-ast ...]
│       └─ :items → (recursive)
│
├─ ["Src" {:language "clj" :lines [...]}]
│
├─ ["Table" {:header [...] :groups [...]}]
│
└─ ["Properties" [[key val refs] ...]]


Inline AST (Within blocks)
│
├─ ["Plain" "text"]
│
├─ ["Emphasis" [[type] [inline-ast ...]]]
│   └─ type: "Bold", "Italic", "Strike_through", etc.
│
├─ ["Link" {:url [...] :label [...] :full_text "..."}]
│   │
│   ├─ :url → ["Page_ref" "PageName"]
│   │      → ["Block_ref" "uuid"]
│   │      → ["Complex" {:protocol "https" :link "..."}]
│   │
│   └─ :label → [inline-ast ...]
│
├─ ["Tag" [inline-ast ...]]
│
├─ ["Code" "code text"]
│
├─ ["Macro" {:name "embed" :arguments ["..."]}]
│
├─ ["Timestamp" [type timestamp-data]]
│   └─ type: "Scheduled", "Deadline", "Date", etc.
│
└─ ["Latex_Fragment" [type content]]
    └─ type: "Inline" or "Displayed"
```

### 13.3 Parsing Pipeline Detail

```
Text String
    │
    ▼
┌────────────────────────────────────┐
│ mldoc (JavaScript library)         │
│ - Lexer: Text → Tokens             │
│ - Parser: Tokens → JSON AST        │
└────────────┬───────────────────────┘
             │ JSON string
             ▼
┌────────────────────────────────────┐
│ parseJson(content, config)         │
│ Returns: JSON string               │
└────────────┬───────────────────────┘
             │
             ▼
┌────────────────────────────────────┐
│ common-util/json->clj              │
│ - Parse JSON                       │
│ - Convert to ClojureScript vectors │
└────────────┬───────────────────────┘
             │ EDN vectors
             ▼
┌────────────────────────────────────┐
│ update-src-full-content            │
│ - Fix code block indentation       │
│ - Extract full source text         │
└────────────┬───────────────────────┘
             │
             ▼
┌────────────────────────────────────┐
│ collect-page-properties            │
│ - Extract Directive nodes          │
│ - Create Properties block          │
│ - Add references from properties   │
└────────────┬───────────────────────┘
             │
             ▼
      Final AST (EDN)
[["Heading" {...}] pos-meta]
[["Paragraph" [...]] pos-meta]
...
```

---

## 14. Common Use Cases

### 14.1 Rendering a Block in UI

**Scenario:** Display a block with formatted text

```clojure
; 1. Fetch block from DB
(def block (db/entity [:block/uuid uuid]))

; 2. Parse title to AST (with cache)
(def parsed-block
  (merge block
         (block/parse-title-and-body
           (:block/uuid block)
           (:block/format block)
           false
           (:block/title block))))

; 3. Extract AST
(def ast-title (:block.temp/ast-title parsed-block))

; 4. Convert to hiccup
(def hiccup (map-inline config ast-title))

; 5. Render with Rum
(rum/defc block-component [config block]
  [:div.block
   (text-block-title config block)])
```

### 14.2 Exporting Page to HTML

**Scenario:** Export entire page with all blocks

```clojure
; 1. Get page content
(def page-content (get-page-content page-uuid))

; 2. Parse to AST
(def ast (mldoc/->edn page-content :markdown))

; 3. Clean AST
(def cleaned-ast
  (-> ast
      (mapv remove-block-ast-pos)
      (removev Properties-block-ast?)))

; 4. Transform references
(def expanded-ast
  (replace-block&page-reference&embed cleaned-ast))

; 5. Convert to hiccup
(def hiccup
  (reduce block-ast->hiccup empty-ul-hiccup expanded-ast))

; 6. Render HTML
(def html (h/render-html hiccup))
```

### 14.3 Extracting Blocks from File

**Scenario:** Import markdown file into Logseq

```clojure
; 1. Read file content
(def content (slurp "file.md"))

; 2. Parse to AST
(def ast (mldoc/->edn "repo" content :markdown))

; 3. Extract blocks
(def blocks
  (block/extract-blocks
    ast
    content
    :markdown
    {:user-config {}
     :block-pattern "-"
     :page-name "imported-page"}))

; 4. Save to DB
(doseq [block blocks]
  (transact! [{:block/uuid (:block/uuid block)
               :block/content (:block/content block)
               :block/page {:block/name "imported-page"}
               ...}]))
```

### 14.4 Custom AST Transformation

**Scenario:** Remove all bold formatting

```clojure
(defn remove-bold [inline-ast]
  (let [[ast-type ast-content] inline-ast]
    (case ast-type
      "Emphasis"
      (let [[[emphasis-type] inline-coll] ast-content]
        (if (= emphasis-type "Bold")
          inline-coll  ; Unwrap bold
          [inline-ast]))
      [inline-ast])))

(defn process-content [content]
  (let [ast (mldoc/->edn "repo" content :markdown)
        transformed (mapv
                      (partial walk-block-ast
                        {:mapcat-fns-on-inline-ast [remove-bold]})
                      ast)]
    (ast->markdown transformed)))
```

---

## 15. Future Considerations and Recommendations

### 15.1 For Developers

**When working with AST:**

1. **Always validate schema** - Use Malli schemas to catch errors early
2. **Cache intelligently** - Understand cache lifecycle and invalidation
3. **Handle both graph types** - Test with file-based AND db-based graphs
4. **Document transformations** - Complex walks need clear documentation
5. **Test edge cases** - Empty blocks, nested structures, special characters

**When extending AST functionality:**

1. **Don't modify mldoc AST structure** - It's external dependency
2. **Add post-processing** - Use `collect-page-properties` pattern
3. **Preserve position metadata** - Useful for error reporting
4. **Consider performance** - AST operations are in hot path

### 15.2 For Plugin Developers

**Accessing AST in plugins:**

```javascript
// Not directly exposed - use published APIs instead
logseq.Editor.getBlock(uuid).then((block) => {
  // Block has :block/content (text)
  // AST must be generated client-side if needed
})
```

**Recommendations:**

- Use query API for structured data extraction
- Don't rely on internal AST implementation
- Use published block/page APIs
- Consider using mldoc library directly in plugin

### 15.3 Potential Improvements

**Performance:**

- ☐ Incremental parsing (only parse changed blocks)
- ☐ Web Worker parsing (off main thread)
- ☐ Streaming AST for large files
- ☐ Compiled parsers (instead of mldoc)

**Features:**

- ☐ AST-based search (faster than text search)
- ☐ Structural diff/merge (better conflict resolution)
- ☐ AST-based refactoring tools
- ☐ Linter/validator based on AST

**Developer Experience:**

- ☐ Better AST debugging tools
- ☐ Visual AST explorer
- ☐ Schema documentation generator
- ☐ AST migration utilities

---

## 16. Reference Index

### 16.1 Key Files

| File                                                          | Purpose                      | Lines |
| ------------------------------------------------------------- | ---------------------------- | ----- |
| `deps/graph-parser/src/logseq/graph_parser/schema/mldoc.cljc` | AST schema definitions       | 220   |
| `deps/graph-parser/src/logseq/graph_parser/mldoc.cljc`        | mldoc wrapper, parsing       | 250   |
| `deps/graph-parser/src/logseq/graph_parser/block.cljs`        | Block extraction from AST    | 911   |
| `src/main/frontend/format/block.cljs`                         | AST caching, parsing helpers | 170   |
| `src/main/frontend/components/block.cljs`                     | AST rendering to hiccup      | 4685  |
| `src/main/frontend/handler/export/html.cljs`                  | HTML export                  | 430   |
| `src/main/frontend/handler/export/opml.cljs`                  | OPML export                  | 450   |
| `deps/cli/src/logseq/cli/common/export/text.cljs`             | Text export                  | 500   |
| `deps/cli/src/logseq/cli/common/export/common.cljs`           | Export utilities             | 800   |

### 16.2 Key Functions

| Function                     | Location                      | Purpose                   |
| ---------------------------- | ----------------------------- | ------------------------- |
| `mldoc/->edn`                | `graph_parser/mldoc.cljc:148` | Parse text → AST          |
| `mldoc/inline->edn`          | `graph_parser/mldoc.cljc:177` | Parse inline text → AST   |
| `block/extract-blocks`       | `graph_parser/block.cljs:713` | AST → block entities      |
| `block/parse-title-and-body` | `format/block.cljs:118`       | Parse & cache block AST   |
| `map-inline`                 | `components/block.cljs`       | AST → hiccup              |
| `walk-block-ast`             | `export/common.cljs:628`      | Transform AST recursively |
| `block-ast->hiccup`          | `export/html.cljs:328`        | Block AST → hiccup        |
| `inline-ast->hiccup`         | `export/html.cljs:283`        | Inline AST → hiccup       |

### 16.3 Key Data Structures

```clojure
; Block AST node
[ast-type ast-content]
["Heading" {:title [...] :level 1 :marker "TODO"}]

; Inline AST node
["Link" {:url ["Page_ref" "PageName"] :label [...] :full_text "[[PageName]]"}]

; AST with position
[[ast-type ast-content] {:start_pos 0 :end_pos 20}]

; Extracted block
{:block/uuid #uuid "..."
 :block/content "..."
 :block/title "..."
 :block/marker "TODO"
 :block/refs [...]
 :block/properties {...}}

; Cached AST
{:block.temp/ast-title [inline-ast ...]
 :block.temp/ast-body [block-ast ...]}
```

### 16.4 External Dependencies

- **mldoc** - OCaml parser compiled to JavaScript ([github.com/logseq/mldoc](https://github.com/logseq/mldoc))
  - Version: Check `package.json`
  - API: `parseJson`, `parseInlineJson`, `astExportMarkdown`, `getReferences`, `parseAndExportMarkdown`, `parseAndExportOPML`, `export`

---

## 17. Glossary

| Term                  | Definition                                                          |
| --------------------- | ------------------------------------------------------------------- |
| **AST**               | Abstract Syntax Tree - structured representation of parsed content  |
| **Block AST**         | Top-level structural elements (headings, paragraphs, lists)         |
| **Inline AST**        | Text-level elements within blocks (links, emphasis, code)           |
| **mldoc**             | External parsing library (OCaml → JS)                               |
| **EDN**               | Extensible Data Notation (Clojure's data format)                    |
| **hiccup**            | ClojureScript format for HTML: `[:div {:class "x"} "content"]`      |
| **Position metadata** | `{:start_pos :end_pos}` character offsets in original text          |
| **Simple AST**        | Intermediate format for exports: `{:type :raw-text :content "..."}` |
| **Block extraction**  | Converting AST → database-ready block entities                      |
| **map-inline**        | Function that converts inline AST → hiccup for rendering            |
| **walk-block-ast**    | Recursive AST transformation function                               |
| `:block.temp/ast-*`   | Temporary attributes for cached AST (not persisted)                 |

---

## 18. Change History

| Version | Date       | Changes                             |
| ------- | ---------- | ----------------------------------- |
| 1.0     | 2025-11-21 | Initial comprehensive investigation |

---

## 19. Related Documentation

- [mldoc GitHub Repository](https://github.com/logseq/mldoc)
- [Logseq Database Schema](../03-features/database-graphs/schema-guide.md)
- [Graph Parser README](../../../deps/graph-parser/README.md)
- [Import/Export Best Practices](../08-processes/import-export-best-practices.md)
- [Query System Developer Guide](../03-features/query-system/developer-guide.md)

---

**End of Investigation**

For questions or clarifications, please refer to the code locations specified or open an issue in the Logseq repository.
