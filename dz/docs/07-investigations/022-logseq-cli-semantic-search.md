# Logseq CLI Semantic Search Investigation

## Question

Does the `logseq search "todo"` command use semantic search when semantic search is enabled on a graph?

## Answer

**Yes.** The CLI search command automatically uses semantic search when it's enabled on the graph. It employs a hybrid search approach that combines both keyword-based and semantic search results.

## How It Works

### Search Flow

The search request flows through multiple layers:

1. **CLI Layer**: `logseq search "query"` → [deps/cli/src/logseq/cli/commands/search.cljs](../../deps/cli/src/logseq/cli/commands/search.cljs)
2. **Handler Layer**: [src/main/frontend/handler/search.cljs](../../src/main/frontend/handler/search.cljs) - calls `search-handler/search`
3. **Frontend Search**: [src/main/frontend/search.cljs](../../src/main/frontend/search.cljs) - delegates to search engine
4. **Search Agency**: [src/main/frontend/search/agency.cljs](../../src/main/frontend/search/agency.cljs) - coordinates search engines
5. **Browser Engine**: [src/main/frontend/search/browser.cljs](../../src/main/frontend/search/browser.cljs) - invokes db-worker
6. **Worker Search**: [src/main/frontend/worker/search.cljs](../../src/main/frontend/worker/search.cljs) - **actual implementation**

### Hybrid Search Implementation

The core search logic in [src/main/frontend/worker/search.cljs](../../src/main/frontend/worker/search.cljs) (lines 347-438) performs:

```clojure
(defn search-blocks
  [repo conn search-db q {:keys [limit page enable-snippet? ...] :as option}]
  (m/sp
    (when-not (string/blank? q)
      (let [;; Multiple search strategies
            matched-result (search-blocks-aux search-db match-sql ...)  ; FTS
            non-match-result (search-blocks-aux search-db non-match-sql ...) ; LIKE
            fuzzy-result (fuzzy-search repo @conn q option)  ; Fuzzy

            ;; Semantic search (line 388)
            semantic-search-result* (m/? (embedding/task--search repo q 10))
            semantic-search-result (->> semantic-search-result*
                                        (map format-semantic-result))

            ;; Combine all results (line 403)
            combined-result (combine-results @conn
                              (concat fuzzy-result matched-result non-match-result)
                              semantic-search-result)
            ...]))))
```

### Result Ranking Algorithm

The `combine-results` function (lines 312-345) merges and re-ranks results using weighted scoring:

```clojure
(def config
  {:keyword-weight 0.9
   :semantic-weight 0.1})

(defn combine-results [db keyword-results semantic-results]
  (let [all-ids (set/union (set (map :id keyword-results))
                           (set (map :id semantic-results)))
        merged (map (fn [id]
                      (let [k-score (or keyword-score 0.0)
                            s-score (or (:semantic-score s-result) 0.0)
                            norm-k-score (normalize-score k-score k-min k-max)
                            combined-score (+ (* 0.9 norm-k-score)      ; 90% keyword
                                              (* 0.1 s-score)           ; 10% semantic
                                              (cond
                                                (ldb/page? block) 0.02  ; page bonus
                                                (:block/tags block) 0.01 ; tag bonus
                                                :else 0))]
                        (merge result {:combined-score combined-score
                                       :keyword-score k-score
                                       :semantic-score s-score})))
                    all-ids)]
    (sort-by :combined-score #(compare %2 %1) merged)))
```

**Scoring formula:**

```
combined_score = (0.9 × normalized_keyword_score) + (0.1 × semantic_score) + bonuses
```

**Bonuses:**

- Pages: +0.02
- Tagged blocks: +0.01

### Semantic Search Execution

The [src/main/frontend/worker/embedding.cljs](../../src/main/frontend/worker/embedding.cljs) `task--search` function (lines 232-254):

```clojure
(defn task--search
  [repo query-string nums-neighbors]
  (m/sp
    (when-not (indexing? repo)  ; Skip if currently indexing
      (when-let [^js infer-worker @worker-state/*infer-worker]
        (when-let [conn (worker-state/get-datascript-conn repo)]
          (let [{:keys [distances neighbors]}
                (js->clj (c.m/<? (.search infer-worker repo query-string nums-neighbors))
                         :keywordize-keys true)]
            (->> (map vector distances neighbors)
                 (keep (fn [[distance label]]
                         ;; Filter by distance threshold
                         (when-not (or (js/isNaN distance)
                                       (>= distance 0.6)  ; Max distance = 0.6
                                       (> label 2147483647))
                           (when-let [block (d/entity @conn label)]
                             (when (:block/title block)
                               {:block block
                                :distance distance}))))))))))))
```

**Key behaviors:**

- Returns empty results if indexing is in progress
- Returns empty results if inference worker is not available
- Filters results with distance ≥ 0.6 (keeping only close matches)
- Converts distance to semantic score: `semantic_score = 1.0 / (1.0 + distance)`

## When Semantic Search is Used

Semantic search is automatically included when:

1. ✅ The graph has semantic search/text-embedding enabled
2. ✅ An embedding model has been loaded
3. ✅ The graph has been indexed (at least partially)
4. ✅ Indexing is not currently in progress

## Graceful Degradation

If semantic search is unavailable or disabled:

- `semantic-search-result` is empty or returns `[]`
- The combine function merges keyword results with empty semantic list
- Final score: `0.9 × keyword_score + 0.1 × 0 = 0.9 × keyword_score`
- Search still works, just without semantic enhancement

**No special CLI flag is needed** - the search automatically adapts based on graph capabilities.

## Search Result Components

The final search results include both scores for transparency:

```clojure
{:id "block-uuid"
 :title "Block content"
 :combined-score 0.85       ; Final ranking score
 :keyword-score 0.92        ; FTS/fuzzy match score
 :semantic-score 0.15       ; Vector similarity score
 :db/id 12345
 :block/uuid #uuid "..."
 ...}
```

## Performance Characteristics

### Search Types Executed

1. **FTS (Full-Text Search)**: SQLite FTS5 on `blocks_fts` table - fast
2. **LIKE queries**: For very short queries (≤2 chars) - slower
3. **Fuzzy search**: Fuse.js client-side - skipped for large graphs (>2500 pages)
4. **Semantic search**: HNSW vector search - runs in parallel with others

### Optimization for Large Graphs

```clojure
page-count (count (d/datoms @conn :avet :block/name))
large-graph? (> page-count 2500)

fuzzy-result (when-not (or page-only? large-graph?)
               (fuzzy-search repo @conn q option))
```

For graphs with >2500 pages, fuzzy search is skipped to maintain performance.

## Implications for CLI Users

### Automatic Hybrid Search

Users get best-of-both-worlds automatically:

- Exact matches ranked highly via keyword search (90% weight)
- Conceptually similar blocks surface via semantic search (10% weight)
- No configuration needed

### Example

```bash
logseq search "machine learning"
```

Will find:

- **High rank**: Blocks containing "machine learning" (keyword match)
- **Medium rank**: Blocks about "neural networks", "AI models" (semantic match)
- **Low rank**: Blocks mentioning "ML" or "deep learning" (partial/semantic match)

### Semantic Search Not Available

If semantic search is not enabled:

```bash
logseq search "todo"
```

Still works normally using only:

- FTS matching
- LIKE pattern matching
- Fuzzy search (for small graphs)

## Code References

| Component       | File                                                                                               | Function                               |
| --------------- | -------------------------------------------------------------------------------------------------- | -------------------------------------- |
| CLI command     | [deps/cli/src/logseq/cli/commands/search.cljs](../../deps/cli/src/logseq/cli/commands/search.cljs) | `search`, `api-search`, `local-search` |
| Search handler  | [src/main/frontend/handler/search.cljs](../../src/main/frontend/handler/search.cljs)               | `search`                               |
| Frontend search | [src/main/frontend/search.cljs](../../src/main/frontend/search.cljs)                               | `block-search`                         |
| Search agency   | [src/main/frontend/search/agency.cljs](../../src/main/frontend/search/agency.cljs)                 | `Agency` protocol impl                 |
| Browser engine  | [src/main/frontend/search/browser.cljs](../../src/main/frontend/search/browser.cljs)               | `Browser` protocol impl                |
| Worker search   | [src/main/frontend/worker/search.cljs](../../src/main/frontend/worker/search.cljs)                 | `search-blocks`, `combine-results`     |
| Embedding       | [src/main/frontend/worker/embedding.cljs](../../src/main/frontend/worker/embedding.cljs)           | `task--search`                         |
| Worker DB core  | [src/main/frontend/worker/db_core.cljs](../../src/main/frontend/worker/db_core.cljs)               | Thread API wrapper                     |

## Related Documentation

- See agent-guide for general CLI documentation
- See [logseq-cli.md](../cli/logseq-cli.md) for search command usage
- Embedding/semantic search settings are per-graph configuration

---

**Investigation Date:** January 29, 2026  
**Conclusion:** CLI search is semantically-aware when the feature is enabled, using a 90/10 keyword/semantic weighted hybrid approach.
