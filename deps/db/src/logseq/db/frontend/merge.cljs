(ns logseq.db.frontend.merge
  "Merge two DB graphs (DataScript connections) without data loss.

  Three merge strategies are supported:

  1. `:last-write-wins` (default)
     For blocks that exist in both source and target, keep the one with the
     latest `:block/updated-at` timestamp.  Blocks only in source are always
     added to target.

  2. `:keep-target`
     Only add blocks that are entirely new (not in target by UUID).  Existing
     target blocks are never overwritten.  Conflicts are still reported but no
     source data overwrites the target.

  3. `:report-conflicts`
     Like `:keep-target` — automatically adds truly new source blocks — but
     also returns a detailed `:conflicts` list so that callers can present
     conflicting blocks to the user for manual resolution.

  Entry point: `merge-graphs`"
  (:require [clojure.set :as set]
            [datascript.core :as d]
            [logseq.db.frontend.entity-util :as entity-util]
            [logseq.db.frontend.schema :as db-schema]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- ref->lookup
  "Convert a pulled ref map `{:db/id N, …}` to a portable lookup ref.
  Returns nil when no stable identifier is available."
  [ref-entity]
  (when (map? ref-entity)
    (cond
      (:block/uuid ref-entity) [:block/uuid (:block/uuid ref-entity)]
      (:db/ident   ref-entity) [:db/ident   (:db/ident   ref-entity)])))

(defn- entity->tx-map
  "Convert a DataScript entity (or pulled map) to a transaction map that is
  portable across connections.

  * Drops internal `:db/id`.
  * Converts cardinality-one ref values to lookup refs.
  * Converts cardinality-many ref values to sets of lookup refs (nils removed).
  * Drops attributes whose ref target has no stable identifier."
  [m]
  (reduce-kv
   (fn [acc attr val]
     (cond
       ;; Skip internal id
       (= :db/id attr)
       acc

       ;; Cardinality-many ref: convert each element
       (contains? db-schema/card-many-ref-type-attributes attr)
       (let [refs (keep ref->lookup (if (set? val) val [val]))]
         (if (seq refs)
           (assoc acc attr (set refs))
           acc))

       ;; Cardinality-one ref: convert single value
       (contains? db-schema/card-one-ref-type-attributes attr)
       (if-let [lref (ref->lookup val)]
         (assoc acc attr lref)
         acc)

       ;; Scalar value — keep as-is
       :else
       (assoc acc attr val)))
   {}
   m))

;; ---------------------------------------------------------------------------
;; Query helpers
;; ---------------------------------------------------------------------------

(defn- all-user-entities
  "Return all user-created entities from `db` that have a `:block/uuid`.
  Built-in system entities (`:logseq.property/built-in? true`) are excluded."
  [db]
  (->> (d/datoms db :avet :block/uuid)
       (keep (fn [datom]
               (let [e (d/entity db (:e datom))]
                 (when-not (entity-util/built-in? e)
                   e))))))

(defn- entity-uuid-map
  "Return a map of uuid -> entity for `db`, excluding built-in entities."
  [db]
  (->> (all-user-entities db)
       (into {} (map (fn [e] [(:block/uuid e) e])))))

;; ---------------------------------------------------------------------------
;; Block-level comparison
;; ---------------------------------------------------------------------------

(def ^:private content-attrs
  "Scalar attributes that determine whether two versions of a block differ."
  #{:block/title :block/order :block/collapsed?
    :block/name :block/journal-day
    :block/created-at :block/updated-at})

(defn- block-content-equal?
  "Return true when the user-visible content of two entities is the same."
  [a b]
  (let [scalar-eq (every? (fn [attr] (= (get a attr) (get b attr))) content-attrs)
        refs-eq   (= (set (map :block/uuid (:block/refs a)))
                     (set (map :block/uuid (:block/refs b))))
        tags-eq   (= (set (map :db/ident (:block/tags a)))
                     (set (map :db/ident (:block/tags b))))]
    (and scalar-eq refs-eq tags-eq)))

;; ---------------------------------------------------------------------------
;; Classification
;; ---------------------------------------------------------------------------

(defn classify-blocks
  "Compare the user-created entities in `source-db` with those in `target-db`.

  Returns a map with four keys:
  - `:new-in-source`   — UUIDs only present in `source-db`
  - `:new-in-target`   — UUIDs only present in `target-db`
  - `:same`            — UUIDs present in both whose content is identical
  - `:conflicts`       — UUIDs present in both whose content differs

  Each entry under `:conflicts` is a map:
  `{:uuid …  :source-block {…}  :target-block {…}}`"
  [source-db target-db]
  (let [src-map  (entity-uuid-map source-db)
        tgt-map  (entity-uuid-map target-db)
        src-uuids (set (keys src-map))
        tgt-uuids (set (keys tgt-map))
        only-src  (set/difference src-uuids tgt-uuids)
        only-tgt  (set/difference tgt-uuids src-uuids)
        in-both   (set/intersection src-uuids tgt-uuids)
        {same-uuids true conflict-uuids false}
        (group-by (fn [uuid]
                    (block-content-equal? (get src-map uuid) (get tgt-map uuid)))
                  in-both)]
    {:new-in-source (into {} (map (fn [u] [u (get src-map u)]) only-src))
     :new-in-target (into {} (map (fn [u] [u (get tgt-map u)]) only-tgt))
     :same          (set same-uuids)
     :conflicts     (mapv (fn [uuid]
                            {:uuid         uuid
                             :source-block (get src-map uuid)
                             :target-block (get tgt-map uuid)})
                          conflict-uuids)}))

;; ---------------------------------------------------------------------------
;; Transaction building
;; ---------------------------------------------------------------------------

(defn- block->tx
  "Convert an entity `e` from `source-db` into a DataScript transaction map
  suitable for transacting into a different connection."
  [e]
  (entity->tx-map (into {} e)))

(defn- sort-txs
  "Sort transaction maps so that page-level entities (those without
  `:block/page`) are processed before content blocks (those with
  `:block/page`). This guarantees that lookup refs from blocks to their
  parent pages can be resolved even when page and blocks are transacted
  together."
  [txs]
  (sort-by (fn [tx] (if (contains? tx :block/page) 1 0)) txs))

(defn- newer-block?
  "Return true when `source-entity` is strictly newer than `target-entity`
  according to `:block/updated-at`.  Blocks without a timestamp are treated as
  older than any timestamped block."
  [source-entity target-entity]
  (let [src-t (or (:block/updated-at source-entity) 0)
        tgt-t (or (:block/updated-at target-entity) 0)]
    (> src-t tgt-t)))

;; ---------------------------------------------------------------------------
;; Public API
;; ---------------------------------------------------------------------------

(defn merge-graphs
  "Merge `source-db` into `target-conn` using the specified `strategy`.

  Supported strategies (keyword, default `:last-write-wins`):
  - `:last-write-wins`    — keep the most recently updated version of each block
  - `:keep-target`        — never overwrite target blocks; only add new ones
  - `:report-conflicts`   — like `:keep-target` but return full conflict details

  Returns a map:
  ```clojure
  {:txs          [...]   ; transaction data applied (or to be applied) to target
   :conflicts    [...]   ; conflict entries (non-empty for :report-conflicts only)
   :stats        {:new        N   ; blocks only in source (added)
                  :overwritten N  ; blocks updated from source (LWW only)
                  :skipped    N   ; conflicting blocks NOT updated
                  :same       N}} ; identical blocks (no-op)
  ```

  Callers are responsible for transacting `:txs` into `target-conn`:
  ```clojure
  (d/transact! target-conn (:txs result))
  ```"
  ([source-db target-conn]
   (merge-graphs source-db target-conn {}))
  ([source-db target-conn {:keys [strategy] :or {strategy :last-write-wins}}]
   (let [target-db   @target-conn
         {:keys [new-in-source same conflicts]} (classify-blocks source-db target-db)

         ;; Blocks only in source — always add these regardless of strategy
         new-txs     (mapv block->tx (vals new-in-source))

         ;; Decide what to do with conflicting blocks based on strategy
         {:keys [overwrite-txs skipped-conflicts reported-conflicts]}
         (case strategy
           :last-write-wins
           (let [to-overwrite (filter #(newer-block? (:source-block %) (:target-block %)) conflicts)
                 to-skip      (remove #(newer-block? (:source-block %) (:target-block %)) conflicts)]
             {:overwrite-txs    (mapv #(block->tx (:source-block %)) to-overwrite)
              :skipped-conflicts (count to-skip)
              :reported-conflicts []})

           :keep-target
           {:overwrite-txs     []
            :skipped-conflicts (count conflicts)
            :reported-conflicts []}

           :report-conflicts
           {:overwrite-txs     []
            :skipped-conflicts (count conflicts)
            :reported-conflicts conflicts}

           ;; Unknown strategy — treat as keep-target
           {:overwrite-txs     []
            :skipped-conflicts (count conflicts)
            :reported-conflicts []})

         all-txs (sort-txs (into new-txs overwrite-txs))]

     {:txs       all-txs
      :conflicts reported-conflicts
      :stats     {:new         (count new-in-source)
                  :overwritten (count overwrite-txs)
                  :skipped     skipped-conflicts
                  :same        (count same)}})))
