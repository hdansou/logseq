(ns frontend.worker.db.migrate
  "DB migration"
  (:require [datascript.core :as d]
            [logseq.db.sqlite.create-graph :as sqlite-create-graph]
            [logseq.db.frontend.property :as db-property]
            [logseq.db :as ldb]
            [logseq.db.frontend.schema :as db-schema]))

;; TODO: fixes/rollback

(def schema-version->updates
  [[3 {:properties [:logseq.property/table-sorting :logseq.property/table-filters
                    :logseq.property/table-hidden-columns :logseq.property/table-ordered-columns]
       :classes    []}]
   [4 {:fix (fn [conn]
              (let [pages (d/datoms @conn :avet :block/name)
                    tx-data (map (fn [d]
                                   {:db/id (:e d)
                                    :block/type "page"}) pages)]
                tx-data))}]
   [5 {:properties [:logseq.property/view-for]
       :classes    []}]])

;; Question: do we need to assign persist UUIDs for later added built-in classes & properties?
;; @zhiyuan will this affect RTC?

(defn migrate
  [conn]
  (let [db @conn
        version-in-db (or (:kv/value (d/entity db :logseq.kv/schema-version)) 0)]
    (cond
      (= version-in-db db-schema/version)
      nil

      (< db-schema/version version-in-db)     ; outdated client, db version could be synced from server
      ;; FIXME: notify users to upgrade to the latest version asap
      nil

      (> db-schema/version version-in-db)
      (let [db-based? (ldb/db-based-graph? @conn)
            built-in-value (:db/id (get (d/entity db :logseq.class/Root) :logseq.property/built-in?))
            updates (keep (fn [[v updates]]
                            (when (> v version-in-db)
                              updates))
                          schema-version->updates)
            properties (mapcat :properties updates)
            ;; TODO: add classes migration support
            ;; classes (mapcat :classes updates)
            new-properties (->> (select-keys db-property/built-in-properties properties)
                                ;; property already exists, this should never happen
                                (remove (fn [[k _]]
                                          (when (d/entity db k)
                                            (assert (str "DB migration: property already exists " k)))))
                                (into {})
                                sqlite-create-graph/build-initial-properties*
                                (map (fn [b] (assoc b :logseq.property/built-in? built-in-value))))
            fixes (mapcat
                   (fn [update]
                     (when-let [fix (:fix update)]
                       (when (fn? fix)
                         (fix conn)))) updates)
            tx-data' (if db-based? (concat new-properties fixes) fixes)]
        (when (seq tx-data')
          (let [tx-data' (concat tx-data' [(sqlite-create-graph/kv :logseq.kv/schema-version db-schema/version)])]
            (ldb/transact! conn tx-data' {:db-migrate? true}))
          (println "DB schema migrated to " db-schema/version " from " version-in-db "."))))))
