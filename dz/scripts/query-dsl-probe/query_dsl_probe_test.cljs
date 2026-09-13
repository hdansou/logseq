(ns frontend.worker.query-dsl-probe-test
  "Scratch probe for investigation 029: runs the simple-query DSL and the template
  resolver, so the claims about (between deadline ...) and <% current page %> rest on
  execution instead of reading. Prints, does not assert.

  Only covers code that is byte-identical between this checkout and master
  be800f1711: worker/query_dsl.cljs, deps/db query_dsl.cljs, config.cljs, property.cljs."
  (:require [cljs.reader :as reader]
            [cljs.test :refer [deftest]]
            [clojure.pprint :as pprint]
            [frontend.config :as config]
            [frontend.worker.query-dsl :as query-dsl]
            [logseq.db.frontend.property :as db-property]
            [logseq.db.frontend.query-dsl :as db-query-dsl]
            [logseq.db.test.helper :as db-test]))

(defn- show
  [label value]
  (println)
  (println "=== " label)
  (pprint/pprint value))

(deftest probe-get-timestamp-property
  ;; 029: a bare `deadline` in the DSL becomes the unnamespaced :deadline, which
  ;; property? rejects because it requires a namespace. Expect nil for deadline and
  ;; scheduled, a mapping for created-at/updated-at, and the ident for a namespaced one.
  ;; Read the forms the way the DSL does. `-7d` is not a valid Clojure number token,
  ;; so these cannot be written as quoted literals -- the real path always starts
  ;; from a string.
  (doseq [s ["(between deadline -7d today)"
             "(between scheduled -7d today)"
             "(between created_at -7d today)"
             "(between updated-at -7d today)"
             "(between :logseq.property/deadline -7d today)"]]
    ;; same two steps as parse (:655-657): pre-transform, then read with custom-readers
    (let [form (->> s
                    db-query-dsl/pre-transform
                    (reader/read-string db-query-dsl/custom-readers))]
      (show (str "get-timestamp-property " (pr-str form))
            (db-query-dsl/get-timestamp-property form))))
  (doseq [k [:deadline :scheduled :logseq.property/deadline :block/created-at]]
    (show (str "db-property/property? " k) (db-property/property? k))))

(deftest probe-between-clauses
  ;; 029: if get-timestamp-property nils, db-based-build-between-three-arg's when-let
  ;; yields nil and the clause disappears from the query rather than filtering.
  (let [conn (db-test/create-conn)]
    (doseq [q ["(between deadline -7d today)"
               "(between scheduled -7d today)"
               "(between created_at -7d today)"
               "(and (task todo) (between deadline -7d today))"]]
      (show (str "parse " (pr-str q))
            (try
              (select-keys (query-dsl/parse q @conn {}) [:query :rules])
              (catch :default e {:threw (ex-message e)}))))))

(deftest probe-dynamic-template
  ;; 029: <% current page %> in a standalone /Query resolves to today's journal,
  ;; because :current-page-title is only set for the journal default queries.
  (doseq [[label opts] [["no current page (standalone /Query)" {}]
                        ["with :current-page-title" {:current-page-title "Some Page"}]]]
    (doseq [q ["(property page <% current page %>)"
               "(between <% today %> <% tomorrow %>)"]]
      ;; the private resolver is what parse-query calls first (:705-709)
      (show (str label " :: " q)
            (try (#'query-dsl/resolve-dynamic-template q opts)
                 (catch :default e {:threw (ex-message e)}))))))

(deftest probe-parse-query-end-to-end
  ;; parse-query is the public path: resolve the template, then parse (:705-709).
  (let [conn (db-test/create-conn)]
    (doseq [q ["(property page <% current page %>)"
               "(between deadline -7d today)"]]
      (show (str "parse-query " (pr-str q))
            (try
              (select-keys (query-dsl/parse-query q @conn {}) [:query :rules])
              (catch :default e {:threw (ex-message e)}))))))

(deftest probe-highlight-mark
  ;; 029: mod+shift+h wraps with this, while typing ^^ inserts ^^.
  (doseq [fmt [:markdown :org]]
    (show (str "config/get-highlight " fmt) (config/get-highlight fmt))))
