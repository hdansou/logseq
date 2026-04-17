(ns logseq.db.frontend.merge-test
  "Tests for logseq.db.frontend.merge

  Three merge strategies are tested:

  1. :last-write-wins — newer block wins when UUID exists in both graphs
  2. :keep-target     — target blocks are never overwritten; only new blocks added
  3. :report-conflicts — conflicts are surfaced; only new blocks auto-added"
  (:require [cljs.test :refer [deftest is testing]]
            [datascript.core :as d]
            [logseq.db.frontend.merge :as db-merge]
            [logseq.db.sqlite.build :as sqlite-build]
            [logseq.db.test.helper :as db-test]))

;; ---------------------------------------------------------------------------
;; Test helpers
;; ---------------------------------------------------------------------------

(defn- create-page-block!
  "Transact a minimal page into conn and return the uuid."
  [conn title]
  (let [uuid (random-uuid)]
    (d/transact! conn [{:block/uuid  uuid
                        :block/name  (clojure.string/lower-case title)
                        :block/title title}])
    uuid))

(defn- create-content-block!
  "Transact a minimal content block attached to `page-uuid` and return its uuid."
  [conn page-uuid content & {:keys [updated-at created-at parent-uuid]
                              :or {created-at 1000}}]
  (let [block-uuid (random-uuid)
        page-ref   [:block/uuid page-uuid]
        parent-ref (if parent-uuid [:block/uuid parent-uuid] page-ref)]
    (d/transact! conn [(cond-> {:block/uuid       block-uuid
                                :block/title       content
                                :block/page        page-ref
                                :block/parent      parent-ref
                                :block/created-at  created-at}
                         updated-at (assoc :block/updated-at updated-at))])
    block-uuid))

;; ---------------------------------------------------------------------------
;; classify-blocks
;; ---------------------------------------------------------------------------

(deftest classify-blocks-new-in-source-test
  (testing "Blocks only in source appear in :new-in-source"
    (let [src-conn (db-test/create-conn)
          tgt-conn (db-test/create-conn)
          page-uuid (create-page-block! src-conn "Source Page")
          block-uuid (create-content-block! src-conn page-uuid "hello")]
      (let [{:keys [new-in-source new-in-target same conflicts]}
            (db-merge/classify-blocks @src-conn @tgt-conn)]
        (is (contains? new-in-source page-uuid) "page is new in source")
        (is (contains? new-in-source block-uuid) "block is new in source")
        (is (empty? new-in-target) "nothing new in target")
        (is (empty? same) "no identical blocks")
        (is (empty? conflicts) "no conflicts")))))

(deftest classify-blocks-same-test
  (testing "Identical blocks in both graphs appear in :same"
    (let [src-conn (db-test/create-conn)
          tgt-conn (db-test/create-conn)
          shared-uuid (random-uuid)
          _ (d/transact! src-conn [{:block/uuid  shared-uuid
                                    :block/name  "shared"
                                    :block/title "Shared"}])
          _ (d/transact! tgt-conn [{:block/uuid  shared-uuid
                                    :block/name  "shared"
                                    :block/title "Shared"}])]
      (let [{:keys [new-in-source same conflicts]}
            (db-merge/classify-blocks @src-conn @tgt-conn)]
        (is (contains? same shared-uuid) "shared block is in :same")
        (is (empty? new-in-source) "no new source blocks")
        (is (empty? conflicts) "no conflicts")))))

(deftest classify-blocks-conflict-test
  (testing "Blocks with same UUID but different content appear in :conflicts"
    (let [src-conn (db-test/create-conn)
          tgt-conn (db-test/create-conn)
          shared-uuid (random-uuid)
          _ (d/transact! src-conn [{:block/uuid  shared-uuid
                                    :block/name  "page"
                                    :block/title "Page (source version)"}])
          _ (d/transact! tgt-conn [{:block/uuid  shared-uuid
                                    :block/name  "page"
                                    :block/title "Page (target version)"}])]
      (let [{:keys [same conflicts]}
            (db-merge/classify-blocks @src-conn @tgt-conn)]
        (is (empty? same) "no identical blocks")
        (is (= 1 (count conflicts)) "one conflict detected")
        (is (= shared-uuid (:uuid (first conflicts))) "conflict uuid matches")
        (is (= "Page (source version)"
               (-> conflicts first :source-block :block/title)) "source title preserved")
        (is (= "Page (target version)"
               (-> conflicts first :target-block :block/title)) "target title preserved")))))

;; ---------------------------------------------------------------------------
;; merge-graphs — :last-write-wins
;; ---------------------------------------------------------------------------

(deftest last-write-wins-adds-new-blocks-test
  (testing "New source blocks are always added to target"
    (let [src-conn (db-test/create-conn)
          tgt-conn (db-test/create-conn)
          page-uuid (create-page-block! src-conn "My Page")
          block-uuid (create-content-block! src-conn page-uuid "block text")]
      (let [{:keys [txs stats]} (db-merge/merge-graphs @src-conn tgt-conn {:strategy :last-write-wins})]
        (is (= 2 (:new stats)) "two new entities (page + block) are reported")
        (is (= 0 (:overwritten stats)))
        (is (= 0 (:skipped stats)))
        ;; Apply the txs
        (d/transact! tgt-conn txs)
        (is (some? (d/entity @tgt-conn [:block/uuid page-uuid])) "page was added to target")
        (is (= 1 (count (d/datoms @tgt-conn :avet :block/page
                                  (:db/id (d/entity @tgt-conn [:block/uuid page-uuid])))))
            "block was added under the page")))))

(deftest last-write-wins-newer-source-overwrites-test
  (testing "Source block with newer timestamp overwrites target block"
    (let [src-conn (db-test/create-conn)
          tgt-conn (db-test/create-conn)
          shared-uuid (random-uuid)
          _ (d/transact! tgt-conn [{:block/uuid       shared-uuid
                                    :block/name       "page"
                                    :block/title      "Old Title"
                                    :block/updated-at 1000}])
          _ (d/transact! src-conn [{:block/uuid       shared-uuid
                                    :block/name       "page"
                                    :block/title      "New Title"
                                    :block/updated-at 2000}])]
      (let [{:keys [txs stats]} (db-merge/merge-graphs @src-conn tgt-conn {:strategy :last-write-wins})]
        (is (= 1 (:overwritten stats)) "one block overwritten from source")
        (is (= 0 (:skipped stats)))
        (d/transact! tgt-conn txs)
        (is (= "New Title"
               (:block/title (d/entity @tgt-conn [:block/uuid shared-uuid])))
            "target block now has source title")))))

(deftest last-write-wins-older-source-skipped-test
  (testing "Source block with older timestamp is skipped"
    (let [src-conn (db-test/create-conn)
          tgt-conn (db-test/create-conn)
          shared-uuid (random-uuid)
          _ (d/transact! tgt-conn [{:block/uuid       shared-uuid
                                    :block/name       "page"
                                    :block/title      "Newer Title"
                                    :block/updated-at 9000}])
          _ (d/transact! src-conn [{:block/uuid       shared-uuid
                                    :block/name       "page"
                                    :block/title      "Older Title"
                                    :block/updated-at 1000}])]
      (let [{:keys [txs stats]} (db-merge/merge-graphs @src-conn tgt-conn {:strategy :last-write-wins})]
        (is (= 0 (:overwritten stats)) "nothing overwritten")
        (is (= 1 (:skipped stats)) "older source block is skipped")
        (d/transact! tgt-conn txs)
        (is (= "Newer Title"
               (:block/title (d/entity @tgt-conn [:block/uuid shared-uuid])))
            "target block title unchanged")))))

;; ---------------------------------------------------------------------------
;; merge-graphs — :keep-target
;; ---------------------------------------------------------------------------

(deftest keep-target-adds-new-blocks-test
  (testing ":keep-target adds blocks that don't exist in target"
    (let [src-conn (db-test/create-conn)
          tgt-conn (db-test/create-conn)
          page-uuid (create-page-block! src-conn "New Page")]
      (let [{:keys [txs stats]} (db-merge/merge-graphs @src-conn tgt-conn {:strategy :keep-target})]
        (is (= 1 (:new stats)) "one new page reported")
        (d/transact! tgt-conn txs)
        (is (some? (d/entity @tgt-conn [:block/uuid page-uuid]))
            "new page was added to target")))))

(deftest keep-target-never-overwrites-test
  (testing ":keep-target never overwrites existing target blocks"
    (let [src-conn (db-test/create-conn)
          tgt-conn (db-test/create-conn)
          shared-uuid (random-uuid)
          _ (d/transact! tgt-conn [{:block/uuid       shared-uuid
                                    :block/name       "page"
                                    :block/title      "Target Title"
                                    :block/updated-at 1000}])
          _ (d/transact! src-conn [{:block/uuid       shared-uuid
                                    :block/name       "page"
                                    :block/title      "Source Title"
                                    :block/updated-at 9000}])]
      (let [{:keys [txs stats]} (db-merge/merge-graphs @src-conn tgt-conn {:strategy :keep-target})]
        (is (= 0 (:overwritten stats)) "nothing overwritten with :keep-target")
        (is (= 1 (:skipped stats)) "conflict is counted as skipped")
        (d/transact! tgt-conn txs)
        (is (= "Target Title"
               (:block/title (d/entity @tgt-conn [:block/uuid shared-uuid])))
            "target title is preserved")))))

;; ---------------------------------------------------------------------------
;; merge-graphs — :report-conflicts
;; ---------------------------------------------------------------------------

(deftest report-conflicts-returns-conflicts-test
  (testing ":report-conflicts returns conflict details"
    (let [src-conn (db-test/create-conn)
          tgt-conn (db-test/create-conn)
          shared-uuid (random-uuid)
          new-uuid    (random-uuid)
          _ (d/transact! tgt-conn [{:block/uuid  shared-uuid
                                    :block/name  "page"
                                    :block/title "Target Title"}])
          _ (d/transact! src-conn [{:block/uuid  shared-uuid
                                    :block/name  "page"
                                    :block/title "Source Title"}
                                   {:block/uuid  new-uuid
                                    :block/name  "new-page"
                                    :block/title "New Page"}])]
      (let [{:keys [txs conflicts stats]}
            (db-merge/merge-graphs @src-conn tgt-conn {:strategy :report-conflicts})]
        ;; New block is added automatically
        (is (= 1 (:new stats)) "new-page added automatically")
        (is (= 0 (:overwritten stats)) "nothing overwritten")
        ;; Conflict is reported, not applied
        (is (= 1 (count conflicts)) "one conflict reported")
        (is (= shared-uuid (:uuid (first conflicts))))
        (is (= "Source Title" (-> conflicts first :source-block :block/title)))
        (is (= "Target Title" (-> conflicts first :target-block :block/title)))
        ;; Apply only the non-conflicting txs
        (d/transact! tgt-conn txs)
        ;; Target's conflicting block is unchanged
        (is (= "Target Title"
               (:block/title (d/entity @tgt-conn [:block/uuid shared-uuid])))
            "conflict block is NOT overwritten")
        ;; New block was added
        (is (some? (d/entity @tgt-conn [:block/uuid new-uuid]))
            "new-page was added to target")))))

;; ---------------------------------------------------------------------------
;; Default strategy
;; ---------------------------------------------------------------------------

(deftest default-strategy-is-last-write-wins-test
  (testing "merge-graphs uses :last-write-wins by default (no options map)"
    (let [src-conn (db-test/create-conn)
          tgt-conn (db-test/create-conn)
          uuid (random-uuid)
          _ (d/transact! tgt-conn [{:block/uuid       uuid
                                    :block/name       "page"
                                    :block/title      "Old"
                                    :block/updated-at 100}])
          _ (d/transact! src-conn [{:block/uuid       uuid
                                    :block/name       "page"
                                    :block/title      "New"
                                    :block/updated-at 999}])]
      (let [{:keys [txs stats]}
            ;; Call without options — should default to :last-write-wins
            (db-merge/merge-graphs @src-conn tgt-conn)]
        (is (= 1 (:overwritten stats)))
        (d/transact! tgt-conn txs)
        (is (= "New" (:block/title (d/entity @tgt-conn [:block/uuid uuid])))
            "default LWW strategy applies source update")))))

;; ---------------------------------------------------------------------------
;; Stats completeness
;; ---------------------------------------------------------------------------

(deftest stats-reflect-full-classification-test
  (testing "stats total covers all classified blocks"
    (let [src-conn (db-test/create-conn)
          tgt-conn (db-test/create-conn)
          new-uuid    (random-uuid)
          shared-same (random-uuid)
          conflict-uuid (random-uuid)
          _ (d/transact! src-conn [{:block/uuid new-uuid
                                    :block/name "new"
                                    :block/title "New"}
                                   {:block/uuid  shared-same
                                    :block/name  "same"
                                    :block/title "Same"}
                                   {:block/uuid       conflict-uuid
                                    :block/name       "conflict"
                                    :block/title      "Source"
                                    :block/updated-at 2000}])
          _ (d/transact! tgt-conn [{:block/uuid  shared-same
                                    :block/name  "same"
                                    :block/title "Same"}
                                   {:block/uuid       conflict-uuid
                                    :block/name       "conflict"
                                    :block/title      "Target"
                                    :block/updated-at 1000}])]
      (let [{:keys [stats]}
            (db-merge/merge-graphs @src-conn tgt-conn {:strategy :last-write-wins})]
        (is (= 1 (:new stats)) "one new block")
        (is (= 1 (:overwritten stats)) "one overwrite (source newer)")
        (is (= 0 (:skipped stats)) "nothing skipped (source was newer)")
        (is (= 1 (:same stats)) "one identical block")))))
