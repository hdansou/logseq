#!/usr/bin/env bb

(require '[clojure.data.csv :as csv]
         '[clojure.java.io :as io]
         '[clojure.edn :as edn])

(defn journal-date-to-str [date-int]
  (when date-int
    (let [s (str date-int)
          year (subs s 0 4)
          month (subs s 4 6)
          day (subs s 6 8)]
      (str year "-" month "-" day))))

(defn timestamp-to-date [ts]
  (when ts
    (-> (java.time.Instant/ofEpochMilli ts)
        (java.time.ZonedDateTime/ofInstant (java.time.ZoneId/systemDefault))
        (.format (java.time.format.DateTimeFormatter/ofPattern "yyyy-MM-dd")))))

(defn extract-tasks [data]
  (let [pages-and-blocks (:pages-and-blocks data)]
    (for [{:keys [page blocks]} pages-and-blocks
          :when blocks
          :let [journal-date (journal-date-to-str (:build/journal page))]
          block blocks
          :when (some #(= :logseq.class/Task %) (:build/tags block))]
      {:date journal-date
       :title (:block/title block "")
       :status (name (get-in block [:build/properties :logseq.property/status] :unknown))
       :priority (when-let [p (get-in block [:build/properties :logseq.property/priority])]
                   (name p))
       :scheduled (timestamp-to-date (get-in block [:build/properties :logseq.property/scheduled]))
       :collapsed? (str (:block/collapsed? block false))
       :tags (->> (:build/tags block)
                  (remove #(= :logseq.class/Task %))
                  (map name)
                  (clojure.string/join ", "))})))

(defn tasks-to-csv [tasks output-file]
  (with-open [writer (io/writer output-file)]
    (csv/write-csv writer
                   (cons ["Date" "Title" "Status" "Priority" "Scheduled" "Collapsed" "Tags"]
                         (map (fn [task]
                                [(:date task)
                                 (:title task)
                                 (:status task)
                                 (or (:priority task) "")
                                 (or (:scheduled task) "")
                                 (:collapsed? task)
                                 (:tags task)])
                              tasks)))))

(defn -main [input-file output-file]
  (let [data (edn/read-string (slurp input-file))
        tasks (extract-tasks data)]
    (tasks-to-csv tasks output-file)
    (println (str "Converted " (count tasks) " tasks to " output-file))))

;; Usage: bb edn-to-csv.clj input.edn output.csv
(when (= *file* (System/getProperty "babashka.file"))
  (if (< (count *command-line-args*) 2)
    (println "Usage: bb edn-to-csv.clj input.edn output.csv")
    (apply -main *command-line-args*)))
