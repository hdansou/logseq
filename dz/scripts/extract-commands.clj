#!/usr/bin/env bb
;; Script to extract all Logseq command palette commands into a table format

(require '[clojure.edn :as edn]
         '[clojure.string :as str]
         '[clojure.java.io :as io])

;; Read the shortcuts config file
(def shortcuts-file "src/main/frontend/modules/shortcut/config.cljs")
(def translations-file "src/resources/dicts/en.edn")

;; Parse shortcuts from config.cljs
(defn parse-shortcuts []
  (let [content (slurp shortcuts-file)
        ;; Extract the all-built-in-keyboard-shortcuts map
        shortcuts-start (.indexOf content "all-built-in-keyboard-shortcuts")
        shortcuts-section (subs content shortcuts-start)
        ;; Find the closing of the def
        first-def-end (.indexOf shortcuts-section "\n\n(def")]
    (println "Found shortcuts section...")
    ;; Manual parsing needed - let's extract key patterns
    (re-seq #":([a-z/-]+)\s+\{:binding\s+([^\n]+)\n\s+:fn\s+" shortcuts-section)))

;; Parse categories from config.cljs
(defn parse-categories []
  (let [content (slurp shortcuts-file)
        ;; Extract the *category atom definition
        category-start (.indexOf content "(defonce ^:large-vars/data-var *category")
        category-section (subs content category-start 5000)]
    (println "Found category section...")
    ;; Extract category mappings
    (let [categories {}]
      ;; Parse each category
      (loop [remaining category-section
             result {}]
        (if-let [match (re-find #":shortcut\.category/([a-z-]+)\s+\[((?:[^\]]+|\[[^\]]*\])*)\]" remaining)]
          (let [[_ cat-name commands] match
                cmd-keys (re-seq #":([a-z/-]+)" commands)
                cmd-keywords (map second cmd-keys)]
            (recur (subs remaining (+ (.indexOf remaining (first match)) (count (first match))))
                   (assoc result cat-name (vec cmd-keywords))))
          result)))))

;; Parse translations
(defn parse-translations []
  (let [content (slurp translations-file)
        ;; Find command descriptions section
        cmds-section (second (str/split content #":command\.\*" 2))]
    (if cmds-section
      (let [matches (re-seq #":command\.([a-z/-]+)\s+\"([^\"]+)\"" content)]
        (into {} (map (fn [[_ k v]] [(keyword k) v]) matches)))
      {})))

;; Main extraction
(defn extract-commands []
  (println "Extracting Logseq commands...")
  (let [shortcuts (parse-shortcuts)
        categories (parse-categories)
        translations (parse-translations)

        ;; Build reverse category lookup
        cmd-to-category (reduce-kv
                         (fn [m cat cmds]
                           (reduce #(assoc %1 %2 cat) m cmds))
                         {}
                         categories)]

    (println "\nFound:" (count shortcuts) "shortcuts")
    (println "Found:" (count categories) "categories")
    (println "Found:" (count translations) "translations")

    ;; Generate markdown table
    (println "\n## Logseq Command Palette - Complete Command Reference\n")
    (println "| Command ID | Keyboard Shortcut | Description | Category |")
    (println "|------------|------------------|-------------|----------|")

    ;; Sort by category then command ID
    (doseq [[cmd-match binding-match] (sort-by first shortcuts)]
      (let [cmd-id (keyword cmd-match)
            ;; Clean binding - remove quotes and brackets
            binding (-> binding-match
                        (str/replace #"\"" "")
                        (str/replace #"\[|\]" "")
                        (str/replace #"if mac\? " "Mac: ")
                        (str/replace #"false" "N/A")
                        (str/trim))
            description (get translations cmd-id (str/replace cmd-match #"/" " "))
            category (get cmd-to-category cmd-match "others")]
        (println (str "| :" cmd-match
                      " | " binding
                      " | " description
                      " | " category " |"))))))

;; Run extraction
(extract-commands)
