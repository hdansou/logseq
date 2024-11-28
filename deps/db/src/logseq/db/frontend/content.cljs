(ns logseq.db.frontend.content
  "Fns to handle block content e.g. internal ids"
  (:require [clojure.string :as string]
            [logseq.common.util.page-ref :as page-ref]
            [datascript.core :as d]
            [logseq.common.util :as common-util]
            [logseq.db.frontend.entity-util :as entity-util]))

#_(defonce page-ref-special-chars "~^")

(defonce id-ref-pattern
  #"\[\[([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\]\]")

(defn content-id-ref->page
  "Convert id ref backs to page name using refs."
  [content refs]
  (reduce
   (fn [content ref]
     (if (:block/title ref)
       (string/replace content (page-ref/->page-ref (:block/uuid ref)) (:block/title ref))
       content))
   content
   refs))

(defn id-ref->title-ref
  "Convert id ref backs to page name refs using refs."
  [content* refs]
  (let [content (str content*)]
    (if (re-find id-ref-pattern content)
      (reduce
       (fn [content ref]
         (if (:block/title ref)
           (string/replace content
                           (page-ref/->page-ref (:block/uuid ref))
                           (page-ref/->page-ref (:block/title ref)))
           content))
       content
       refs)
      content)))

(defn get-matched-ids
  [content]
  (->> (re-seq id-ref-pattern content)
       (distinct)
       (map second)
       (map uuid)))

(defn- replace-tag-ref
  [content page-name id]
  (let [[page wrapped-id] (if (string/includes? page-name " ")
                            (map page-ref/->page-ref [page-name id])
                            [page-name id])
        page-name (common-util/format "#%s" page)
        r (common-util/format "#%s" wrapped-id)]
    ;; hash tag parsing rules https://github.com/logseq/mldoc/blob/701243eaf9b4157348f235670718f6ad19ebe7f8/test/test_markdown.ml#L631
    ;; Safari doesn't support look behind, don't use
    ;; TODO: parse via mldoc
    (string/replace content
                    (re-pattern (str "(?i)(^|\\s)(" (common-util/escape-regex-chars page-name) ")(?=[,\\.]*($|\\s))"))
                    ;;    case_insense^    ^lhs   ^_grp2                       look_ahead^         ^_grp3
                    (fn [[_match lhs _grp2 _grp3]]
                      (str lhs r)))))

(defn- replace-page-ref
  [content page-name id]
  (let [[page wrapped-id] (map page-ref/->page-ref [page-name id])]
    (common-util/replace-ignore-case content page wrapped-id)))

(defn- replace-page-ref-with-id
  [content page-name id replace-tag?]
  (let [page-name (-> (str page-name)
                      (string/replace "HashTag-" "#"))
        id (str id)
        content' (replace-page-ref content page-name id)]
    (if replace-tag?
      (replace-tag-ref content' page-name id)
      content')))

(defn title-ref->id-ref
  "Convert ref to id refs e.g. `[[page name]] -> [[uuid]]."
  [title refs & {:keys [replace-tag?]
                 :or {replace-tag? true}}]
  (assert (string? title))
  (let [refs' (map
               (fn [ref]
                 (if (and (vector? ref) (= :block/uuid (first ref)))
                   {:block/uuid (second ref)
                    :block/title (str (first ref))}
                   ref))
               refs)]
    (reduce
     (fn [content {uuid' :block/uuid :block/keys [title] :as block}]
       (let [title' (or (:block.temp/original-page-name block) title)]
         (replace-page-ref-with-id content title' uuid' replace-tag?)))
     title
     (filter :block/title refs'))))

(defn update-block-content
  "Replace `[[internal-id]]` with `[[page name]]`"
  [db item eid]
  (if (entity-util/db-based-graph? db)
    (if-let [content (:block/title item)]
      (let [refs (:block/refs (d/entity db eid))]
        (assoc item :block/title (id-ref->title-ref content refs)))
      item)
    item))

(defn replace-tags-with-page-refs
  "Replace tags in content with page-ref ids. Ignore case because tags in
  content can have any case and still have a valid ref"
  [content tags]
  (->>
   (reduce
    (fn [content tag]
      (let [id-ref (page-ref/->page-ref (:block/uuid tag))]
        (-> content
            ;; #[[favorite book]]
            (common-util/replace-ignore-case
             (str "#" page-ref/left-brackets (:block/title tag) page-ref/right-brackets)
             id-ref)
            ;; #book
            (common-util/replace-ignore-case (str "#" (:block/title tag)) id-ref))))
    content
    (sort-by :block/title > tags))
   (string/trim)))
