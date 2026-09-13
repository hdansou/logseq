(ns frontend.handler.paste-syntax-probe-test
  "Scratch probe: runs pasted Markdown/HTML and typed text through the real DB-graph
  paste/save code, inserts the result into a test graph and prints the mirror output."
  (:require ["/frontend/utils" :as utils]
            [cljs.test :refer [async deftest is]]
            [clojure.string :as string]
            [datascript.core :as d]
            [frontend.commands :as commands]
            [frontend.db.async :as db-async]
            [frontend.extensions.html-parser :as html-parser]
            [frontend.handler.db-based.editor :as db-editor-handler]
            [frontend.handler.editor :as editor-handler]
            [frontend.handler.notification :as notification]
            [frontend.handler.paste :as paste-handler]
            [frontend.state :as state]
            [frontend.util :as util]
            [frontend.worker.markdown-mirror :as markdown-mirror]
            [logseq.db.test.helper :as db-test]
            [logseq.outliner.core :as outliner-core]
            [promesa.core :as p]))

(def test-repo "logseq_db_graph-paste-probe")
(def *notices (atom []))

(defn- fake-platform []
  (let [files (atom {})]
    {:platform {:env {:runtime :node}
                :storage {:read-text! (fn [path] (p/resolved (get @files path)))
                          :write-text-atomic! (fn [path content] (swap! files assoc path content) (p/resolved nil))
                          :delete-file! (fn [path] (swap! files dissoc path) (p/resolved nil))}
                :broadcast {:post-message! (fn [& _] nil)}}
     :files files}))

(defn- new-conn []
  (db-test/create-conn-with-blocks
   {:classes {:Project {:block/title "Project"}}
    :pages-and-blocks [{:page {:block/title "Paste Target"}
                        :blocks [{:block/title "PLACEHOLDER"}]}
                       {:page {:block/title "Existing Page"}
                        :blocks [{:block/title "existing" :block/uuid #uuid "e0000000-0000-4000-8000-000000000001" :build/keep-uuid? true}]}]}))

(defn- <mirror [conn]
  (let [{:keys [platform files]} (fake-platform)
        page (db-test/find-page-by-title @conn "Paste Target")]
    (p/let [_ (markdown-mirror/<mirror-page! test-repo @conn (:db/id page) {:platform platform})]
      (->> (get @files (str (markdown-mirror/repo-mirror-dir test-repo) "/"
                            (markdown-mirror/page-relative-path @conn page)))
           string/split-lines
           rest
           (drop-while string/blank?)
           (string/join "\n")))))

(defn- stored-summary [conn]
  (let [page (db-test/find-page-by-title @conn "Paste Target")]
    (->> (d/q '[:find [?b ...] :in $ ?p :where [?b :block/page ?p]] @conn (:db/id page))
         (map #(d/entity @conn %))
         (sort-by :block/order)
         (map (fn [b]
                (cond-> {:title (:block/title b)}
                  (:logseq.property/heading b) (assoc :heading (:logseq.property/heading b))
                  (:logseq.property.node/display-type b) (assoc :display (:logseq.property.node/display-type b))
                  (:logseq.property.code/lang b) (assoc :lang (:logseq.property.code/lang b))
                  (seq (:block/tags b)) (assoc :tags (mapv :block/title (:block/tags b)))
                  (seq (:block/refs b)) (assoc :refs (mapv :block/title (:block/refs b)))
                  (not= (:db/id (:block/parent b)) (:db/id page)) (assoc :parent (:block/title (:block/parent b))))))
         vec)))

(defn- <paste! [conn text html]
  (let [page (db-test/find-page-by-title @conn "Paste Target")
        target (db-test/find-block-by-content @conn "PLACEHOLDER")
        _ (d/transact! conn [{:db/id (:db/id target) :block/title ""}])
        target (d/entity @conn (:db/id target))
        captured (atom nil)
        inserted-text (atom nil)]
    (p/with-redefs
     [state/get-current-repo (constantly test-repo)
      state/get-date-formatter (constantly "MMM do, yyyy")
      state/get-edit-block (constantly {:db/id (:db/id target) :block/uuid (:block/uuid target)})
      state/get-edit-input-id (constantly "probe-input")
      state/get-input (constantly #js {:value ""})
      editor-handler/get-selection-and-format (constantly {:selection-start 0 :selection-end 0 :selection "" :value ""})
      db-async/<get-block-page-info (fn [& _] (p/resolved {:db/id (:db/id page) :block/name "paste target"}))
      state/<invoke-db-worker (fn [_op _repo selector lookup]
                                (p/resolved (when (d/entity @conn lookup) (d/pull @conn selector lookup))))
      editor-handler/paste-blocks (fn [blocks _opts] (reset! captured blocks) (p/resolved nil))
      commands/delete-selection! (constantly nil)
      commands/simple-insert! (fn [_ t _] (reset! inserted-text t) (p/resolved nil))
      util/stop (constantly nil)
      util/get-selected-text (constantly "")
      paste-handler/get-copied-blocks (constantly (p/resolved nil))
      utils/getCopiedBlocksFromMemory (constantly nil)
      notification/show! (fn [msg & _] (swap! *notices conj (str msg)))
      state/pub-event! (fn [e] (swap! *notices conj (str "event " (first e) " " (some-> e second :error ex-message))))]
     (p/let [_ ((paste-handler/editor-on-paste! nil)
                #js {:clipboardData #js {:getData (fn [k] (if (= k "text/html") (or html "") text))
                                         :files #js []}})]
       (cond
         @captured
         (let [blocks (map (fn [b] (assoc b :block/page {:db/id [:block/uuid (:block/uuid page)]})) @captured)]
           (outliner-core/insert-blocks! conn blocks target {:sibling? true :outliner-op :paste
                                                           :outliner-real-op :paste-text
                                                           :replace-empty-target? true :keep-uuid? true})
           {:path :blocks :captured (mapv #(select-keys % [:block/title :logseq.property/heading
                                                          :logseq.property.node/display-type
                                                          :logseq.property.code/lang :block/level]) @captured)})

         @inserted-text
         (let [parsed (db-editor-handler/wrap-parse-block {:block/uuid (:block/uuid target) :block/title @inserted-text})]
           (d/transact! conn [(merge {:db/id (:db/id target)}
                                     (select-keys parsed [:block/title :logseq.property/heading
                                                          :logseq.property.node/display-type :logseq.property.code/lang]))])
           {:path :inline-text :inserted @inserted-text})

         :else {:path :nothing})))))

(def paste-cases
  [["recommended template" "- # Project kickoff\n- ## Goals\n  - Ship the **beta** by Friday\n  - Owner: [[Existing Page]]\n  - Spec: [design doc](https://logseq.com)\n- ## Install\n  - Run this once:\n  - ```bash\n    npm install\n    ```\n- ## Formula\n  - $$\n    E = mc^2\n    $$\n- ## Notes\n  - > Quotes stay quotes\n  - | Step | Owner |\n    | --- | --- |\n    | Build | Ana |\n  - #+BEGIN_TIP\n    Callouts work too.\n    #+END_TIP"]
   ["nested bullets" "- Parent\n  - Child\n    - Grandchild\n- Sibling"]
   ["tab indentation" "- A\n\t- B\n\t\t- C"]
   ["markdown document without bullets" "# Title\n\nIntro paragraph.\n\n## Section\n\nText under section.\n\n- a bullet\n- another"]
   ["heading bullets" "- # H1 bullet\n- ### H3 bullet\n- normal"]
   ["star and plus bullets" "* star bullet\n+ plus bullet"]
   ["numbered list only" "1. one\n2. two\n3. three"]
   ["numbered list after a bullet" "- intro\n1. one\n2. two"]
   ["standalone code fence" "```python\nprint('hi')\n```"]
   ["code fence under bullets" "- Setup:\n  ```bash\n  npm install\n  ```\n- next step"]
   ["standalone math" "$$\nE = mc^2\n$$"]
   ["task markers and checkboxes" "- TODO buy milk\n- DONE ship it\n- [ ] checkbox\n- [x] checked box"]
   ["priority and scheduled" "- TODO task [#A]\n  SCHEDULED: <2026-09-14 Mon>"]
   ["block properties" "- Book\n  author:: Jane\n  rating:: 5"]
   ["page properties first" "title:: Something\ntags:: a, b\n\n- first block"]
   ["links and tags" "- See [[Existing Page]] and [[Brand New Page]]\n- Tags #Project #[[Multi Word]] #newtag"]
   ["block ref to missing block" "- ref ((6f1c0000-0000-4000-8000-000000000000))"]
   ["block ref to existing block (both styles)" "- old ((e0000000-0000-4000-8000-000000000001)) new [[e0000000-0000-4000-8000-000000000001]]"]
   ["bullet whose first line is a property" "- status:: draft
- after"]
   ["inline formatting" "- **bold** _italic_ ~~strike~~ ==mark== `code` [link](https://logseq.com) ![img](https://logseq.com/a.png)"]
   ["quote and table" "- > quoted text\n- | a | b |\n  | --- | --- |\n  | 1 | 2 |"]
   ["callout" "- #+BEGIN_NOTE\n  a note\n  #+END_NOTE"]
   ["multi-paragraph bullet" "- first paragraph\n\n  second paragraph of same bullet\n- next"]
   ["plain paragraphs" "First paragraph.\n\nSecond paragraph."]
   ["single line" "just text with **bold** and [[Existing Page]]"]
   ["mirror output pasted back" "- DOING Task\n  * Priority:: Urgent\n- note\n  * notes::\n    - value\n- ```js #Example\n  code\n  ```"]
   ["obsidian syntax" "- [ ] obsidian task\n- > [!note] callout\n- %%comment%%\n- ![[Embedded Note]]"]
   ["youtube url" "https://www.youtube.com/watch?v=xu9p5ynlhZk"]
   ["embed macro" "{{embed [[Existing Page]]}}"]])

(def html-cases
  [["html headings, paragraph, lists, link"
    [[:h1 {} "Title"] [:p {} "Hello " [:strong {} "bold"] " " [:em {} "italic"] " " [:a {:href "https://logseq.com"} "link"]]
     [:ul {} [:li {} "one" [:ul {} [:li {} "nested"]]] [:li {} "two"]]
     [:ol {} [:li {} "first"] [:li {} "second"]]]]
   ["html pre code, blockquote, table"
    [[:pre {} [:code {:class "language-js"} "const a = 1;"]]
     [:blockquote {} [:p {} "quoted"]]
     [:table {} [:thead {} [:tr {} [:th {} "a"] [:th {} "b"]]] [:tbody {} [:tr {} [:td {} "1"] [:td {} "2"]]]]]]
   ["html inline styles"
    [[:p {} [:s {} "strike"] " " [:del {} "del"] " " [:mark {} "mark"] " " [:u {} "under"] " " [:code {} "inline"] " "
      [:sub {} "sub"] " " [:sup {} "sup"] " " [:span {:style "font-weight: 700"} "span bold"] " " [:img {:src "https://logseq.com/a.png" :alt "alt"}]]]]
   ["html h2 then paragraphs (Google Docs style)"
    [[:h2 {} "Meeting notes"] [:p {} "First point"] [:p {} "Second point"]]]
   ["html checklist and hr"
    [[:ul {} [:li {} [:input {:type "checkbox" :checked ""}] " done item"] [:li {} [:input {:type "checkbox"}] " open item"]]
     [:hr {}] [:p {} "after rule"]]]
   ["html with figure element"
    [[:h1 {} "Article"] [:p {} "Some " [:strong {} "bold"] " text"] [:figure {} [:img {:src "https://logseq.com/a.png"}] [:figcaption {} "cap"]]]]])

(defn- <run-case [label text html]
  (let [conn (new-conn)]
    (reset! *notices [])
    (-> (p/let [html-md (when html (try (html-parser/hiccup->doc (html-parser/html-decode-hiccup html))
                                        (catch :default e (str "CONVERT THROWS (app falls back to plain text): " (ex-message e)))))
                result (<paste! conn (if (and html-md (not (string/starts-with? html-md "CONVERT THROWS"))) html-md text) nil)
                mirror (<mirror conn)]
          (println (str "\n##### " label " #####"))
          (println "--- clipboard text:") (println text)
          (when html (println "--- clipboard html (hiccup):") (println (pr-str html)) (println "--- html->markdown:") (println html-md))
          (println "--- path:" (pr-str (:path result)))
          (when (seq @*notices) (println "--- notifications:" (pr-str @*notices)))
          (println "--- stored:") (doseq [b (stored-summary conn)] (println (pr-str b)))
          (println "--- mirror:") (println mirror))
        (p/catch (fn [e] (println (str "\n##### " label " ##### ERROR " e " " (pr-str (ex-data e)))))))))

(def typing-cases
  ["# Heading one" "### Heading three" "```clojure\n(+ 1 2)\n```" "$$\nx^2\n$$" "> quote typed" "1. numbered typed"
   "TODO typed task" "[#A] typed priority" "key:: typed property" "SCHEDULED: <2026-09-14 Mon>"
   "#tag typed" "- [ ] typed checkbox" "## heading with **bold**" "text\n# second-line heading" "Book\nauthor:: Jane" "status:: draft" "**bold** start" "* star typed" "---" "| a | b |\n| --- | --- |\n| 1 | 2 |" "[[Existing Page]] ref" "((e0000000-0000-4000-8000-000000000001))"])

(deftest paste-syntax-probe
  (async done
    (-> (p/do!
         (println "\n===== TYPED THEN SAVED (wrap-parse-block) =====")
         (doseq [t typing-cases]
           (reset! *notices [])
           (let [r (with-redefs [state/get-state (constantly nil)
                                 notification/show! (fn [msg & _] (swap! *notices conj (str msg)))
                                 state/pub-event! (fn [e] (swap! *notices conj (str "event " (first e) " " (some-> e second :error ex-message))))]
                     (try (db-editor-handler/wrap-parse-block {:block/title t})
                          (catch :default e {:error (ex-message e)})))]
             (println (pr-str t) "=>" (pr-str (select-keys r [:block/title :logseq.property/heading
                                                              :logseq.property.node/display-type
                                                              :logseq.property.code/lang :error]))
                      (if (seq @*notices) (str " NOTIFY " (pr-str @*notices)) ""))))
         (println "\n===== PASTE =====")
         (reduce (fn [acc [label text html]] (p/then acc (fn [_] (<run-case label text html))))
                 (p/resolved nil)
                 (concat paste-cases (map (fn [[l h]] [l "plain-text fallback" h]) html-cases)))
         (is true))
        (p/catch (fn [e] (println "PROBE ERROR" e) (is false)))
        (p/finally done))))
