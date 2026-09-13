(ns frontend.worker.mirror-syntax-probe-test
  "Scratch probe: prints the markdown mirror output for a matrix of DB constructs."
  (:require [cljs.test :refer [async deftest is]]
            [clojure.string :as string]
            [datascript.core :as d]
            [frontend.worker.markdown-mirror :as markdown-mirror]
            [logseq.db :as ldb]
            [logseq.db.frontend.property :as db-property]
            [logseq.db.test.helper :as db-test]
            [promesa.core :as p]))

(def test-repo "logseq_db_graph-probe")

(defn- fake-platform []
  (let [files (atom {})]
    {:platform {:env {:runtime :node}
                :storage {:read-text! (fn [path] (p/resolved (get @files path)))
                          :write-text-atomic! (fn [path content]
                                                (swap! files assoc path content)
                                                (p/resolved nil))
                          :delete-file! (fn [path] (swap! files dissoc path) (p/resolved nil))}
                :broadcast {:post-message! (fn [& _] nil)}}
     :files files}))

(defn- block-by-title [db title]
  (db-test/find-block-by-content db title))

(defn- set-title!
  "Mimic editor storage: title holds [[uuid]] / #[[uuid]] and refs/tags are attributes."
  [conn placeholder title & {:keys [refs tags extra]}]
  (let [b (block-by-title @conn placeholder)]
    (d/transact! conn [(merge {:db/id (:db/id b)
                               :block/title title}
                              (when refs {:block/refs (mapv (fn [u] [:block/uuid u]) refs)})
                              (when tags {:block/tags (mapv (fn [u] [:block/uuid u]) tags)})
                              extra)])))

(def foo-uuid #uuid "f0000000-0000-4000-8000-000000000001")
(def multi-uuid #uuid "f0000000-0000-4000-8000-000000000002")
(def target-uuid #uuid "f0000000-0000-4000-8000-000000000003")
(def book-uuid #uuid "f0000000-0000-4000-8000-000000000004")
(def scifi-uuid #uuid "f0000000-0000-4000-8000-000000000005")
(def embed-page-uuid #uuid "f0000000-0000-4000-8000-000000000006")

(defn- build-conn []
  (db-test/create-conn-with-blocks
   {:classes {:Book {:block/title "Book" :block/uuid book-uuid :build/keep-uuid? true}
              :SciFi {:block/title "Sci Fi" :block/uuid scifi-uuid :build/keep-uuid? true}}
    :properties {:user.property/notes {:logseq.property/type :default :block/title "notes"}
                 :user.property/rating {:logseq.property/type :number :block/title "rating"}
                 :user.property/website {:logseq.property/type :url :block/title "website"}
                 :user.property/done-flag {:logseq.property/type :checkbox :block/title "reviewed"}
                 :user.property/due {:logseq.property/type :date :block/title "Due Date"}
                 :user.property/when {:logseq.property/type :datetime :block/title "meeting at"}
                 :user.property/people {:logseq.property/type :node :db/cardinality :db.cardinality/many
                                        :block/title "people"}
                 :user.property/aliases-txt {:logseq.property/type :default :db/cardinality :db.cardinality/many
                                             :block/title "keywords"}
                 :user.property/color {:logseq.property/type :default :block/title "color"
                                       :build/closed-values [{:value "Red"} {:value "Green"}]}
                 :user.property/secret {:logseq.property/type :default :block/title "secret"
                                        :logseq.property/hide? true}}
    :pages-and-blocks
    [{:page {:block/title "Foo" :block/uuid foo-uuid :build/keep-uuid? true}
      :blocks [{:block/title "Target block" :block/uuid target-uuid :build/keep-uuid? true
                :build/children [{:block/title "target child"}]}]}
     {:page {:block/title "Multi Word Page" :block/uuid multi-uuid :build/keep-uuid? true}}
     {:page {:block/title "Embedded Page" :block/uuid embed-page-uuid :build/keep-uuid? true}
      :blocks [{:block/title "embedded page body"}]}
     {:page {:build/journal 20260912}
      :blocks [{:block/title "journal entry"}]}
     {:page {:block/title "Inline"}
      :blocks (mapv (fn [i] {:block/title (str "P" i)}) (range 30))}
     {:page {:block/title "Structure"}
      :blocks [{:block/title "Heading one" :build/properties {:logseq.property/heading 1}}
               {:block/title "Heading three" :build/properties {:logseq.property/heading 3}}
               {:block/title "Auto heading parent" :build/properties {:logseq.property/heading true}
                :build/children [{:block/title "Auto heading child" :build/properties {:logseq.property/heading true}}]}
               {:block/title "Heading task" :build/tags [:logseq.class/Task]
                :build/properties {:logseq.property/heading 2 :logseq.property/status :logseq.property/status.todo}}
               {:block/title "parent"
                :build/children [{:block/title "child"
                                  :build/children [{:block/title "grandchild"}]}]}
               {:block/title "collapsed parent" :block/collapsed? true
                :build/children [{:block/title "hidden child"}]}
               {:block/title "one" :build/properties {:logseq.property/order-list-type "number"}
                :build/children [{:block/title "one.a" :build/properties {:logseq.property/order-list-type "number"}}
                                 {:block/title "one.b" :build/properties {:logseq.property/order-list-type "number"}}]}
               {:block/title "two" :build/properties {:logseq.property/order-list-type "number"}}
               {:block/title "bullet between"}
               {:block/title "three?" :build/properties {:logseq.property/order-list-type "number"}}
               {:block/title ""}]}
     {:page {:block/title "Display Blocks"}
      :blocks [{:block/title "quote line 1\n\nquote line 3" :build/tags [:logseq.class/Quote-block]
                :build/properties {:logseq.property.node/display-type :quote}}
               {:block/title "(defn f [x]\n  x)" :build/tags [:logseq.class/Code-block]
                :build/properties {:logseq.property.node/display-type :code :logseq.property.code/lang "clojure"}}
               {:block/title "no lang" :build/tags [:logseq.class/Code-block]
                :build/properties {:logseq.property.node/display-type :code}}
               {:block/title "has ``` fence inside" :build/tags [:logseq.class/Code-block]
                :build/properties {:logseq.property.node/display-type :code :logseq.property.code/lang "md"}}
               {:block/title "E = mc^2" :build/tags [:logseq.class/Math-block]
                :build/properties {:logseq.property.node/display-type :math}}
               {:block/title "tagged code" :build/tags [:logseq.class/Code-block :Book]
                :build/properties {:logseq.property.node/display-type :code :logseq.property.code/lang "js"}}]}
     {:page {:block/title "Tasks"}
      :blocks [{:block/title "backlog" :build/tags [:logseq.class/Task] :build/properties {:logseq.property/status :logseq.property/status.backlog}}
               {:block/title "todo" :build/tags [:logseq.class/Task] :build/properties {:logseq.property/status :logseq.property/status.todo}}
               {:block/title "doing" :build/tags [:logseq.class/Task] :build/properties {:logseq.property/status :logseq.property/status.doing}}
               {:block/title "in review" :build/tags [:logseq.class/Task] :build/properties {:logseq.property/status :logseq.property/status.in-review}}
               {:block/title "done" :build/tags [:logseq.class/Task] :build/properties {:logseq.property/status :logseq.property/status.done}}
               {:block/title "canceled" :build/tags [:logseq.class/Task] :build/properties {:logseq.property/status :logseq.property/status.canceled}}
               {:block/title "default status task" :build/tags [:logseq.class/Task]}
               {:block/title "urgent with dates" :build/tags [:logseq.class/Task]
                :build/properties {:logseq.property/status :logseq.property/status.todo
                                   :logseq.property/priority :logseq.property/priority.urgent
                                   :logseq.property/deadline 1789221600000
                                   :logseq.property/scheduled 1789135200000}}
               {:block/title "low priority no status" :build/properties {:logseq.property/priority :logseq.property/priority.low}}]}
     {:page {:block/title "Props"
             :build/properties {:user.property/notes "page note"
                                :user.property/rating 4.5
                                :user.property/website "https://logseq.com"}}
      :blocks [{:block/title "all types"
                :build/properties {:user.property/notes "block note"
                                   :user.property/rating 3
                                   :user.property/website "https://example.com"
                                   :user.property/done-flag true
                                   :user.property/due [:build/page {:build/journal 20260912}]
                                   :user.property/when 1789221600000
                                   :user.property/people #{[:build/page {:block/title "Foo"}]
                                                           [:build/page {:block/title "Multi Word Page"}]}
                                   :user.property/aliases-txt #{"alpha" "beta"}
                                   :user.property/color "Red"
                                   :user.property/secret "hidden value"}}
               {:block/title "unchecked" :build/properties {:user.property/done-flag false}}]}
     {:page {:block/title "Embeds"}
      :blocks [{:block/title "EMBED-BLOCK"}
               {:block/title "EMBED-PAGE"}
               {:block/title "after embeds"}]}]}))

(def inline-cases
  [["**bold** *italic* _italic2_ ~~strike~~ ^^highlight^^ ==mark== `code` $E=mc^2$"]
   ["[Logseq](https://logseq.com) <https://example.com> https://bare.example.com"]
   ["![alt text](https://example.com/a.png){:width 300}"]
   ["line one\nline two\n\nparagraph after blank"]
   [(str "See [[" foo-uuid "]] and [[" multi-uuid "]]") :refs [foo-uuid multi-uuid]]
   [(str "Block ref [[" target-uuid "]] here") :refs [target-uuid]]
   [(str "Reading #[[" book-uuid "]] and #[[" scifi-uuid "]]") :refs [book-uuid scifi-uuid] :tags [book-uuid scifi-uuid]]
   ["tag only via picker" :tags [book-uuid scifi-uuid]]
   [(str "[label]([[" foo-uuid "]])") :refs [foo-uuid]]
   ["footnote[^1]\n\n[^1]: the note"]
   ["| a | b |\n| --- | --- |\n| 1 | 2 |"]
   ["#+BEGIN_NOTE\nnote body\n#+END_NOTE"]
   ["> [!note] obsidian callout"]
   ["{{video https://youtu.be/xyz}}"]
   ["---"]
   ["text\nkey:: value inside text"]
   ["- [ ] checkbox item\n- [x] done item"]
   ["((6f1c0000-0000-4000-8000-000000000000))"]
   ["<!-- html comment --> %%obsidian comment%% \\*escaped\\*"]
   ["- looks like a bullet"]
   ["# literal hash heading kept?"]
   ["TODO literal todo text"]
   ["line\n- nested-looking line\n  - deeper"]
   ["<div>inline <b>html</b></div>"]
   ["[:div [:b \"hiccup\"]]"]
   ["#+BEGIN_QUERY\n{:query [:find (pull ?b [*]) :where [?b :block/title]]}\n#+END_QUERY"]])

(defn- <dump [conn platform files title]
  (let [page (db-test/find-page-by-title @conn title)]
    (p/let [_ (markdown-mirror/<mirror-page! test-repo @conn (:db/id page) {:platform platform})]
      (let [path (str (markdown-mirror/repo-mirror-dir test-repo) "/"
                      (markdown-mirror/page-relative-path @conn page))]
        (println (str "\n===== " title " -> " (markdown-mirror/page-relative-path @conn page) " =====\n"
                      (get @files path)
                      "\n===== END " title " ====="))))))

(deftest mirror-syntax-probe
  (async done
    (let [{:keys [platform files]} (fake-platform)
          conn (build-conn)]
      (doseq [[i [title & {:keys [refs tags]}]] (map-indexed vector inline-cases)]
        (set-title! conn (str "P" i) title :refs refs :tags tags))
      ;; delete unused placeholders
      (let [unused (keep #(block-by-title @conn (str "P" %)) (range (count inline-cases) 30))]
        (d/transact! conn (mapv (fn [b] [:db/retractEntity (:db/id b)]) unused)))
      ;; embeds: editor stores an empty-title block linked to the target
      (let [eb (block-by-title @conn "EMBED-BLOCK")
            ep (block-by-title @conn "EMBED-PAGE")]
        (d/transact! conn [{:db/id (:db/id eb) :block/title "" :block/link [:block/uuid target-uuid]}
                           {:db/id (:db/id ep) :block/title "" :block/link [:block/uuid embed-page-uuid]}]))
      ;; page tags and alias on a page
      (let [props (db-test/find-page-by-title @conn "Props")]
        (d/transact! conn [{:db/id (:db/id props)
                            :block/tags [[:block/uuid book-uuid]]
                            :block/alias [[:block/uuid multi-uuid]]}]))
      (println "\n===== visible built-in properties (not :hide?) =====")
      (doseq [[k v] (sort-by key db-property/built-in-properties)
              :when (not (get-in v [:schema :hide?]))]
        (println (pr-str k) (pr-str (:title v)) (pr-str (get-in v [:schema :type])) (pr-str (get-in v [:schema :public?]))))
      (-> (p/do!
           (<dump conn platform files "Inline")
           (<dump conn platform files "Structure")
           (<dump conn platform files "Display Blocks")
           (<dump conn platform files "Tasks")
           (<dump conn platform files "Props")
           (<dump conn platform files "Embeds")
           (<dump conn platform files "Foo")
           (<dump conn platform files "Embedded Page")
           (let [j (db-test/find-journal-by-journal-day @conn 20260912)]
             (<dump conn platform files (:block/title j)))
           (is (ldb/page? (db-test/find-page-by-title @conn "Props"))))
          (p/catch (fn [e] (println "PROBE ERROR" e (ex-data e)) (is false)))
          (p/finally done)))))

(def asset-uuid #uuid "a0000000-0000-4000-8000-000000000001")
(def red-uuid #uuid "c0000000-0000-4000-8000-000000000001")
(def green-uuid #uuid "c0000000-0000-4000-8000-000000000002")

(deftest mirror-syntax-probe-2
  (async done
    (let [{:keys [platform files]} (fake-platform)
          conn (db-test/create-conn-with-blocks
                {:classes {:Book {:block/title "Book" :block/uuid book-uuid :build/keep-uuid? true
                                  :build/class-properties [:user.property/color]}
                           :Novel {:block/title "Novel" :build/class-extends [:Book]}}
                 :properties {:user.property/color {:logseq.property/type :default :block/title "color"
                                                    :build/closed-values [{:value "Red" :uuid red-uuid}
                                                                          {:value "Green" :uuid green-uuid}]}
                              :user.property/moods {:logseq.property/type :default :block/title "moods"
                                                    :db/cardinality :db.cardinality/many
                                                    :build/closed-values [{:value "Joy"} {:value "Calm"}]}
                              :user.property/notes {:logseq.property/type :default :block/title "notes"}}
                 :pages-and-blocks
                 [{:page {:block/title "Round Two"
                          :build/properties {:logseq.property/icon {:type :emoji :id "books"}}}
                   :blocks [{:block/title "screenshot 2026-09-12" :block/uuid asset-uuid :build/keep-uuid? true
                             :build/tags [:logseq.class/Asset]
                             :build/properties {:logseq.property.asset/type "png"
                                                :logseq.property.asset/checksum "abc"
                                                :logseq.property.asset/size 42}}
                            {:block/title "ASSET-REF"}
                            {:block/title "QUERY"}
                            {:block/title "closed color" :build/properties {:user.property/color [:block/uuid red-uuid]}}
                            {:block/title "no-lang code tagged"
                             :build/properties {:logseq.property.node/display-type :code}}
                            {:block/title "yellow background" :build/properties {:logseq.property/background-color "yellow"}}
                            {:block/title "What is {{cloze mirror}}?" :build/tags [:logseq.class/Card]}
                            {:block/title "a template" :build/tags [:logseq.class/Template]
                             :build/children [{:block/title "template child"}]}
                            {:block/title "text\n* note:: typed, not a property"}
                            {:block/title "value with nested property"
                             :build/properties {:user.property/notes {:build/property-value :block
                                                                      :block/title "value block"
                                                                      :build/properties {:user.property/color [:block/uuid green-uuid]}}}}
                            {:block/title "Ask [[Projects/Alpha]]"}
                            {:block/title "  leading spaces and trailing  "}]}
                  {:page {:block/title "Projects/Alpha"}}
                  {:page {:block/title "Note: A?B*"}}]})
          book (db-test/find-page-by-title @conn "Book")
          query-block (block-by-title @conn "QUERY")]
      (set-title! conn "ASSET-REF" (str "Look ![shot]([[" asset-uuid "]]) and [[" asset-uuid "]]") :refs [asset-uuid])
      (set-title! conn "no-lang code tagged" "no-lang code tagged" :tags [book-uuid])
      ;; /Query: block title "" + Query tag + query value block
      (let [qv-uuid (random-uuid)
            q-page (:db/id (:block/page query-block))]
        (d/transact! conn [{:block/uuid qv-uuid :block/title "(task todo doing)"
                            :block/page q-page :block/parent (:db/id query-block) :block/order "a0"
                            :logseq.property/created-from-property :logseq.property/query}
                           {:db/id (:db/id query-block) :block/title ""
                            :block/tags [:logseq.class/Query]
                            :logseq.property/query [:block/uuid qv-uuid]}]))
      (set-title! conn "a template" "a template")
      (-> (p/do!
           (<dump conn platform files "Round Two")
           (<dump conn platform files "Book")
           (<dump conn platform files "Novel")
           (<dump conn platform files "Projects/Alpha")
           (<dump conn platform files "Note: A?B*")
           (<dump conn platform files "Contents")
           (is (some? book)))
          (p/catch (fn [e] (println "PROBE2 ERROR" e (ex-data e)) (is false)))
          (p/finally done)))))

(deftest mirror-syntax-probe-3
  (async done
    (let [{:keys [platform files]} (fake-platform)
          conn (db-test/create-conn-with-blocks
                {:classes {:Book {:block/title "Book" :block/uuid book-uuid :build/keep-uuid? true}}
                 :pages-and-blocks
                 [{:page {:block/title "Round Three"}
                   :blocks [{:block/title "cleared priority" :build/tags [:logseq.class/Task]
                             :build/properties {:logseq.property/priority :logseq.property/empty-placeholder}}
                            {:block/title "first line\nsecond line"}
                            {:block/title "quoted" :build/tags [:logseq.class/Quote-block]
                             :build/properties {:logseq.property.node/display-type :quote}}
                            {:block/title "math" :build/tags [:logseq.class/Math-block]
                             :build/properties {:logseq.property.node/display-type :math}}
                            {:block/title "Tagged heading" :build/properties {:logseq.property/heading 2}}]}]})]
      (set-title! conn "first line\nsecond line" "first line\nsecond line" :tags [book-uuid])
      (set-title! conn "quoted" "quoted" :tags [book-uuid])
      (set-title! conn "math" "math" :tags [book-uuid])
      (set-title! conn "Tagged heading" "Tagged heading" :tags [book-uuid])
      (-> (p/do! (<dump conn platform files "Round Three") (is true))
          (p/catch (fn [e] (println "PROBE3 ERROR" e (ex-data e)) (is false)))
          (p/finally done)))))

(deftest mirror-syntax-probe-4
  (async done
    (let [{:keys [platform files]} (fake-platform)
          conn (db-test/create-conn-with-blocks
                {:pages-and-blocks
                 [{:page {:block/title "Math Desync"}
                   :blocks [{:block/title "before task" :build/tags [:logseq.class/Task]
                             :build/properties {:logseq.property/status :logseq.property/status.doing}}
                            {:block/title "x^2" :build/tags [:logseq.class/Math-block]
                             :build/properties {:logseq.property.node/display-type :math}}
                            {:block/title "after task" :build/tags [:logseq.class/Task]
                             :build/properties {:logseq.property/status :logseq.property/status.done}}
                            {:block/title "numbered after" :build/properties {:logseq.property/order-list-type "number"}}]}
                  {:page {:block/title "No Math"}
                   :blocks [{:block/title "after task" :build/tags [:logseq.class/Task]
                             :build/properties {:logseq.property/status :logseq.property/status.done}}
                            {:block/title "numbered after" :build/properties {:logseq.property/order-list-type "number"}}]}]})]
      (-> (p/do! (<dump conn platform files "Math Desync") (<dump conn platform files "No Math") (is true))
          (p/catch (fn [e] (println "PROBE4 ERROR" e (ex-data e)) (is false)))
          (p/finally done)))))

(deftest mirror-syntax-probe-5
  (async done
    (let [{:keys [platform files]} (fake-platform)
          conn (db-test/create-conn-with-blocks
                {:classes {:Book {:block/title "Book" :block/uuid book-uuid :build/keep-uuid? true}}
                 :pages-and-blocks
                 [{:page {:block/title "Round Five"}
                   :blocks [{:block/title "js code" :build/tags [:logseq.class/Code-block]
                             :build/properties {:logseq.property.node/display-type :code :logseq.property.code/lang "js"}}
                            {:block/title "\nstarts with newline"}
                            {:block/title "after newline block" :build/tags [:logseq.class/Task]
                             :build/properties {:logseq.property/status :logseq.property/status.done}}]}]})]
      (let [b (block-by-title @conn "js code")]
        (d/transact! conn [{:db/id (:db/id b) :block/tags [[:block/uuid book-uuid]]}]))
      (-> (p/do! (<dump conn platform files "Round Five") (is true))
          (p/catch (fn [e] (println "PROBE5 ERROR" e (ex-data e)) (is false)))
          (p/finally done)))))

(comment string/join)
