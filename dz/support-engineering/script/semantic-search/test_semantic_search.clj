#!/usr/bin/env bb
;; Exercise Logseq's zvec-backed semantic search end to end against a live
;; db-worker-node.
;;
;; Semantic search is not a standalone API: it is folded into `search-blocks`.
;; A vector query runs only when the option map carries BOTH
;; `:feature/enable-semantic-search? true` and a 384-float `:query-embedding`.
;; The worker fills in `:query-embedding` from the embedding sidecar, but a
;; caller may supply it directly and bypass the sidecar entirely.
;;
;; This script runs three probes and diffs them:
;;
;;   A  keyword only            semantic flag off             -> the baseline
;;   B  worker-side embedding   flag on, no :query-embedding   -> tests the sidecar wiring
;;   C  injected embedding      flag on, :query-embedding set  -> tests the zvec index itself
;;
;; C is the real assertion. B can silently fall back to keyword-only results
;; because the worker gives the sidecar just 50ms (query-embedding-timeout-ms)
;; before degrading, so a cold model makes B look broken when it is only slow.
;;
;; Usage:
;;   ./test_semantic_search.clj "outliner conflict resolution"
;;   ./test_semantic_search.clj --repo logseq_db_mygraph "my query"
;;   ./test_semantic_search.clj --port 3000 --embedding-url http://127.0.0.1:8765/v1/embeddings "q"
;;   ./test_semantic_search.clj --json "q"
;;
;; Exit codes: 0 semantic search verified, 1 verified broken, 2 could not test.

(require '[babashka.fs :as fs]
         '[babashka.http-client :as http]
         '[cheshire.core :as json]
         '[clojure.string :as str]
         '[cognitect.transit :as transit])

(import '[java.io ByteArrayInputStream ByteArrayOutputStream]
        '[java.net URLDecoder])

;; ---------------------------------------------------------------------------
;; constants mirrored from the app
;; ---------------------------------------------------------------------------

;; src/main/frontend/worker/search.cljs
(def vector-embedding-dimension 384)
(def min-vector-search-score 0.5)
(def max-vector-search-results 10)
;; src/main/frontend/worker/handler/search.cljs
(def query-embedding-timeout-ms 50)
;; deps/common/src/logseq/common/config.cljs
(def db-version-prefix "logseq_db_")

(def default-embedding-url "http://127.0.0.1:8765/v1/embeddings")

;; ---------------------------------------------------------------------------
;; transit — must match logseq.db.sqlite.util/{write,read}-transit-str
;; ---------------------------------------------------------------------------

(def ^:private transit-read-opts
  ;; The worker writes datascript/Entity, error and js/Error tags. Nothing in a
  ;; search-blocks result should use them, but registering them keeps an
  ;; unexpected tag readable. Anything still unknown falls back to transit's
  ;; built-in TaggedValue rather than throwing.
  {:handlers {"datascript/Entity" (transit/read-handler identity)
              "error" (transit/read-handler #(ex-info (str (:message %)) (or (:data %) {})))
              "js/Error" (transit/read-handler #(ex-info (str (:message %)) {}))}})

(defn write-transit ^String [x]
  (let [out (ByteArrayOutputStream.)]
    (transit/write (transit/writer out :json) x)
    (str out)))

(defn read-transit [^String s]
  (when (seq s)
    (transit/read (transit/reader (ByteArrayInputStream. (.getBytes s "UTF-8"))
                                  :json
                                  transit-read-opts))))

;; ---------------------------------------------------------------------------
;; output
;; ---------------------------------------------------------------------------

(def ^:dynamic *json?* false)

(defn- say [fmt & args]
  (when-not *json?* (println (apply format fmt args))))

(defn- section [title]
  (when-not *json?*
    (println)
    (println title)
    (println (apply str (repeat (count title) \-)))))

;; ---------------------------------------------------------------------------
;; http
;; ---------------------------------------------------------------------------

(defn- get-json [url {:keys [timeout] :or {timeout 3000}}]
  (try
    (let [{:keys [status body]} (http/get url {:timeout timeout :throw false})]
      {:status status :body (try (json/parse-string body true) (catch Exception _ body))})
    (catch Exception e
      {:error (ex-message e)})))

(defn- post-json [url payload {:keys [timeout] :or {timeout 120000}}]
  (try
    (let [{:keys [status body]} (http/post url {:headers {"Content-Type" "application/json"
                                                          "Accept" "application/json"}
                                                :body (json/generate-string payload)
                                                :timeout timeout
                                                :throw false})]
      {:status status :body (try (json/parse-string body true) (catch Exception _ body))})
    (catch Exception e
      {:error (ex-message e)})))

;; ---------------------------------------------------------------------------
;; server discovery — ~/logseq/server-list holds "<pid> <port>" lines
;; ---------------------------------------------------------------------------

(defn- server-list-ports [root-dir]
  (let [f (fs/file root-dir "server-list")]
    (if-not (fs/exists? f)
      []
      (->> (str/split-lines (slurp f))
           (keep (fn [line]
                   (when-let [[_ _pid port] (re-matches #"\s*(\d+)\s+(\d+)\s*" line)]
                     (parse-long port))))
           distinct
           vec))))

(defn- probe-health [port]
  (let [{:keys [status body error]} (get-json (format "http://127.0.0.1:%d/healthz" port) {})]
    (cond
      error {:port port :reachable? false :error error}
      (map? body) (assoc body :port port :reachable? true :http-status status)
      :else {:port port :reachable? false :error (str "unexpected /healthz body: " body)})))

(defn- discover-server [{:keys [root-dir port repo]}]
  (let [ports (if port [port] (server-list-ports root-dir))
        healths (mapv probe-health ports)
        ready (filterv #(and (:reachable? %) (= "ready" (:status %))) healths)
        matching (if repo
                   (filterv #(= (str/replace (str (:repo %)) db-version-prefix "")
                                (str/replace repo db-version-prefix ""))
                            ready)
                   ready)]
    {:ports ports :healths healths :chosen (first matching)}))

;; ---------------------------------------------------------------------------
;; /v1/invoke
;; ---------------------------------------------------------------------------

(defn invoke
  "POST a thread-api method. Args go over as transit so keyword keys survive:
   the endpoint accepts a plain JSON `args` array but re-encodes it verbatim,
   turning :feature/enable-semantic-search? into an inert string key."
  [port method args & [{:keys [timeout] :or {timeout 60000}}]]
  (let [url (format "http://127.0.0.1:%d/v1/invoke" port)
        started (System/currentTimeMillis)
        {:keys [status body error]} (post-json url
                                               {:method (if (keyword? method)
                                                          (subs (str method) 1)
                                                          (str method))
                                                :argsTransit (write-transit args)}
                                               {:timeout timeout})
        elapsed (- (System/currentTimeMillis) started)]
    (cond
      error {:ok? false :elapsed-ms elapsed :error error}
      (not= 200 status) {:ok? false :elapsed-ms elapsed
                         :error (str "HTTP " status " " (pr-str (:error body)))}
      (false? (:ok body)) {:ok? false :elapsed-ms elapsed :error (pr-str (:error body))}
      :else {:ok? true :elapsed-ms elapsed :result (read-transit (:resultTransit body))})))

;; ---------------------------------------------------------------------------
;; embedding sidecar
;; ---------------------------------------------------------------------------

(defn- l2-norm [v]
  (Math/sqrt (reduce + 0.0 (map #(* % %) v))))

(defn embed
  "Call the OpenAI-compatible sidecar. Returns {:vector [...] :elapsed-ms n}."
  [embedding-url text]
  (let [started (System/currentTimeMillis)
        {:keys [status body error]} (post-json embedding-url {:input [text]} {:timeout 120000})
        elapsed (- (System/currentTimeMillis) started)]
    (cond
      error {:error error :elapsed-ms elapsed}
      (not= 200 status) {:error (str "HTTP " status " " (pr-str body)) :elapsed-ms elapsed}
      :else (let [v (some-> body :data first :embedding vec)]
              (if (seq v)
                {:vector v :elapsed-ms elapsed :model (:model body)}
                {:error (str "no embedding in response: " (pr-str body)) :elapsed-ms elapsed})))))

;; ---------------------------------------------------------------------------
;; on-disk zvec index
;; ---------------------------------------------------------------------------

(defn- decode-graph-dir-name
  "Inverse of logseq.common.graph-dir/encode-graph-dir-name."
  [dir-name]
  (try
    (URLDecoder/decode (str/replace dir-name "~" "%") "UTF-8")
    (catch Exception _ nil)))

(defn vector-index-info
  "Locate ~/logseq/graphs/<encoded>/search/vector for `repo` and read its metadata."
  [root-dir repo]
  (let [graphs-dir (fs/file root-dir "graphs")
        want (str/replace (or repo "") db-version-prefix "")]
    (when (fs/exists? graphs-dir)
      (->> (fs/list-dir graphs-dir)
           (keep (fn [d]
                   (let [name' (fs/file-name d)
                         decoded (decode-graph-dir-name name')]
                     (when (or (str/blank? want)
                               (= decoded want)
                               (= name' want))
                       (let [vdir (fs/file d "search" "vector")
                             meta-file (fs/file vdir "metadata.json")]
                         {:graph decoded
                          :dir (str vdir)
                          :exists? (fs/exists? vdir)
                          :size-bytes (when (fs/exists? vdir)
                                        (reduce + 0 (map fs/size (filter fs/regular-file?
                                                                         (file-seq (fs/file vdir))))))
                          :metadata (when (fs/exists? meta-file)
                                      (try (json/parse-string (slurp meta-file) true)
                                           (catch Exception _ nil)))})))))
           first))))

;; ---------------------------------------------------------------------------
;; probes
;; ---------------------------------------------------------------------------

(defn- result-seq [result]
  (cond
    (map? result) (:items result)
    (sequential? result) result
    :else nil))

(defn- summarize [items]
  (let [items (vec (result-seq items))]
    {:count (count items)
     :ids (mapv :id items)
     :vector-hits (filterv :vector-score items)
     :titles (mapv (fn [m] {:id (:id m)
                            :title (some-> (or (:block/title m) (:title m)) str)
                            :vector-score (:vector-score m)})
                   items)}))

(defn run-probe [port repo query option label]
  (let [{:keys [ok? result error elapsed-ms]} (invoke port :thread-api/search-blocks
                                                      [repo query option])]
    (merge {:label label :ok? ok? :elapsed-ms elapsed-ms}
           (if ok? (summarize result) {:error error}))))

(defn- print-probe [probe]
  (let [{:keys [label ok? error elapsed-ms vector-hits titles]} probe
        hit-count (:count probe)]
    (if-not ok?
      (say "  %-28s FAILED  %s" label error)
      (say "  %-28s %3d hits  %2d vector-scored  (%d ms)"
           label hit-count (count vector-hits) elapsed-ms))
    (when (and ok? (seq titles) (not *json?*))
      (doseq [{:keys [id title vector-score]} (take 5 titles)]
        (say "      %-10s %s%s"
             id
             (let [t (or title "")]
               (if (> (count t) 60) (str (subs t 0 60) "…") t))
             (if vector-score (format "   [vec %.3f]" (double vector-score)) ""))))))

;; ---------------------------------------------------------------------------
;; args
;; ---------------------------------------------------------------------------

(defn- parse-args [argv]
  (loop [args (vec argv) opts {} positional []]
    (if-let [a (first args)]
      (case a
        "--repo"           (recur (subvec args 2) (assoc opts :repo (second args)) positional)
        "--port"           (recur (subvec args 2) (assoc opts :port (parse-long (second args))) positional)
        "--root-dir"       (recur (subvec args 2) (assoc opts :root-dir (second args)) positional)
        "--embedding-url"  (recur (subvec args 2) (assoc opts :embedding-url (second args)) positional)
        "--limit"          (recur (subvec args 2) (assoc opts :limit (parse-long (second args))) positional)
        "--json"           (recur (subvec args 1) (assoc opts :json? true) positional)
        ("-h" "--help")    (recur (subvec args 1) (assoc opts :help? true) positional)
        (recur (subvec args 1) opts (conj positional a)))
      (assoc opts :query (str/join " " positional)))))

(def usage
  (str/join
   \newline
   ["Usage: test_semantic_search.clj [options] <query>"
    ""
    "  --repo <repo>           graph to search (default: whatever the server is bound to)"
    "  --port <port>           db-worker-node port (default: discovered via ~/logseq/server-list)"
    "  --root-dir <dir>        Logseq root dir (default: $LOGSEQ_ROOT_DIR or ~/logseq)"
    "  --embedding-url <url>   sidecar endpoint (default: $LOGSEQ_EMBEDDINGS_URL or"
    (str "                          " default-embedding-url ")")
    "  --limit <n>             result limit per probe (default 20)"
    "  --json                  emit a machine-readable report on stdout"
    "  -h, --help              this message"
    ""
    "Exit: 0 semantic search verified, 1 verified broken, 2 could not test."]))

;; ---------------------------------------------------------------------------
;; main
;; ---------------------------------------------------------------------------

(defn -main [& argv]
  (let [{:keys [help? query repo port root-dir embedding-url limit json?]} (parse-args argv)]
    (when (or help? (str/blank? query))
      (println usage)
      (System/exit (if help? 0 2)))

    (binding [*json?* (boolean json?)]
      (let [root-dir (or root-dir (System/getenv "LOGSEQ_ROOT_DIR")
                         (str (fs/file (System/getProperty "user.home") "logseq")))
            embedding-url (or embedding-url (System/getenv "LOGSEQ_EMBEDDINGS_URL") default-embedding-url)
            limit (or limit 20)
            report (atom {:query query :root-dir root-dir :embedding-url embedding-url})]

        ;; -- environment ------------------------------------------------------
        (section "Environment")
        (let [os (System/getProperty "os.name")
              arch (System/getProperty "os.arch")
              supported? (and (str/includes? (str/lower-case os) "mac")
                              (contains? #{"aarch64" "arm64"} arch))]
          (swap! report assoc :platform {:os os :arch arch :vector-capable? supported?})
          (say "  platform            %s / %s" os arch)
          (say "  vector index built  %s"
               (if supported?
                 "yes (macOS arm64)"
                 "NO — the node platform only builds a zvec index on macOS arm64"))
          (say "  root-dir            %s" root-dir))

        ;; -- server -----------------------------------------------------------
        (section "db-worker-node")
        (let [{:keys [ports healths chosen]} (discover-server {:root-dir root-dir :port port :repo repo})]
          (swap! report assoc :servers healths)
          (say "  server-list ports   %s" (if (seq ports) (str/join ", " ports) "(none)"))
          (doseq [h healths]
            (say "    :%-6s %-9s repo=%s" (:port h)
                 (if (:reachable? h) (or (:status h) "?") "unreachable")
                 (or (:repo h) (:error h))))
          (when-not chosen
            (say "\n  No ready db-worker-node found. Start one with:")
            (say "    logseq server start --graph <name>")
            (say "  or point at a running instance with --port.")
            (when json? (println (json/generate-string (assoc @report :verdict "no-server") {:pretty true})))
            (System/exit 2))

          (let [port (:port chosen)
                repo (or repo (:repo chosen))]
            (swap! report assoc :port port :repo repo)
            (say "  using              :%d  repo=%s  revision=%s" port repo (:revision chosen))

            ;; -- on-disk index -------------------------------------------------
            (section "zvec collection")
            (let [info (vector-index-info root-dir repo)]
              (swap! report assoc :vector-index info)
              (if (:exists? info)
                (do (say "  path                %s" (:dir info))
                    (say "  size                %.1f MB" (/ (double (or (:size-bytes info) 0)) 1048576.0))
                    (if-let [m (:metadata info)]
                      (do (say "  model               %s" (:embedding-model-id m))
                          (say "  dimension           %s" (:embedding-dimension m))
                          (say "  context-version     %s" (:context-version m))
                          (when (and (:embedding-dimension m)
                                     (not= (:embedding-dimension m) vector-embedding-dimension))
                            (say "  !! dimension does not match the expected %d" vector-embedding-dimension)))
                      (say "  metadata.json       MISSING — index was never finalized")))
                (say "  none found for this graph (expected %s)"
                     (str (fs/file root-dir "graphs" "<graph>" "search" "vector")))))

            ;; -- sidecar ---------------------------------------------------------
            (section "Embedding sidecar")
            (let [health-url (str/replace embedding-url #"/v1/embeddings/?$" "/healthz")
                  health (get-json health-url {})
                  embedding (embed embedding-url query)]
              (swap! report assoc :sidecar {:url embedding-url
                                            :healthz (dissoc health :body)
                                            :model (:model embedding)
                                            :elapsed-ms (:elapsed-ms embedding)
                                            :error (:error embedding)})
              (say "  endpoint            %s" embedding-url)
              (say "  /healthz            %s" (or (some-> health :body :status)
                                                  (:error health)
                                                  (str "HTTP " (:status health))))
              (if (:error embedding)
                (say "  embed query         FAILED — %s" (:error embedding))
                (let [v (:vector embedding)
                      n (l2-norm v)]
                  (say "  embed query         %d dims in %d ms (model %s)"
                       (count v) (:elapsed-ms embedding) (or (:model embedding) "?"))
                  (say "  L2 norm             %.4f%s" n
                       (if (< (abs (- n 1.0)) 0.01) "  (normalized, cosine == dot)" "  !! not normalized"))
                  (when (not= (count v) vector-embedding-dimension)
                    (say "  !! expected %d dims" vector-embedding-dimension))
                  (when (> (:elapsed-ms embedding) query-embedding-timeout-ms)
                    (say "  note                slower than the worker's %dms budget — probe B will"
                         query-embedding-timeout-ms)
                    (say "                      fall back to keyword-only. Probe C is the real test."))))

              ;; -- probes --------------------------------------------------------
              (section "Probes")
              (let [base {:limit limit :enable-snippet? false}
                    a (run-probe port repo query
                                 (assoc base :feature/enable-semantic-search? false)
                                 "A keyword only")
                    b (run-probe port repo query
                                 (assoc base :feature/enable-semantic-search? true)
                                 "B worker-side embedding")
                    c (when-let [v (:vector embedding)]
                        (run-probe port repo query
                                   (assoc base :feature/enable-semantic-search? true
                                          :query-embedding v)
                                   "C injected embedding"))
                    probes (vec (remove nil? [a b c]))]
                (run! print-probe probes)
                (swap! report assoc :probes (mapv #(dissoc % :titles) probes))

                ;; -- diff ---------------------------------------------------------
                (section "Diff")
                (let [ids-a (set (:ids a))
                      semantic (or c b)
                      ids-s (set (:ids semantic))
                      only-semantic (remove ids-a (:ids semantic))
                      vec-hits (:vector-hits semantic)]
                  (say "  keyword ids         %d" (count ids-a))
                  (say "  semantic ids        %d" (count ids-s))
                  (say "  new from vectors    %d %s" (count only-semantic)
                       (if (seq only-semantic) (str "-> " (str/join ", " (take 8 only-semantic))) ""))
                  (say "  vector-scored hits  %d (cap %d, score floor %.1f)"
                       (count vec-hits) max-vector-search-results min-vector-search-score)

                  ;; -- verdict -----------------------------------------------------
                  (section "Verdict")
                  (let [verdict
                        (cond
                          (and c (not (:ok? c))) [:broken 1 (str "probe C failed: " (:error c))]
                          (nil? c) [:blocked 2 (str "could not embed the query, so probe C never ran — "
                                                    "start the sidecar: python3 sidecar/embedding_server.py --port 8765")]
                          (seq vec-hits) [:ok 0 (format "semantic search WORKS — %d vector-scored hits, %d results keyword search alone would have missed"
                                                        (count vec-hits) (count only-semantic))]
                          (not (:exists? (:vector-index @report)))
                          [:broken 1 "no zvec collection on disk — nothing was ever indexed for this graph"]
                          :else
                          [:broken 1 (str "no vector-scored hits. The index exists and the query embedded, so either "
                                         "nothing scored above the " min-vector-search-score
                                         " cosine floor, or the index is empty/stale (rebuild the search index).")])
                        [kind code message] verdict]
                    (say "  %s" message)
                    (swap! report assoc :verdict (name kind) :message message)
                    (when json?
                      (println (json/generate-string @report {:pretty true})))
                    (System/exit code)))))))))))

(apply -main *command-line-args*)
