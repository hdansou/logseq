#!/usr/bin/env bb
;; Generate the db-worker thread-api reference from source, and gate drift.
;;
;; Every worker RPC is declared with one macro:
;;
;;   (def-thread-api :thread-api/NAME [params] body...)
;;
;; That declaration IS the source of truth, so the reference is generated from
;; it rather than hand-written. Prose that a generator cannot write (what a
;; method is for, whether it is safe to build on) lives in thread-api-notes.edn
;; and is joined in by method name.
;;
;; Also extracted, because they are enumerable and duplicated in prose today:
;;   - which methods are exempt from repo binding (non-repo-methods)
;;   - how many renderer / CLI call sites each method has (dead-method finder)
;;   - the SSE event catalogue (worker emit sites + renderer handlers)
;;   - the outliner op vocabulary and arg tuples (malli op-schema)
;;
;; Usage:
;;   gen_thread_api_docs.clj --root <repo> --out ddocs/reference/api/thread-api.generated.md
;;   gen_thread_api_docs.clj --root <repo> --out <same> --check     # exit 1 if stale
;;   gen_thread_api_docs.clj --root <repo> --edn                     # machine-readable
;;
;; Drift findings (a method listed in an allow-set but never defined, a notes
;; entry for a method that no longer exists) are printed AND embedded in the
;; document, so they cannot be missed.

(require '[babashka.fs :as fs]
         '[clojure.pprint :as pp]
         '[clojure.string :as str]
         '[edamame.core :as e])

;; ---------------------------------------------------------------------------
;; parsing
;; ---------------------------------------------------------------------------

(defn- file-ns
  "Namespace symbol from the (ns ...) form, found by regex so it is available
   before parsing (edamame needs it to resolve ::keywords correctly)."
  [text]
  (some-> (re-find #"\(ns\s+\^?[^\s\(\)]*?\s*([A-Za-z][\w.\-]*)" text) second symbol))

(defn- parse-opts [current-ns]
  {:all true
   :read-cond :allow
   :features #{:cljs}
   :auto-resolve (fn [alias] (if (= :current alias) (or current-ns 'user) (symbol (name alias))))
   :readers (fn [_tag] identity)
   :location? seq?})

(defn- parse-file [file]
  (try
    (let [text (slurp (str file))]
      (e/parse-string-all text (parse-opts (file-ns text))))
    (catch Exception ex
      (binding [*out* *err*]
        (println "warn: could not parse" (str file) "-" (ex-message ex)))
      [])))

(defn- clj-files
  "All .cljs/.cljc under dir, including files directly in dir.
   (fs/glob \"**/*.cljs\" silently skips top-level files, which is where
   db_listener.cljs lives — so walk the tree instead.)"
  [dir]
  (when (fs/exists? dir)
    (->> (file-seq (fs/file dir))
         (filter #(re-find #"\.clj[sc]$" (str %)))
         (map str)
         (remove #(str/includes? % "/test/"))
         sort)))

;; Clojure keyword characters, so :thread-api/ensure-id&access-token and
;; :thread-api/export-get-all-page->content are matched whole.
(def ^:private kw-char "[A-Za-z0-9*+!\\-_'?<>=&.]")

(defn- ns-of [forms]
  (some (fn [f] (when (and (seq? f) (= 'ns (first f))) (second f))) forms))

(defn- def-thread-api-form? [f]
  (and (seq? f)
       (symbol? (first f))
       (= "def-thread-api" (name (first f)))
       (keyword? (second f))))

(defn- walk-forms
  "Top-level forms plus one level of (do ...) / (comment ...) nesting."
  [forms]
  (mapcat (fn [f]
            (if (and (seq? f) (contains? #{'do} (first f)))
              (rest f)
              [f]))
          forms))

(defn- clj-sources [root]
  (concat (clj-files (fs/file root "src" "main"))
          (clj-files (fs/file root "deps"))))

(defn- worker-side?
  "def-thread-api is used in both directions: worker methods (invoked by the
   renderer/CLI) and main-thread methods (invoked BY the worker via
   <invoke-main-thread). Only worker-side ones are reachable over HTTP."
  [rel-path]
  (boolean (or (str/starts-with? rel-path "src/main/frontend/worker")
               (str/starts-with? rel-path "src/main/frontend/worker_common")
               (str/starts-with? rel-path "src/main/logseq/db_worker")
               (str/starts-with? rel-path "deps/"))))

(defn extract-thread-apis [root]
  (vec
   (for [file (clj-sources root)
         :let [forms (parse-file file)
               ns-sym (ns-of forms)
               rel (str (fs/relativize root file))]
         f (walk-forms forms)
         :when (def-thread-api-form? f)]
     (let [[_ kw params & body] f
           doc (when (string? (first body)) (first body))
           params (if doc (first (rest body)) params)]
       {:name kw
        :params (pr-str params)
        :arity (count (remove #{'&} params))
        :variadic? (boolean (some #{'&} params))
        :doc doc
        :ns ns-sym
        :file rel
        :side (if (worker-side? rel) :worker :main)
        :line (:row (meta f))}))))

;; ---------------------------------------------------------------------------
;; classification data that lives elsewhere in the code
;; ---------------------------------------------------------------------------

(defn- find-def-set
  "Value of (def ^:private NAME #{...}) in a file, or nil."
  [root rel-path sym-name]
  (let [file (fs/file root rel-path)]
    (when (fs/exists? file)
      (some (fn [f]
              (when (and (seq? f) (= 'def (first f))
                         (symbol? (second f)) (= sym-name (name (second f))))
                (let [v (last f)] (when (set? v) v))))
            (parse-file file)))))

(defn- count-call-sites
  "Occurrences of :thread-api/NAME (cljs keyword) or \"thread-api/NAME\" (OCaml
   string) across files, keyed by keyword."
  [files]
  (let [re (re-pattern (str "(?::thread-api/|\"thread-api/)(" kw-char "+)"))]
    (reduce (fn [acc file]
              (reduce (fn [acc [_ nm]] (update acc (keyword "thread-api" nm) (fnil inc 0)))
                      acc
                      (re-seq re (slurp file))))
            {}
            files)))

(defn- renderer-files [root]
  (->> (clj-files (fs/file root "src" "main" "frontend"))
       (remove #(re-find #"/src/main/frontend/(worker|worker_common|common)/" %))))

(defn- cli-files [root]
  (concat (clj-files (fs/file root "src" "main" "logseq" "cli"))
          (when (fs/exists? (fs/file root "cli" "lib"))
            (->> (file-seq (fs/file root "cli" "lib"))
                 (filter #(re-find #"\.ml$" (str %)))
                 (map str)))))

(defn- extract-events [root]
  (let [handler-file (fs/file root "src/main/frontend/handler/worker.cljs")
        handled (when (fs/exists? handler-file)
                  (->> (re-seq (re-pattern (str "\\(defmethod handle (:" kw-char "+(?:/" kw-char "+)?)")) (slurp handler-file))
                       (map (comp keyword #(subs % 1) second)) set))
        worker-files (concat (clj-files (fs/file root "src/main/frontend/worker"))
                             (clj-files (fs/file root "deps")))
        emit-re (re-pattern (str "(?:broadcast-to-clients!|post-message!\\s*\\(platform/current\\)|worker-util/post-message)\\s+(:" kw-char "+(?:/" kw-char "+)?)"))
        emitted (reduce (fn [acc file]
                          (reduce (fn [acc [_ kw]]
                                    (update acc (keyword (subs kw 1)) (fnil conj #{})
                                            (str (fs/relativize root file))))
                                  acc (re-seq emit-re (slurp file))))
                        {} worker-files)]
    (->> (into (set (keys emitted)) handled)
         (remove #{:default})
         sort
         (mapv (fn [k] {:type k
                        :handled-by-renderer? (contains? handled k)
                        :emitted-from (sort (get emitted k []))})))))

(defn- extract-outliner-ops [root]
  (let [op-file (fs/file root "deps/outliner/src/logseq/outliner/op.cljs")
        construct-file (fs/file root "deps/outliner/src/logseq/outliner/op/construct.cljc")
        schema (when (fs/exists? op-file)
                 (some (fn [f] (when (and (seq? f) (= 'def (first f))
                                          (symbol? (second f))
                                          (= "op-schema" (name (second f))))
                                 (last f)))
                       (parse-file op-file)))
        semantic (or (find-def-set root "deps/outliner/src/logseq/outliner/op/construct.cljc" "semantic-outliner-ops")
                     #{})]
    (when (and (vector? schema) (= :multi (first schema)))
      (->> (drop 2 schema)
           (keep (fn [[op-name [_catn _op [_args-kw [_tuple & args]]]]]
                   (when (keyword? op-name)
                     {:op op-name
                      ;; ::block resolves to :logseq.outliner.op/block; show just `block`
                      :args (mapv #(if (and (keyword? %) (namespace %)) (name %) (pr-str %)) args)
                      :semantic? (contains? semantic op-name)})))
           vec))))

;; ---------------------------------------------------------------------------
;; assemble + drift
;; ---------------------------------------------------------------------------

(defn build-model [root notes]
  (let [apis (extract-thread-apis root)
        defined (set (map :name apis))
        non-repo (or (find-def-set root "src/main/frontend/worker/db_worker_node.cljs" "non-repo-methods") #{})
        write-methods (or (find-def-set root "src/main/frontend/worker/db_worker_node.cljs" "write-methods") #{})
        renderer-calls (count-call-sites (renderer-files root))
        cli-calls (count-call-sites (cli-files root))
        apis (->> apis
                  (map (fn [{:keys [name] :as a}]
                         (assoc a
                                :repo-bound? (not (contains? non-repo name))
                                :write? (contains? write-methods name)
                                :renderer-call-sites (get renderer-calls name 0)
                                :cli-call-sites (get cli-calls name 0)
                                :notes (get notes name))))
                  (sort-by (comp str :name)))
        drift (concat
               (for [m (sort (remove defined non-repo))]
                 {:kind :allowlisted-but-undefined :method m
                  :detail "listed in non-repo-methods but no def-thread-api defines it; invoking it fails"})
               (for [m (sort (remove defined (keys notes)))]
                 {:kind :notes-for-undefined-method :method m
                  :detail "thread-api-notes.edn documents a method that no longer exists"})
               (for [m (sort (remove defined (keys renderer-calls)))
                     :when (not (contains? non-repo m))]
                 {:kind :called-but-undefined :method m
                  :detail "renderer references a thread-api keyword with no definition"})
               (for [m (sort (remove defined (keys cli-calls)))
                     :when (not (contains? non-repo m))]
                 {:kind :cli-calls-undefined :method m
                  :detail "CLI references a thread-api method with no definition"}))
        dupes (->> apis (map :name) frequencies (filter #(> (val %) 1)) keys sort)]
    {:apis apis
     :count (count apis)
     :undocumented (count (remove :notes apis))
     :non-repo-methods (sort non-repo)
     :duplicates dupes
     :drift (vec drift)
     :events (extract-events root)
     :outliner-ops (extract-outliner-ops root)}))

;; ---------------------------------------------------------------------------
;; render
;; ---------------------------------------------------------------------------

(defn- code [s] (str "`" s "`"))
(defn- yn [b] (if b "●" ""))
(defn- esc [s] (str/replace (str s) "|" "\\|"))

(defn render-markdown [{:keys [apis count undocumented drift events outliner-ops duplicates]} {:keys [revision]}]
  (let [by-file (group-by :file apis)
        lines (atom [])
        out! (fn [& xs] (swap! lines conj (apply str xs)))]
    (out! "<!-- GENERATED by dz/support-engineering/script/thread-api-docs/gen_thread_api_docs.clj -->")
    (out! "<!-- Source revision: " revision ". Do not edit by hand; regenerate. -->")
    (out! "---")
    (out! "title: 'db-worker thread-api Reference (generated)'")
    (out! "description: 'Every db-worker RPC method, extracted from def-thread-api declarations, with repo binding, call-site counts, SSE events, and outliner ops'")
    (out! "type: reference")
    (out! "status: generated")
    (out! "sourceRevision: '" revision "'")
    (out! "relatedDocs:")
    (out! "  reference:")
    (out! "    - '[db-worker HTTP API](db-worker-http-api.md)'")
    (out! "---")
    (out! "")
    (out! "# db-worker thread-api Reference")
    (out! "")
    (out! "> Generated from source at revision " (code revision) ". Every method below is reachable over"
          " [`POST /v1/invoke`](db-worker-http-api.md#post-v1invoke) as `thread-api/<name>`.")
    (out! "")
    (out! "| | |")
    (out! "|---|---|")
    (out! "| Methods declared | " count " (" (clojure.core/count (filter #(= :worker (:side %)) apis)) " worker-side, "
          (clojure.core/count (filter #(= :main (:side %)) apis)) " main-thread) |")
    (out! "| With hand-written notes | " (- count undocumented) " |")
    (out! "| Exempt from repo binding | " (clojure.core/count (filter (complement :repo-bound?) apis)) " |")
    (out! "| Never called by renderer or CLI | " (clojure.core/count (filter #(and (zero? (:renderer-call-sites %)) (zero? (:cli-call-sites %))) apis)) " |")
    (out! "| Drift findings | " (clojure.core/count drift) " |")
    (out! "")
    (when (seq drift)
      (out! "## Drift findings")
      (out! "")
      (out! "These are inconsistencies between code and code, or code and notes. They are regenerated on every run and should be fixed at the source.")
      (out! "")
      (out! "| Kind | Method | Detail |")
      (out! "|---|---|---|")
      (doseq [{:keys [kind method detail]} drift]
        (out! "| " (code (name kind)) " | " (code method) " | " (esc detail) " |"))
      (out! ""))
    (when (seq duplicates)
      (out! "> **Duplicate definitions:** " (str/join ", " (map code duplicates)) " — the last loaded wins.")
      (out! ""))
    (out! "## How to read the table")
    (out! "")
    (out! "- **Params** — the declaration's parameter vector. The first parameter is `repo` for every repo-bound method.")
    (out! "- **Repo** — `●` means the first argument must match the worker's bound graph (`409 repo-mismatch` otherwise) and lock ownership is asserted.")
    (out! "- **Renderer / CLI** — occurrences of the method name in `src/main/frontend` (excluding the worker) and in the CLI (`cli/lib/*.ml`, `src/main/logseq/cli`). Zero in both usually means dead or driven by another worker-side path.")
    (out! "- **Notes** — hand-written, from `thread-api-notes.edn`. Absent means nobody has verified it yet; treat as internal.")
    (out! "")
    (out! "Methods under **main-thread** files are the reverse direction — defined in the renderer and invoked *by* the worker (`<invoke-main-thread`). They are not reachable over HTTP and are listed for completeness.")
    (out! "")
    (out! "## Worker methods (reachable over `/v1/invoke`)")
    (out! "")
    (letfn [(table! [entries]
              (out! "| Method | Params | Repo | Renderer | CLI | Notes |")
              (out! "|---|---|:-:|--:|--:|---|")
              (doseq [{:keys [name params repo-bound? renderer-call-sites cli-call-sites notes]} (sort-by :line entries)]
                (out! "| " (code (subs (str name) 1)) " | " (code (esc params)) " | " (yn repo-bound?) " | "
                      renderer-call-sites " | " cli-call-sites " | "
                      (esc (or (:doc notes) ""))
                      (when-let [s (:stability notes)] (str " _(" (clojure.core/name s) ")_"))
                      " |"))
              (out! ""))]
      (doseq [[file entries] (sort-by key by-file) :when (= :worker (:side (first entries)))]
        (out! "### " (code file))
        (out! "")
        (table! entries))
      (out! "## Main-thread methods (invoked by the worker)")
      (out! "")
      (doseq [[file entries] (sort-by key by-file) :when (= :main (:side (first entries)))]
        (out! "### " (code file))
        (out! "")
        (table! entries)))
    (out! "## SSE events (`GET /v1/events`)")
    (out! "")
    (out! "Worker broadcasts arrive as `data: {\"type\": ..., \"payload\": <transit>}`; the decoded payload is usually a `[type payload]` pair. **Handled** means `frontend/handler/worker.cljs` has a `defmethod` for it — the renderer-facing contract. **Emitted from** lists worker files that send it.")
    (out! "")
    (out! "| Event | Handled by renderer | Emitted from |")
    (out! "|---|:-:|---|")
    (doseq [{:keys [type handled-by-renderer? emitted-from]} events]
      (out! "| " (code type) " | " (yn handled-by-renderer?) " | " (str/join "<br>" (map code emitted-from)) " |"))
    (out! "")
    (out! "## Outliner ops (`thread-api/apply-outliner-ops`)")
    (out! "")
    (out! "Each op is a vector `[op-keyword & args]` validated against the malli `op-schema` in `deps/outliner/src/logseq/outliner/op.cljs`. **Semantic** ops are the ones undo/redo and sync treat as intent-preserving.")
    (out! "")
    (out! "| Op | Args | Semantic |")
    (out! "|---|---|:-:|")
    (doseq [{:keys [op args semantic?]} outliner-ops]
      (out! "| " (code op) " | " (code (esc (str/join " " args))) " | " (yn semantic?) " |"))
    (out! "")
    (str/join "\n" @lines)))

;; ---------------------------------------------------------------------------
;; main
;; ---------------------------------------------------------------------------

(defn- parse-args [argv]
  (loop [a (vec argv) o {}]
    (if-let [x (first a)]
      (case x
        "--root" (recur (subvec a 2) (assoc o :root (second a)))
        "--out" (recur (subvec a 2) (assoc o :out (second a)))
        "--notes" (recur (subvec a 2) (assoc o :notes (second a)))
        "--revision" (recur (subvec a 2) (assoc o :revision (second a)))
        "--check" (recur (subvec a 1) (assoc o :check? true))
        "--edn" (recur (subvec a 1) (assoc o :edn? true))
        (recur (subvec a 1) o))
      o)))

(defn- git-revision [root]
  (try
    (str/trim (:out (babashka.process/sh {:dir (str root)} "git" "rev-parse" "--short" "HEAD")))
    (catch Exception _ nil)))

(let [{:keys [root out notes revision check? edn?]} (parse-args *command-line-args*)
      root (fs/absolutize (or root "."))
      notes-file (or notes (str (fs/file (fs/parent *file*) "thread-api-notes.edn")))
      notes (if (fs/exists? notes-file) (read-string (slurp notes-file)) {})
      revision (or revision
                   (let [f (fs/file root "REVISION")] (when (fs/exists? f) (str/trim (slurp f))))
                   (git-revision root)
                   "unknown")
      model (build-model root notes)]
  (cond
    edn?
    (pp/pprint (dissoc model :apis-by-file))

    :else
    (let [md (render-markdown model {:revision revision})]
      (binding [*out* *err*]
        (println (format "thread-api: %d methods, %d undocumented, %d drift finding(s), %d events, %d outliner ops"
                         (:count model) (:undocumented model) (count (:drift model))
                         (count (:events model)) (count (:outliner-ops model))))
        (doseq [{:keys [kind method detail]} (:drift model)]
          (println (format "  drift %-32s %-45s %s" (name kind) method detail))))
      (cond
        (and check? out)
        ;; Compare content, not stamps: re-render using whatever revision the
        ;; committed file claims, so a rebuild at a new sha with no API change
        ;; passes and only a real change in extracted facts fails.
        (let [current (when (fs/exists? out) (slurp out))
              claimed (some->> current (re-find #"sourceRevision: '([^']+)'") second)
              expected (if claimed (render-markdown model {:revision claimed}) md)]
          (if (= current expected)
            (println "thread-api docs are current")
            (do (println (str "thread-api docs are STALE. Regenerate with:\n  bb " *file* " --root " root " --out " out))
                (System/exit 1))))

        out
        (do (fs/create-dirs (fs/parent out))
            (spit out md)
            (println "wrote" out))

        :else
        (println md)))))
