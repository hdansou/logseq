#!/usr/bin/env bb
;; Verify every number in ddocs/reference/features/sync-limits.md against source.
;;
;; sync-limits.edn declares each limit as either a (def symbol value) to locate
;; and evaluate, or a regex whose first capture group is compared as a string.
;; Any mismatch, or any symbol/regex that no longer matches at all, fails the run
;; with the file, the expected value, and what source now says.
;;
;;   check_sync_limits.clj --root <logseq-checkout>
;;   check_sync_limits.clj --root <logseq-checkout> --declaration sync-limits.edn
;;
;; Exit 0 all current · 1 drift · 2 could not run.

(require '[babashka.fs :as fs]
         '[clojure.string :as str]
         '[edamame.core :as e])

(defn- parse-args [argv]
  (loop [a (vec argv) o {}]
    (if-let [x (first a)]
      (case x
        "--root" (recur (subvec a 2) (assoc o :root (second a)))
        "--declaration" (recur (subvec a 2) (assoc o :declaration (second a)))
        (recur (subvec a 1) o))
      o)))

(def ^:private parse-opts
  {:all true :read-cond :allow :features #{:cljs}
   :auto-resolve (fn [a] (if (= :current a) 'user (symbol (name a))))
   :readers (fn [_] identity)})

(defn- def-form? [f sym]
  (and (seq? f)
       (contains? #{'def 'defonce} (first f))
       (symbol? (second f))
       (= (name sym) (name (second f)))))

(defn- eval-literal
  "Evaluate the value of a def: literals as-is, (* …)/(+ …)/(- …) arithmetic folded.
   Anything else is returned unevaluated so the mismatch message shows it."
  [v]
  (cond
    (and (seq? v) (contains? #{'* '+ '-} (first v)) (every? number? (rest v)))
    (apply ({'* * '+ + '- -} (first v)) (rest v))
    :else v))

(defn- find-def-value [text sym]
  (some (fn [f] (when (def-form? f sym) (eval-literal (last f))))
        (e/parse-string-all text parse-opts)))

(defn- check-entry [root {:keys [symbol regex file expected doc]}]
  (let [path (fs/file root file)
        base {:file file :doc doc :expected expected :what (or symbol regex)}]
    (if-not (fs/exists? path)
      (assoc base :status :missing-file :actual nil)
      (let [text (slurp (str path))
            actual (cond
                     symbol (find-def-value text symbol)
                     regex (some-> (re-find (re-pattern regex) text) second))]
        (cond
          (nil? actual) (assoc base :status :not-found :actual nil)
          (= actual expected) (assoc base :status :ok :actual actual)
          :else (assoc base :status :drift :actual actual))))))

(let [{:keys [root declaration]} (parse-args *command-line-args*)
      root (fs/absolutize (or root "."))
      decl-file (or declaration (str (fs/file (fs/parent *file*) "sync-limits.edn")))
      _ (when-not (fs/exists? decl-file)
          (println "declaration not found:" decl-file) (System/exit 2))
      entries (read-string (slurp decl-file))
      results (mapv #(check-entry root %) entries)
      bad (remove #(= :ok (:status %)) results)]
  (doseq [{:keys [status what file expected actual doc]} results]
    (println (format "%-14s %-42s %s" (name status) what doc))
    (when (not= :ok status)
      (println (format "               %s" file))
      (println (format "               expected %s  actual %s" (pr-str expected) (pr-str actual)))))
  (println)
  (println (format "%d checked, %d current, %d need attention"
                   (count results) (- (count results) (count bad)) (count bad)))
  (when (seq bad)
    (println (str "Update the reference page AND " (fs/file-name decl-file) " to match source, or fix the source.")))
  (System/exit (if (seq bad) 1 0)))
