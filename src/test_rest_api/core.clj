(ns test_rest_api.core
  (:require [clojure.data.csv :as csv]
            [org.httpkit.client :as http]
            [bigml.sampling.reservoir :as reservoir]
            [clojure.java.io :as io]
            [test_rest_api.response :as rsp]
            [cheshire.core :as json]
            [taoensso.timbre :as timbre]))

(def staging-api "http://api-es-staging.crossref.org")

(def production-api "http://api.crossref.org")

(def output-file-name "results.tsv")

;; based on a population size of 200,000 with a 3% margin of error
;; verified on some stats websites
(def total-sample-size 1062)

(def file-dir "files/")

(defn file-line-number
  "gets number of lines for a specified file"
  [file]
  (with-open [reader (io/reader (str file-dir file))]
  (count (line-seq reader))))

(defn rest-sample-size
    "requires a file, gets the number of lines, returns difference between the total sample size and the file"
    [curated-queries]
    (let [curated-queries-line-number (file-line-number curated-queries)]
         (- total-sample-size curated-queries-line-number)))

(defn read-specific-line
    "reads a specified line"
    [file line-number]
    (with-open [rdr (io/reader (str file-dir file))]
    (nth (line-seq rdr) line-number)))

(defn sample-lines
    "gets random line numbers to read"
    [curated-queries large-dataset]
    (let [lines (file-line-number large-dataset)
          limit (rest-sample-size curated-queries)]
    (reservoir/sample (range lines) limit)))

(defn read-curated-queries
    [curated-queries]
    (with-open [rdr (io/reader (str file-dir curated-queries))]
    (doall (line-seq rdr))))



(defn all-queries
    "concatenates both types of queries"
    [curated-queries dataset]
    (let [cq (read-curated-queries curated-queries)
         samples (sample-lines curated-queries dataset)
         uq (for [s samples] (read-specific-line dataset s))]
         (concat cq uq)))

(defn get-query
  [query type]
  (cond
    (= type :production) (str production-api query)
    (= type :staging)(str staging-api query)))

(defn query-api
  [query]
  @(http/get query))

(defn process-error-response
  [stage prod]
  (when-not (nil? stage)
    (timbre/error stage))
  (when-not (nil? prod)
      (timbre/error prod)))

(defn write-response
  [log]
  (let [r-keys (keys (first (first log)))
          filter-keys (remove #(= % :type) r-keys)
          write-header (into [] (map clojure.string/capitalize (map name filter-keys)))
          write-me (for [x log] (into [] (map #(% (first x)) filter-keys)))
          all (cons write-header write-me)]
     (with-open [wrtr (io/writer output-file-name)]
        (doseq [i all]
        (.write wrtr (str (clojure.string/join "\t" i) "\n"))))
     (str "Output here: " output-file-name)))

(defn run-query
  [query]
  (let [staging-query (get-query query :staging)
       production-query (get-query query :production)
       staging-response (query-api staging-query)
       production-response (query-api production-query)]
       (if (or (contains? staging-response :error) (contains? production-response :error))
         (process-error-response (:error staging-response) (:error production-response))
         (rsp/compare-response query staging-response production-response))))



(defn run-me
    [curated-queries dataset]
    (let [query-samples (all-queries curated-queries dataset)
          subset (take 100 query-samples)]
     (write-response (map run-query subset))))


(defn -main
    "Benchmarks queries against rest-api instances"
    [curated-queries dataset]
    (if-let [query-samples (<= (rest-sample-size curated-queries) 0)]  (read-curated-queries curated-queries)
    (all-queries curated-queries dataset)))
