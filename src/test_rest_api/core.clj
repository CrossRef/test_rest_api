(ns test_rest_api.core
  (:require [clojure.data.csv :as csv]
            [org.httpkit.client :as http]
            [bigml.sampling.reservoir :as reservoir]
            [clojure.java.io :as io]))

(def staging-api "http://api-es-staging.crossref.org")

(def production-api "http://api.crossref.org")

;; based on a population size of 200,000 with a 3% margin of error
;; verified on some stats websites
(def total-sample-size 1062)

(def file-dir "files/")
(defn me-test
  [file]
  (with-open [reader (io/reader file)]
  (count (line-seq reader))))

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
    [curated-queries dataset]
    (let [cq (read-curated-queries curated-queries)
         samples (sample-lines curated-queries dataset)
         uq (for [s samples] (read-specific-line dataset s))]
         (concat cq uq)))

(defn run-staging
    [queries]
    (map #(str staging-api %) queries))

(defn run-production
    [queries]
    (map #(str production-api %) queries))

(defn run-me
    [curated-queries dataset]
    (let [query-samples (all-queries curated-queries dataset)
          staging-samples (run-staging query-samples)
          production-samples (run-production query-samples)
          {:keys [status headers body error] :as rsp} @(http/get (nth production-samples 50))]
          (prn (nth production-samples 50))
          status))
  ;; next steps: query the api, look for criteria like status, response time, top 2 results in prod, compare them to staging results


(defn -main
    "Benchmarks queries against rest-api instances"
    [curated-queries dataset]
    ;(prn @(http/get "http://example.com"))
    (if-let [query-samples (<= (rest-sample-size curated-queries) 0)]  (read-curated-queries curated-queries)
    (all-queries curated-queries dataset)))
