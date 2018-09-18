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
  (let [filename (str file-dir file)]
   (try
      (with-open [reader (io/reader filename)]
      (count (line-seq reader)))
      (catch Exception e (timbre/error (.getMessage e))))))

(defn is-file-empty?
  "returns a boolean check to see if file is empty"
  [file]
  (> (file-line-number file) 0))

(defn rest-sample-size
    "requires a file, gets the number of lines, returns difference between the total sample size and the file"
    [curated-queries]
    (let [curated-queries-line-number (file-line-number curated-queries)]
         (- total-sample-size curated-queries-line-number)))

(defn read-specific-line
    "reads a specified line"
    [file line-number]
    (let [filename (str file-dir file)]
    (try
      (with-open [rdr (io/reader filename)]
      (nth (line-seq rdr) line-number))
      (catch Exception e (timbre/error (.getMessage e))))))

(defn sample-lines
    "gets random line numbers to read"
    [curated-queries large-dataset]
    (let [lines (file-line-number large-dataset)
          limit (rest-sample-size curated-queries)]
      (reservoir/sample (range lines) limit)))

(defn read-curated-queries
    "reads a file that contains curated queries"
    [curated-queries]
    (let [filename (str file-dir curated-queries)]
     (try
       (with-open [reader (io/reader filename)]
       (doall (line-seq reader)))
       (catch Exception e (timbre/error (.getMessage e))))))

(defn all-queries
    "concatenates both types of queries"
    [curated-queries dataset]
    (let [cq (read-curated-queries curated-queries)
         samples (sample-lines curated-queries dataset)
         uq (for [s samples] (read-specific-line dataset s))]
         (concat cq uq)))

(defn get-query
  "returns a url for the type of rest-api being queried"
  [query type]
  (cond
    (= type :production) (str production-api query)
    (= type :staging)(str staging-api query)))

(defn query-api
  [query]
  (try
    @(http/get query)
    (catch Exception e (timbre/error (.getMessage e)))))

(defn process-error-response
  "processes an error response after querying the stage and prod instances of the rest-api"
  [stage prod]
  (when-not (nil? stage)
    (timbre/error stage))
  (when-not (nil? prod)
      (timbre/error prod)))

(defn write-response
  "outputs a tsv file by parsing through the received messages"
  [log]
  (let [write-messages (map #(dissoc (first %) :type) log)
        header-keys (keys (first write-messages))
        write-header (into [] (map clojure.string/capitalize (map name header-keys)))
        write-me (for [x write-messages] (into [] (map #(% x) header-keys)))
        all (cons write-header write-me)]
    (try
      (with-open [wrtr (io/writer output-file-name)]
          (doseq [i all]
            (.write wrtr (str (clojure.string/join "\t" i) "\n"))))
            (timbre/info (str "Output here: " output-file-name))
            (catch Exception e (timbre/error (.getMessage e))))))


(defn run-query
  "runs one query"
  [query]
  (let [staging-query (get-query query :staging)
       production-query (get-query query :production)
       staging-response (query-api staging-query)
       production-response (query-api production-query)]
       (if (or (contains? staging-response :error) (contains? production-response :error))
         (process-error-response (:error staging-response) (:error production-response))
         (rsp/compare-response query staging-response production-response))))



(defn run-me
    "iterates through the whole query set"
    [curated-queries dataset number]
    (let [query-samples (if (nil? number) (all-queries curated-queries dataset) (take number (all-queries curated-queries dataset)))]
     (if (empty? query-samples)
       (timbre/error (str "Empty query set: script can not run"))
       (write-response (map run-query query-samples)))))


(defn -main
    "Benchmarks queries against rest-api instances"
    [curated-queries dataset number]
    (let [num (if-not (nil? number) (Integer. number))]
      (when (> num total-sample-size)
        (timbre/warn (str "User specified number: " num " is bigger than sample size: " total-sample-size)))
      (if (and (> num 0) (<= num total-sample-size))
        (run-me curated-queries dataset num)
        (run-me curated-queries dataset nil))))
