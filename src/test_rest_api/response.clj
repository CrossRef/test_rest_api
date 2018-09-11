(ns test_rest_api.response
  (:require [clojure.data.csv :as csv]
            [org.httpkit.client :as http]
            [clojure.java.io :as io]
            [cheshire.core :as json]
            [clojure.set]
            [taoensso.timbre :as timbre]))

; global vars
; can't use delay because it caches the value across threads
; and that's not the behavior I want
; I want it to change per call
; but be accessible across the methods to prevent having to pass all of these values to all methods
(def prod-rsp (atom ""))

(def staging-rsp (atom ""))

(def query (atom ""))

(def result-id (atom ""))

(defn set-query [q]
  (reset! query q))

(defn set-prod-rsp [rsp] (reset! prod-rsp rsp))

(defn set-staging-rsp [rsp] (reset! staging-rsp rsp))


(def available-resources
  "hashmap of resources and their respective unique identifiers"
  {:works :DOI :funders :id :members :id :licenses :URL :journals [:title :publisher] :types :id})

(def singular-query
  "for prefixes"
  {:prefixes :member})


(def ar-vector
  "vector of keys for available resources"
  (into [] (map name (keys available-resources))))

(def version "v1")

(defn split-query
  "splits up the route fragment and removes the query string"
  [query]
  (let [split-q (filter #(not (= % version)) (remove clojure.string/blank? (clojure.string/split query #"[]\/|?]")))
      last-fragment (last split-q)]
    (if (re-find #"\?" query)
       (remove #(= % last-fragment) split-q)
       split-q)))

(defn plural-query?
  "checks to see if it's a list query"
  [query]
  (let [q (into [] (split-query query))
        last-route (last q)]
   (if (some #(= % last-route) ar-vector) true false)))

(defn get-unique-id-type
  "get the unique id of resource"
  [query]
  (let [q (into [] (split-query query))
        first-route (first q)
        last-route (last q)]
   (cond
      (some #(= % last-route) ar-vector) ((keyword last-route) available-resources)
      (some #(= % first-route) ar-vector) ((keyword first-route) available-resources)
      (some #(= % (keyword first-route)) (keys singular-query)) ((keyword first-route) singular-query)
      :else false)))

(defn get-results-id
  "method to set the global var result id"
  [query]
  (reset! result-id (get-unique-id-type query)))

(def status-ok 200)

(defn convert-to-json
  "converts to json"
  [rsp]
  (json/parse-string rsp true))

(defn status-code
  "checks if status code between two responses is equal"
  [stage prod]
  (= stage prod))

(defn ok?
  [status]
  (= status-ok status))

(defn compare-result-num
  "are the total results equal across the two responses"
  [stage-results prod-results]
  (= stage-results prod-results))

(defn process-compound-key
  "this is for a resource requiring a compound key"
  [rsp-body id]
     (for [r rsp-body] (apply str (map #(% r) id))))

(defn api-results->plural-id
  "returns the values of unique id by resource"
  [rsp-body]
  (let [body (->> rsp-body convert-to-json :message :items)
        id @result-id]
    (if (= (type id) clojure.lang.Keyword)
      (map id body)
      (process-compound-key body id))))


(defn api-results->doi-seq
  "returns dois"
 [rsp-body]
 (->> rsp-body convert-to-json :message :items (map :DOI)))

(defn api-results
  "returns the json body depending on if it's a singleton or list query"
 [rsp-body type]
 (condp = type
   :singleton (->> rsp-body convert-to-json :message)
   :plural (->> rsp-body convert-to-json :message :items)))


(defn get-position
 "return mapping of prod to staging index of top n results"
  [n]
  (let [prod-ids (take n (api-results->plural-id (:body @prod-rsp)))
        stage-ids (api-results->plural-id (:body @staging-rsp))
        stage-prod-position (map #(.indexOf stage-ids %) prod-ids)]
  (into {} (map-indexed vector stage-prod-position))))


(defn top-n-intersection
  "returns the intersection of the two sets(stage and prod), requires a number to process the top n of production results"
 [n]
 (let [prod-ids (set (take n (api-results->plural-id (:body @prod-rsp))))
       stage-ids (set (api-results->plural-id (:body @staging-rsp)))]
   (clojure.set/intersection prod-ids stage-ids)))

(defn process-position
  "checks to see if returned prod and stage positions are equal"
  [n]
  (for [x (get-position n)]
      (let [prod-position (first x)
            stage-position (second x)]
      (cond
         (= prod-position stage-position)
             :exact-match
         (= stage-position -1)
            :no-match
         :else
            :first-page))))

(defn compare-results
  "returns messages of intersection and position tests"
  [n log]
  (let [intersect (top-n-intersection n)
        result-check (if (not (empty? intersect)) (process-position n) (log {:type "no-matching-results-found" :msg "Top 2 production results not found in the first page of stage results" :stage "" :production "" :query @query}))]
        (when (some #(= :exact-match %) result-check)
           (log {:type "result-position-match" :msg "exact match" :stage "" :production "" :query @query}))
        (when (some #(= :no-match %) result-check)
          (log {:type "result-no-match" :msg "no match" :stage "" :production "" :query @query}))
        (when (some #(= :first-page %) result-check)
            (log {:type "result-first-page" :msg "one or both prod results are on the first page of stage results" :stage "" :production "" :query @query}))
        (when-not (= (type result-check) clojure.lang.LazySeq) result-check)

))

(defn check-inequality
  "returns hash-map of responses where there are unequal values between stage and prod"
  [prod stage log]
  (let [equal-response (into {} (for [x prod] (filter #(= x %) stage)))
        equal-key-set (set (keys equal-response))
        prod-keys (keys prod)
        prod-key-set (set (keys prod))
        differences (into [] (clojure.set/difference prod-key-set equal-key-set))
        stage-val (into [] (map #(% stage) differences))
        prod-val (into [] (map #(% prod) differences))]
    (log {:type "result-no-match" :msg "non equal values between the same keys in stage and production" :stage (zipmap differences stage-val) :production (zipmap differences prod-val) :query @query})))


(defn chk-singleton-response
  "processes singleton query response"
  [log]
  (let [prod-result (api-results (:body @prod-rsp) :singleton)
       stage-result (api-results (:body @staging-rsp) :singleton)]
       (if-not (= prod-result stage-result)
          (check-inequality prod-result stage-result log))))


(defn chk-json-keys
  "checks first level of json keys to see if they are equal between prod and stage"
  [log]
  (let [stage-body (convert-to-json (:body @staging-rsp))
        prod-body  (convert-to-json (:body @prod-rsp))
        stage-keys (into [] (keys (first (take 1 (:items (:message (first stage-body)))))))
        prod-keys (into [] (keys (first (take 1 (:items (:message (first prod-body)))))))]
        (when-not (= stage-keys prod-keys)
          (log {:type "json-keys-mismatch" :msg "json keys don't match" :stage stage-keys :production prod-keys :query @query}))))


(defn chk-result-totals
  "returns messages for result totals"
  [log]
  (let [stage-body (convert-to-json (:body @staging-rsp))
        prod-body  (convert-to-json (:body @prod-rsp))
        stage-results-num (:total-results (:message stage-body))
        prod-results-num (:total-results (:message prod-body))
        result-num-match (compare-result-num stage-results-num prod-results-num)
        err-msg "Total number of results mismatch, 0 results found"
        warn-msg "Total number of results mismatch"]
        (when-not result-num-match
          (if (or (= stage-results-num 0) (= prod-results-num 0))
            (log {:type "result-total-error" :msg err-msg :stage stage-results-num :production prod-results-num :query @query})
            (log {:type "result-total-warn" :msg warn-msg :stage stage-results-num :production prod-results-num :query @query})))))

(defn status-code-check
  "returns message on status code checks"
  [log]
  (let [stage-status (:status @staging-rsp)
        prod-status  (:status @prod-rsp)
        equal-status? (status-code stage-status prod-status)
        err-msg (str "non 200 status code errors")]
        (when-not (and equal-status? (ok? stage-status))
             (log {:type "status-error" :msg err-msg :stage stage-status :production prod-status :query @query}))))


(defn compare-response
  "wrapper for running all tests"
  [query staging-rsp prod-rsp]
  (let [result-log (atom [])
        log-f (fn [item]
                (swap! result-log
                       #(cons item %)))]
       (timbre/info (str "Processing query: " query))
       (set-query query)
       (set-prod-rsp prod-rsp)
       (set-staging-rsp staging-rsp)
       (status-code-check log-f)
       (when (= (count (filter #(and (= (:type %) "status-error") (= (:query %) query)) @result-log)) 0)
           (get-results-id query)
           (when (plural-query? query)
              (chk-result-totals log-f)
              (when (= (count (filter #(and (= (:type %) "result-total-error") (= (:query %) query)) @result-log)) 0)
                (chk-json-keys log-f)
                (compare-results 2 log-f)))
            (when-not (plural-query? query)
               (chk-singleton-response log-f)))
        (when (empty? (filter #(= (:query %) query) @result-log))
           (log-f {:type "exact-match" :msg "exact match" :stage "" :production "" :query query}))
        @result-log))


  ;; heartbeat response is different -- look into that
