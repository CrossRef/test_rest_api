(ns test_rest_api.response
  (:require [clojure.data.csv :as csv]
            [org.httpkit.client :as http]
            [clojure.java.io :as io]
            [cheshire.core :as json]
            [clojure.set]))

(def prod-rsp (atom ""))

(def staging-rsp (atom ""))

(def query (atom ""))

(def result-id (atom ""))

(defn set-query [q]
  (reset! query q))

(defn set-prod-rsp [rsp] (reset! prod-rsp rsp))

(defn set-staging-rsp [rsp] (reset! staging-rsp rsp))


(def available-resources
  {:works :DOI :funders :id :members :id :licenses :URL :journals [:title :publisher]})

(def singular-query
  {:prefixes :member})


(def ar-vector
  (into [] (map name (keys available-resources))))

(def version "v1")

(defn split-query
  [query]
  (let [split-q (filter #(not (= % version)) (remove clojure.string/blank? (clojure.string/split query #"[]\/|?]")))
      last-fragment (last split-q)]
    (if (re-find #"\?" query)
       (remove #(= % last-fragment) split-q)
       split-q)))

(defn plural-query?
  [query]
  (let [q (into [] (split-query query))
        last-route (last q)]
   (if (some #(= % last-route) ar-vector) true false)))

(defn get-unique-id-type
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
  [query]
  (reset! result-id (get-unique-id-type query)))

(def status-ok 200)

(defn message
  [msg]
  (clojure.string/split msg #","))

(defn convert-to-json
  [rsp]
  (json/parse-string rsp true))

(defn status-code
  [stage prod]
  (= stage prod))

(defn ok?
  [status]
  (= status-ok status))

(defn compare-result-num
  [stage-results prod-results]
  (= stage-results prod-results))

(defn process-compound-key
  [rsp-body id]
     (for [r rsp-body] (apply str (map #(% r) id))))

(defn api-results->plural-id
  [rsp-body]
  (let [body (->> rsp-body convert-to-json :message :items)
        id @result-id]
    (if (= (type id) clojure.lang.Keyword)
      (map id body)
      (process-compound-key body id))))


(defn api-results->doi-seq
 [rsp-body]
 (->> rsp-body convert-to-json :message :items (map :DOI)))

(defn api-results
 [rsp-body]
 (->> rsp-body convert-to-json :message :items))

(defn get-position
 "return mapping of prod to staging index of top n results"
  [n]
  (let [prod-ids (take n (api-results->plural-id (:body @prod-rsp)))
        stage-ids (api-results->plural-id (:body @staging-rsp))
        stage-prod-position (map #(.indexOf stage-ids %) prod-ids)]
  (into {} (map-indexed vector stage-prod-position))))


(defn top-n-intersection
 [n]
 (let [prod-ids (set (take n (api-results->plural-id (:body @prod-rsp))))
       stage-ids (set (api-results->plural-id (:body @staging-rsp)))]
   (clojure.set/intersection prod-ids stage-ids)))

(defn process-position
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
  [n log]
  (let [intersect (top-n-intersection n)
        result-check (if (not (empty? intersect)) (process-position n) (log {:type "no-matching-results-found" :msg "Top 2 production results not found in the first page of stage results" :stage "" :prod "" :query @query}))]
        (when (some #(= :exact-match %) result-check)
           (log {:type "result-position-match" :msg "exact match" :stage "get stage result" :prod "get prod result" :query @query}))
        (when (some #(= :no-match %) result-check)
          (log {:type "result-no-match" :msg "no match" :stage "get stage result" :prod "get prod result" :query @query}))
        (when (some #(= :first-page %) result-check)
            (log {:type "result-first-page" :msg "not an exact match" :stage "stage-blah" :prod "prod-blah" :query @query}))
        (when-not (= (type result-check) clojure.lang.LazySeq) result-check)

))

(defn chk-json-keys
  [log]
  (let [stage-body (convert-to-json (:body @staging-rsp))
        prod-body  (convert-to-json (:body @prod-rsp))
        stage-keys (into [] (keys (first (take 1 (:items (:message (first stage-body)))))))
        prod-keys (into [] (keys (first (take 1 (:items (:message (first prod-body)))))))]
        (when-not (= stage-keys prod-keys)
          (log {:type "json-keys-mismatch" :msg "json keys don't match" :stage stage-keys :prod prod-keys :query @query}))))


(defn chk-result-totals
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
  [log]
  (let [stage-status (:status @staging-rsp)
        prod-status  (:status @prod-rsp)
        equal-status? (status-code stage-status prod-status)
        err-msg (str "non 200 status code errors")]
        (when-not (and equal-status? (ok? stage-status))
             (log {:type "status-error" :msg err-msg :stage stage-status :production prod-status :query @query}))))


(defn compare-response2
  [query staging-rsp prod-rsp]
  (set-query query)
  (set-prod-rsp prod-rsp)
  (set-staging-rsp staging-rsp)
  (get-results-id query)
  @result-id)

(defn compare-response
  [query staging-rsp prod-rsp]
  (let [result-log (atom [])
        log-f (fn [item]
                (swap! result-log
                       #(cons item %)))]
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
                (compare-results 2 log-f))))
        @result-log))


  ;; heartbeat response is different -- look into that
  ;; body - first two results in prod
  ;; check if they occur in the results of staging
  ;; if they do which position
  ;; if same position, match the highest score out of a 5
  ;; if within one or 2 positions, match it

  ;;(json/parse-string (:body staging-rsp) true))

  ;; why does subset behave so weirdly
  ;; if I want to find position of something, is there a less painful way to do it
