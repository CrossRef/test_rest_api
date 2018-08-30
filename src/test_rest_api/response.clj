(ns test_rest_api.response
  (:require [clojure.data.csv :as csv]
            [org.httpkit.client :as http]
            [clojure.java.io :as io]
            [cheshire.core :as json]
            [clojure.set]))
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


(def score
  [{:type "first page-no existence" :score 0}
   {:type "both results different order - exists first page" :score 1}
   {:type "exact match - 1 result, not exact match - another result" :score 3}
   {:type "exact match - 1 result, not exact match - another result" :score 2}])


;(defn compare-results
  ;[n log {prod :prod stage :stage}]))

(defn api-results->doi-seq
  [rsp-body]
  (->> rsp-body convert-to-json :message :items (map :DOI)))

(defn api-results
  [rsp-body]
  (->> rsp-body convert-to-json :message :items))


(defn get-position
  "return mapping of prod to staging index of top n results"
  [n {prod :prod stage :stage}]
  (let [prod-dois (->> prod api-results->doi-seq (take n))
        stage-dois (-> stage api-results->doi-seq)
        stage-prod-position (map #(.indexOf stage-dois %) prod-dois)]
   (into {} (map-indexed vector stage-prod-position))))


(defn top-n-intersection
  [n {prod :prod stage :stage}]
  (let [prod-dois (->> prod api-results->doi-seq (take n) set)
        stage-dois (-> stage api-results->doi-seq set)]
        (clojure.set/intersection prod-dois stage-dois)))

(defn chk-json-keys
  [results log]
  (let [stage-body (convert-to-json (:stage results))
        prod-body  (convert-to-json (:prod results))
        stage-keys (into [] (keys (first (take 1 (:items (:message (first stage-body)))))))
        prod-keys (into [] (keys (first (take 1 (:items (:message (first prod-body)))))))]
        (when-not (= stage-keys prod-keys)
          (log {:type "json-keys-mismatch" :stage stage-keys :prod prod-keys :query (:query results)}))))


(defn chk-result-totals
  [results log]
  (let [stage-body (convert-to-json (:stage results))
        prod-body  (convert-to-json (:prod results))
        stage-results-num (:total-results (:message stage-body))
        prod-results-num (:total-results (:message prod-body))
        result-num-match (compare-result-num stage-results-num prod-results-num)
        err-msg "Total number of results mismatch, 0 results found"
        warn-msg "Total number of results mismatch"]
        (when-not result-num-match
          (if (or (= stage-results-num 0) (= prod-results-num 0))
            (log {:type "result-total-error" :msg err-msg :stage stage-results-num :production prod-results-num :query (:query results)})
            (log {:type "result-total-warn" :msg warn-msg :stage stage-results-num :production prod-results-num :query (:query results)})))))

(defn status-code-check
  [results log]
  (let [stage-status (:status (:stage results))
        prod-status  (:status (:prod results))
        equal-status? (status-code stage-status prod-status)
        err-msg (str "non 200 status code errors")]
        (when-not (and equal-status? (ok? stage-status))
             (log {:type "status-error" :msg err-msg :stage stage-status :production prod-status :query (:query results)}))))

(def available-resources
  {:works :DOI :funders :id :members :id :licenses :URL :journals [:title :publisher]})

(def ar-vector
  (into [] (map name (keys available-resources))))


(def version "v1")

(defn split-query
  [query]
  (filter #(not (= % version)) (remove clojure.string/blank? (clojure.string/split query #"\/"))))

(defn plural-query?
  [query]
  (let [q (into [] (split-query query))
        last-route (.indexOf ar-vector (last q))]
   (if (> last-route -1)
      true
      false)))

(defn get-unique-id-type
  [query]
  (let [q (into [] (split-query query))
        first-route (.indexOf ar-vector (first q))]
   (if (> first-route -1)
      ((keyword (first q)) available-resources)
      false)))

(defn compare-response2
  [query staging-rsp prod-rsp]
  (plural-query? query))

(defn compare-response
  [query staging-rsp prod-rsp]
  (let [result-log (atom [])
        log-f (fn [item]
                (swap! result-log
                       #(cons item %)))]
        (status-code-check {:stage staging-rsp :prod prod-rsp :query query} log-f)
        (when (= (count (filter #(and (= (:type %) "status-error") (= (:query %) query)) @result-log)) 0)
           (if (plural-query? query)
              (chk-result-totals {:stage (:body staging-rsp) :prod (:body prod-rsp) :query query} log-f)
              (when (= (count (filter #(and (= (:type %) "result-total-error") (= (:query %) query)) @result-log)) 0)
                (chk-json-keys {:stage (:body staging-rsp) :prod (:body prod-rsp) :query query} log-f)
                (top-n-intersection 2 {:stage (:body staging-rsp) :prod (:body prod-rsp)})
                (get-position 2 {:stage (:body staging-rsp) :prod (:body prod-rsp)}))))))
        ;@result-log))


  ;; heartbeat response is different -- look into that
  ;; body - first two results in prod
  ;; check if they occur in the results of staging
  ;; if they do which position
  ;; if same position, match the highest score out of a 5
  ;; if within one or 2 positions, match it

  ;;(json/parse-string (:body staging-rsp) true))

  ;; why does subset behave so weirdly
  ;; if I want to find position of something, is there a less painful way to do it
