(ns test_rest_api.response
  (:require [clojure.data.csv :as csv]
            [org.httpkit.client :as http]
            [clojure.java.io :as io]
            [cheshire.core :as json]))
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

(defn cmp-results
  [stage prod]
  )

(defn cmp-top-2-results
  [stage prod]
  (let [stage-body (convert-to-json stage)
        prod-body  (convert-to-json prod)
        stage-results-num (:total-results (:message stage-body))
        prod-results-num (:total-results (:message prod-body))
        stage-top-2-results (take 2 (:items (:message stage-body)))
        prod-top-2-results (take 2 (:items (:message prod-body)))
        result-num-match (compare-result-num stage-results-num prod-results-num)]
        (if result-num-match
          (assoc {} :prod prod-top-2-results :stage stage-top-2-results)
          (message (str "Total result number mismatch:," stage-results-num ","  prod-results-num)))))

(defn chk-result-totals
  [results log]
  (let [stage-body (convert-to-json (:stage results))
        prod-body  (convert-to-json (:prod results))
        stage-results-num (:total-results (:message stage-body))
        prod-results-num (:total-results (:message prod-body))
        result-num-match (compare-result-num stage-results-num prod-results-num)
        err-msg (str "Total number of results mismatch")]
        (when-not result-num-match
            (log {:msg err-msg :stage stage-results-num :production prod-results-num :query (:query results)}))))

(defn status-code-check
  [results log]
  (let [stage-status (:status (:stage results))
        prod-status  (:status (:prod results))
        equal-status? (status-code stage-status prod-status)
        err-msg (str "non 200 status code errors")]
        (when-not (and equal-status? (ok? stage-status))
             (log {:msg err-msg :stage stage-status :production prod-status :query (:query results)}))))

(defn compare-response
  [query staging-rsp prod-rsp]
  (let [result-log (atom [])
        log-f (fn [item]
                (swap! result-log
                       #(cons item %)))]
        (prn t)
        (status-code-check {:stage staging-rsp :prod prod-rsp :query query} log-f)
        (when (= (count @result-log) 0)
           (chk-result-totals {:stage (:body staging-rsp) :prod (:body prod-rsp) :query query} log-f))
        @result-log))



  ;;write out to a message holder))

  ;; if 200, check body
  ;; heartbeat response is different -- look into that
  ;; body - first two results in prod
  ;; check if they occur in the results of staging
  ;; if they do which position
  ;; if same position, match the highest score out of a 5
  ;; if within one or 2 positions, match it

  ;;(json/parse-string (:body staging-rsp) true))
