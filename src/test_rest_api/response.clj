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

(defn cmp-instances
  [stage prod]
  (let [stage-body (convert-to-json stage)
        prod-body  (convert-to-json prod)
        stage-results-num (:total-results (:message stage-body))
        prod-results-num (:total-results (:message prod-body))
        stage-top-2-results (take 2 (:items (:message stage-body)))
        prod-top-2-results (take 2 (:items (:message prod-body)))
        result-num-match (compare-result-num stage-results-num prod-results-num)]

        (if (not result-num-match)
          (message (str "Total result number mismatch:," stage-results-num ","  prod-results-num)))
          (assoc {} :prod prod-top-2-results :stage stage-top-2-results)))

(defn status-code-check
  [query staging-rsp prod-rsp]
  (let [stage-status (:status staging-rsp)
        prod-status  (:status prod-rsp)
        equal-status (status-code stage-status prod-status)]
  (if (and equal-status (ok? stage-status))
      (cmp-instances (:body staging-rsp) (:body prod-rsp))
      (message (str "Status code difference," stage-status "," prod-status ","query)))))

(defn compare-response
  [query staging-rsp prod-rsp]
  (prn query)
  (status-code-check query staging-rsp prod-rsp))


  ;;write out to a message holder))

  ;; if 200, check body
  ;; heartbeat response is different -- look into that
  ;; body - first two results in prod
  ;; check if they occur in the results of staging
  ;; if they do which position
  ;; if same position, match the highest score out of a 5
  ;; if within one or 2 positions, match it

  ;;(json/parse-string (:body staging-rsp) true))
