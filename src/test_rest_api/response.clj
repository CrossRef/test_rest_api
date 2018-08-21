(ns test_rest_api.response
  (:require [clojure.data.csv :as csv]
            [org.httpkit.client :as http]
            [clojure.java.io :as io]
            [cheshire.core :as json]))
(def status-ok 200)

(defn convert-to-json
  [rsp]
  (json/parse-string rsp true))

(defn status-code
  [stage prod]
  (= stage prod))

(defn ok?
  [status]
  (= status-ok status))

(defn cmp-results
  [stage prod]
  (let [stage-body (convert-to-json stage)
        prod-body  (convert-to-json prod)]
         (prn (:status stage-body))
         (prn (:status prod-body))))


(defn compare-response
  [staging-rsp prod-rsp]
  (let [stage-status (:status staging-rsp)
        prod-status (:status prod-rsp)
        status-equality (status-code stage-status prod-status)]
  (when (and status-equality (ok? stage-status))
      (cmp-results (:body staging-rsp) (:body prod-rsp)))))

  ;; check status code equality
  ;; if 200, check body
  ;; heartbeat response is different -- look into that
  ;; body - first two results in prod
  ;; check if they occur in the results of staging
  ;; if they do which position
  ;; if same position, match the highest score out of a 5
  ;; if within one or 2 positions, match it

  ;;(json/parse-string (:body staging-rsp) true))
