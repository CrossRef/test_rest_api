(ns test_rest_api.response
  (:require [clojure.data.csv :as csv]
            [org.httpkit.client :as http]
            [clojure.java.io :as io]
            [cheshire.core :as json]))

(defn compare-response
  [staging-rsp prod-rsp]
  (:status (json/parse-string (:body staging-rsp) true)))
