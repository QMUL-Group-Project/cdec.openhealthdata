(ns cdec.health-analytics.transform-load
  (:require [cascalog.api :refer :all]
            [cascalog.ops :as ops]
            [cascalog.tap :as tap]
            [cascalog.more-taps :refer [hfs-delimited]]
            [clojure.string :as s]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [infof errorf]]
            [clojure-csv.core :as csv]
            ))

(defn data-line? [^String row]
  (and (not= -1 (.indexOf row ","))
       (not (.endsWith row ","))
       (not (.startsWith row ","))))

(defn numbers-as-strings? [& strings]
  (every? #(re-find #"^-?\d+(?:\.\d+)?$" %) strings))

(defn parse-double [txt]
  (Double/parseDouble txt))

(defn split-line [line]
  (first (csv/parse-csv line)))
