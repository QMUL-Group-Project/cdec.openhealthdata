(ns cdec.health-analytics.transform-load
  (:require [cascalog.api :refer :all]
            [cascalog.ops :as ops]
            [cascalog.tap :as tap]
            [cascalog.more-taps :refer [hfs-delimited]]
            [clojure.string :as s]
            [clojure.java.io :as io]
            [clojure.tools.logging :refer [infof errorf]]
            [clojure-csv.core :as csv]
            [clj-time.format :as tf]
            [clj-time.core :as t]))

(defn data-line? [^String row]
  (and (not= -1 (.indexOf row ","))
       (not (.endsWith row ","))
       (not (.startsWith row ","))))

(defn numbers-as-strings? [& strings]
 ;; (infof "Numbers as strings: %s" strings)
  (every? #(re-find #"^-?\d+(?:\.\d+)?$" %) strings))

(defn parse-double [txt]
;;  (infof "Parsing double: %s" txt)
  (Double/parseDouble txt))

(defn split-line [line]
  (first (csv/parse-csv line)))

(def custom-formatter (tf/formatter "MMM"))

(defn parse-date [date]
  (->
   (tf/unparse custom-formatter (t/date-time 0 date))))

(defn convert-month [input]
  (<- [?ccg ?year ?month ?ccg-registered-patients ?ccg-diabetes-patients ?ccg-total-net-ingredient-cost ?spend-per-head]
      (input :> ?ccg ?year ?month-string ?ccg-registered-patients ?ccg-diabetes-patients ?ccg-total-net-ingredient-cost ?spend-per-head)

      (numbers-as-strings? ?month-string)
      (parse-double ?month-string :> ?month-raw)
      (parse-date ?month-raw :> ?month)))

;; Replace integer representation of a month with a string name (used for chart labels)
#_ (?- (hfs-delimited "./output/diabetes-per-head-per-ccg-spend-difference-str" :delimiter "," :sinkmode :replace)
       (convert-month (hfs-delimited "./input/spend/spend_month_by_month2011.csv" :delimiter ",")))
