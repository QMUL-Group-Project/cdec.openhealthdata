(ns cdec.health-analytics.gp-outcomes
  (:require [cascalog.api :refer :all]
            [cascalog.ops :as ops]
            [cascalog.tap :as tap]
            [clojure.tools.logging :refer [infof errorf]]
            [cascalog.more-taps :refer [hfs-delimited]]
            [cdec.conversions :as conv]))

#_(use 'cascalog.playground)
#_(bootstrap-emacs)

(defn registered-list-size-indicator? [indicator]
  (let [registered-list-size-code "P01107A"]
    (= registered-list-size-code indicator)))

(defn total-registered-list-in-ccg [gp-outcomes]
  (<- [?ccg ?registered-in-ccg]
      (gp-outcomes :#> 15 {3 ?value
                           7 ?indicator-code
                           8 ?ccg})
      (registered-list-size-indicator? ?indicator-code)
      (conv/numbers-as-strings? ?value)
      (conv/parse-double ?value :> ?total-registered-list-size)
      (ops/sum ?total-registered-list-size :> ?registered-in-ccg)))

#_(?- (hfs-delimited "./output/total-registered-in-ccg/" :delimiter "," :sinkmode :replace)
      (total-registered-list-in-ccg
       (hfs-delimited "./input/gp-outcomes/results.csv" :delimiter ","))
      (:trap (stdout)))

(defn total-registered-list-in-pct [gp-outcomes]
  (<- [?pct-code ?registered-in-ccg]
      (gp-outcomes :#> 15 {3 ?value
                           7 ?indicator-code
                           11 ?pct-code})
      (registered-list-size-indicator? ?indicator-code)
      (conv/numbers-as-strings? ?value)
      (conv/parse-double ?value :> ?total-registered-list-size)
      (ops/sum ?total-registered-list-size :> ?registered-in-ccg)))

#_(?- (hfs-delimited "./output/total-registered-in-pct/" :delimiter "," :sinkmode :replace)
      (total-registered-list-in-pct
       (hfs-delimited "./input/gp-outcomes/results.csv" :delimiter ","))
      (:trap (stdout)))

(defn unknown-ccg? [^String ccg]
  (not= 3 (.length ccg)))

(defn practices-with-no-ccg [gp-outcomes]
  (<- [?practice-code ?practice-name ?ccg]
      (gp-outcomes :#> 15 {0 ?practice-code 1 ?practice-name 8 ?ccg 11 ?pct-code})
      (unknown-ccg? ?ccg)
      (:distinct true)))

#_(?- (hfs-delimited "./output/unknown-ccgs/" :delimiter "," :sinkmode :replace)
      (practices-with-no-ccg
       (hfs-delimited "./input/gp-outcomes/results.csv" :delimiter ","))
      (:trap (stdout)))

;; FlatFile.csv
;; (hfs-delimited "./input/gp-outcomes/" :delimiter ",")
;; 15 fields

;; 0 practice-code
;; 1 practice-name
;; 2 indicator-name
;; 3 value
;; 4 extract-date
;; 5 indicator-group-name
;; 6 indicator-sub-group-name
;; 7 indicator-code
;; 8 ccg
;; 9 sha-code
;; 10 sha-name
;; 11 pct-code
;; 12 pct-name
;; 13 age-band
;; 14 deprivation-band