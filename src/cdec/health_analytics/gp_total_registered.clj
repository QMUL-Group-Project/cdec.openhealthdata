(ns cdec.health-analytics.gp-total-registered
  (:require [cascalog.api :refer :all]
            [cascalog.ops :as ops]
            [cascalog.tap :as tap]
            [cascalog.more-taps :refer [hfs-delimited]]
            [clojure.string :as s]
            [clojure.java.io :as io]
            [cdec.health-analytics.transform-load :as tl]
            [clojure.tools.logging :refer [infof errorf]]
            ))

(defn scrub-number [num]
  (s/replace num #"," ""))

(defn gp-patients [input]
  (infof "Passing line to gp-patients-2: %s" input)
  (<- [?gp-code ?total]      (input ?line)
      (tl/data-line? ?line)
      (tl/split-line ?line :#> 36 {4 ?gp-code 6 ?total-dirty})
      (scrub-number ?total-dirty :> ?total)
      ))

#_(let [data-in "./input/PRACTICE_LIST_AGE_GENDER_BREAKDOWN2.csv"
        data-out "./output/gp_total_registered/"]
    (?- (hfs-delimited data-out :sinkmode :replace :delimiter ",")
        (gp-patients
         (hfs-textline data-in))))
