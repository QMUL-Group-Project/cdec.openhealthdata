(ns cdec.health-analytics.diabetes_prevalence
  (:require [cascalog.api :refer :all]
            [cascalog.ops :as ops]
            [cascalog.tap :as tap]
            [cascalog.more-taps :refer [hfs-delimited]]
            [clojure.string :as s]
            [clojure.java.io :as io]
            [cdec.health-analytics.transform-load :as tl]
            [cdec.health-analytics.gp-total-registered :as gpt]
            [clojure.tools.logging :refer [infof errorf]]
            [clojure.math.numeric-tower :as math]
            ))

(defn calc-no-patients [percentile total-gp-patients]
  (-> (* total-gp-patients percentile)
      (/ 100)
      (math/floor)))

(defn scrub-data [txt]
  (s/replace txt #"%" ""))

(defn total-patients-with-diabetes [gp-percentile gp-total]
  (<- [?gp-code ?total]
      (gp-percentile :> ?gp-code ?percentile-string)
      (gp-total :> ?gp-code ?total-patients-string)
      (tl/numbers-as-strings? ?percentile-string)
      (tl/numbers-as-strings? ?total-patients-string)
      (tl/parse-double ?percentile-string :> ?percentile)
      (tl/parse-double ?total-patients-string :> ?total-patients)
      (calc-no-patients ?total-patients ?percentile :> ?total)))

(defn diabetes-prevalence [input]
  (<- [?gp-code ?prevalence]
      (input ?line)
      (tl/data-line? ?line)
      (tl/split-line ?line :#> 9 {4 ?gp-code 8 ?prevalence-dirty})
      (scrub-data ?prevalence-dirty :> ?prevalence)))

#_(let [data-in1 "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv"
         data-in2 "./input/PRACTICE_LIST_AGE_GENDER_BREAKDOWN2.csv"
         data-out "./output/gp_patients_with_diabetes_total/"]
     (?- (hfs-delimited data-out :sinkmode :replace :delimiter ",")
         (total-patients-with-diabetes
          (diabetes-prevalence (hfs-textline data-in1))
          (gpt/gp-patients (hfs-textline data-in2)))))
