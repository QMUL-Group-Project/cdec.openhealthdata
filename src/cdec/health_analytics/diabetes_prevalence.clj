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
            [cdec.health-analytics.gp-outcomes :as gpo]))

(defn calc-no-patients [percentile total-gp-patients]
  (-> (* total-gp-patients percentile)
      (/ 100)
      (math/floor)))

(defn calc-percentage [value1 value2]
  (-> (/ value1 value2)
      (* 100)))

(defn scrub-data [txt]
  (s/replace txt #"%" ""))

(defn gp-ccg-mapping [input]
  (<- [?gp-code ?ccg-code]
      (input ?line)
      (tl/data-line? ?line)
      (tl/split-line ?line :#> 5 {0 ?gp-code 2 ?ccg-code})))

(defn diabetes-prevalence [input]
  (<- [?gp-code ?prevalence]
      (input ?line)
      (tl/data-line? ?line)
      (tl/split-line ?line :#> 9 {4 ?gp-code 8 ?prevalence-dirty})
      (scrub-data ?prevalence-dirty :> ?prevalence)))

(defn total-patients-with-diabetes-per-gp [gp-percentile gp-total]
  (<- [?gp-code ?total-patients ?total ?percentile]
      (gp-percentile :> ?gp-code ?percentile-string)
      (gp-total :> ?gp-code ?total-patients-string)
      (tl/numbers-as-strings? ?percentile-string)
      (tl/numbers-as-strings? ?total-patients-string)
      (tl/parse-double ?percentile-string :> ?percentile)
      (tl/parse-double ?total-patients-string :> ?total-patients)
      (calc-no-patients ?total-patients ?percentile :> ?total)))

(defn diabetes-prevalence-per-ccg [ccg-gp-list gp-prevalence-list]
  (<- [?ccg-code ?ccg-total ?prevalence-total ?percentile]
      (ccg-gp-list ?ccg-line)
      (gp-prevalence-list :> ?gp-code _ ?gp-prevalence _)
      (tl/data-line? ?ccg-line)
      (tl/split-line ?ccg-line :#> 5 {0 ?gp-code 2 ?ccg-code 4 ?gp-total-string})
      (s/replace ?gp-total-string #"," "" :> ?gp-total-cleaned)
      (tl/numbers-as-strings? ?gp-total-cleaned)
      (tl/parse-double ?gp-total-cleaned :> ?gp-total)
      (ops/sum ?gp-total :> ?ccg-total)
      (ops/sum ?gp-prevalence :> ?prevalence-total)
      (calc-percentage ?prevalence-total ?ccg-total :> ?percentile)))

(defn gp-percentage-within-ccg [gp-prevalence ccg-prevalence gp-ccg-mapping]
  (<- [?gp-code ?gp-total ?gp-prev ?gp-percentile ?ccg-code ?ccg-total ?ccg-prev ?ccg-percentile ?percentile]
      (gp-ccg-mapping :> ?gp-code ?ccg-code)
      (gp-prevalence ?gp-list)
      (ccg-prevalence ?ccg-list)
      (tl/data-line? ?gp-list)
      (tl/split-line ?gp-list :> ?gp-code ?gp-total-string ?gp-prev-string ?gp-percentile)
      (tl/numbers-as-strings? ?gp-prev-string)
      (tl/parse-double ?gp-prev-string :> ?gp-prev)
      (tl/numbers-as-strings? ?gp-total-string)
      (tl/parse-double ?gp-total-string :> ?gp-total)
      (tl/data-line? ?ccg-list)
      (tl/split-line ?ccg-list :> ?ccg-code ?ccg-total ?ccg-prev-string ?ccg-percentile)
      (tl/numbers-as-strings? ?ccg-prev-string)
      (tl/parse-double ?ccg-prev-string :> ?ccg-prev)
      (calc-percentage ?gp-prev ?ccg-prev :> ?percentile)))

;; Total registered and prevalence per CCG (number of patients)
#_(let [data-in1 "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv"
        data-in2 "./input/PRACTICE_LIST_AGE_GENDER_BREAKDOWN2.csv"
        data-in3 "./input/list-of-proposed-practices-ccg.csv"
        data-out "./output/ccg_patients_with_diabetes_total/"]
    (?- (hfs-delimited data-out :sinkmode :replace :delimiter ",")
        (diabetes-prevalence-per-ccg
         (hfs-textline data-in3)
         (total-patients-with-diabetes-per-gp
          (diabetes-prevalence (hfs-textline data-in1))
          (gpt/gp-patients (hfs-textline data-in2))))))

;; Percentage of patients with diabetes GP vs CCG
#_(let [data-in1 "./output/gp_patients_with_diabetes_total/"
        data-in2 "./output/ccg_patients_with_diabetes_total/"
        data-in3 "./input/list-of-proposed-practices-ccg.csv"
        data-out "./output/gp-vs-ccg-percentage/"]
    (?- (hfs-delimited data-out :sinkmode :replace :delimiter ",")
        (gp-percentage-within-ccg
         (hfs-textline data-in1)
         (hfs-textline data-in2)
         (gp-ccg-mapping (hfs-textline data-in3)))))

;; Number n of high and low GP surgeries per CCG (use first-n)



;; Top and bottom 10 GP surgeries per CCG
