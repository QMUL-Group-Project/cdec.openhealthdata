(ns cdec.health-analytics.adhd 
  (:require [cascalog.api :refer :all] 
            [cascalog.ops :as ops] 
            [clojure.tools.logging :refer [infof errorf]] 
            [cascalog.more-taps :refer [hfs-delimited]]
            [cdec.predicates :as pred]
            [clj-time.format :as tf]
            [clj-time.core :as t]
            [cascalog.math.stats :as stats]
            [clojure.math.numeric-tower :as math]
            [cdec.health-analytics.gp-prescriptions :as prescriptions]
            [cdec.health-analytics.diabetes-prevalence :as prevalence]
            [cdec.health-analytics.organisational-data :as ods]
            [cdec.conversions :as conv]  
            [cdec.health-analytics.transform-load :as tl]))

#_(use 'cascalog.playground)
#_(bootstrap-emacs)

(defn adhd-drug? [bnf-code]
  (pred/in? bnf-code [ 
                      ;;Amfetamine Sulfate
                      "0404000A0"         
                      ;;Dexamfetamine Sulfate
                      "0404000L0"         
                      ;;Methylphenidate Hydrochloride
                      "0404000M0"         
                      ;;Methylamfetamine Hydrochloride
                      "0404000N0"         
                      ;;Atomoxetine Hydrochloride
                      "0404000S0"        
                      ;;Dexmethylphenidate Hydrochloride
                      "0404000T0" 
                      ;;Lisdexamfetamine Dimesylate
                      "0404000U0"
                      ]))

(defn adhd-drugs [scrips epraccur]
  (<- [          ?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month]
      (scrips :> ?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month)
      (adhd-drug? ?bnf-chemical)
      (epraccur :#> 20 {0 ?practice 12 ?status-code 14 ?ccg}))) 

(defn total-cost-of-adhd-per-month [scrips]
  (<- [?total-net-ingredient-cost ?year ?month]
      (scrips :> ?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month)
      (ops/sum ?net-ingredient-cost :> ?total-net-ingredient-cost)))

(defn total-cost-of-adhd-per-month-per-gp [scrips]
  (<- [?practice ?total-net-ingredient-cost ?year ?month]
      (scrips :> ?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month)
      (ops/sum ?net-ingredient-cost :> ?total-net-ingredient-cost)))

(defn total-cost-of-adhd-per-month-per-ccg [scrips epraccur]
  (<- [?ccg ?total-net-ingredient-cost ?year ?month]
      (scrips :> ?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month)
      (ops/sum ?net-ingredient-cost :> ?total-net-ingredient-cost)
      (epraccur :#> 20 {0 ?practice 12 ?status-code 14 ?ccg})))

(defn spend-per-head [patients total]
  (/ total patients))

(defn adhd-spend-per-head-per-ccg-per-month [scrips epraccur counts]
  (<- [?ccg ?ccg-spend-per-head ?year ?month]
      (scrips :> ?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month)
      (ops/sum ?net-ingredient-cost :> ?ccg-net-ingredient-cost)
      (ops/sum ?gp-patient-count :> ?ccg-patient-count)
      (spend-per-head ?ccg-patient-count ?ccg-net-ingredient-cost :> ?ccg-spend-per-head)
      (epraccur :#> 20 {0 ?practice 12 ?status-code 14 ?ccg})
      (counts :> ?practice ?gp-patient-count)))

(defn adhd-scrips-per-head-per-ccg-per-month [scrips epraccur counts]
  (<- [?ccg ?ccg-patient-count ?ccg-quantity ?ccg-average ?year ?month]
      (scrips :> ?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month)
      (ops/sum ?quantity :> ?ccg-quantity)
      (ops/sum ?gp-patient-count :> ?ccg-patient-count)
      (/ ?ccg-quantity ?ccg-patient-count :> ?ccg-average)
      (epraccur :#> 20 {0 ?practice 12 ?status-code 14 ?ccg})
      (counts :> ?practice ?gp-patient-count)))

(defn adhd-stats [summary] 
  (<- [?scrips-avg ?spend-avg ?scrips-std-dev ?spend-std-dev] 
      (summary :> ?ccg ?ccg-name ?ccg-patient-count ?ccg-scrips-per-head ?ccg-spend-per-head)
      (ops/avg ?ccg-spend-per-head :> ?spend-avg)
      (ops/avg ?ccg-scrips-per-head :> ?scrips-avg)
      (stats/variance ?ccg-scrips-per-head :> ?scrips-variance)
      (math/sqrt ?scrips-variance :> ?scrips-std-dev)
      (stats/variance ?ccg-spend-per-head :> ?spend-variance)
      (math/sqrt ?spend-variance :> ?spend-std-dev)))

(defn ccg-summaries [epraccur counts ccgs]
  (<- [?ccg ?ccg-name ?ccg-patient-count]
        (counts :> ?practice ?practice-patient-count)  
        (ccgs :> ?ccg ?ccg-name)
        (epraccur :#> 20 {0 ?practice 14 ?ccg})
        (ops/sum ?practice-patient-count :> ?ccg-patient-count)))

(defn per-ccg-totals [epraccur scrips]
  (<- [?ccg ?ccg-total-scrips ?ccg-total-spend] 
      (scrips :> ?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month)
      (epraccur :#> 20 {0 ?practice 14 ?ccg})
      (ops/sum ?items :> ?ccg-total-scrips)
      (ops/sum ?net-ingredient-cost :> ?ccg-total-spend)))

(defn adhd-summary-per-ccg [scrips epraccur counts ccgs]
  (<- [?ccg ?ccg-name ?ccg-patient-count ?ccg-scrips-per-head ?ccg-spend-per-head]
      ((ccg-summaries epraccur counts ccgs) :> ?ccg ?ccg-name ?ccg-patient-count)
      ((per-ccg-totals epraccur scrips) :> ?ccg ?ccg-total-scrips ?ccg-total-spend)
      (/ ?ccg-total-scrips ?ccg-patient-count :> ?ccg-scrips-per-head)
      (/ ?ccg-total-spend ?ccg-patient-count :> ?ccg-spend-per-head)))

(defn adhd-spend-outliers [summary average-spend spend-std-dev]
  (<- [?ccg ?ccg-name ?ccg-patient-count ?ccg-scrips-per-head ?ccg-spend-per-head]
      (summary :> ?ccg ?ccg-name ?ccg-patient-count ?ccg-scrips-per-head ?ccg-spend-per-head)
      (- ?ccg-spend-per-head average-spend :> ?spend-difference)
      (math/abs ?spend-difference :> ?spend-difference-abs)
      (< ?spend-difference-abs (* 2 spend-std-dev) :> false)))

(defn adhd-scrips-outliers [summary average-scrips scrips-std-dev]
  (<- [?ccg ?ccg-name ?ccg-patient-count ?ccg-scrips-per-head ?ccg-spend-per-head]
      (summary :> ?ccg ?ccg-name ?ccg-patient-count ?ccg-scrips-per-head ?ccg-spend-per-head)
      (- ?ccg-scrips-per-head average-scrips :> ?scrips-difference)
      (math/abs ?scrips-difference :> ?scrips-difference-abs)
      (< ?scrips-difference-abs (* 2 scrips-std-dev) :> false)))

(defn adhd-spend-drift [summary average-spend spend-std-dev]
  (<- [?ccg ?ccg-name ?ccg-patient-count ?ccg-scrips-per-head ?ccg-spend-per-head ?ccg-spend-drift]
      (summary :> ?ccg ?ccg-name ?ccg-patient-count ?ccg-scrips-per-head ?ccg-spend-per-head)
      (- ?ccg-spend-per-head average-spend :> ?spend-difference)
      (math/abs ?spend-difference :> ?spend-difference-abs)
      (/ ?spend-difference-abs spend-std-dev :> ?ccg-spend-drift)))

(defn adhd-scrips-drift [summary average-scrips scrips-std-dev]
  (<- [?ccg ?ccg-name ?ccg-patient-count ?ccg-scrips-per-head ?ccg-spend-per-head ?ccg-scrips-drift]
      (summary :> ?ccg ?ccg-name ?ccg-patient-count ?ccg-scrips-per-head ?ccg-spend-per-head)
      (- ?ccg-scrips-per-head average-scrips :> ?scrips-difference)
      (math/abs ?scrips-difference :> ?scrips-difference-abs)
      (/ ?scrips-difference-abs scrips-std-dev :> ?ccg-scrips-drift)))

(defn filtered-prescriptions [in]
  (<- [?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month]
      (in :> ?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items-string ?net-ingredient-cost-string ?act-cost-string ?quantity-string ?year ?month)
      (conv/numbers-as-strings? ?items-string ?net-ingredient-cost-string ?act-cost-string ?quantity-string)
      (conv/parse-double ?items-string :> ?items)
      (conv/parse-double ?net-ingredient-cost-string :> ?net-ingredient-cost)
      (conv/parse-double ?act-cost-string :> ?act-cost)
      (conv/parse-double ?quantity-string :> ?quantity)))

(defn practice-patient-counts [in]
  (<- [?practice-code ?patient-count]
    ((prevalence/diabetes-prevalence-gp in) :#> 5 { 0 ?practice-code 2 ?patient-count})))

(defn ccg-names [in]
  (<- [?ccg-code ?ccg-name]
      (in _ _ ?ccg-code ?ccg-name) (:distinct true)))

;; Create a filtered sink containing only the prescriptions we care about
#_(?- (hfs-delimited "./input/prescriptions/adhd" :delimiter "," :sinkmode :replace)
      (adhd-drugs 
        (prescriptions/gp-prescriptions 
          (hfs-delimited "./input/prescriptions/pdpi" :delimiter ","))
        (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))))

#_ ;; Read from the filtered list and work out the overall total per month
#_(?- (hfs-delimited "./output/total-cost-of-adhd" :delimiter "," :sinkmode :replace)
      (total-cost-of-adhd-per-month 
        (filtered-prescriptions
          (hfs-delimited "./input/prescriptions/adhd" :delimiter ","))))

#_ ;; Read from the filtered list and work out the total per gp per month
#_(?- (hfs-delimited "./output/total-cost-of-adhd-per-gp" :delimiter "," :sinkmode :replace)
      (total-cost-of-adhd-per-month-per-gp
        (filtered-prescriptions
          (hfs-delimited "./input/prescriptions/adhd" :delimiter ","))))

#_ ;; Read from the filtered list and work out the total per ccg per month 
#_(?- (hfs-delimited "./output/total-cost-of-adhd-per-ccg" :delimiter "," :sinkmode :replace)
      (total-cost-of-adhd-per-month-per-ccg
        (filtered-prescriptions
          (hfs-delimited "./input/prescriptions/adhd" :delimiter ","))
        (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))))

#_ ;; I can't find prevalence data, so any stats will have to be done based on per-head data
#_ ;; but can probably make some estimates based off the assumptions found here:
#_ ;; http://www.nice.org.uk/usingguidance/commissioningguides/adhd/adhdassumptionsusedinestimatingapopulationbenchmark.jsp
#_ ;; SHA Code,Strategic Health Authority Name,PCT Code,PCT Name,Practice Code,Practice Name,Estimated number 17+,Register (ages 17+),Prevalence
#_ ;; "/input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv"
#_ ;; Seems to have patient numbers in them as, and we can ignore the diabetes count - if I understand the data anyway
#_ ;; Of course, this only gives us 17+ aged people and we're looking at ADHD so...

;; Reading from the filtered list, spend per head per CCG for ADHD
#_(?- (stdout)
      (adhd-spend-per-head-per-ccg-per-month
        (filtered-prescriptions
          (hfs-delimited "./input/prescriptions/adhd" :delimiter ","))
        (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))
        (practice-patient-counts (hfs-textline "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv" :delimiter ","))))

(defn test-patient-counts [scrips epraccur counts]
  (<- [?ccg ?total-count]
           (ops/sum ?gp-patient-count :> ?total-count)
           (epraccur :#> 20 {0 ?practice 12 ?status-code 14 ?ccg})
           (counts :> ?practice ?gp-patient-count)))

#_ (?- (stdout)
      (test-patient-counts 
        (filtered-prescriptions
          (hfs-delimited "./input/prescriptions/adhd" :delimiter ","))
        (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))
        (practice-patient-counts (hfs-textline "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv" :delimiter ","))))

;; Reading from the filtered list, scrips per head per CCG for ADHD
#_(?- (hfs-delimited "./output/scrips-per-head-on-adhd-per-ccg" :delimiter "," :sinkmode :replace)
      (adhd-scrips-per-head-per-ccg-per-month
        (filtered-prescriptions
          (hfs-delimited "./input/prescriptions/adhd" :delimiter ","))
        (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))
        (practice-patient-counts (hfs-textline "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv" :delimiter ",")))) 

(defn adhd-summary-from-data []
  (adhd-summary-per-ccg
    (filtered-prescriptions
      (hfs-delimited "./input/prescriptions/adhd" :delimiter ","))
    (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))
    (practice-patient-counts (hfs-textline "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv" :delimiter ","))
    (ccg-names (hfs-delimited  "./input/ods/ccglist/ccg-lsoa.csv" :delimiter ","))))

#_ (?- (stdout)
       (adhd-summary-per-ccg
          (filtered-prescriptions
            (hfs-delimited "./input/prescriptions/adhd" :delimiter ","))
          (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))
          (practice-patient-counts (hfs-textline "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv" :delimiter ","))
          (ccg-names (hfs-delimited  "./input/ods/ccglist/ccg-lsoa.csv" :delimiter ","))  
         ))

#_ (?- (stdout) (adhd-summary-from-data))
#_ (?- (stdout) (adhd-stats (adhd-summary-from-data)))
#_ 0.07739164550331913 0.07296365966422276 0.048713321368423615  0.05144734865609667

(def scrips-avg 0.07739164550331913)
(def spend-avg 0.07296365966422276)
(def scrips-stddev 0.048713321368423615)
(def spend-stddev 0.05144734865609667)

#_ (?- (stdout) (adhd-spend-outliers (adhd-summary-from-data) spend-avg spend-stddev))
#_ (?- (stdout) (adhd-scrips-outliers (adhd-summary-from-data) scrips-avg scrips-stddev))

#_ (?- (hfs-delimited "./output/adhd-spend-drift" :delimiter "," :sinkmode :replace) 
       (adhd-spend-drift (adhd-summary-from-data) spend-avg spend-stddev))

#_ (?- (hfs-delimited "./output/adhd-scrips-drift" :delimiter "," :sinkmode :replace) 
       (adhd-scrips-drift (adhd-summary-from-data) scrips-avg scrips-stddev))

