(ns cdec.health-analytics.diabetes (:require [cascalog.api :refer :all] [cascalog.ops :as ops] [clojure.tools.logging :refer [infof errorf]] [cascalog.more-taps :refer [hfs-delimited]]
            [cdec.predicates :as pred]
            [clj-time.format :as tf]
            [clj-time.core :as t]
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
  (<- [          ?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month] ;; Output is same as input
      (scrips :> ?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month)
      (adhd-drug? ?bnf-chemical)
      (epraccur :#> 20 {0 ?practice 12 ?status-code 14 ?ccg}))) ;; Ignoring this, this is just a join to remove empties

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
  (<- [?ccg ?ccg-average ?year ?month]
      (scrips :> ?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month)
      (ops/sum ?quantity :> ?ccg-quantity)
      (ops/sum ?gp-patient-count :> ?ccg-patient-count)
      (/ ?ccg-patient-count ?ccg-quantity :> ?ccg-average)
      (epraccur :#> 20 {0 ?practice 12 ?status-code 14 ?ccg})
      (counts :> ?practice ?gp-patient-count)))

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

;; I can't find prevalence data, so any stats will have to be done based on per-head data
;; but can probably make some estimates based off the assumptions found here:
;; http://www.nice.org.uk/usingguidance/commissioningguides/adhd/adhdassumptionsusedinestimatingapopulationbenchmark.jsp
;; SHA Code,Strategic Health Authority Name,PCT Code,PCT Name,Practice Code,Practice Name,Estimated number 17+,Register (ages 17+),Prevalence
;; "/input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv"
;; Seems to have patient numbers in them as, and we can ignore the diabetes count - if I understand the data anyway
;; Of course, this only gives us 17+ aged people and we're looking at ADHD so...

;; Reading from the filtered list, spend per head per CCG for ADHD
#_(?- (hfs-delimited "./output/spend-per-head-on-adhd-per-ccg" :delimiter "," :sinkmode :replace)
      (adhd-spend-per-head-per-ccg-per-month
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

