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


(defn spend-per-head [patients total]
  (/ total patients))


;;
;; SHARED
;;


(defn adhd-stats [summary] 
  (<- [?scrips-avg ?spend-avg ?scrips-std-dev ?spend-std-dev] 
      (summary :> ?id ?name ?patient-count ?scrips-per-head ?spend-per-head)
      (ops/avg ?spend-per-head :> ?spend-avg)
      (ops/avg ?scrips-per-head :> ?scrips-avg)
      (stats/variance ?scrips-per-head :> ?scrips-variance)
      (math/sqrt ?scrips-variance :> ?scrips-std-dev)
      (stats/variance ?spend-per-head :> ?spend-variance)
      (math/sqrt ?spend-variance :> ?spend-std-dev)))

(defn adhd-spend-outliers [summary average-spend spend-std-dev]
  (<- [?id ?name ?patient-count ?scrips-per-head ?spend-per-head]
      (summary :> ?id ?name ?patient-count ?scrips-per-head ?spend-per-head)
      (- ?spend-per-head average-spend :> ?spend-difference)
      (math/abs ?spend-difference :> ?spend-difference-abs)
      (< ?spend-difference-abs (* 2 spend-std-dev) :> false)))

(defn adhd-scrips-outliers [summary average-scrips scrips-std-dev]
  (<- [?id ?name ?patient-count ?scrips-per-head ?spend-per-head]
      (summary :> ?id ?name ?patient-count ?scrips-per-head ?spend-per-head)
      (- ?scrips-per-head average-scrips :> ?scrips-difference)
      (math/abs ?scrips-difference :> ?scrips-difference-abs)
      (< ?scrips-difference-abs (* 2 scrips-std-dev) :> false)))

(defn adhd-spend-drift [summary average-spend spend-std-dev]
  (<- [?id ?name ?patient-count ?scrips-per-head ?spend-per-head ?spend-drift-percentage]
      (summary :> ?id ?name ?patient-count ?scrips-per-head ?spend-per-head)
      (- ?spend-per-head average-spend :> ?spend-difference)
      (/ ?spend-difference spend-std-dev :> ?spend-drift)
      (* 100 ?spend-drift :> ?spend-drift-percentage)))

(defn adhd-spend-account-for-scrips-drift [spend-drifts script-drifts]
  (<- [?id ?name ?patient-count ?spend-accounting-for-scrips]
      (spend-drifts :> ?id ?name ?patient-count _ _ ?spend-drift-percentage)
      (script-drifts :> ?id _ _ _ _ ?scrips-drift-percentage)
      (- ?spend-drift-percentage ?scrips-drift-percentage :> ?spend-accounting-for-scrips)))

(defn adhd-scrips-drift [summary average-scrips scrips-std-dev]
  (<- [?id ?name ?patient-count ?scrips-per-head ?spend-per-head ?scrips-drift-percentage]
      (summary :> ?id ?name ?patient-count ?scrips-per-head ?spend-per-head)
      (- ?scrips-per-head average-scrips :> ?scrips-difference)
      (/ ?scrips-difference scrips-std-dev :> ?scrips-drift)
      (* 100 ?scrips-drift :> ?scrips-drift-percentage)))

;;
;;
;; Helpers


(defn filtered-prescriptions [in]
  (<- [?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month]
      (in :> ?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items-string ?net-ingredient-cost-string ?act-cost-string ?quantity-string ?year ?month)
      (conv/numbers-as-strings? ?items-string ?net-ingredient-cost-string ?act-cost-string ?quantity-string)
      (conv/parse-double ?items-string :> ?items)
      (conv/parse-double ?net-ingredient-cost-string :> ?net-ingredient-cost)
      (conv/parse-double ?act-cost-string :> ?act-cost)
      (conv/parse-double ?quantity-string :> ?quantity)))

(defn test-patient-counts [scrips epraccur counts]
  (<- [?ccg ?total-count]
           (ops/sum ?gp-patient-count :> ?total-count)
           (epraccur :#> 20 {0 ?practice 12 ?status-code 14 ?ccg})
           (counts :> ?practice ?gp-patient-count)))


(defn practice-patient-counts [in]
  (<- [?practice-code ?patient-count]
    ((prevalence/diabetes-prevalence-gp in) :#> 5 { 0 ?practice-code 2 ?patient-count})))

(defn ccg-names [in]
  (<- [?ccg-code ?ccg-name]
      (in _ _ ?ccg-code ?ccg-name) (:distinct true)))


;;
;; GP SUMMARIES
;;

(defn gp-summaries [epraccur counts]
  (<- [?gp ?gp-name ?gp-patient-count]
        (counts :> ?gp ?gp-patient-count)  
        (epraccur :#> 20 {0 ?gp 1 ?gp-name})))

(defn per-gp-totals [epraccur scrips]
  (<- [?gp ?gp-total-scrips ?gp-total-spend] 
      (scrips :> ?sha ?pct ?gp ?bnf-code ?bnf-chemical ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month)
      (ops/sum ?items :> ?gp-total-scrips)
      (ops/sum ?net-ingredient-cost :> ?gp-total-spend)))

(defn adhd-summary-per-gp [scrips epraccur counts]
  (<- [?gp ?gp-name ?gp-patient-count ?gp-scrips-per-head ?gp-spend-per-head]
      ((gp-summaries epraccur counts) :> ?gp ?gp-name ?gp-patient-count)
      ((per-gp-totals epraccur scrips) :> ?gp ?gp-total-scrips ?gp-total-spend)
      (/ ?gp-total-scrips ?gp-patient-count :> ?gp-scrips-per-head)
      (/ ?gp-total-spend ?gp-patient-count :> ?gp-spend-per-head)))

(defn adhd-gp-summary-from-data []
  (adhd-summary-per-gp
    (filtered-prescriptions
      (hfs-delimited "./input/prescriptions/adhd" :delimiter ","))
    (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))
    (practice-patient-counts (hfs-textline "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv" :delimiter ","))))

;;
;; CCG SUMMARIES
;;

(defn total-cost-of-adhd-per-month-per-ccg [scrips epraccur]
  (<- [?ccg ?total-net-ingredient-cost ?year ?month]
      (scrips :> ?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month)
      (ops/sum ?net-ingredient-cost :> ?total-net-ingredient-cost)
      (epraccur :#> 20 {0 ?practice 12 ?status-code 14 ?ccg})))

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

(defn adhd-ccg-summary-from-data []
  (adhd-summary-per-ccg
    (filtered-prescriptions
      (hfs-delimited "./input/prescriptions/adhd" :delimiter ","))
    (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))
    (practice-patient-counts (hfs-textline "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv" :delimiter ","))
    (ccg-names (hfs-delimited  "./input/ods/ccglist/ccg-lsoa.csv" :delimiter ","))))


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


#_ (?- (stdout)
       (adhd-summary-per-ccg
          (filtered-prescriptions
            (hfs-delimited "./input/prescriptions/adhd" :delimiter ","))
          (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))
          (practice-patient-counts (hfs-textline "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv" :delimiter ","))
          (ccg-names (hfs-delimited  "./input/ods/ccglist/ccg-lsoa.csv" :delimiter ","))))

#_ (?- (stdout) (adhd-ccg-summary-from-data))
#_ (?- (stdout) (adhd-stats (adhd-ccg-summary-from-data)))
#_ (?- (stdout) (adhd-stats (adhd-gp-summary-from-data)))

;; CCG
;; 0.001700475480246234 0.07296365966422276 0.001241849517555931  0.05144734865609667
;; GP
;; 0.001984996982676855	0.08321750012684408	0.003203439737844418	0.09398640061215113

(def ccg-scrips-avg 0.001700475480246234)
(def ccg-spend-avg 0.07296365966422276)
(def ccg-scrips-stddev 0.001241849517555931)
(def ccg-spend-stddev 0.05144734865609667)

(def gp-scrips-avg 0.001984996982676855)
(def gp-spend-avg 0.08321750012684408)
(def gp-scrips-stddev 0.003203439737844418)
(def gp-spend-stddev 0.09398640061215113)

#_ (?- (stdout) (adhd-spend-outliers (adhd-ccg-summary-from-data) ccg-spend-avg ccg-spend-stddev))
#_ (?- (stdout) (adhd-scrips-outliers (adhd-ccg-summary-from-data) ccg-scrips-avg ccg-scrips-stddev))

#_ (?- (stdout) (adhd-spend-outliers (adhd-gp-summary-from-data) gp-spend-avg gp-spend-stddev))
#_ (?- (stdout) (adhd-scrips-outliers (adhd-gp-summary-from-data) gp-scrips-avg gp-scrips-stddev))

#_ (?- (hfs-delimited "./output/adhd-ccg-spend-drift" :delimiter "," :sinkmode :replace) 
       (adhd-spend-drift (adhd-ccg-summary-from-data) ccg-spend-avg ccg-spend-stddev))

#_ (?- (hfs-delimited "./output/adhd-ccg-scrips-drift" :delimiter "," :sinkmode :replace) 
       (adhd-scrips-drift (adhd-ccg-summary-from-data) ccg-scrips-avg ccg-scrips-stddev))

#_ (?- (hfs-delimited "./output/adhd-gp-spend-drift" :delimiter "," :sinkmode :replace) 
       (adhd-spend-drift (adhd-gp-summary-from-data) gp-spend-avg gp-spend-stddev))

#_ (?- (hfs-delimited "./output/adhd-gp-scrips-drift" :delimiter "," :sinkmode :replace) 
       (adhd-scrips-drift (adhd-gp-summary-from-data) gp-scrips-avg gp-scrips-stddev))

#_ (?- (hfs-delimited "./output/adhd-spend-accounting-for-scrips" :delimiter "," :sinkmode :replace) 
       (adhd-ccg-spend-account-for-scrips-drift 
         (adhd-ccg-spend-drift (adhd-ccg-summary-from-data) ccg-spend-avg ccg-spend-stddev) 
         (adhd-ccg-scrips-drift (adhd-ccg-summary-from-data) ccg-scrips-avg ccg-scrips-stddev)))

