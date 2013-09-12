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
      (< ?spend-difference-abs (* 4 spend-std-dev) :> false)))

(defn adhd-scrips-outliers [summary average-scrips scrips-std-dev]
  (<- [?id ?name ?patient-count ?scrips-per-head ?spend-per-head]
      (summary :> ?id ?name ?patient-count ?scrips-per-head ?spend-per-head)
      (- ?scrips-per-head average-scrips :> ?scrips-difference)
      (math/abs ?scrips-difference :> ?scrips-difference-abs)
      (< ?scrips-difference-abs (* 4 scrips-std-dev) :> false)))

(defn adhd-spend-drift [summary average-spend spend-std-dev]
  (<- [?id ?name ?patient-count ?scrips-per-head ?spend-per-head ?spend-drift]
      (summary :> ?id ?name ?patient-count ?scrips-per-head ?spend-per-head)
      (- ?spend-per-head average-spend :> ?spend-difference)
      (/ ?spend-difference spend-std-dev :> ?spend-drift)))

(defn adhd-spend-account-for-scrips-drift [spend-drifts script-drifts]
  (<- [?id ?name ?patient-count ?spend-accounting-for-scrips]
      (spend-drifts :> ?id ?name ?patient-count _ _ ?spend-drift-percentage)
      (script-drifts :> ?id _ _ _ _ ?scrips-drift-percentage)
      (- ?spend-drift-percentage ?scrips-drift-percentage :> ?spend-accounting-for-scrips)))

(defn adhd-scrips-drift [summary average-scrips scrips-std-dev]
  (<- [?id ?name ?patient-count ?scrips-per-head ?spend-per-head ?scrips-drift]
      (summary :> ?id ?name ?patient-count ?scrips-per-head ?spend-per-head)
      (- ?scrips-per-head average-scrips :> ?scrips-difference)
      (/ ?scrips-difference scrips-std-dev :> ?scrips-drift)))

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

(defn practice-summaries [in]
  (<- [?gp ?ccg ?gp-name ?count]
      (in :> ?gp ?ccg ?gp-name ?count-string)
      (conv/numbers-as-strings? ?count-string)
      (conv/parse-double ?count-string :> ?count)))

(defn test-patient-counts [scrips epraccur counts]
  (<- [?ccg ?total-count]
           (ops/sum ?gp-patient-count :> ?total-count)
           (epraccur :#> 20 {0 ?practice 12 ?status-code 14 ?ccg})
           (counts :> ?practice ?gp-patient-count)))


(defn ccg-names [in]
  (<- [?ccg-code ?ccg-name]
      (in _ _ ?ccg-code ?ccg-name) (:distinct true)))


;;
;; GP SUMMARIES
;;
;;
;;


(defn per-gp-totals [scrips]
  (<- [?gp ?gp-total-scrips ?gp-total-spend] 
      (scrips :> ?sha ?pct ?gp ?bnf-code ?bnf-chemical ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month)
      (ops/sum ?items :> ?gp-total-scrips)
      (ops/sum ?net-ingredient-cost :> ?gp-total-spend)))

(defn adhd-summary-per-gp [scrips practises]
  (<- [?gp ?gp-name ?gp-patient-count ?gp-scrips-per-head ?gp-spend-per-head]
      (practises :> ?gp ?ccg ?gp-name ?gp-patient-count)
      ((per-gp-totals scrips) :> ?gp ?gp-total-scrips ?gp-total-spend)
      (/ ?gp-total-scrips ?gp-patient-count :> ?gp-scrips-per-head)
      (/ ?gp-total-spend ?gp-patient-count :> ?gp-spend-per-head)))

(defn sane-practises [scrips practises gp-scrips-avg gp-scrips-std-dev]
 (let [summary-data (adhd-summary-per-gp scrips practises)]
   (<- [?gp ?name ?patientcount]
       (practises :> ?gp ?name ?patientcount)
       ((adhd-scrips-outliers summary-data gp-scrips-avg gp-scrips-std-dev) ?gp _ _ _ _ :> false) )))

;;       
;;  This will be broken up
;;  Going to pre-process the data into a sensible format so I can run any of the above functions either against fitered data
;;  Or un-filtered data
;;

(defn adhd-gp-summary-from-data [practices-path]
  (let [scrips (filtered-prescriptions (hfs-delimited "./input/prescriptions/adhd" :delimiter ","))
        practises (practice-summaries (hfs-delimited practices-path :delimiter ","))]
    (adhd-summary-per-gp scrips practises)))

;;
;; CCG SUMMARIES
;;

(defn ccg-summaries [practices ccgs]
  (<- [?ccg ?ccg-name ?ccg-patient-count]
        (practices :> _ ?ccg _ ?practice-patient-count)  
        (ccgs :> ?ccg ?ccg-name)
        (ops/sum ?practice-patient-count :> ?ccg-patient-count)))

(defn per-ccg-totals [practices scrips]
  (<- [?ccg ?ccg-total-scrips ?ccg-total-spend] 
      (scrips :> ?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month)
      (practices :> ?practice ?ccg _ _ )
      (ops/sum ?items :> ?ccg-total-scrips)
      (ops/sum ?net-ingredient-cost :> ?ccg-total-spend)))

(defn adhd-summary-per-ccg [scrips practices ccgs]
  (<- [?ccg ?ccg-name ?ccg-patient-count ?ccg-scrips-per-head ?ccg-spend-per-head]
      ((ccg-summaries practices ccgs) :> ?ccg ?ccg-name ?ccg-patient-count)
      ((per-ccg-totals practices scrips) :> ?ccg ?ccg-total-scrips ?ccg-total-spend)
      (/ ?ccg-total-scrips ?ccg-patient-count :> ?ccg-scrips-per-head)
      (/ ?ccg-total-spend ?ccg-patient-count :> ?ccg-spend-per-head)))

(defn adhd-ccg-summary-from-data [practice-path]
  (adhd-summary-per-ccg
    (filtered-prescriptions
      (hfs-delimited "./input/prescriptions/adhd" :delimiter ","))
    (practice-summaries (hfs-delimited practice-path :delimiter ","))  
    (ccg-names (hfs-delimited  "./input/ods/ccglist/ccg-lsoa.csv" :delimiter ","))))


;; OVERALL SUMMARY

(defn total-patient-count [practices]
  (<- [?patient-count]
        (practices :> _ _ _ ?practice-patient-count)  
        (ops/sum ?practice-patient-count :> ?patient-count)))

(defn national-totals [scrips]
  (<- [?total-scrips ?total-spend] 
      (scrips :> ?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month)
      (ops/sum ?items :> ?total-scrips)
      (ops/sum ?net-ingredient-cost :> ?total-spend)))

(defn adhd-summary [scrips practices]
  (<- [?patient-count ?scrips-per-head ?spend-per-head]
      ((total-patient-count practices) :> ?patient-count)
      ((national-totals scrips) :> ?total-scrips ?total-spend)
      (/ ?total-scrips ?patient-count :> ?scrips-per-head)
      (/ ?total-spend ?patient-count :> ?spend-per-head)))

(defn adhd-summary-from-data [practice-path]
  (adhd-summary
    (filtered-prescriptions
      (hfs-delimited "./input/prescriptions/adhd" :delimiter ","))
    (practice-summaries (hfs-delimited practice-path :delimiter ","))))

;; We'll pre-process the practice information and generate with gp ccg gp-name patient-count
;; This will allow us to filter this list and pass in either the raw list or filtered list to the above queries
;;

(defn practice-patient-counts [in]
  (<- [?practice-code ?patient-count]
    ((prevalence/diabetes-prevalence-gp in) :#> 5 { 0 ?practice-code 2 ?patient-count})))

(defn gp-summaries [epraccur counts]
  (<- [?gp ?ccg ?gp-name ?gp-patient-count]
        (counts :> ?gp ?gp-patient-count)  
        (epraccur :#> 20 {0 ?gp 1 ?gp-name 14 ?ccg})))

;; So this will process the practices into a standard list for us to consume all over the place
#_ (?- (hfs-delimited "./input/practice-summaries" :delimiter "," :sinkmode :replace)
        (gp-summaries 
          (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))
          (practice-patient-counts (hfs-textline "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv" :delimiter ","))))


;; We'll also process all of the prescriptions into just a list of the adhd meds
;; This will save time
#_(?- (hfs-delimited "./input/prescriptions/adhd" :delimiter "," :sinkmode :replace)
      (adhd-drugs 
        (prescriptions/gp-prescriptions 
          (hfs-delimited "./input/prescriptions/pdpi" :delimiter ","))
        (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))))

;; These will give us the basic stats with which we can calculate deviations etc from
#_ (?- (stdout) (adhd-stats (adhd-ccg-summary-from-data "./input/practice-summaries")))
#_ (?- (stdout) (adhd-stats (adhd-gp-summary-from-data "./input/practice-summaries")))

;; These are the averages when running the summaries with no args
;; We can use these to eliminate outliers

;; GP
;; 0.020095359392228413	0.8408870599774393	0.03728198208303282	0.9633129101524941
;; CCG
;; 0.018088732158245645	0.7752476354544771	0.013512501232318414	0.5645800176174897

(def initial-gp-scrips-avg 0.020095359392228413)
(def initial-gp-spend-avg 0.8408870599774393)
(def initial-gp-scrips-stddev 0.03728198208303282)
(def initial-gp-spend-stddev 0.9633129101524941)

(def initial-ccg-scrips-avg 0.018088732158245645)
(def initial-ccg-spend-avg 0.7752476354544771)
(def initial-ccg-scrips-stddev 0.013512501232318414)
(def initial-ccg-spend-stddev 0.5645800176174897)

#_ (?- (hfs-delimited "./output/adhd-ccg-spend-drift-unclean" :delimiter "," :sinkmode :replace :skip-header? true) 
       (adhd-spend-drift 
         (adhd-ccg-summary-from-data "./input/practice-summaries") initial-ccg-spend-avg initial-ccg-spend-stddev))

;;  ?id ?name ?patient-count ?scrips-per-head ?spend-per-head ?drift]
#_ (?- (hfs-delimited "./output/adhd-ccg-scrips-drift-unclean" :delimiter "," :sinkmode :replace :skip-header? true) 
       (adhd-scrips-drift (adhd-ccg-summary-from-data "./input/practice-summaries") initial-ccg-scrips-avg initial-ccg-scrips-stddev))

;;  ?id ?name ?patient-count ?scrips-per-head ?spend-per-head ?drift]
#_ (?- (hfs-delimited "./output/adhd-gp-spend-drift-unclean" :delimiter "," :sinkmode :replace :skip-header? true) 
       (adhd-spend-drift (adhd-gp-summary-from-data "./input/practice-summaries") initial-gp-spend-avg initial-gp-spend-stddev))

;;  ?id ?name ?patient-count ?scrips-per-head ?spend-per-head ?drift]
#_ (?- (hfs-delimited "./output/adhd-gp-scrips-drift-unclean" :delimiter "," :sinkmode :replace :skip-header? true)
       (adhd-scrips-drift (adhd-gp-summary-from-data "./input/practice-summaries") initial-gp-scrips-avg initial-gp-scrips-stddev))


;; And this will filter the practice list for any serious outliers
;; Ideally I should be filtering the prescription data itself for crap data though

(defn sane-practises [scrips practises gp-scrips-avg gp-scrips-std-dev]
 (let [summary-data (adhd-summary-per-gp scrips practises)]
   (<- [?gp ?ccg ?name ?patientcount]
       (practises :> ?gp ?ccg ?name ?patientcount)
       ((adhd-scrips-outliers summary-data gp-scrips-avg gp-scrips-std-dev) ?gp _ _ _ _ :> false))))

#_(?- (hfs-delimited "./input/practices-without-outliers" :delimiter "," :sinkmode :replace)
      (sane-practises 
        (filtered-prescriptions
          (hfs-delimited "./input/prescriptions/adhd" :delimiter ","))
        (practice-summaries (hfs-delimited "./input/practice-summaries" :delimiter ","))
        initial-gp-scrips-avg
        initial-gp-scrips-stddev))


#_ (?- (stdout) (adhd-stats (adhd-gp-summary-from-data "./input/practices-without-outliers")))
#_ (?- (stdout) (adhd-stats (adhd-ccg-summary-from-data "./input/practices-without-outliers")))

;; These are the real averages
;; GP
;; 0.019082595957077238	0.8151024144786462	0.019324589539216036	0.7979322405600238
;; CCG
;; 0.017915081593757237	0.7688596457867932	0.012805620810334001	0.530798477605835
;;

(def gp-scrips-avg 0.019082595957077238	)
(def gp-spend-avg 0.8151024144786462)
(def gp-scrips-stddev 0.019324589539216036)
(def gp-spend-stddev 0.7979322405600238)

(def ccg-scrips-avg 0.017915081593757237)
(def ccg-spend-avg 0.7688596457867932)
(def ccg-scrips-stddev 0.012805620810334001)
(def ccg-spend-stddev 0.530798477605835)


;;  ?id ?name ?patient-count ?scrips-per-head ?spend-per-head ?drift]
#_ (?- (hfs-delimited "./output/adhd-ccg-spend-drift" :delimiter "," :sinkmode :replace :skip-header? true) 
       (adhd-spend-drift (adhd-ccg-summary-from-data "./input/practices-without-outliers") ccg-spend-avg ccg-spend-stddev))

;;  ?id ?name ?patient-count ?scrips-per-head ?spend-per-head ?drift]
#_ (?- (hfs-delimited "./output/adhd-ccg-scrips-drift" :delimiter "," :sinkmode :replace :skip-header? true) 
       (adhd-scrips-drift (adhd-ccg-summary-from-data "./input/practices-without-outliers") ccg-scrips-avg ccg-scrips-stddev))

;; TODO: Export CCG info here too
;;  ?id ?name ?patient-count ?scrips-per-head ?spend-per-head ?drift]
#_ (?- (hfs-delimited "./output/adhd-gp-spend-drift" :delimiter "," :sinkmode :replace :skip-header? true) 
       (adhd-spend-drift (adhd-gp-summary-from-data "./input/practices-without-outliers") gp-spend-avg gp-spend-stddev))

;; TODO: Export CCG info here too
;;  ?id ?name ?patient-count ?scrips-per-head ?spend-per-head ?drift]
#_ (?- (hfs-delimited "./output/adhd-gp-scrips-drift" :delimiter "," :sinkmode :replace :skip-header? true)
       (adhd-scrips-drift (adhd-gp-summary-from-data "./input/practices-without-outliers") gp-scrips-avg gp-scrips-stddev))

;; Now we've got this far and generated some sensible data, we'll no doubt have some interesting outliers that we want to
;; expand on - so a good idea would be to create gp level data and then compare this against the national averages
;; So we'll work out the national average
;; And we'll create a csv containing averages per gp and averages per ccg

#_ (?- (stdout) (adhd-stats (adhd-summary-from-data "./input/practices-without-outliers")))
