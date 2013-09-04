(ns cdec.health-analytics.diabetes
  (:require [cascalog.api :refer :all]
            [cascalog.ops :as ops]
            [clojure.tools.logging :refer [infof errorf]]
            [cascalog.more-taps :refer [hfs-delimited]]
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

(defn filtered-prescriptions [in]
  (<- [?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month]
      (in :> ?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items-string ?net-ingredient-cost-string ?act-cost-string ?quantity-string ?year ?month)
      (conv/numbers-as-strings? ?items-string ?net-ingredient-cost-string ?act-cost-string ?quantity-string)
      (conv/parse-double ?items-string :> ?items)
      (conv/parse-double ?net-ingredient-cost-string :> ?net-ingredient-cost)
      (conv/parse-double ?act-cost-string :> ?act-cost)
      (conv/parse-double ?quantity-string :> ?quantity)))

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

