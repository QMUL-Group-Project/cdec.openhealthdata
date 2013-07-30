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
            [cdec.health-analytics.transform-load :as tl]))

#_(use 'cascalog.playground)
#_(bootstrap-emacs)

(defn diabetes-drug? [bnf-code bnf-name]
  (or (pred/in? bnf-code [;; Metformin
                          "0601022B0" "0601023AD" "0601023AF" "0601023AH" "0601023V0" "0601023W0" "0601023Z0"
                          ;; Sulfonylurea
                          "0601021B0" "0601021E0" "0601021X0" "0601021V0" "0601021P0" "0601021M0" "0601021H0"
                          "0601021K0" "0601021R0" "0601021A0"
                          ;; Insulin
                          "0601011A0" "0601011I0" "0601011L0" "0601011N0" "0601011P0" "0601011Q0" "0601011R0"
                          "0601012C0" "0601012D0" "0601012F0" "0601012G0" "0601012L0" "0601012N0" "0601012S0"
                          "0601012U0" "0601012V0" "0601012W0" "0601012X0" "0601012Z0" "060101200"
                          ;; DPP-4 inhibitors / Sitagliptin / Vildagliptin
                          "0601023X0" "0601023AA" "0601023AC"
                          ;; Thiazolidinedione
                          "0601023B0"
                          ;; Exenatide
                          "0601023Y0"
                          ;; Acarbose
                          "0601023A0"
                          ])
      (pred/in? bnf-name [;; kit
                          "Lancets" ".*Glucose \\(Reagent\\)_ Strip.*"])))

(defn split-bnf-code [bnf-code]
  (> (count bnf-code) 8)
  (subs bnf-code 0 9))

(defn active? [status-code]
    (not= status-code "C" ))

(defn diabetes-drugs [scrips epraccur]
  (<- [?ccg ?practice ?bnf-code ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month]
      (scrips :> ?sha ?pct ?practice ?bnf-code-string ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month)
      (split-bnf-code ?bnf-code-string :> ?bnf-code)
      (diabetes-drug? ?bnf-code ?bnf-name)
      (epraccur :#> 20 {0 ?practice 12 ?status-code 14 ?ccg})
      (active? ?status-code)))

#_(?- (hfs-delimited "./output/diabetes-drugs" :delimiter "," :sinkmode :replace)
      (diabetes-drugs
       (prescriptions/gp-prescriptions
        (hfs-delimited "./input/prescriptions/pdpi" :delimiter ","))
       (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ",")))
      (:trap (stdout)))

(defn diabetes-spend-per-gp-per-month [diabetes-drugs]
  (<- [?ccg ?practice ?year ?month ?total-net-ingredient-cost]
      (diabetes-drugs :#> 10 {0 ?ccg 1 ?practice 5 ?net-ingredient-cost 8 ?year 9 ?month})
      (ops/sum ?net-ingredient-cost :> ?total-net-ingredient-cost)))

#_(?- (hfs-delimited "./output/diabetes-per-gppractice-per-month" :delimiter "," :sinkmode :replace)
      (diabetes-spend-per-gp-per-month
       (diabetes-drugs
        (prescriptions/gp-prescriptions
         (hfs-delimited "./input/prescriptions/pdpi" :delimiter ","))
        (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))))
      (:trap (stdout)))

(defn spend-per-head [spend patients]
  (/ spend patients))

(defn has-patients? [patients]
  (> patients 0))

(defn diabetes-spend-per-head-per-gp-per-month [gp-spend gp-prevalence]
  (<- [?practice ?year ?month ?registered-patients ?diabetes-patients ?total-net-ingredient-cost ?spend-per-head]
      (gp-spend :> ?ccg ?practice ?year ?month ?total-net-ingredient-cost)
      (gp-prevalence :> ?practice ?gp-name ?registered-patients ?diabetes-patients ?prevalence)
      (has-patients? ?diabetes-patients)
      (spend-per-head ?total-net-ingredient-cost ?diabetes-patients :> ?spend-per-head)))


;; prevalence data
;; http://indicators.ic.nhs.uk/webview/index.jsp?v=2&submode=ddi&study=http%3A%2F%2Fhg-l-app-472.ic.green.net%3A80%2Fobj%2FfStudy%2FP01121&mode=documentation&top=yes
;; https://indicators.ic.nhs.uk/download/Demography/Data/QOF1011_Pracs_Prevalence_DiabetesMellitus.xls
#_(?- (hfs-delimited "./output/diabetes-per-head-per-gp-per-month" :delimiter "," :sinkmode :replace)
      (diabetes-spend-per-head-per-gp-per-month
       (diabetes-spend-per-gp-per-month
        (diabetes-drugs
         (prescriptions/gp-prescriptions
          (hfs-delimited "./input/prescriptions/pdpi" :delimiter ","))
         (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))))
       (prevalence/diabetes-prevalence-gp
        (hfs-textline "./input/diabetes-prevalence/")))
      (:trap (stdout)))

(defn diabetes-spend-per-ccg-per-month [diabetes-drugs]
  (<- [?ccg ?year ?month ?total-net-ingredient-cost]
      (diabetes-drugs :#> 10 {0 ?ccg 5 ?net-ingredient-cost 8 ?year 9 ?month})
      (ops/sum ?net-ingredient-cost :> ?total-net-ingredient-cost)))

#_(?- (hfs-delimited "./output/diabetes-per-ccg-per-month" :delimiter "," :sinkmode :replace)
      (diabetes-spend-per-ccg-per-month
       (diabetes-drugs
        (prescriptions/gp-prescriptions
         (hfs-delimited "./input/prescriptions/pdpi" :delimiter ","))
        (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))))
      (:trap (stdout)))

(defn diabetes-spend-per-head-per-ccg-per-month [gp-spend gp-prevalence]
  (<- [?ccg ?year ?month ?ccg-registered-patients ?ccg-diabetes-patients ?ccg-total-net-ingredient-cost ?spend-per-head]
      (gp-spend :> ?ccg ?practice ?year ?month ?gp-total-net-ingredient-cost)
      (gp-prevalence :> ?practice ?gp-name ?gp-registered-patients ?gp-diabetes-patients ?gp-prevalence)
      (ops/sum ?gp-registered-patients :> ?ccg-registered-patients)
      (ops/sum ?gp-diabetes-patients :> ?ccg-diabetes-patients)
      (ops/sum ?gp-total-net-ingredient-cost :> ?ccg-total-net-ingredient-cost)
      (has-patients? ?ccg-diabetes-patients)
      (spend-per-head ?ccg-total-net-ingredient-cost ?ccg-diabetes-patients :> ?spend-per-head)))

;; ccg_code,year,month,registered_patients,diabetes_patients,total_spend,per_capita_spend
#_(?- (hfs-delimited "./output/diabetes-per-head-per-ccg-per-month-2011-12" :delimiter "," :sinkmode :replace)
      (diabetes-spend-per-head-per-ccg-per-month
       (diabetes-spend-per-gp-per-month
        (diabetes-drugs
         (prescriptions/gp-prescriptions
          (hfs-delimited "./input/prescriptions/pdpi" :delimiter ","))
         (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))))
       (prevalence/diabetes-prevalence-gp
        (hfs-textline "./input/diabetes-prevalence/")))
      (:trap (stdout)))

(defn total-spend-per-month-england [input]
  (<- [?month ?total-spend]
      (input :> ?ccg ?year ?month ?total-net-ingredient-cost)
      (ops/sum ?total-net-ingredient-cost :> ?total-spend-exp)
      (long ?total-spend-exp :> ?total-spend)))

;; month,total_spend
#_ (?- (hfs-delimited "./output/diabetes-total-spend-per-month-england" :delimiter "," :sinkmode :replace)
       (total-spend-per-month-england
        (diabetes-spend-per-ccg-per-month
         (diabetes-drugs
          (prescriptions/gp-prescriptions
           (hfs-delimited "./input/prescriptions/pdpi" :delimiter ","))
          (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))))))

(defn spend-per-head-stats [input]
  (<- [?max ?min ?average]
      (input :> ?ccg ?year ?month ?ccg-registered-patients ?ccg-diabetes-patients ?ccg-total-net-ingredient-cost ?spend-per-head)
      (ops/min ?spend-per-head :> ?min)
      (ops/max ?spend-per-head :> ?max)
      (ops/avg ?spend-per-head :> ?average)))

;; Mean, min and max spend per head of diabetes
#_ (?- (stdout)
       (spend-per-head-stats
        (diabetes-spend-per-head-per-ccg-per-month
          (diabetes-spend-per-gp-per-month
        (diabetes-drugs
         (prescriptions/gp-prescriptions
          (hfs-delimited "./input/prescriptions/pdpi" :delimiter ","))
         (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))))
       (prevalence/diabetes-prevalence-gp
        (hfs-textline "./input/diabetes-prevalence/")))))

(defn calculate-spend-difference [spend1 spend2]
  (- spend1 spend2))

(defn spend-difference [year1 year2]
  (<- [?ccg-code ?month ?spend-difference]
      (year1 :> ?ccg-code _ ?month  _ _ _ ?spend1-string)
      (year2 :> ?ccg-code _ ?month  _ _ _ ?spend2-string)

      (tl/numbers-as-strings? ?spend1-string)
      (tl/numbers-as-strings? ?spend2-string)
      (tl/numbers-as-strings? ?month-string)

      (tl/parse-double ?spend1-string :> ?spend1)
      (tl/parse-double ?spend2-string :> ?spend2)
      (tl/parse-double ?month-string :> ?month-raw)

      (tl/parse-date ?month-raw :> ?month)

      (calculate-spend-difference ?spend1 ?spend2 :> ?spend-difference)))

#_ (?- (hfs-delimited "./output/diabetes-per-head-per-ccg-spend-difference" :delimiter "," :sinkmode :replace)
       (spend-difference
        (hfs-delimited "./input/spend/spend_month_by_month2012.csv" :delimiter ",")
        (hfs-delimited "./input/spend/spend_month_by_month2011.csv" :delimiter ",")))
