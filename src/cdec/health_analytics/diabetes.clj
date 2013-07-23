(ns cdec.health-analytics.diabetes
  (:require [cascalog.api :refer :all]
            [cascalog.ops :as ops]
            [clojure.tools.logging :refer [infof errorf]]
            [cascalog.more-taps :refer [hfs-delimited]]
            [cdec.predicates :as pred]
            [cdec.health-analytics.gp-prescriptions :as prescriptions]
            [cdec.health-analytics.diabetes-prevalence :as prevalence]
            [cdec.health-analytics.organisational-data :as ods]))

#_(use 'cascalog.playground)
#_(bootstrap-emacs)

(defn diabetes-drug? [bnf-name]
  (pred/in? bnf-name [;; short acting insulin
                      "Humalog"  "Actrapid" "Humulin" "NovoRapid" "Apidra"
                      ;; intermediate and long-acting insulins
                      "Insulatard" "Lantus" "Levemir" "Mixtard" "Insuman" "NovoMix"
                      ;; Sulphonyleureas
                      "Gliclazide" "Glimepiride" "Tolbutamide"
                      ;; Biguanides
                      "Metformin"
                      ;; Repaglinide & Nateglinide
                      "Nateglinide" "Repaglinide"
                      ;; Thiazolidinediones (Glitazones)
                      "Pioglitazone" "Acarbose" "Sitagliptin" "Linagliptin"
                      ;; Exenatide
                      "Exenatide"
                      ;; Liraglutide
                      "Liraglutide"
                      ;; mypoglyceaemia
                      "Glucagon"
                      ;; kit
                      "Lancet" "Strips"]))

(defn diabetes-drugs [scrips epraccur]
  (<- [?sha ?ccg ?practice ?bnf-code ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month]
      (scrips :> ?sha ?pct ?practice ?bnf-code ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month)
      (diabetes-drug? ?bnf-name)
      (epraccur :#> 20 {0 ?practice 14 ?ccg})))

#_(?- (hfs-delimited "./output/diabetes-drugs" :delimiter "," :sinkmode :replace)
      (diabetes-drugs
       (prescriptions/gp-prescriptions
        (hfs-delimited "./input/prescriptions/pdpi" :delimiter ","))
       (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ",")))
      (:trap (stdout)))

(defn diabetes-spend-per-gp-per-month [diabetes-drugs]
  (<- [?sha ?ccg ?practice ?year ?month ?total-net-ingredient-cost]
      (diabetes-drugs :#> 11 {0 ?sha 1 ?ccg 2 ?practice 6 ?net-ingredient-cost 9 ?year 10 ?month})
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
      (gp-spend :> ?sha ?ccg ?practice ?year ?month ?total-net-ingredient-cost)
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
      (diabetes-drugs :#> 11 {1 ?ccg 6 ?net-ingredient-cost 9 ?year 10 ?month})
      (ops/sum ?net-ingredient-cost :> ?total-net-ingredient-cost)))

#_(?- (hfs-delimited "./output/diabetes-per-ccg-per-month" :delimiter "," :sinkmode :replace)
      (diabetes-spend-per-ccg-per-month
       (diabetes-drugs
        (prescriptions/gp-prescriptions
         (hfs-delimited "./input/prescriptions/pdpi" :delimiter ","))
        (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))))
      (:trap (stdout)))

(defn diabetes-spend-per-head-per-ccg-per-month [gp-spend gp-prevalence]
  (<- [?sha ?ccg ?year ?month ?ccg-registered-patients ?ccg-diabetes-patients ?ccg-total-net-ingredient-cost ?spend-per-head]
      (gp-spend :> ?sha ?ccg ?practice ?year ?month ?gp-total-net-ingredient-cost)
      (gp-prevalence :> ?practice ?gp-name ?gp-registered-patients ?gp-diabetes-patients ?gp-prevalence)
      (ops/sum ?gp-registered-patients :> ?ccg-registered-patients)
      (ops/sum ?gp-diabetes-patients :> ?ccg-diabetes-patients)
      (ops/sum ?gp-total-net-ingredient-cost :> ?ccg-total-net-ingredient-cost)
      (spend-per-head ?ccg-total-net-ingredient-cost ?ccg-diabetes-patients :> ?spend-per-head)))

(defn mean-spend-per-head [input]
  (<- [?mean-spend]
      (input :> ?sha ?ccg ?year ?month ?ccg-registered-patients ?ccg-diabetes-patients ?ccg-total-net-ingredient-cost ?spend-per-head)
      (ops/avg ?spend-per-head :> ?mean-spend)))

;; Mean spend per head of diabetes
#_ (?- (stdout)
       (mean-spend-per-head-england
        (diabetes-spend-per-head-per-ccg-per-month
          (diabetes-spend-per-gp-per-month
        (diabetes-drugs
         (prescriptions/gp-prescriptions
          (hfs-delimited "./input/prescriptions/pdpi" :delimiter ","))
         (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))))
       (prevalence/diabetes-prevalence-gp
        (hfs-textline "./input/diabetes-prevalence/")))))

;; sha,ccg_code,year,month,registered_patients,diaabetes_patients,total_spend,per_capita_spend
#_(?- (hfs-delimited "./output/diabetes-per-head-per-ccg-per-month" :delimiter "," :sinkmode :replace)
      (diabetes-spend-per-head-per-ccg-per-month
       (diabetes-spend-per-gp-per-month
        (diabetes-drugs
         (prescriptions/gp-prescriptions
          (hfs-delimited "./input/prescriptions/pdpi" :delimiter ","))
         (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))))
       (prevalence/diabetes-prevalence-gp
        (hfs-textline "./input/diabetes-prevalence/")))
      (:trap (stdout)))

(defn diabetes-spend-per-sha-per-month [diabetes-drugs]
  (<- [?sha ?year ?month ?total-net-ingredient-cost]
      (diabetes-drugs :#> 11 {0 ?sha 6 ?net-ingredient-cost 9 ?year 10 ?month})
      (ops/sum ?net-ingredient-cost :> ?total-net-ingredient-cost)))

#_(?- (hfs-delimited "./output/diabetes-per-sha-per-month" :delimiter "," :sinkmode :replace)
      (diabetes-spend-per-sha-per-month
       (diabetes-drugs
        (prescriptions/gp-prescriptions
         (hfs-delimited "./input/prescriptions/pdpi" :delimiter ","))
        (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ","))))
      (:trap (stdout)))


