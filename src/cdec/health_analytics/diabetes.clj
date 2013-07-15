(ns cdec.health-analytics.diabetes
  (:require [cascalog.api :refer :all]
            [cascalog.ops :as ops]
            [clojure.tools.logging :refer [infof errorf]]
            [cascalog.more-taps :refer [hfs-delimited]]
            [cdec.predicates :as pred]
            [cdec.health-analytics.gp-prescriptions :as prescriptions]))

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

(defn diabetes-scrips [scrips]
  (<- [?sha ?pct ?practice ?bnf-code ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month]
      (scrips :> ?sha ?pct ?practice ?bnf-code ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month)
      (diabetes-drug? ?bnf-name)))

#_(?- (hfs-delimited "./output/diabetes-scrips/" :delimiter "," :sinkmode :replace)
      (diabetes-scrips
       (prescriptions/gp-prescriptions
        (hfs-delimited "./input/prescriptions/pdpi" :delimiter ",")))
      (:trap (stdout)))

(defn diabetes-drugs [scrips]
  (<- [?sha ?pct ?practice ?bnf-code ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month]
      (scrips :> ?sha ?pct ?practice ?bnf-code ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month)
      (diabetes-drug? ?bnf-name)
      (:distinct true)))

#_(?- (hfs-delimited "./output/diabetes-drugs" :delimiter "," :sinkmode :replace)
      (diabetes-drugs
       (prescriptions/gp-prescriptions
        (hfs-delimited "./input/prescriptions/pdpi" :delimiter ",")))
      (:trap (stdout)))

(defn diabetes-spend-per-gp-per-month [diabetes-drugs]
  (<- [?practice ?year ?month ?total-net-ingredient-cost]
      (diabetes-drugs :#> 11 {2 ?practice 6 ?net-ingredient-cost 9 ?year 10 ?month})
      (ops/sum ?net-ingredient-cost :> ?total-net-ingredient-cost)))

#_(?- (hfs-delimited "./output/diabetes-per-gppractice-per-month" :delimiter "," :sinkmode :replace)
      (diabetes-spend-per-gp-per-month
       (diabetes-drugs
        (prescriptions/gp-prescriptions
         (hfs-delimited "./input/prescriptions/pdpi" :delimiter ","))))
      (:trap (stdout)))

(defn diabetes-spend-per-ccg-per-month [diabetes-drugs]
  (<- [?ccg ?year ?month ?total-net-ingredient-cost]
      (diabetes-drugs :#> 11 {1 ?ccg 6 ?net-ingredient-cost 9 ?year 10 ?month})
      (ops/sum ?net-ingredient-cost :> ?total-net-ingredient-cost)))

#_(?- (hfs-delimited "./output/diabetes-per-ccg-per-month" :delimiter "," :sinkmode :replace)
      (diabetes-spend-per-ccg-per-month
       (diabetes-drugs
        (prescriptions/gp-prescriptions
         (hfs-delimited "./input/prescriptions/pdpi" :delimiter ","))))
      (:trap (stdout)))

(defn diabetes-spend-per-sha-per-month [diabetes-drugs]
  (<- [?sha ?year ?month ?total-net-ingredient-cost]
      (diabetes-drugs :#> 11 {0 ?sha 6 ?net-ingredient-cost 9 ?year 10 ?month})
      (ops/sum ?net-ingredient-cost :> ?total-net-ingredient-cost)))

#_(?- (hfs-delimited "./output/diabetes-per-sha-per-month" :delimiter "," :sinkmode :replace)
      (diabetes-spend-per-sha-per-month
       (diabetes-drugs
        (prescriptions/gp-prescriptions
         (hfs-delimited "./input/prescriptions/pdpi" :delimiter ","))))
      (:trap (stdout)))


