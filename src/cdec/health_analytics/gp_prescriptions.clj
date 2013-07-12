(ns cdec.health-analytics.gp-prescriptions
  (:require [cascalog.api :refer :all]
            [cascalog.ops :as ops]
            [cascalog.tap :as tap]
            [clojure.string :as string]
            [clojure.tools.logging :refer [infof errorf]]
            [cascalog.more-taps :refer [hfs-delimited]]))

#_(use 'cascalog.playground)
#_(bootstrap-emacs)

(defn gp-prescriptions [in]
  (<- [?sha ?pct ?practice ?bnf_code ?bnf_name ?items ?nic ?act_cost ?quantity ?period]
      (in :> ?sha ?pct ?practice ?bnf_code ?bnf_name ?items ?nic ?act_cost ?quantity ?period _)))

(defn contains-string? [search-term src]
  (< -1 (.indexOf (string/lower-case src) (string/lower-case search-term))))

(defn in? [src terms]
  (re-find (re-pattern (clojure.string/join \| terms)) src))

(defn diabetes-drug? [bnf-name]
  (in? bnf-name [;; short acting insulin
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

(defn humalog [scrips]
  (<- [?sha ?pct ?practice ?bnf-code ?bnf-name ?items ?nic ?act-cost ?quantity ?period]
      (scrips :> ?sha ?pct ?practice ?bnf-code ?bnf-name ?items ?nic ?act-cost ?quantity ?period)
      (contains-string? "Humalog" ?bnf-name)))

#_(?- (hfs-delimited "./output/humalog/" :delimiter "," :sinkmode :replace)
      (humalog
       (gp-prescriptions
        (hfs-delimited "./input/prescriptions/pdpi" :delimiter ",")))
      (:trap (stdout)))

(defn diabetes-scrips [scrips]
  (<- [?sha ?pct ?practice ?bnf-code ?bnf-name ?items ?nic ?act-cost ?quantity ?period]
      (scrips :> ?sha ?pct ?practice ?bnf-code ?bnf-name ?items ?nic ?act-cost ?quantity ?period)
      (diabetes-drug? ?bnf-name)))

#_(?- (hfs-delimited "./output/humalog/" :delimiter "," :sinkmode :replace)
      (diabetes-scrips
       (gp-prescriptions
        (hfs-delimited "./input/prescriptions/pdpi" :delimiter ",")))
      (:trap (stdout)))

(defn diabetes-drugs [scrips]
  (<- [?bnf-code ?bnf-name]
      (scrips :#> 10 {3 ?bnf-code 4 ?bnf-name})
      (diabetes-drug? ?bnf-name)
      (:distinct true)))

#_(?- (hfs-delimited "./output/diabetes-drugs" :delimiter "," :sinkmode :replace)
      (diabetes-drugs
       (gp-prescriptions
        (hfs-delimited "./input/prescriptions/pdpi" :delimiter ",")))
      (:trap (stdout)))

;; 0 sha
;; 1 pct
;; 2 practice
;; 3 bnf-code
;; 4 bnf-name
;; 5 items
;; 6 nic
;; 7 act-cost
;; 8 quantity
;; 9 period