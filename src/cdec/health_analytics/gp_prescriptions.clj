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

(defn contains-string? [src search-term]
  (< -1 (.indexOf (string/lower-case src) (string/lower-case search-term))))

(defn humalog [scrips]
  (<- [?sha ?pct ?practice ?bnf-code ?bnf-name ?items ?nic ?act-cost ?quantity ?period]
      (scrips :> ?sha ?pct ?practice ?bnf-code ?bnf-name ?items ?nic ?act-cost ?quantity ?period)
      (contains-string? ?bnf-name "Humalog")))

#_(?- (hfs-delimited "./output/humalog/" :delimiter "," :sinkmode :replace)
      (humalog
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