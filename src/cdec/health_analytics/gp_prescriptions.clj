(ns cdec.health-analytics.gp-prescriptions
  (:require [cascalog.api :refer :all]
            [cascalog.ops :as ops]
            [cascalog.tap :as tap]
            [clojure.string :as string]
            [clojure.tools.logging :refer [infof errorf]]
            [cascalog.more-taps :refer [hfs-delimited]]
            [cdec.conversions :as conv]))

#_(use 'cascalog.playground)
#_(bootstrap-emacs)

(defn year-month [period]
  [(.substring period 0 4) (.substring period 4 6)])

(defn split-bnf-code [bnf-code]
  (if (> (count bnf-code) 8)
                     (subs bnf-code 0 9)
                   bnf-code))

(defn gp-prescriptions [in]
  (<- [?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month]
      (in :> ?sha ?pct ?practice ?bnf-code ?bnf-name ?items-string ?net-ingredient-cost-string ?act-cost-string ?quantity-string ?period _)
      (split-bnf-code ?bnf-code :> ?bnf-chemical)
      (conv/numbers-as-strings? ?items-string ?net-ingredient-cost-string ?act-cost-string ?quantity-string)
      (conv/parse-double ?items-string :> ?items)
      (conv/parse-double ?net-ingredient-cost-string :> ?net-ingredient-cost)
      (conv/parse-double ?act-cost-string :> ?act-cost)
      (conv/parse-double ?quantity-string :> ?quantity)
      (year-month ?period :> ?year ?month)))

#_(?- (hfs-delimited "./output/gp-prescriptions/" :delimiter "," :sinkmode :replace)
      (humalog
       (gp-prescriptions
        (hfs-delimited "./input/prescriptions/pdpi" :delimiter ",")))
      (:trap (stdout)))

;; Data files from:
;; http://www.hscic.gov.uk/searchcatalogue?q=title:%22GP+Practice+Prescribing+Presentation-Level+Data%22&area=&size=100&sort=Relevance

;; 0 sha
;; 1 pct
;; 2 practice
;; 3 bnf-code
;; 4 bnf-name
;; 5 items
;; 6 net-ingredient-cost
;; 7 act-cost
;; 8 quantity

;; 9 period

;; 9 year
;; 10 month
