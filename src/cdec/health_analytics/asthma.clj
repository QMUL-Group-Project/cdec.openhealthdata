(ns cdec.asthma
  (:require [cascalog.api :refer :all]
            [cascalog.ops :as ops]
            [clojure.tools.logging :refer [infof errorf]]
            [cascalog.more-taps :refer [hfs-delimited]]
            [cdec.predicates :as pred]
            [clj-time.format :as tf]
            [clj-time.core :as t]
            [cdec.health-analytics.gp-prescriptions :as prescriptions]
            [cdec.health-analytics.organisational-data :as ods]
            [cdec.health-analytics.transform-load :as tl]))

#_(use 'cascalog.playground)
#_(bootstrap-emacs)

(defn asthma-drug? [bnf-code bnf-name]
  (or (pred/in? bnf-code [;; Salbutamol
                          "0301011R0" "0301040R0"
                          ;; Terbutaline
                          "0301011V0" "0701030T0" "0301040T0"
                          ;; Pirbuterol
                          "0301011J0" "0301011K0"
                          ;; Fenoterol Hydrobromide
                          "0301011F0" "0301040M0"
                          ;; Ipratropium Bromide
                          "0301020I0" "1202020I0"
                          ;; Salmeterol
                          "0301011U0"
                          ;; Bambuterol Hydrochloride
                          "0301011B0"
                          ;; Formoterol Fumarate Dihydrate
                          "301011E0"
                          ;; Beclometasone Dipropionate                                  
                          "0105020G0" "0302000C0" "1202010C0" "1304000C0"
                          ;; Budesonide
                          "0105020A0" "0302000K0" "1202010I0"
                          ;; Ciclesonide
                          "0302000U0"
                          ;; Dexamethasone Sodium Phosphate
                          "0603020H0" "1304000K0"
                          ;; Dexsol Oral Soln 2mg/5ml S/F
                          "0603020G0BEAABX"
                          ;; Fluticasone
                          "0302000N0" "1202010M0" "1202010Y0" "1304000S0"
                          ;; Hydrocortisone Sodium Phosphate
                          "0603020L0"
                          ;; Hydrocortisone Sodium Succinate
                          "0603020M0" "1203040W0" "1304000Q0"
                          ;; Methylprednisolone
                          "0603020S0" "1001022K0" "0603020K0"
                          ;; Mometasone Furoate
                          "0302000R0" "1202010U0" "1304000Y0"
                          ;; Prednisolone
                          "0603020T0" "1001022N0" "105020" "0603020V0"
                          ;; Triamcinolone                                               
                          "0603020Y0"
                          ;; Triamcinolone Acetonide
                          "0302000T0" "1001022U0" "0603020Z0"
                          ;; Zafirlukast
                          "0303020Z0"
                          ;; Montelukast
                          "0303020G0"
                          ;; Ketotifen Fumarate
                          "0303010D0" "0304010AG" "1104020Y0"
                          ;; Aminophylline
                          "0301030B0" "0301030C0"
                          ;; Nedocromil Sodium
                          "0303010J0" "1104020N0" "1202010Q0"
                          ;; Theophylline
                          "0301030S0"
                          ;; Sodium Cromoglicate
                          "0105040A0" "0303010Q0" "1104020T0" "1202010P0"
                          ;; Ipratropium Bromide
                          "0301020I0" "1202020I0"
                          ;; Omalizumab
                          "0304020X0"
                          ])
      (pred/in? bnf-name [;; kit
                          "Lancets"])))

(defn asthma-drugs [scrips epraccur]
  (<- [?ccg ?practice ?bnf-code ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month]
      (scrips :> ?sha ?pct ?practice ?bnf-code ?bnf-chemical ?bnf-name ?items ?net-ingredient-cost ?act-cost ?quantity ?year ?month)
      (asthma-drug? ?bnf-chemical ?bnf-name)
      (epraccur :#> 20 {0 ?practice 12 ?status-code 14 ?ccg})))

#_(?- (hfs-delimited "./output/asthma-drugs" :delimiter "," :sinkmode :replace)
      (asthma-drugs
       (prescriptions/gp-prescriptions
        (hfs-delimited "./input/prescriptions/pdpi" :delimiter ","))
       (ods/current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ",")))
      (:trap (stdout)))
