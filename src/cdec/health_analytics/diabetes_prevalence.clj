(ns cdec.health-analytics.diabetes-prevalence
  (:require [cascalog.api :refer :all]
            [cascalog.ops :as ops]
            [cascalog.tap :as tap]
            [cascalog.more-taps :refer [hfs-delimited]]
            [clojure.string :as s]
            [clojure.java.io :as io]
            [cdec.health-analytics.transform-load :as tl]
            [cdec.health-analytics.gp-total-registered :as gpt]
            [clojure.tools.logging :refer [infof errorf]]
            [clojure.math.numeric-tower :as math]
            [cdec.health-analytics.gp-outcomes :as gpo]
            [cdec.health-analytics.organisational-data :as ods]))

(defn calc-no-patients [percentile total-gp-patients]
  (-> (* total-gp-patients percentile)
      (/ 100)
      (math/floor)))

(defn calculate-percentage [value1 value2]
 ;; (infof "Calculating percentage: %s %s" value1 value2)
  (-> (/ value1 value2)
      (* 100)))

(defn scrub-data [txt]
  (-> (s/replace txt #"%" "")
      (s/replace #"," "")))

(defn gp-ccg-mapping [epraccur]
  (<- [?gp-code ?ccg-code ?ccg-name]
      (tl/split-line ?line :#> 5 {0 ?gp-code 2 ?ccg-code 3 ?ccg-name})))

(defn diabetes-prevalence-gp [input]
  (<- [?gp-code ?gp-name ?registered-patients ?diabetes-patients ?prevalence]
      (input ?line)
      (tl/data-line? ?line)
      (tl/split-line ?line :#> 9 {4 ?gp-code 5 ?gp-name 6 ?registered-patients-dirty 7 ?diabetes-patients-dirty 8 ?prevalence-dirty})

      (scrub-data ?registered-patients-dirty :> ?registered-patients-string)
      (scrub-data ?diabetes-patients-dirty :> ?diabetes-patients-string)
      (scrub-data ?prevalence-dirty :> ?prevalence-string)

      (tl/numbers-as-strings? ?registered-patients-string)
      (tl/numbers-as-strings? ?diabetes-patients-string)
      (tl/numbers-as-strings? ?prevalence-string)

      (tl/parse-double ?registered-patients-string :> ?registered-patients)
      (tl/parse-double ?diabetes-patients-string :> ?diabetes-patients)
      (tl/parse-double ?prevalence-string :> ?prevalence)))

(defn total-diabetes-patients [input]
  (<- [?registered-patients ?diabetes-patients ?prevalence]
      (input :> ?gp-code ?gp-name ?gp-registered-patients ?gp-diabetes-patients ?gp-prevalence)
      (ops/sum ?gp-registered-patients :> ?registered-patients-exp)
      (long ?registered-patients-exp :> ?registered-patients)
      (ops/sum ?gp-diabetes-patients :> ?diabetes-patients)
      (calculate-percentage ?diabetes-patients ?registered-patients :> ?prevalence)))

(defn mean-prevalence-gp [input]
  (<- [?mean-prevalence]
      (input :> ?gp-code ?gp-name ?registered-patients ?diabetes-patients ?prevalence)
      (ops/avg ?prevalence :> ?mean-prevalence)))

(defn diabetes-prevalence-ccg [epraccur gp-prevalence ccglist]
  (<- [?ccg-code ?ccg-name ?ccg-registered-patients ?ccg-diabetes-patients ?ccg-prevalence]

      (gp-prevalence :> ?gp-code ?gp-name ?gp-registered-patients ?gp-diabetes-patients ?gp-prevalence)

      (epraccur :#> 20 {0 ?gp-code 14 ?ccg-code})

      (ccglist :> _ _ ?ccg-code ?ccg-name)

      (ops/sum ?gp-registered-patients :> ?ccg-registered-patients)
      (ops/sum ?gp-diabetes-patients :> ?ccg-diabetes-patients)
      (calculate-percentage ?ccg-diabetes-patients ?ccg-registered-patients :> ?ccg-prevalence)))

(defn mean-prevalence-ccg [input]
  (<- [?mean-prevalence]
      (input :> ?ccg-code ?ccg-name ?ccg-registered-patients ?ccg-diabetes-patients ?ccg-prevalence)
      (ops/avg ?ccg-prevalence :> ?mean-prevalence)))

(defn gp-percentage-within-ccg [gp-join-ccg-prevalence ccg-prevalence]
  (<- [?gp-code ?gp-name ?gp-registered-patients ?gp-diabetes-patients ?gp-prevalence ?ccg-code ?ccg-name ?ccg-registered-patients ?ccg-diabetes-patients ?ccg-prevalence ?gp-ccg-prevalence]
      (gp-join-ccg-prevalence :> ?gp-code ?gp-name ?ccg-code ?gp-registered-patients ?gp-diabetes-patients ?gp-prevalence)
      (ccg-prevalence :> ?ccg-code ?ccg-name ?ccg-registered-patients ?ccg-diabetes-patients ?ccg-prevalence)
      (calculate-percentage ?gp-diabetes-patients ?ccg-diabetes-patients :> ?gp-ccg-prevalence)
      ))

(defn top-n [input n order]
  (<- [?gp-code-out ?gp-name-out ?gp-registered-out ?gp-percentage-out]
      (input :> ?gp-code ?gp-name ?gp-registered ?gp-prevalence ?gp-percentage)
      (:sort ?gp-percentage)
      (:reverse order)
      (ops/limit [n] ?gp-code ?gp-name ?gp-registered ?gp-percentage :> ?gp-code-out ?gp-name-out ?gp-registered-out ?gp-percentage-out)))

(defn top-n-per-ccg [input n order]
  (<- [?ccg-code-out ?gp-code-out ?gp-name-out ?gp-prevalence-out ?r]
      (input :> ?gp-code ?gp-name ?gp-registered-patients ?gp-diabetes-patients ?gp-prevalence ?ccg-code-out ?ccg-name ?ccg-registered-patients ?ccg-diabetes-patients ?ccg-prevalence ?gp-ccg-prevalence)
      (:sort ?gp-prevalence)
      (:reverse order)
      (ops/limit-rank [n] ?gp-code ?gp-name ?gp-prevalence :> ?gp-code-out ?gp-name-out ?gp-prevalence-out ?r)))

(defn top-n-ccg [input n order]
  (<- [?ccg-code-out ?ccg-name-out ?ccg-registered-patients-out ?ccg-prevalence-out]
      (input :> ?ccg-code ?ccg-name ?ccg-registered-patients ?ccg-diabetes-patients ?ccg-prevalence)
      (:sort ?ccg-prevalence)
      (:reverse order)
      (ops/limit [n] ?ccg-code ?ccg-name ?ccg-registered-patients ?ccg-prevalence :> ?ccg-code-out ?ccg-name-out ?ccg-registered-patients-out ?ccg-prevalence-out)))

(defn join-gp-with-ccg [gp-prevalence epraccur]
  (<- [?gp-code ?gp-name ?ccg-code ?gp-registered-patients ?gp-diabetes-patients ?gp-prevalence]
      (gp-prevalence :> ?gp-code ?gp-name ?gp-registered-patients ?gp-diabetes-patients ?gp-prevalence)
      (epraccur :#> 20 {0 ?gp-code 14 ?ccg-code})))


;; Prevalence per GP
#_(let [data-out "./output/gp_prevalence/"
        data-in "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv"]
    (?- (hfs-delimited data-out :sinkmode :replace :delimiter ",")
        (diabetes-prevalence-gp (hfs-textline data-in))))

;; Prevalence per CCG
;; CCG list file: http://www.england.nhs.uk/resources/ccg-directory/
#_(let [data-in1 "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv"
        data-in2 "./input/ods/gppractice/epraccur.csv"
        data-in3 "./input/ods/ccglist/ccg-lsoa.csv"
        data-out "./output/ccg_prevalence/"]
    (?- (hfs-delimited data-out :sinkmode :replace :delimiter ",")
        (diabetes-prevalence-ccg
         (ods/current-practices (hfs-delimited data-in2 :delimiter ","))
         (diabetes-prevalence-gp (hfs-textline data-in1))
         (hfs-delimited data-in3 :delimiter ","))))

;; Total number of diabetes patients in England and prevalence
#_ (let [data-in "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv"]
     (?- (stdout)
         (total-diabetes-patients
          (diabetes-prevalence-gp (hfs-textline data-in)))))

;; Average diabetes prevalence per GP
#_ (let [data-in "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv"]
     (?- (stdout)
         (mean-prevalence-gp
          (diabetes-prevalence-gp (hfs-textline data-in)))))

;; Average diabetes prevalence per CCG
#_ (let [data-in1 "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv"
         data-in2 "./input/ods/gppractice/epraccur.csv"
         data-in3 "./input/ods/ccglist/ccg-lsoa.csv"]
     (?- (stdout)
         (mean-prevalence-ccg
          (diabetes-prevalence-ccg
           (ods/current-practices (hfs-delimited data-in2 :delimiter ","))
           (diabetes-prevalence-gp (hfs-textline data-in1))
           (hfs-delimited data-in3 :delimiter ",")))))

;; Left outer join of gp and ccg data
#_ (let [data-in1 "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv"
         data-in2"./input/ods/gppractice/epraccur.csv"
         data-out "./output/gp-joined-with-ccg/"]
     (?- (hfs-delimited data-out :sinkmode :replace :delimiter ",")
         (join-gp-with-ccg
          (diabetes-prevalence-gp (hfs-textline data-in1))
          (ods/current-practices (hfs-delimited data-in2 :delimiter ",")))))

;; Percentage of patients in each GP that constitutes patients in CCG
#_ (let [data-in1 "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv"
         data-in2 "./input/ods/gppractice/epraccur.csv"
         data-out "./output/gp-ccg-prevalence/"
         data-in3 "./input/ods/ccglist/ccg-lsoa.csv"]
     (?- (hfs-delimited data-out :sinkmode :replace :delimiter ",")
         (gp-percentage-within-ccg
          (join-gp-with-ccg
           (diabetes-prevalence-gp (hfs-textline data-in1))
           (ods/current-practices (hfs-delimited data-in2 :delimiter ",")))
          (diabetes-prevalence-ccg
           (ods/current-practices (hfs-delimited data-in2 :delimiter ","))
           (diabetes-prevalence-gp (hfs-textline data-in1))
           (hfs-delimited data-in3 :delimiter ",")))))

;; Number n of high and low GP surgeries per CCG
#_(let [data-in1 "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv"
        data-in2 "./input/ods/gppractice/epraccur.csv"
        data-in3 "./input/ods/ccglist/ccg-lsoa.csv"
        data-out "./output/low-surgeries-per-ccg/"]
    (?- (hfs-delimited data-out :sinkmode :replace :delimiter ",")
        (top-n-per-ccg
         (gp-percentage-within-ccg
          (join-gp-with-ccg
           (diabetes-prevalence-gp (hfs-textline data-in1))
           (ods/current-practices (hfs-delimited data-in2 :delimiter ",")))
          (diabetes-prevalence-ccg
           (ods/current-practices (hfs-delimited data-in2 :delimiter ","))
           (diabetes-prevalence-gp (hfs-textline data-in1))
           (hfs-delimited data-in3 :delimiter ",")))
         10 false)))

;; Number n of high and low GP surgeries per CCG
#_(let [data-in1 "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv"
        data-in2 "./input/ods/gppractice/epraccur.csv"
        data-in3 "./input/ods/ccglist/ccg-lsoa.csv"
        data-out "./output/high-surgeries-per-ccg/"]
    (?- (hfs-delimited data-out :sinkmode :replace :delimiter ",")
        (top-n-per-ccg
          (gp-percentage-within-ccg
          (join-gp-with-ccg
           (diabetes-prevalence-gp (hfs-textline data-in1))
           (ods/current-practices (hfs-delimited data-in2 :delimiter ",")))
          (diabetes-prevalence-ccg
           (ods/current-practices (hfs-delimited data-in2 :delimiter ","))
           (diabetes-prevalence-gp (hfs-textline data-in1))
           (hfs-delimited data-in3 :delimiter ",")))
         10 true)))

;; Top 10 GP surgeries in all England
#_ (let [data-in "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv"
         data-out "./output/top-10-gp-england/"]
     (?- (hfs-delimited data-out :sinkmode :replace :delimiter ",")
         (top-n
          (diabetes-prevalence-gp (hfs-textline data-in))
          10 false)))

;; Bottom 10 GP surgeries in all England
#_ (let [data-in "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv"
         data-out "./output/bottom-10-gp-england/"]
     (?- (hfs-delimited data-out :sinkmode :replace :delimiter ",")
         (top-n
          (diabetes-prevalence-gp (hfs-textline data-in))
          10 true)))

#_ (let [data-in1 "./input/QOF1011_Pracs_Prevalence_DiabetesMellitus.csv"
         data-in2 "./input/ods/gppractice/epraccur.csv"
         data-in3 "./input/ods/ccglist/ccg-lsoa.csv"
         data-out "./output/top-10-ccg/"]
     (?- (hfs-delimited data-out :sinkmode :replace :delimiter ",")
         (top-n-ccg
         (diabetes-prevalence-ccg
           (ods/current-practices (hfs-delimited data-in2 :delimiter ","))
           (diabetes-prevalence-gp (hfs-textline data-in1))
           (hfs-delimited data-in3 :delimiter ","))
         10 false)))
