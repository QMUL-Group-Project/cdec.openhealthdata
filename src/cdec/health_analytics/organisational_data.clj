(ns cdec.health-analytics.organisational-data
  (:require [cascalog.api :refer :all]
            [cascalog.ops :as ops]
            [cascalog.tap :as tap]
            [clojure.string :as string]
            [clojure.tools.logging :refer [infof errorf]]
            [cascalog.more-taps :refer [hfs-delimited]]
            [cdec.conversions :as conv]))

#_(use 'cascalog.playground)
#_(bootstrap-emacs)

;; http://systems.hscic.gov.uk/data/ods/supportinginfo/filedescriptions#_Toc350757591
(defn current-practices [epraccur]
  (<- [?organisation-code ?name !national-grouping !high-level-health-authority
       !address-1 !address-2 !address-3 !address-4 !address-5 !postcode
       ?open-date !close-date ?status-code ?org-sub-type-code
       !parent-org-code !join-parent-date !left-parent-date
       !contact-telephone ?amend-record-indicator ?practice-type]
      (epraccur :#> 27 {0 ?organisation-code 1 ?name 2 !national-grouping 3 !high-level-health-authority
                        4 !address-1 5 !address-2 6 !address-3 7 !address-4 8 !address-5 9 !postcode
                        10 ?open-date 11 !close-date 12 ?status-code 13 ?org-sub-type-code
                        14 !parent-org-code 15 !join-parent-date 16 !left-parent-date
                        17 !contact-telephone 21 ?amend-record-indicator 25 ?practice-type})))

#_(?- (hfs-delimited "./output/epraccur" :delimiter "," :sinkmode :replace)
      (current-practices (hfs-delimited "./input/ods/gppractice/epraccur.csv" :delimiter ",")))

;; Output Format
;; 0 ?organisation-code
;; 1 ?name
;; 2 !national-grouping
;; 3 !high-level-health-authority
;; 4 !address-1
;; 5 !address-2
;; 6 !address-3
;; 7 !address-4
;; 8 !address-5
;; 9 !postcode
;; 10 ?open-date
;; 11 !close-date
;; 12 ?status-code
;; 13 ?org-sub-type-code
;; 14 !parent-org-code
;; 15 !join-parent-date
;; 16 !left-parent-date
;; 17 !contact-telephone
;; 18 ?amend-record-indicator
;; 19 ?practice-type

;; Original File
;; | Field Num | Name                        | Length | Required | Notes                                                                                                                 |
;; |         1 | Organisation Code           |      6 | Yes      |                                                                                                                       |
;; |         2 | Name                        |     50 | Yes      |                                                                                                                       |
;; |         3 | National Grouping           |      3 | No       | Linked Geographically n/a for Channel Islands or Isle of Man                                                          |
;; |         4 | High Level Health Authority |      3 | No       | Linked Geographically n/a for Channel Islands or Isle of Man                                                          |
;; |         5 | Address Line 1              |     35 | No       |                                                                                                                       |
;; |         6 | Address Line 2              |     35 | No       |                                                                                                                       |
;; |         7 | Address Line 3              |     35 | No       |                                                                                                                       |
;; |         8 | Address Line 4              |     35 | No       |                                                                                                                       |
;; |         9 | Address Line 5              |     35 | No       |                                                                                                                       |
;; |        10 | Postcode                    |      8 | No       |                                                                                                                       |
;; |        11 | Open Date                   |      8 | Yes      |                                                                                                                       |
;; |        12 | Close Date                  |      8 | No       |                                                                                                                       |
;; |        13 | Status Code                 |      1 | Yes      | A = Active C = Closed D = Dormant P = Proposed                                                                        |
;; |        14 | Organisation Sub-Type Code  |      1 | Yes      | B = Allocated to a Parent Organisation Z = Not allocated to a Parent Organisation                                     |
;; |        15 | Parent Organisation Code    |      3 | No       | Code for the Primary Care Organisation the GP Practice is linked to                                                   |
;; |        16 | Join Parent Date            |      8 | No       |                                                                                                                       |
;; |        17 | Left Parent Date            |      8 | No       |                                                                                                                       |
;; |        18 | Contact Telephone Number    |     12 | No       |                                                                                                                       |
;; |        19 | Null                        |      0 | No       |                                                                                                                       |
;; |        20 | Null                        |      0 | No       |                                                                                                                       |
;; |        21 | Null                        |      0 | No       |                                                                                                                       |
;; |        22 | Amended Record Indicator    |      1 | Yes      |                                                                                                                       |
;; |        23 | Null                        |      0 | No       |                                                                                                                       |
;; |        24 | Null                        |      0 | No       |                                                                                                                       |
;; |        25 | Null                        |      0 | No       |                                                                                                                       |
;; |        26 | Practice Type               |      1 | Yes      | 0 = Other 1 = WIC Practice 2 = OOH Practice 3 = WIC + OOH Practice 4 = GP Practice 5 = Prison Prescribing Cost Centre |
;; |        27 | Null                        |      0 | No       |                                                                                                                       |
