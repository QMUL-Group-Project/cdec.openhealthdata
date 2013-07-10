(ns cdec.conversions
  (:require [cascalog.api :refer :all]
            [cascalog.ops :as ops]
            [cascalog.tap :as tap]
            [clojure.tools.logging :refer [infof errorf]]
            [cascalog.more-taps :refer [hfs-delimited]]))

#_(use 'cascalog.playground)
#_(bootstrap-emacs)

(defn parse-double [txt]
  (Double/parseDouble txt))

(defn numbers-as-strings? [& strings]
  (every? #(re-find #"^-?\d+(?:\.\d+)?$" %) strings))
