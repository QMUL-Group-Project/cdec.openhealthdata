(ns cdec.predicates
  (:require [clojure.string :as string]))

(defn in? [src terms]
  (re-find (re-pattern (string/join \| terms)) src))
