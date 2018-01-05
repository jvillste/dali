(ns argumentica.EdnComparator
  (:require (argumentica [storage :as storage]
                         [comparator :as comparator]))
  (:gen-class :implements [java.util.Comparator]
              :prefix "method-"))

(defn method-compare [this x y]
  (comparator/cc-cmp (storage/bytes-to-edn x)
                     (storage/bytes-to-edn y)))

(comment
  (.compare (argumentica.EdnComparator.)
            (storage/edn-to-bytes 1)
            (storage/edn-to-bytes 1)))
