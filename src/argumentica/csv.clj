(ns argumentica.csv
  (:require [clj-time.format :as format]
            [clj-time.core :as clj-time]
            [clj-time.coerce :as coerce]
            [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:use clojure.test))

(defn transduce-lines
  ([file-name transducer]
   (transduce-lines file-name transducer (constantly nil) nil))

  ([file-name transducer reducer]
   (transduce-lines file-name transducer reducer (reducer)))

  ([file-name transducer reducer initial-value]
   (with-open [rdr (clojure.java.io/reader file-name)]
     (let [reducing-function (transducer reducer)]
       (loop [lines (line-seq rdr)
              value initial-value]

         (if-let [line (first lines)]
           (let [result (reducing-function value
                                           line)]
             (if (reduced? result)
               (do (reducing-function @result))
               (recur (rest lines)
                      result)))
           (reducing-function value)))))))


(defn read-value [string]
  (if (= "" string)
    ""
    (let [value (try (edn/read-string string)
                     (catch Exception e
                       nil))]
      (if (number? value)
        value
        string))))

(defn split-line [separator string]
  (clojure.string/split string  separator))

(defn read-line [separator line]
  (map read-value
       (split-line separator line)))

(defn read-headers [file-name separator]
  (first (transduce-lines file-name
                          (comp (take 1)
                                (map (partial read-line separator))
                                (map (fn [values]
                                       (map keyword
                                            values))))
                          conj
                          [])))

(defn pad [n coll val]
  (take n (concat coll (repeat val))))

(def default-options {:separator #";"})

(defn transduce-maps-with-headers [file-name options headers transducer reducer initial-value]
  (let [options (merge default-options options)]
    (transduce-lines file-name
                     (comp (drop 1)
                           (map (partial read-line (:separator options)))
                           (map (fn [values]
                                  (dissoc (apply hash-map (interleave headers (pad (count headers)
                                                                            values
                                                                            nil)))
                                          nil)))
                           transducer)
                     reducer
                     initial-value)))

(defn transduce-maps [file-name options transducer reducer initial-value]
  (transduce-maps-with-headers file-name
                               options
                               (read-headers file-name (:separator options))
                               transducer
                               reducer
                               initial-value))


(comment


  (transduce-lines "/Users/jukka/Downloads/title.basics.tsv"
                   (comp (map (partial split-line #"\t"))
                         #_(map (partial read-line "\t"))
                         (take 2))
                   conj
                   [])

  (transduce-maps "/Users/jukka/Downloads/title.basics.tsv"
                  {:separator #"\t"}
                  (comp (take 2)
                        (map pr))
                  conj
                  [])

  (take 2 (line-seq (io/reader "/Users/jukka/Downloads/title.basics.tsv"))))
