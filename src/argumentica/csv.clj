(ns argumentica.csv
  (:require [clj-time.format :as format]
            [clj-time.core :as clj-time]
            [clj-time.coerce :as coerce]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.string :as string])
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

(defn strip-quotes [quote string]
  (if (and (string/starts-with? string quote)
           (string/ends-with? string "\""))
    (.substring string 1 (dec (.length string)))
    string))

(deftest test-strip-quotes
  (is (= "a" (strip-quotes "\"" "\"a\"")))
  (is (= "a" (strip-quotes "\"" "a"))))

(defn maybe-parse-number [string]
  (try
    (Integer/parseInt string)
    (catch Exception e
      (try
        (Double/parseDouble string)
        (catch Exception e
          string)))))

(defn read-line [separator quote line]
  (map (partial strip-quotes quote)
       (clojure.string/split line
                             (re-pattern separator))))

(defn read-headers [file-name separator quote]
  (first (transduce-lines file-name
                          (comp (take 1)
                                (map (partial read-line separator quote))
                                (map (fn [values]
                                       (map keyword
                                            values))))
                          conj
                          [])))

(defn pad [n coll val]
  (take n (concat coll (repeat val))))

(def default-options {:separator ";"
                      :quote "\""})

(defn transduce-maps-with-headers [file-name options headers transducer reducer initial-value]
  (let [options (merge default-options options)]
    (transduce-lines file-name
                     (comp (drop 1)
                           (map (partial read-line
                                         (:separator options)
                                         (:quote options)))
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
                               (read-headers file-name (:separator options) (:quote options))
                               transducer
                               reducer
                               initial-value))


(comment

  (transduce-maps "/Users/jukka/Downloads/title.basics.tsv"
                  {:separator #"\t"}
                  (comp (take 2)
                        (map pr))
                  conj
                  [])

  (take 2 (line-seq (io/reader "/Users/jukka/Downloads/title.basics.tsv"))))
