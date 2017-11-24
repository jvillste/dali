(ns argumentica.match
  (:require [clojure.string :as string]
            [clojure.data :as data]))

(defmacro def-matcher [name & body]
  (let [type-name (symbol (string/capitalize name))]
    `(do 
       (deftype ~type-name []
         Object
         (equals ~@body))

       (defmethod print-method ~type-name
         [this# writer#] (.write writer# ~(str name)))

       (def ~name (~(symbol (str type-name ".")))))))

(def-matcher any-string [_ value] (string? value))
(def-matcher any-function [_ value] (fn? value))


(defn contains-map? [desired-map target-map]
  (let [diff (data/diff desired-map
                        (select-keys target-map
                                     (keys desired-map)))]
    (if (empty? (first diff))
      true
      (do (prn (take 2 diff))
          false))))
