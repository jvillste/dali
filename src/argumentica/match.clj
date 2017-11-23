(ns argumentica.match
  (:require [clojure.string :as string]))

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
  (= desired-map
     (select-keys target-map
                  (keys desired-map))))
