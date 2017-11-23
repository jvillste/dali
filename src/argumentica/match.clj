(ns argumentica.match
  (:require [clojure.string :as string]))

(defmacro def-matcher [name & body]
  (let [type-name (symbol (string/capitalize name))]
    `(do 
       (deftype ~type-name []
         Object
         (equals ~@body))

       (defmethod print-method ~type-name
         [this# w#] (.write w# ~(str name)))

       (def ~name (~(symbol (str type-name ".")))))))




#_(do
    (deftype
        Any-string
        []
        java.lang.Object
        (equals [_ value] (string? value)))
    (defmethod print-method Any-string [this w] (.write w "any-string"))
    (def any-string (Any-string.)))

(def-matcher any-string [_ value] (string? value))

(def-matcher any-function [_ value] (fn? value))


