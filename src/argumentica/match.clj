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

(def-matcher any-string [_ value] (string? value))
(def-matcher any-function [_ value] (fn? value))


