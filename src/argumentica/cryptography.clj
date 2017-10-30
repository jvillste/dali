(ns argumentica.cryptography
  (:import [java.security MessageDigest]))

(defn sha-256 [bytes]
  (.digest (MessageDigest/getInstance "SHA-256")
           bytes))

(defn sha-1 [bytes]
  (.digest (MessageDigest/getInstance "SHA-1")
           bytes))
