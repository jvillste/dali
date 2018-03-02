(ns argumentica.encode
  (:import [com.google.common.io BaseEncoding]))

(defn base-64-encode [bytes]
  (.encode (BaseEncoding/base64)
           bytes))

(defn base-64-decode [string]
  (.decode (BaseEncoding/base64)
           string))

(defn base-16-encode [bytes]
  (.encode (BaseEncoding/base16)
           bytes))

(defn string-to-bytes [string]
  (.getBytes string "UTF-8"))

(defn bytes-to-string [bytes]
  (.String bytes "UTF-8"))
