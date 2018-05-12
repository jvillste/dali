(ns argumentica.storage
  (:require (argumentica [zip :as zip])
            [taoensso.nippy :as nippy]))

(defmulti put-to-storage!
  (fn [storage key value]
    (type storage)))

(defmulti get-from-storage!
  (fn [storage key]
    (type storage)))

(defmulti remove-from-storage!
  (fn [storage key]
    (type storage)))

(defmulti storage-keys!
  (fn [storage]
    (type storage)))

(defmulti storage-contains?
  (fn [storage key]
    (type storage)))



(defn edn-to-byte-array [edn]
  (zip/compress-byte-array (.getBytes (pr-str edn)
                                      "UTF-8")))


(defn key-to-storage-key [key]
  (if (string? key)
    key
    (pr-str key)))

(defn edn-to-bytes [edn]
  (nippy/freeze edn))

(defn put-edn-to-storage! [storage key edn]
  (put-to-storage! storage
                   (key-to-storage-key key)
                   (edn-to-bytes edn)
                   #_(edn-to-byte-array edn)))

(defn safely-read-string [string]
  (binding [*read-eval* false]
    (read-string string)))

(defn bytes-to-string [string]
  (String. string
           "UTF-8"))

(defn string-to-bytes [string]
  (.getBytes string
             "UTF-8"))

(defn byte-array-to-edn [byte-array]
  (try
    (safely-read-string (String. (zip/uncompress-byte-array byte-array)
                                 "UTF-8"))
    (catch Exception e
      (prn (String. (zip/uncompress-byte-array byte-array)
                    "UTF-8"))
      (throw e))))

(defn bytes-to-edn [bytes]
  (prn bytes)
  (nippy/thaw bytes))

(defn get-edn-from-storage! [storage key]
  (if-let [byte-array (get-from-storage! storage
                                         (key-to-storage-key key))]
    (bytes-to-edn #_byte-array-to-edn byte-array)
    nil))

(comment
  (count (nippy/thaw (nippy/freeze [(java.util.UUID/randomUUID) :friend 120 :set (java.util.UUID/randomUUID)]))))
