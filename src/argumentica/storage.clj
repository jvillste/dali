(ns argumentica.storage
  (:require (argumentica [zip :as zip])))

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



(defn edn-to-byte-array [edn]
  (zip/compress-byte-array (.getBytes (pr-str edn)
                                      "UTF-8")))


(defn key-to-storage-key [key]
  (if (string? key)
    key
    (pr-str key)))

(defn put-edn-to-storage! [storage key edn]
  (put-to-storage! storage
                           (key-to-storage-key key)
                           (edn-to-byte-array edn)))

(defn safely-read-string [string]
  (binding [*read-eval* false]
    (read-string string)))

(defn byte-array-to-edn [byte-array]
  (try 
    (safely-read-string (String. (zip/uncompress-byte-array byte-array)
                                 "UTF-8"))
    (catch Exception e
      (prn (String. (zip/uncompress-byte-array byte-array)
                    "UTF-8"))
      (throw e))))

(defn get-edn-from-storage! [storage key]
  (if-let [byte-array (get-from-storage! storage
                                                 (key-to-storage-key key))]
    (byte-array-to-edn byte-array)
    nil))
