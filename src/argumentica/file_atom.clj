(ns argumentica.file-atom
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import clojure.lang.IAtom
           java.io.PushbackReader))

(defn write-on-disk! [{:keys [value-atom file-name]}
                      value]
  (spit file-name (pr-str value))
  (reset! value-atom value))

(defrecord FileAtom [value-atom file-name]
  clojure.lang.IDeref
  (deref [this] @value-atom)

  IAtom
  (swap [this function]
    (locking this
      (write-on-disk! this (function @value-atom))))

  (swap [this function & arguments]
    (locking this
      (write-on-disk! this (apply function @value-atom arguments))))

  (reset [this value]
    (locking this
      (write-on-disk! this value))))

(defmethod print-method FileAtom [on-disk-counter
                                  ^java.io.Writer writer]
  (.write writer (pr-str {:value @(:value-atom on-disk-counter)
                          :file-name (:file-name on-disk-counter)})))

(defn create [file-name & [initial-value]]
  (->FileAtom (atom (if (.exists (io/file file-name))
                      (edn/read (PushbackReader. (io/reader file-name)))
                      initial-value))
              file-name))

(comment
  (reset! (create "temp/test-count")
          0)

  (swap! (create "temp/test-count")
         inc)

  )
