(ns argumentica.file-atom
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io])
  (:import clojure.lang.IAtom
           java.io.PushbackReader))

(defn- write-on-disk! [{:keys [value-atom file-name]}
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
  (.write writer
          (pr-str {:value @(:value-atom on-disk-counter)
                   :file-name (:file-name on-disk-counter)})))

(defn create [file-or-file-name & [initial-value]]
  (if (.exists (io/file file-or-file-name))
    (->FileAtom (atom (edn/read (PushbackReader. (io/reader file-or-file-name))))
                file-or-file-name)
    (let [file-atom (->FileAtom (atom nil)
                                file-or-file-name)]
      (reset! file-atom initial-value)
      file-atom)))

(comment
  (reset! (create "temp/test-count")
          0)

  (create "temp/test-count2"
          (java.util.UUID/randomUUID))


  (swap! (create "temp/test-count")
         inc)

  )
