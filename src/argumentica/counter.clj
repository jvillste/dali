(ns argumentica.counter
  (:require [clojure.test :refer [deftest is]]
            [clojure.java.io :as io]))

(defprotocol Counter
  (add! [this value]))

(defrecord InMemoryCounter [count-atom]
  clojure.lang.IDeref
  (deref [this] @count-atom)

  Counter
  (add! [this value] (swap! count-atom + value)))

(defmethod print-method InMemoryCounter [in-memory-counter ^java.io.Writer w]
  (.write w (pr-str {:count @(:count-atom in-memory-counter)})))


(defn in-memory [initial-value]
  (->InMemoryCounter (atom initial-value)))

(deftest test-in-memory
  (let [counter (in-memory 0)]
    (is (= 0 @counter))
    (is (= 1 (add! counter 1)))
    (is (= 2 (add! counter 1)))))


(defrecord OnDiskCounter [count-atom file-name]
  clojure.lang.IDeref
  (deref [this] @count-atom)

  Counter
  (add! [this value]
    (locking count-atom
      (let [next-value (swap! count-atom + value)]
        (spit file-name (str next-value))
        next-value))))


(defmethod print-method OnDiskCounter [on-disk-counter ^java.io.Writer w]
  (.write w (pr-str {:count @(:count-atom on-disk-counter)
                     :file-name (:file-name on-disk-counter)})))

(defn on-disk [initial-value file-name]
  (->OnDiskCounter (atom (if (.exists (io/file file-name))
                           (read-string (slurp file-name))
                           initial-value))
                   file-name))

(comment
  (let [counter (on-disk 0 "temp/test-count")]
    (add! counter 100))
  )
