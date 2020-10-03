(ns argumentica.node-serialization
  (:require [clojure.java.io :as io]
            [taoensso.nippy :as nippy]
            [clojure.test :refer :all])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream DataInputStream DataOutputStream EOFException]
           clojure.lang.IReduceInit
           clojure.lang.IReduce))

(defn- with-data-output-stream [function]
  (with-open [byte-array-output-stream (ByteArrayOutputStream.)
              data-output-stream (DataOutputStream. byte-array-output-stream)]
    (function data-output-stream)
    (.toByteArray byte-array-output-stream)))

(deftest test-with-data-output-stream
  (is (= [0 0 0 5]
         (into [] (with-data-output-stream (fn [data-output-stream]
                                             (.writeInt data-output-stream 5)))))))

(defn- metadata [node]
  (merge {:count (count (:values node))}
         (if (:children node)
           {:children (mapv :storage-key (:children node))}
           {})))

(deftest test-metadata
  (is (= {:children ["1" "2"], :count 1}
         (metadata {:children [{:values #{1}
                                :storage-key "1"}
                               {:values #{3}
                                :storage-key "2"}]
                    :values #{2}})))

  (is (= {:count 1}
         (metadata {:values #{2}}))))

(defn- write-value-to-data-output-stream [data-output-stream value]
  (let [byte-array (nippy/freeze value)
        length (alength byte-array)]
    (.writeInt data-output-stream length)
    (.write data-output-stream byte-array 0 length)))

(defn- read-value-from-data-input-stream [data-input-stream]
  (let [buffer (byte-array (.readInt data-input-stream))]
    (.read data-input-stream buffer)
    (nippy/thaw buffer)))

(defn serialize [node]
  (with-data-output-stream
    (fn [data-output-stream]
      (write-value-to-data-output-stream data-output-stream (metadata node))
      (write-value-to-data-output-stream data-output-stream (:values node)))))

(defn deserialize [input-stream]
  (with-open [data-input-stream (DataInputStream. input-stream)]
    (let [children (:children (read-value-from-data-input-stream data-input-stream))
          values (read-value-from-data-input-stream data-input-stream)]
      (if children
        {:values values
         :children (mapv #(hash-map :storage-key %)
                         children)}
        {:values values}))))

(deftest test-serialization
  (is (= {:children [{:storage-key "1"}
                     {:storage-key "2"}]
          :values #{2}}
         (deserialize (ByteArrayInputStream. (serialize {:children [{:values #{1}
                                                                     :storage-key "1"}
                                                                    {:values #{3}
                                                                     :storage-key "2"}]
                                                         :values #{2}}))))))

(defn deserialize-metadata [input-stream]
  (with-open [data-input-stream (DataInputStream. input-stream)]
    (read-value-from-data-input-stream data-input-stream)))

(deftest test-deserialize-metadata
  (is (= {:children ["1" "2"]
          :count 1}
         (deserialize-metadata (ByteArrayInputStream. (serialize {:children [{:values #{1}
                                                                              :storage-key "1"}
                                                                             {:values #{3}
                                                                              :storage-key "2"}]
                                                                  :values #{2}}))))))
