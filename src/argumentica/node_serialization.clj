(ns argumentica.node-serialization
  (:require [clojure.java.io :as io]
            [taoensso.nippy :as nippy]
            [clojure.test :refer :all]
            [argumentica.leaf-node-serialization :as leaf-node-serialization]
            [argumentica.util :as util])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream DataInputStream DataOutputStream EOFException]
           clojure.lang.IReduceInit
           clojure.lang.IReduce))

(defn with-data-output-stream [function]
  (with-open [byte-array-output-stream (ByteArrayOutputStream.)
              data-output-stream (DataOutputStream. byte-array-output-stream)]
    (function data-output-stream)
    (.toByteArray byte-array-output-stream)))

(deftest test-with-data-output-stream
  (is (= [0 0 0 5]
         (into [] (with-data-output-stream (fn [data-output-stream]
                                             (.writeInt data-output-stream 5)))))))

(defn- metadata [node]
  (if (:values node)
    {:count (count (:values node))}
    {:children (mapv (fn [[splitter child]]
                       {:splitter splitter
                        :storage-key (:storage-key child)})
                     (:children node))}))

(deftest test-metadata
  (is (= {:children [{:splitter 1, :storage-key "1"}
                     {:splitter 2, :storage-key "2"}]}
         (metadata {:children {1 {:values #{1}
                                  :storage-key "1"}
                               2 {:values #{2}
                                  :storage-key "2"}}})))
  (is (= {:count 1}
         (metadata {:values #{2}}))))

(defn- write-byte-array-to-data-output-stream [byte-array data-output-stream]
  (let [length (alength byte-array)]
    (.writeInt data-output-stream length)
    (.write data-output-stream byte-array 0 length)))

(defn write-value-to-data-output-stream [data-output-stream value]
  (write-byte-array-to-data-output-stream (nippy/freeze value)
                                          data-output-stream))

(defn- read-byte-array-from-data-input-stream [data-input-stream]
  (try (let [buffer (byte-array (.readInt data-input-stream))]
         (.read data-input-stream buffer)
         buffer)
       (catch EOFException e
         nil)))

(defn- read-value-from-data-input-stream [data-input-stream]
  (nippy/thaw (read-byte-array-from-data-input-stream data-input-stream)))

(defn write-rows [values data-output-stream]
  (let [written-node (leaf-node-serialization/write-node values)]
    (write-byte-array-to-data-output-stream (-> written-node :dictionary :index)
                                            data-output-stream)
    (write-byte-array-to-data-output-stream (-> written-node :dictionary :rows)
                                            data-output-stream)
    (.writeInt data-output-stream (:row-length written-node))
    (write-byte-array-to-data-output-stream (-> written-node :index)
                                            data-output-stream)
    (write-byte-array-to-data-output-stream (-> written-node :rows)
                                            data-output-stream)))

(defn read-leaf-node [data-input-stream]
  (let [dictionary-index (read-byte-array-from-data-input-stream data-input-stream)
        dictionary-rows (read-byte-array-from-data-input-stream data-input-stream)
        row-length (.readInt data-input-stream)
        index (read-byte-array-from-data-input-stream data-input-stream)
        rows (read-byte-array-from-data-input-stream data-input-stream)]
    {:dictionary {:index dictionary-index
                  :rows dictionary-rows}
     :row-length row-length
     :index index
     :rows rows}))

(deftest test-write-leaf-node
  (is (= {:dictionary {:index [], :rows []},
          :row-length 1,
          :index ["00" "00"],
          :rows ["01"]}
         (util/byte-array-values-to-vectors
          (read-leaf-node
           (DataInputStream. (ByteArrayInputStream. (with-data-output-stream
                                                      (fn [data-output-stream]
                                                        (write-rows [[1]]
                                                                    data-output-stream))))))))))

(defn serialize [node]
  (with-data-output-stream
    (fn [data-output-stream]
      (write-value-to-data-output-stream data-output-stream (metadata node))
      (when-let [values (:values node)]
        (write-rows values data-output-stream)))))

(defn deserialize [input-stream]
  (with-open [data-input-stream (DataInputStream. input-stream)]
    (if-let [children (:children (read-value-from-data-input-stream data-input-stream))]
      {:children children}
      {:values (read-leaf-node data-input-stream)})))

(deftest test-serialization
  (is (= {:children [{:splitter 1, :storage-key "1"}
                     {:splitter 2, :storage-key "2"}]}
         (deserialize (ByteArrayInputStream. (serialize {:children {1 {:values #{1}
                                                                       :storage-key "1"}
                                                                    2 {:values #{2}
                                                                       :storage-key "2"}}})))))
  (is (= {:values
          {:dictionary {:index [], :rows []},
           :row-length 1,
           :index ["00" "00"],
           :rows ["01"]}}
         (util/byte-array-values-to-vectors
          (deserialize (ByteArrayInputStream. (serialize {:values #{[1]}})))))))

(defn deserialize-metadata [input-stream]
  (with-open [data-input-stream (DataInputStream. input-stream)]
    (read-value-from-data-input-stream data-input-stream)))

(deftest test-deserialize-metadata
  (is (= {:count 1}
         (deserialize-metadata (ByteArrayInputStream. (serialize {:values #{[1]}}))))))
