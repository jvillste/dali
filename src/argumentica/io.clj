(ns argumentica.io
  (:require [clojure.java.io :as io])
  (:import [java.io DataInputStream DataOutputStream]))

(defn with-data-output-stream [file-name function]
  (with-open [data-output-stream (DataOutputStream. (io/output-stream (io/file file-name)))]
    (function data-output-stream)))

(defn with-data-input-stream [file-name function]
  (with-open [data-input-stream (DataInputStream. (io/input-stream (io/file file-name)))]
    (function data-input-stream)))

(defn call-with-open [closeable function & arguments]
  (try
    (apply function closeable arguments)
    (finally
      (.close closeable))))
