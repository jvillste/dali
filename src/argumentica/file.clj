(ns argumentica.file
  (:require [clojure.java.io :as io]))

(defn appending-output-stream [file-path]
  (io/output-stream (io/file file-path)
                    :append true))
