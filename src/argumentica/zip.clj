(ns argumentica.zip
  (:require [clojure.java.io :as io])
  (:import java.util.zip.ZipInputStream
           java.util.zip.ZipOutputStream
           java.util.zip.ZipEntry
           [java.io ByteArrayOutputStream
            ByteArrayInputStream
            BufferedOutputStream]
           java.nio.charset.Charset))

(defn read-first-entry-from-zip [input-stream]
  (let [zip-input-stream (ZipInputStream. input-stream)
        output-stream (ByteArrayOutputStream.)]
    (when (.getNextEntry zip-input-stream)
      (io/copy zip-input-stream
               output-stream)
      (.toByteArray output-stream))))


(defn string-input-stream [string charset-name]
  (ByteArrayInputStream. (-> (Charset/forName charset-name)
                             (.encode string)
                             (.array))))

(defn byte-array-output-stream []
  (ByteArrayOutputStream.))

(defn byte-array-input-stream [bytes]
  (ByteArrayInputStream. bytes))

(defn write-zip-file-with-one-entry [input-stream output-stream]
  (let [zip-output-stream (ZipOutputStream. output-stream)]
    (.putNextEntry zip-output-stream
                   (ZipEntry. "results.csv"))
    (io/copy input-stream
             zip-output-stream)
    (.closeEntry zip-output-stream)
    (.close zip-output-stream)))


(comment
  (let [string (prn-str (repeat 100 "abc"))
        zip-output-stream (byte-array-output-stream)]
    
    (write-zip-file-with-one-entry (string-input-stream string
                                                        "UTF-8")
                                   zip-output-stream)
    
    {:unzipped (count (.getBytes string
                                 "UTF-8"))

     :zipped (count (.toByteArray zip-output-stream))
     
     :unzipped-value (String. (read-first-entry-from-zip (ByteArrayInputStream. (.toByteArray zip-output-stream)))
                              "UTF-8")
     :value string})
  
  (write-zip-file-with-one-entry (string-input-stream "haa\nhoo")
                                 (io/output-stream "/Users/jukka/Downloads/results.zip"))
  )
