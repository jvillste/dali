(ns argumentica.zip
  (:require [clojure.java.io :as io])
  (:import [java.util Arrays]
           [java.util.zip
            ZipInputStream
            ZipOutputStream
            ZipEntry
            Deflater
            Inflater] 
           [java.io
            ByteArrayOutputStream
            ByteArrayInputStream
            BufferedOutputStream]
           java.nio.charset.Charset)
  (:use clojure.test))

(defn compress [input-stream output-stream]
  (let [buffer-size 99
        input-buffer (byte-array buffer-size)
        output-buffer (byte-array buffer-size)
        deflater (Deflater.)]
    #_(.setLevel deflater Deflater/BEST_COMPRESSION)

    (loop [read-count (.read input-stream
                             input-buffer
                             0
                             buffer-size)]
      (when (< 0 read-count)
        (do (.setInput deflater
                       input-buffer
                       0
                       read-count)
            (when (> buffer-size
                     read-count)
              (.finish deflater))
            (loop [write-count (.deflate deflater
                                         output-buffer)]
              (when (< 0 write-count)
                (.write output-stream
                        output-buffer
                        (int 0)
                        (int write-count))
                (recur (.deflate deflater
                                 output-buffer))))
            (recur (.read input-stream
                          input-buffer
                          0
                          buffer-size)))))
    
    (.end deflater)))

(defn uncompress [input-stream output-stream]
  (let [buffer-size 100
        input-buffer (byte-array buffer-size)
        output-buffer (byte-array buffer-size)
        inflater (Inflater.)]

    (loop [read-count (.read input-stream
                             input-buffer
                             0
                             buffer-size)]
      (when (< 0 read-count)
        (do (.setInput inflater
                       input-buffer
                       0
                       read-count)

            (loop [write-count (.inflate inflater
                                         output-buffer)]
              (if (< 0 write-count)
                (do (.write output-stream
                            output-buffer
                            (int 0)
                            (int write-count))
                    (recur (.inflate inflater
                                     output-buffer)))))
            (recur (.read input-stream
                          input-buffer
                          0
                          buffer-size)))))))

(defn compress-byte-array [byte-array]
  (let [output-stream (ByteArrayOutputStream.)
        input-stream (ByteArrayInputStream. byte-array)]

    (compress input-stream
              output-stream)

    (Arrays/copyOf (.toByteArray output-stream)
                   (.size output-stream))))

(deftest test-compress-byte-array
  (is (= [120 -100 75 76 74 6 0 2 77 1 39]
         (into [] (compress-byte-array (.getBytes "abc" "UTF-8"))))))

(defn uncompress-byte-array [byte-array]
  (let [output-stream (ByteArrayOutputStream.)
        input-stream (ByteArrayInputStream. byte-array)]
    (uncompress input-stream
                output-stream)
    (Arrays/copyOf (.toByteArray output-stream)
                   (.size output-stream))))


(deftest test-uncompress-byte-aray
  (is (= [97, 98, 99]
         (into [] (uncompress-byte-array (byte-array [120 -100 75 76 74 6 0 2 77 1 39])))))

  
  (let [string (apply str (repeat 100 "abc"))]
    (is (= string
           (String. (uncompress-byte-array (compress-byte-array (.getBytes string "UTF-8")))
                    "UTF-8")))))

(comment

  

  (.length (byte-array 19))
  (count (byte-array 19))
  
  (let [compress-output-stream (ByteArrayOutputStream.)
        uncompress-output-stream (ByteArrayOutputStream.)
        input-stream (ByteArrayInputStream. (.getBytes "abc" "UTF-8"))]
    (compress input-stream
              compress-output-stream)
    (count (.toByteArray compress-output-stream))
    (into [] (.toByteArray compress-output-stream))
    #_(uncompress (ByteArrayInputStream. (.toByteArray compress-output-stream))
                  uncompress-output-stream)
    #_(into [] (.toByteArray uncompress-output-stream))
    #_(String. (.toByteArray uncompress-output-stream)
               "UTF-8"))


  
  )

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
