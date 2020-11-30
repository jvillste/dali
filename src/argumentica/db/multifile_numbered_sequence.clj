(ns argumentica.db.multifile-numbered-sequence
  (:require [argumentica.db.compressed-numbered-sequence :as compressed-numbered-sequence]
            [argumentica.db.persistent-numbered-sequence :as persistent-numbered-sequence]
            [argumentica.file :as file]
            [argumentica.io :as argumentica-io]
            [argumentica.reduction :as reduction]
            [clojure.java.io :as io]
            [me.raynes.fs :as fs])
  (:import java.io.File))

(defn open [directory-path maximum-values-in-head]
  (let [head-file-path (str directory-path "/head")
        archive-file-path (str directory-path "/archive")]
    (merge {:head-file-path head-file-path
            :archive-file-path archive-file-path
            :maximum-values-in-head maximum-values-in-head}
           (if (.exists (File. head-file-path))
             (let [head-value-count (argumentica-io/call-with-open (io/reader head-file-path)
                                                                   persistent-numbered-sequence/reduce-values-from-reader
                                                                   reduction/value-count
                                                                   0
                                                                   0)
                   first-number-in-head (argumentica-io/call-with-open (io/reader head-file-path)
                                                                       persistent-numbered-sequence/first-number-from-reader)]
               {:head-output-stream (file/appending-output-stream head-file-path)
                :archive-output-stream (file/appending-output-stream archive-file-path)
                :next-number (+ first-number-in-head
                                head-value-count)
                :head-value-count head-value-count
                :first-number-in-head first-number-in-head})

             {:head-output-stream (persistent-numbered-sequence/initialize-numbered-sequence (file/appending-output-stream head-file-path)
                                                                                             0)
              :archive-output-stream (io/output-stream archive-file-path)
              :next-number 0
              :head-value-count 0
              :first-number-in-head 0}))))

(defn last-number [multifile-numbered-sequence]
  (let [next-number (:next-number multifile-numbered-sequence)]
    (if (= 0 next-number)
      nil
      (dec next-number))))

(defn- append-head-to-archive [multifile-numbered-sequence]
  (.close (:head-output-stream multifile-numbered-sequence))
  (compressed-numbered-sequence/write-values-to-stream! (:archive-output-stream multifile-numbered-sequence)
                                                        (:first-number-in-head multifile-numbered-sequence)
                                                        (persistent-numbered-sequence/reduce-values-from-reader (io/reader (:head-file-path multifile-numbered-sequence))
                                                                                                                conj
                                                                                                                []
                                                                                                                0))
  (assoc multifile-numbered-sequence
         :head-output-stream (persistent-numbered-sequence/initialize-numbered-sequence (io/output-stream (:head-file-path multifile-numbered-sequence))
                                                                                        (:next-number multifile-numbered-sequence))
         :head-value-count 0
         :first-number-in-head (:next-number multifile-numbered-sequence)))

(defn add-value [multifile-numbered-sequence value]
  (if (< (:head-value-count multifile-numbered-sequence)
         (:maximum-values-in-head multifile-numbered-sequence))
    (do (persistent-numbered-sequence/write-to-stream! (:head-output-stream multifile-numbered-sequence)
                                                       value)
        (-> multifile-numbered-sequence
            (update :head-value-count inc)
            (update :next-number inc)))
    (add-value (append-head-to-archive multifile-numbered-sequence)
               value)))

(defn close! [multifile-numbered-sequence]
  (.close (:head-output-stream multifile-numbered-sequence))
  (.close (:archive-output-stream multifile-numbered-sequence))
  nil)

(defn reduce-values [multifile-numbered-sequence reducing-function initial-value first-number]
  (let [result (if (> first-number (:first-number-in-head multifile-numbered-sequence))
                 initial-value
                 (argumentica-io/call-with-open (io/input-stream (:archive-file-path multifile-numbered-sequence))
                                                compressed-numbered-sequence/reduce-values-from-stream
                                                (reduction/double-reduced reducing-function)
                                                initial-value
                                                first-number))]
    (if (reduced? result)
      @result
      (argumentica-io/call-with-open (io/reader (:head-file-path multifile-numbered-sequence))
                                     persistent-numbered-sequence/reduce-values-from-reader
                                     reducing-function
                                     result
                                     first-number))))

(comment
  (def test-directory "temp/multifile-numbered-sequence")

  (do (fs/delete-dir test-directory)
      (.mkdir (File. test-directory)))

  (let [multifile-numbered-sequence (open test-directory 2)]
    (-> multifile-numbered-sequence
        (add-value :a)
        (add-value :b)
        (add-value :c)
        (close!)))

  (let [multifile-numbered-sequence (open test-directory 2)]
    (try (reduce-values multifile-numbered-sequence
                        conj
                        []
                        0)
         (finally (close! multifile-numbered-sequence))))

  )
