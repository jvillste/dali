(ns argumentica.thread-pool
  (:require [clojure.test :refer :all])
  (:import java.util.concurrent.Executors))

(defn available-processors []
  (.. Runtime getRuntime availableProcessors))

(defn create
  ([]
   (create (+ 2 (available-processors))))
  ([size]
   (Executors/newFixedThreadPool size)))

(defn submit [thread-pool function]
  (.submit thread-pool function))

(defn shutdown [thread-pool]
  (.shutdown thread-pool))
