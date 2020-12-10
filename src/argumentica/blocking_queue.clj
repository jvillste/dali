(ns argumentica.blocking-queue
  (:refer-clojure :exclude [take])
  (:import java.util.concurrent.ArrayBlockingQueue
           java.util.concurrent.TimeUnit)
  (:require [clojure.test :refer :all]))

(defn create
  ([] (ArrayBlockingQueue. 1))

  ([capacity]
   (ArrayBlockingQueue. capacity)))

(defn put [queue value]
  (.put queue value))

(defn take
  ([queue]
   (.take queue))

  ([queue timeout]
   (.poll queue timeout TimeUnit/MILLISECONDS)))



;; tests

(defmacro start-thread [& body]
  `(.start (Thread. (fn [] ~@body))))

(deftest test-queue
  (let [the-queue (create)]
    (start-thread (Thread/sleep 50)
                  (put the-queue :message))

    (is (= :message (take the-queue))))

  (testing "timeout"
    (let [the-queue (create)]
      (start-thread (Thread/sleep 50)
                    (put the-queue :message))
      (is (= nil (take the-queue 10)))))

  (testing "values are delivered in order of the take calls"
    (let [the-queue (create)
          results-atom (atom {})]

      (put the-queue :message-1)

      (start-thread (while true
                      (swap! results-atom update :thread-1 conj (take the-queue))))

      (Thread/sleep 50)

      (start-thread (while true
                      (swap! results-atom update :thread-2 conj (take the-queue))))

      (Thread/sleep 50)

      (put the-queue :message-2)

      (Thread/sleep 50)

      (is (= {:thread-1 [:message-2 :message-1]}
             @results-atom)))))
