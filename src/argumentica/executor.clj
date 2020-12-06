(ns argumentica.executor
  (:require [clojure.test :refer :all]
            [clojure.set :as set]
            [argumentica.thread-pool :as thread-pool]
            [argumentica.blocking-queue :as blocking-queue]
            [clojure.core.async :as async]))

(def print-lock (Object.))
(defn debug-print [& arguments]
  (locking print-lock
    (apply prn arguments)))

(defn- runnable? [tasks task]
  (not (some (fn [other-task]
               (and (not (= task other-task))
                    ((:depends-on? task) other-task task)))
             tasks)))

(defn- start-task [thread-pool task]
  (let [result-channel (async/chan)]
    (thread-pool/submit thread-pool
                        (fn []
                          ((:function task))
                          (async/>!! result-channel task)
                          (async/close! result-channel)))
    {:result-channel result-channel
     :task task}))

(defn- thread-count []
  (+ 2 (thread-pool/available-processors)))

(defn start [input-channel]
  (let [thread-count (thread-count)
        thread-pool (thread-pool/create thread-count)]
    (loop [tasks (set (async/<!! input-channel))
           running-tasks #{}
           result-channels #{}
           input-channel-is-closed false]
      (debug-print 'tasks (map (fn [task]
                                 [(:index task)
                                  (:first-transaction-number task)])
                               tasks))
      (debug-print 'running-tasks (map (fn [task]
                                         [(:index task)
                                          (:first-transaction-number task)])
                                       running-tasks))
      (cond (and (< (count tasks)
                    thread-count)
                 (not input-channel-is-closed))
            (if-let [new-tasks (async/<!! input-channel)]
              (recur (set/union tasks (set new-tasks))
                     running-tasks
                     result-channels
                     false)
              (recur tasks
                     running-tasks
                     result-channels
                     true))

            (and input-channel-is-closed
                 (empty? tasks))
            (do (thread-pool/shutdown thread-pool)
                nil)

            :default
            (let [started-tasks-and-channels (map (partial start-task thread-pool)
                                                  (filter (partial runnable? tasks)
                                                          (set/difference tasks
                                                                          running-tasks)))
                  result-channels (set/union result-channels
                                             (set (map :result-channel started-tasks-and-channels)))
                  running-tasks (set/union running-tasks
                                           (set (map :task started-tasks-and-channels)))]

              (let [[completed-task result-channel] (async/alts!! (vec result-channels))]
                (recur (disj tasks completed-task)
                       (disj running-tasks completed-task)
                       (disj result-channels result-channel)
                       input-channel-is-closed)))))))



;;;  Testing


(defn indexing-task-depends-on? [index-dependencies possible-dependency indexing-task]
  (and (contains? (get index-dependencies
                       (:index indexing-task))
                  (:index possible-dependency))
       (< (:first-transaction-number possible-dependency)
          (+ (:first-transaction-number indexing-task)
             (:count indexing-task)))))

(deftest test-indexing-task-depends-on?
  (is (indexing-task-depends-on? {2 #{1}}
                   {:first-transaction-number 0 :count 2 :index 1}
                   {:first-transaction-number 0 :count 2 :index 2}))

  (is (not (indexing-task-depends-on? {}
                        {:first-transaction-number 0 :count 2 :index 1}
                        {:first-transaction-number 0 :count 2 :index 2})))

  (is (indexing-task-depends-on? {2 #{1}}
                   {:first-transaction-number 0 :count 2 :index 1}
                   {:first-transaction-number 2 :count 2 :index 2}))

  (is (not (indexing-task-depends-on? {2 #{1}}
                        {:first-transaction-number 2 :count 2 :index 1}
                        {:first-transaction-number 0 :count 2 :index 2}))))



(defn create-test-task [index first-transaction-number count]
  {:first-transaction-number first-transaction-number
   :count count
   :index index
   :depends-on? (partial indexing-task-depends-on? {0 #{}
                                                    1 #{0}
                                                    2 #{0}
                                                    3 #{1 2}})
   :function (fn []
               (debug-print "starting" index first-transaction-number)
               (Thread/sleep (rand-int 200))
               (debug-print "ended" index first-transaction-number))})

(comment

  ((:function (create-test-task 1 0 10)))

  (let [input-channel (async/chan)
        executor-future (future (start input-channel))]

    (doseq [first-transaction-number [0 10]]
      (async/>!! input-channel
                 (for [index [0 1 2 3]]
                   (create-test-task index first-transaction-number 10))))
    (async/close! input-channel)
    (debug-print "all tasks have been sent")
    @executor-future
    (debug-print "executor ended"))

  )
