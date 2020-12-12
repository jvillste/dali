(ns argumentica.reduction
  (:require [clojure.test :refer :all]
            [clojure.string :as string])
  (:import clojure.lang.IReduceInit
           clojure.lang.IReduce))

(defn with-index []
  (fn [rf]
    (let [index (volatile! 0)]
      (fn
        ([result]
         (rf result))

        ([result input]
         (let [current-index @index]
           (vreset! index (inc current-index))
           (rf result
               [current-index input])))))))

(deftest test-add-count
  (is (= [[0 3] [1 4] [2 5]]
         (into [] (with-index) (range 3 6)))))

(defn double-reduced [reducing-function]
  (fn
    ([result]
     (reducing-function result))
    ([accumulator value]
     (let [result (reducing-function accumulator value)]
       (if (reduced? result)
         (reduced result)
         result)))))

(deftest test-double-reduced
  (is (reduced? (transduce (comp double-reduced
                                 (take 2))
                           conj
                           []
                           (range 4)))))

(def value-count
  (completing (fn [count value_]
                (inc count))))

(defn last-value
  ([] nil)
  ([result] result)
  ([accumulator_ value] value))

(defn first-value
  ([] ::initial-value)
  ([result] result)
  ([reduced-value value]
   (reduced (if (= ::initial-value reduced-value)
              value
              reduced-value))))

(deftest test-first-value
  (is (= 0
         (reduce first-value (range 10))))

  (is (= nil
         (reduce first-value [nil 1 2])))

  (is (= 4
         (reduce ((filter #(> % 3))
                  first-value)
                 ::initial-value
                 (range 10))))

  (is (= 4
         (transduce (filter #(> % 3))
                    first-value
                    (range 10))))

  (is (= nil
         (transduce identity
                    first-value
                    [nil 1 2]))))

(defn find-first [predicate]
  (fn
    ([] nil)
    ([result] result)
    ([_result value]
     (if (not (predicate value))
       nil
       (reduced value)))))

(deftest test-find-first
  (is (= 4
         (reduce (find-first #(> % 3))
                 (range 10)))))

(defn take-up-to [predicate]
  (fn [reducing-function]
    (fn
      ([result]
       (reducing-function result))
      ([reduced-value new-value]
       (if (not (predicate new-value))
         (reducing-function reduced-value new-value)
         (ensure-reduced (reducing-function reduced-value new-value)))))))

(deftest test-take-up-to
  (is (= 4
         (reduce ((take-up-to #(> % 3))
                  last-value)
                 (range 10)))))

(comment
  (reduce ((comp (drop-while #(< % 4))
                 (take 1))
           last-value)
          (range 10)))

(defn process! [reducible & transducers]
  (transduce (apply comp transducers)
             (constantly nil)
             reducible))

(defmacro do-reducible [binding & body]
  (let [[variable reducible] binding]
    `(reduce (fn [result_# ~variable]
               ~@body
               nil)
             nil
             ~reducible)))

(deftest test-do-reducible
  (is (= [0 1 2]
         (let [result-atom (atom [])]
           (do-reducible [x (range 3)]
                         (swap! result-atom conj x))
           @result-atom))))

(defn educe [reducible & transducers]
  (eduction (apply comp transducers)
            reducible))

(deftest test-educe
  (is (= [1 2]
         (into []
               (educe (range 10)
                      (map inc)
                      (take 2))))))

(defn reduce-tree [root children reducing-function initial-value]
  (loop [reduced-value initial-value
         nodes [root]]
    (if-let [node (first nodes)]
      (if-let [the-children (children node)]
        (recur reduced-value
               (concat the-children
                       (rest nodes)))
        (let [reusult (reducing-function reduced-value
                                         node)]
          (if (reduced? reusult)
            @reusult
            (recur reusult
                   (rest nodes)))))
      reduced-value)))

(defn tree-reducible [root children]
  (reify
    IReduceInit
    (reduce [this reducing-function initial-reduced-value]
      (reducing-function (reduce-tree root
                                      children
                                      reducing-function
                                      initial-reduced-value)))

    IReduce
    (reduce [this reducing-function]
      (reducing-function (reduce-tree root
                                      children
                                      reducing-function
                                      (reducing-function))))))

(deftest test-tree-reducible
  (is (= [2 3 4]
         (into []
               (map inc)
               (tree-reducible {:a {:b 1
                                    :c 2}
                                :d 3}
                               (fn [value]
                                 (when (map? value)
                                   (vals value))))))))

(defn reducible [the-reduce]
  (reify
    IReduceInit
    (reduce [this reducing-function initial-value]
      (reducing-function (the-reduce reducing-function initial-value)))

    IReduce
    (reduce [this reducing-function]
      (reducing-function (the-reduce reducing-function (reducing-function))))))



(defn- nano-seconds-to-seconds [nano-seconds]
  (/ nano-seconds 1E9))

(defn- format-time-interval [seconds]
  (str (int (/ seconds 60 60))
       ":"
       (int (/ seconds 60))
       ":"
       (format "%.1f" seconds)))

(defn- progress-report-lines [{:keys [total-count
                                      latest-batch-size
                                      current-time
                                      start-time
                                      batch-start-time
                                      total-processed-value-count]}]

  [(str "Progress:                                 "
        (format "%.1f"
                (float (* 100 (/ total-processed-value-count total-count))))
        "% ("
        total-processed-value-count
        "/"
        total-count
        ")")

   (str "Processing time of the latest batch:      "
        (format-time-interval (nano-seconds-to-seconds (- current-time batch-start-time))))

   (str "Total time passed:                        "
        (format-time-interval (nano-seconds-to-seconds (- current-time start-time))))

   (str "Remaining time based on the latest batch: "
        (format-time-interval (nano-seconds-to-seconds (* (/ (- current-time
                                                                batch-start-time)
                                                             latest-batch-size)
                                                          (- total-count total-processed-value-count)))))
   (str "Remaining time based on total progress:   "
        (format-time-interval (nano-seconds-to-seconds (* (/ (- current-time
                                                                start-time)
                                                             total-processed-value-count)
                                                          (- total-count total-processed-value-count)))))])

(deftest test-progress-report
  (is (= ["Progress:                                 50.0% (3/6)"
          "Processing time of the latest batch:      0:0:1.0"
          "Total time passed:                        0:0:1.0"
          "Remaining time based on the latest batch: 0:0:1.0"
          "Remaining time based on total progress:   0:0:1.0"]
         (progress-report-lines {:total-count 6
                                 :latest-batch-size 3
                                 :current-time 1E9
                                 :start-time 0
                                 :batch-start-time 0
                                 :total-processed-value-count 3}))))

#_(defn create-batch-end-handler [handle-status]
    (fn [start-time
         batch-start-time
         batch-size
         total-processed-value-count]
      (handle-status {:latest-batch-size batch-size
                      :current-time (System/nanoTime)
                      :start-time start-time
                      :batch-start-time batch-start-time
                      :total-processed-value-count total-processed-value-count})))

(defn handle-batch-ending-by-printing-report [title total-count]
  (fn [status]
    (println (string/join "\n" (concat [title]
                                       (progress-report-lines (assoc status
                                                                     :total-count total-count)))))))

(defn report-progress-in-fixed-size-batches [batch-size handle-batch-ending]
  (fn [rf]
    (let [total-processed-value-count (volatile! 0)
          start-time (volatile! nil)
          batch-start-time (volatile! nil)
          run-handle-batch-ending (fn []
                                    (handle-batch-ending {:latest-batch-size batch-size
                                                          :current-time (System/nanoTime)
                                                          :start-time @start-time
                                                          :batch-start-time @batch-start-time
                                                          :total-processed-value-count @total-processed-value-count}))]
      (fn
        ([] (rf))

        ([result]
         (run-handle-batch-ending)
         (rf result))

        ([result input]
         (let [result (rf result input)]
           (when (not @start-time)
             (vreset! start-time (System/nanoTime)))

           (vreset! total-processed-value-count (inc @total-processed-value-count))

           (when (= 0 (mod @total-processed-value-count
                           batch-size))

             (when @batch-start-time
               (run-handle-batch-ending))

             (vreset! batch-start-time (System/nanoTime)))
           result))))))

(comment
  (process! (range 15)
            (map (fn [number]
                   (Thread/sleep (rand 300))
                   number))
            (report-progress-in-fixed-size-batches 2
                                                   #(prn %)
                                                   #_(handle-batch-ending-by-printing-report "\nTest"
                                                                                           15)))
  )

(defn report-progress-every-n-seconds [seconds-per-batch handle-batch-ending]
  (fn [rf]
    (let [total-processed-value-count (volatile! 0)
          batch-value-count (volatile! 0)
          start-time (volatile! nil)
          batch-start-time (volatile! nil)
          run-handle-batch-ending (fn []
                                    (handle-batch-ending {:latest-batch-size @batch-value-count
                                                          :current-time (System/nanoTime)
                                                          :start-time @start-time
                                                          :batch-start-time @batch-start-time
                                                          :total-processed-value-count @total-processed-value-count}))]
      (fn
        ([] (rf))

        ([result]
         (run-handle-batch-ending)

         (rf result))

        ([result input]
         (let [result  (rf result input)]
           (when (not @start-time)
             (vreset! start-time (System/nanoTime))
             (vreset! batch-start-time (System/nanoTime)))

           (vreset! total-processed-value-count (inc @total-processed-value-count))
           (vreset! batch-value-count (inc @batch-value-count))

           (when (> (System/nanoTime)
                    (+ @batch-start-time
                       (* seconds-per-batch
                          1E9)))

             (run-handle-batch-ending)

             (vreset! batch-start-time (System/nanoTime)))
           result))))))

(comment
  (process! (range 15)
            (map (fn [number]
                   (Thread/sleep (rand 300))
                   number))
            (report-progress-every-n-seconds 1
                                             ;; #(prn %)
                                             (handle-batch-ending-by-printing-report "\nTest"
                                                                                     15)))
  )
