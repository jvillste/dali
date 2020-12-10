(ns argumentica.partial-order-planning
  (:require [clojure.test :refer :all]
            [clojure.set :as set]
            [argumentica.thread-pool :as thread-pool]
            [argumentica.blocking-queue :as blocking-queue]))

(defn partial-order-plan [all-tasks]
  (let [initial-tasks (set (filter (fn [task]
                                     (empty? (:preconditions task)))
                                   all-tasks))]
    (loop [remaining-tasks (set/difference all-tasks
                                           initial-tasks)
           steps [(set (map :id initial-tasks))]]
      (let [next-step (set (filter (fn [task]
                                     (empty? (set/difference (:preconditions task)
                                                             (apply set/union steps))))
                                   remaining-tasks))]
        (if (empty? next-step)
          steps
          (recur (set/difference remaining-tasks
                                 next-step)
                 (conj steps (set (map :id next-step)))))))))

(deftest test-partial-order-plan
  (is (= [#{1} #{2 3} #{4}]
         (partial-order-plan #{{:id 1 :preconditions #{}}
                               {:id 2 :preconditions #{1}}
                               {:id 3 :preconditions #{1}}
                               {:id 4 :preconditions #{2 3}}})))

  (is (= [#{}]
         (partial-order-plan #{{:id 1 :preconditions #{2}}
                               {:id 2 :preconditions #{1}}})))

  (is (= [#{1 2} #{4 3}]
         (partial-order-plan #{{:id 1 :preconditions #{}}
                               {:id 2 :preconditions #{}}
                               {:id 3 :preconditions #{1}}
                               {:id 4 :preconditions #{1}}
                               {:id 5 :preconditions #{4}}
                               {:id 6 :preconditions #{3}}}))))

(defn depends-on? [index-dependencies possible-dependency task]
  (and (contains? (get index-dependencies
                       (:index task))
                  (:index possible-dependency))
       (< (:first-transaction-number possible-dependency)
          (+ (:first-transaction-number task)
             (:count task)))))

(deftest test-depends-on?
  (is (depends-on? {2 #{1}}
                   {:first-transaction-number 0 :count 2 :index 1}
                   {:first-transaction-number 0 :count 2 :index 2}))

  (is (not (depends-on? {}
                        {:first-transaction-number 0 :count 2 :index 1}
                        {:first-transaction-number 0 :count 2 :index 2})))

  (is (depends-on? {2 #{1}}
                   {:first-transaction-number 0 :count 2 :index 1}
                   {:first-transaction-number 2 :count 2 :index 2}))

  (is (not (depends-on? {2 #{1}}
                        {:first-transaction-number 2 :count 2 :index 1}
                        {:first-transaction-number 0 :count 2 :index 2}))))

(defn executor [depends-on?]
  {:thread-pool (thread-pool/create)
   :completion-queue (blocking-queue/create)
   :waiting-tasks #{}
   :running-tasks #{}
   :depends-on? depends-on?})

(comment
  (def index-dependencies {:eav #{}
                           :text #{:eav}})


  #{{:first-transaction-number 0
     :count 10
     :index :eav}
    {:first-transaction-number 0
     :count 10
     :index :text}
    {:first-transaction-number 10
     :count 10
     :index :eav}
    {:first-transaction-number 10
     :count 10
     :index :text}}
  ) ;; TODO: remove-me
