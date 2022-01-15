(ns argumentica.branch
  (:require
   [argumentica.comparator :as comparator]
   [argumentica.db.common :as db-common]
   [clojure.test :refer :all])
  (:import
   (argumentica.comparator DatomComparator)))

;; TODO: implement this, how to differentiate local ids in up and down stream?
;; locals could be {:id 1} foreign could be {:stream-id asdfasdf :id 1} branch should always add stream-id to entity ids

(defn set-stream-id [stream-id datom]
  (when datom
    (mapv (fn [value]
            (if (and (comparator/entity-id? value)
                     (not (some? (:stream-id value))))
              (assoc value :stream-id stream-id)
              value))
          datom)))

(defn remove-stream-id [stream-id datom]
  (when datom
    (mapv (fn [value]
            (if (and (comparator/entity-id? value)
                     (= stream-id (:stream-id value)))
              (dissoc value :stream-id)
              value))
          datom)))

(defn add-to-transaction-number [number datom]
  (vec (concat (drop-last 2 datom)
               [(+ number
                   (db-common/datom-transaction-number datom))
                (db-common/datom-command datom)])))

(defn lazy-merged-sequence [upstream-sequence upstream-id downstream-sequence downstream-id]
  (let [upstream-datom (first upstream-sequence)
        downstream-datom (first downstream-sequence)]
    (when (or (some? upstream-datom)
              (some? downstream-datom))
      (lazy-seq (if (and (some? upstream-datom)
                         (or (nil? downstream-datom)
                             (= -1 (comparator/compare-datoms upstream-datom
                                                              downstream-datom))))
                  (cons upstream-datom
                        (lazy-merged-sequence (rest upstream-sequence)
                                              upstream-id
                                              downstream-sequence
                                              downstream-id))
                  (cons downstream-datom
                        (lazy-merged-sequence upstream-sequence
                                              upstream-id
                                              (rest downstream-sequence)
                                              downstream-id)))))))

(defn offset-transaction-number-in-pattern [offset sorted pattern]
  (let [datom-length (count (first (subseq sorted >= [])))]
    (cond (= (count pattern)
             datom-length)
          (add-to-transaction-number offset pattern)

          (= (count pattern)
             (dec datom-length))
          (vec (concat (drop-last 1 pattern)
                       [(+ (last pattern)
                           offset)]))

          :else
          pattern)))

(defn merged-datom-sequence [upstream-sorted upstream-id downstream-sorted downstream-id last-upstream-transaction-number starting-pattern direction]
  (lazy-merged-sequence (->> (if (= :forwards direction)
                               (subseq upstream-sorted
                                       >=
                                       (remove-stream-id upstream-id starting-pattern))
                               (rsubseq upstream-sorted
                                        <=
                                        (remove-stream-id upstream-id starting-pattern)))
                             (filter (fn [datom]
                                       (>= last-upstream-transaction-number
                                           (db-common/datom-transaction-number datom))))
                             (map (fn [datom]
                                    (set-stream-id upstream-id datom))))
                        upstream-id
                        (let [downstream-starting-pattern (->> starting-pattern
                                                               (remove-stream-id downstream-id)
                                                               (offset-transaction-number-in-pattern (- (inc last-upstream-transaction-number))
                                                                                                     upstream-sorted))]
                          (->> (if (= :forwards direction)
                                 (subseq downstream-sorted
                                         >=
                                         downstream-starting-pattern)
                                 (rsubseq downstream-sorted
                                          <=
                                          downstream-starting-pattern))
                               (map (fn [datom]
                                      (->> datom
                                           (set-stream-id downstream-id)
                                           (add-to-transaction-number (inc last-upstream-transaction-number)))))))
                        downstream-id))

(deftype Branch [upstream-sorted upstream-id downstream-sorted downstream-id last-upstream-transaction-number]
  clojure.lang.Seqable
  (seq [this]
    (merged-datom-sequence upstream-sorted
                           upstream-id
                           downstream-sorted
                           downstream-id
                           last-upstream-transaction-number
                           []
                           :forwards))
  clojure.lang.Sorted
  (comparator [this]
    (DatomComparator.))
  (entryKey [this entry]
    entry)
  (seq [this ascending?]
    (merged-datom-sequence upstream-sorted
                           upstream-id
                           downstream-sorted
                           downstream-id
                           last-upstream-transaction-number
                           (if ascending? [] :comparator/max)
                           (if ascending? :forwards :backwards)))
  (seqFrom [this value ascending?]
    (merged-datom-sequence upstream-sorted
                           upstream-id
                           downstream-sorted
                           downstream-id
                           last-upstream-transaction-number
                           value
                           (if ascending? :forwards :backwards))))

(defmethod print-method Branch [branch ^java.io.Writer writer]
  (.write writer (with-out-str (clojure.pprint/pprint (seq branch)))))

(defn create-test-branch [upstream-datoms downstream-datoms last-upstream-transaction-number]
  (->Branch (apply sorted-set-by
                   comparator/compare-datoms
                   upstream-datoms)
            1
            (apply sorted-set-by
                   comparator/compare-datoms
                   downstream-datoms)
            2
            last-upstream-transaction-number))

(deftest test-branch
  (is (= '([{:id 1, :stream-id 1} :name "bar" 1 :add]
           [{:id 1, :stream-id 1} :name "foo" 0 :add]
           [{:id 1, :stream-id 1} :name "foo" 1 :remove])
         (subseq (create-test-branch [[{:id 1} :name "foo" 0 :add]
                                      [{:id 2} :name "baz" 1 :add]]
                                     [[{:id 1 :stream-id 1} :name "foo" 0 :remove]
                                      [{:id 1 :stream-id 1} :name "bar" 0 :add]]
                                     0)
                 >=
                 [])))

  (is (= '([{:id 1, :stream-id 1} :name "bar" 2 :add]
           [{:id 1, :stream-id 1} :name "foo" 0 :add]
           [{:id 1, :stream-id 1} :name "foo" 2 :remove]
           [{:id 2, :stream-id 1} :name "baz" 1 :add])
         (subseq (create-test-branch [[{:id 1} :name "foo" 0 :add]
                                      [{:id 2} :name "baz" 1 :add]]
                                     [[{:id 1 :stream-id 1} :name "foo" 0 :remove]
                                      [{:id 1 :stream-id 1} :name "bar" 0 :add]]
                                     1)
                 >=
                 [])))

  (is (= '([{:id 1, :stream-id 1} :name "foo" 0 :add]
           [{:id 1, :stream-id 1} :name "foo" 1 :remove])
         (subseq (create-test-branch [[{:id 1} :name "foo" 0 :add]
                                      [{:id 2} :name "baz" 1 :add]]
                                     [[{:id 1 :stream-id 1} :name "foo" 0 :remove]
                                      [{:id 1 :stream-id 1} :name "bar" 0 :add]]
                                     0)
                 >=
                 [{:id 1, :stream-id 1} :name "foo" 0 :add])))

  (is (= '([{:id 1, :stream-id 1} :name "foo" 0 :add]
           [{:id 1, :stream-id 1} :name "foo" 1 :remove])
         (subseq (create-test-branch [[{:id 1} :name "foo" 0 :add]
                                      [{:id 2} :name "baz" 1 :add]]
                                     [[{:id 1 :stream-id 1} :name "foo" 0 :remove]
                                      [{:id 1 :stream-id 1} :name "bar" 0 :add]]
                                     0)
                 >=
                 [{:id 1, :stream-id 1} :name "foo" 0])))

  (is (= '([{:id 1, :stream-id 1} :name "bar" 1 :add]
           [{:id 1, :stream-id 1} :name "foo" 0 :add]
           [{:id 1, :stream-id 1} :name "foo" 1 :remove])
         (subseq (create-test-branch [[{:id 1} :name "foo" 0 :add]
                                      [{:id 2} :name "baz" 1 :add]]
                                     [[{:id 1 :stream-id 1} :name "foo" 0 :remove]
                                      [{:id 1 :stream-id 1} :name "bar" 0 :add]]
                                     0)
                 >=
                 [{:id 1, :stream-id 1} :name]))))
