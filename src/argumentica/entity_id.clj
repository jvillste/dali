(ns argumentica.entity-id
  (:require [clojure.test :refer [deftest is]]))

(defn local [number]
  {:id number})

(defn make-local [stream-id entity-id]
  (if (= stream-id (:stream-id entity-id))
    (dissoc entity-id :stream-id)
    entity-id))

(defn entity-id? [value]
  (and (map? value)
       (number? (:id value))
       (or (= 1 (count (keys value)))
           (and (= 2 (count (keys value)))
                (:stream-id value)))))

(deftest test-entity-id
  (is (entity-id? {:id 1}))
  (is (entity-id? {:id 1 :stream-id 1}))
  (is (entity-id? {:id 1 :stream-id "foo"}))
  (is (not (entity-id? {:stream-id 1})))
  (is (not (entity-id? {:id 1 :foo 1}))))


(defn local? [value]
  (and (entity-id? value)
       (nil? (:stream-id value))))
