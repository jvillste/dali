(ns argumentica.temporary-ids
  (:require [clojure.string :as string]
            [clojure.test :refer [deftest is]]))

(defn- temporary-id? [value]
  (and (keyword? value)
       (= "id"
          (namespace value))
       (string/starts-with? (name value)
                            "t")))

(deftest test-temporal-id?
  (is (temporary-id? :id/t1))
  (is (not (temporary-id? :id/l1)))
  (is (not (temporary-id? :a)))
  (is (not (temporary-id? 1)))
  (is (not (temporary-id? nil))))

(defn- local-id [number]
  (keyword "id" (str "l" number)))

(defn- distinct-temporary-ids [changes]
  (->> (apply concat changes)
       (filter temporary-id?)
       (distinct)))

(defn temporary-id-resolution [next-id changes]
  (into {}
        (map vector
             (distinct-temporary-ids changes)
             (map (comp local-id
                        (partial + next-id))
                  (range)))))

(deftest test-temporary-id-resolution
  (is (= #:id{:ta :id/l100,
              :tb :id/l101}
         (temporary-id-resolution 100
                                  [[:add :id/ta "Foo" :id/tb]
                                   [:add :id/tb "Foo" :id/ta]]))))

(defn assign-temporary-ids [temporary-id-resolution temporary-changes]
  (map (fn [change]
         (vec (map (fn [value]
                     (or (get temporary-id-resolution
                              value)
                         value))
                   change)))
       temporary-changes))

(deftest test-assign-temporary-ids
  (is (= '([:add 100 :name 101]
           [:add 101 :language "fi"]
           [:add 101 :text "parsa"])
         (assign-temporary-ids {:temporary/a 100
                                :temporary/b 101}
                               [[:add :temporary/a :name :temporary/b]
                                [:add :temporary/b :language "fi"]
                                [:add :temporary/b :text "parsa"]]))))

(defn- number-from-local-id [local-id]
  (Integer/parseInt (subs (name local-id)
                          1)))

(deftest test-number-from-local-id
  (is (= 123
         (number-from-local-id :id/l123))))

(defn new-next-id [temporary-id-resolution]
  (->> temporary-id-resolution
       vals
       (map number-from-local-id)
       (apply max)
       inc))

(deftest test-new-next-id
  (is (= 102
         (new-next-id #:id{:ta :id/l100,
                           :tb :id/l101}))))
