(ns argumentica.temporary-ids
  (:require [clojure.string :as string]
            [clojure.test :refer [deftest is]]))

(defn temporary-id [name]
  (keyword "tmp" name))

(defn- temporary-id? [value]
  (and (keyword? value)
       (= "tmp"
          (namespace value))))

(deftest test-temporary-id?
  (is (temporary-id? :tmp/foo))
  (is (not (temporary-id? :id/foo)))
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
             (map (comp identity #_local-id
                        (partial + next-id))
                  (range)))))

(deftest test-temporary-id-resolution
  (is (= #:tmp{:a 100, :b 101}
         (temporary-id-resolution 100
                                  [[:add :tmp/a "Foo" :tmp/b]
                                   [:add :tmp/b "Foo" :tmp/a]]))))

(defn assign-temporary-id [temporary-id-resolution value]
  (or (get temporary-id-resolution
           value)
      value))

(defn assign-temporary-ids [temporary-id-resolution temporary-changes]
  (map (fn [change]
         (vec (map (fn [value]
                     (if (vector? value)
                       (vec (map (partial assign-temporary-id temporary-id-resolution)
                                 value))
                       (assign-temporary-id temporary-id-resolution
                                            value)))
                   change)))
       temporary-changes))

(deftest test-assign-temporary-ids
  (is (= '([:add 100 :name 101]
           [:add 100 :friends [101]]
           [:add 101 :language "fi"]
           [:add 101 :text "parsa"])
         (assign-temporary-ids {:temporary/a 100
                                :temporary/b 101}
                               [[:add :temporary/a :name :temporary/b]
                                [:add :temporary/a :friends [:temporary/b]]
                                [:add :temporary/b :language "fi"]
                                [:add :temporary/b :text "parsa"]]))))

(defn- number-from-local-id [local-id]
  local-id
  #_(Integer/parseInt (subs (name local-id)
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
