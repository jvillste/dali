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

(defn- temporal-id [number]
  (keyword "id" (str "t" number)))

(defn resolve-temporary-ids [next-id temporary-statements]
  (let [temporary-ids (->> (concat (->> temporary-statements
                                        (map second))
                                   (->> temporary-statements
                                        (map last)))
                           (filter temporary-id?)
                           (distinct))]

    (->> (map (comp local-id
                    (partial + next-id))
              (range))
         (map vector
              temporary-ids)
         (into {}))))

(deftest testresolve-temporary-ids
  (is (= #:id{:ta :id/l100,
              :tb :id/l101}
         (resolve-temporary-ids 100
                                [[:add :id/ta :name :id/tb]
                                 [:add :id/tb :language "fi"]
                                 [:add :id/tb :text "parsa"]])))

  (is (= #:id{:ta :id/l100,
              :tb :id/l101}
         (resolve-temporary-ids 100
                                [[:add :id/ta :name :id/tb]]))))

(defn- assign-temporary-ids [temporary-id-resolution temporary-statements]
  (let [substitute-temporary-id (fn [value]
                                  (or (get temporary-id-resolution
                                           value)
                                      value))]
    (for [[operator entity attribute value] temporary-statements]
      [operator
       (substitute-temporary-id entity)
       attribute
       (substitute-temporary-id value)])))

(deftest test-assign-temporary-ids
  (is (= '([:add 100 :name 101]
           [:add 101 :language "fi"]
           [:add 101 :text "parsa"])
         (assign-temporary-ids {:temporary/a 100
                                :temporary/b 101}
                               [[:add :temporary/a :name :temporary/b]
                                [:add :temporary/b :language "fi"]
                                [:add :temporary/b :text "parsa"]]))))

(defn temporary-statements-to-statements [next-id temporary-statements]
  (let [temporary-id-resolution (resolve-temporary-ids next-id temporary-statements)]
    {:temporary-id-resolution temporary-id-resolution
     :statements (assign-temporary-ids temporary-id-resolution
                                       temporary-statements)}))

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
