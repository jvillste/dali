(ns argumentica.comparator
  (:use clojure.test)
  (:require [argumentica.util :as util]))

;; from https://clojure.org/guides/comparators

;; comparison-class throws exceptions for some types that might be
;; useful to include.

(defn comparison-class [x]
  (cond (nil? x) ""
        ;; Lump all numbers together since Clojure's compare can
        ;; compare them all to each other sensibly.
        (number? x) "java.lang.Number"

        ;; sequential? includes lists, conses, vectors, and seqs of
        ;; just about any collection, although it is recommended not
        ;; to use this to compare seqs of unordered collections like
        ;; sets or maps (vectors should be OK).  This should be
        ;; everything we would want to compare using cmp-seq-lexi
        ;; below.  TBD: Does it leave anything out?  Include anything
        ;; it should not?
        (sequential? x) "clojure.lang.Sequential"

        (set? x) "clojure.lang.IPersistentSet"
        (map? x) "clojure.lang.IPersistentMap"
        (.isArray (class x)) "java.util.Arrays"

        ;; Comparable includes Boolean, Character, String, Clojure
        ;; refs, and many others.
        (instance? Comparable x) (.getName (class x))
        :else (throw
               (ex-info (format "cc-cmp does not implement comparison of values with class %s"
                                (.getName (class x)))
                        {:value x}))))

(defn cmp-seq-lexi
  [cmpf x y]
  (loop [x x
         y y]
    (if (seq x)
      (if (seq y)
        (let [c (cmpf (first x) (first y))]
          (if (zero? c)
            (recur (rest x) (rest y))
            c))
        ;; else we reached end of y first, so x > y
        1)
      (if (seq y)
        ;; we reached end of x first, so x < y
        -1
        ;; Sequences contain same elements.  x = y
        0))))

;; The same result can be obtained by calling cmp-seq-lexi on two
;; vectors, but cmp-vec-lexi should allocate less memory comparing
;; vectors.
(defn cmp-vec-lexi
  [cmpf x y]
  (let [x-len (count x)
        y-len (count y)
        len (min x-len y-len)]
    (loop [i 0]
      (if (== i len)
        ;; If all elements 0..(len-1) are same, shorter vector comes
        ;; first.
        (compare x-len y-len)
        (let [c (cmpf (x i) (y i))]
          (if (zero? c)
            (recur (inc i))
            c))))))

(defn cmp-array-lexi
  [cmpf x y]
  (let [x-len (alength x)
        y-len (alength y)
        len (min x-len y-len)]
    (loop [i 0]
      (if (== i len)
        ;; If all elements 0..(len-1) are same, shorter array comes
        ;; first.
        (compare x-len y-len)
        (let [c (cmpf (aget x i) (aget y i))]
          (if (zero? c)
            (recur (inc i))
            c))))))

(defn compare-extremes [x y]
  (cond (and (= ::max x)
             (not= ::max y))
        1

        (and (not= ::max x)
             (= ::max y))
        -1

        (and (= ::min x)
             (not= ::min y))
        -1

        (and (not= ::min x)
             (= ::min y))
        1

        :default
        nil))

(deftest test-compare-with-extremes
  (is (= 1 (compare-extremes ::max 1 )))
  (is (= -1 (compare-extremes 1 ::max)))

  (is (= -1 (compare-extremes ::min 1)))
  (is (= 1 (compare-extremes 1 ::min)))

  (is (= nil (compare-extremes ::min ::min))))

(defn compare-datoms
  [x y]
  (if-let [result (compare-extremes x y)]
    result
    (let [x-cls (comparison-class x)
          y-cls (comparison-class y)
          c (compare x-cls y-cls)]
      (cond (not= c 0) c                ; different classes

            ;; Compare sets to each other as sequences, with elements in
            ;; sorted order.
            (= x-cls "clojure.lang.IPersistentSet")
            (cmp-seq-lexi compare-datoms (sort compare-datoms x) (sort compare-datoms y))

            ;; Compare maps to each other as sequences of [key val]
            ;; pairs, with pairs in order sorted by key.
            (= x-cls "clojure.lang.IPersistentMap")
            (cmp-seq-lexi compare-datoms
                          (sort-by key compare-datoms (seq x))
                          (sort-by key compare-datoms (seq y)))

            (= x-cls "java.util.Arrays")
            (cmp-array-lexi compare-datoms x y)

            ;; Make a special check for two vectors, since cmp-vec-lexi
            ;; should allocate less memory comparing them than
            ;; cmp-seq-lexi.  Both here and for comparing sequences, we
            ;; must use compare-datoms recursively on the elements, because if
            ;; we used compare we would lose the ability to compare
            ;; elements with different types.
            (and (vector? x) (vector? y)) (cmp-vec-lexi compare-datoms x y)

            ;; This will compare any two sequences, if they are not both
            ;; vectors, e.g. a vector and a list will be compared here.
            (= x-cls "clojure.lang.Sequential")
            (cmp-seq-lexi compare-datoms x y)

            :else (compare x y)))))

(defmacro run-compare-datoms-test [collection]
  `(is (= ~collection (sort-by identity
                               compare-datoms
                               ~collection))))

(deftest test-compare-datoms
  (run-compare-datoms-test [::min ::max])
  (run-compare-datoms-test [10000 ::max])
  (run-compare-datoms-test [::min 10000])
  (run-compare-datoms-test [::min "zzzzz"])
  (run-compare-datoms-test ["zzzzz" ::max])

  (run-compare-datoms-test [::min [1 2]])
  (run-compare-datoms-test [[1 2] ::max])

  (run-compare-datoms-test [[1 :name 1 :set "Name 1"]
                            [1 :name 2 :set "Name 1"]])

  (run-compare-datoms-test [[1 :name nil nil nil]
                            [1 :name 2 :set "Name 1"]])

  (run-compare-datoms-test [[1 :name 2 :set "Name 1"]
                            [1 :name ::max ::max ::max]])

  (run-compare-datoms-test [[1 :name 2 ::min ::min]
                            [1 :name 2 :set "Bar"]])

  (let [sorted-set (sorted-set-by compare-datoms
                                  [1 :name 1 :set "Foo"]
                                  [1 :name 2 :set "Bar"]
                                  [1 :age 2 :set 30]
                                  [1 :name 3 :set "Baz"])]

    (is (= '([1 :age 2 :set 30]
             [1 :name 1 :set "Foo"]
             [1 :name 2 :set "Bar"]
             [1 :name 3 :set "Baz"])
           (subseq sorted-set
                   >=
                   ::min)))

    (is (= nil
           (subseq sorted-set
                   >=
                   ::max)))

    (is (= '([1 :name 2 :set "Bar"]
             [1 :name 3 :set "Baz"])
           (subseq sorted-set
                   >=
                   [1 :name 2 ::min ::min])))

    (is (= '([1 :age 2 :set 30]
             [1 :name 1 :set "Foo"]
             [1 :name 2 :set "Bar"])
           (subseq sorted-set
                   <=
                   [1 :name 2 ::max ::max])))

    (is (= '([1 :age 2 :set 30]
             [1 :name 1 :set "Foo"]
             [1 :name 2 :set "Bar"]
             [1 :name 3 :set "Baz"])
           (subseq sorted-set
                   >=
                   [0 :name 0 ::min ::min])))))

(deftype DatomComparator []
  java.util.Comparator
  (compare [this x y]
    (compare-datoms x y)))
