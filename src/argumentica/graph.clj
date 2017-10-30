(ns argumentica.graph
  (:use clojure.test))

(defn sort-topologically [root get-children get-value]
  (loop [sorted-nodes []
         to-be-added [root]]
    (if (empty? to-be-added)
      sorted-nodes
      (let [children (->> (get-children (peek to-be-added))
                          (reverse)
                          (drop-while (fn [child]
                                        (some #{(get-value child)}
                                              sorted-nodes))))]
        (if (empty? children)

          (recur (conj sorted-nodes
                       (get-value (peek to-be-added)))
                 (pop to-be-added))
          
          (recur sorted-nodes
                 (vec (concat to-be-added
                              children))))))))

(deftest test-sort-topologically
  (is (= [2 3 1]
         (sort-topologically {:value 1
                              :children [{:value 2}
                                         {:value 3}]}
                             :children
                             :value))))
