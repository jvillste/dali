(ns argumentica.db
  (:require [datascript.core :as d]
            [datascript.db :as db])
  (:use clojure.test))

(deftest test-entity
  (let [db (-> (d/empty-db)
               (d/db-with [{:db/id -1, :name "Ivan", :age 19}
                           {:db/id 2, :name "Katerina", :sex "female"}]))
        e  (d/entity db 1)]
    (prn (d/datoms db)) 
    (is (= (:name e) "Ivan"))))

(let [db (-> (d/empty-db)
               (d/db-with [{:db/id 1, :name "Ivan", :age 19}
                           {:db/id -1, :name "Katerina", :sex "female"}]))]
    (d/datoms db :eavt))
