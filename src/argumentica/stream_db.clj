(ns argumentica.stream-db
  (:require [argumentica.stream :as stream]
            [argumentica.db.common :as db-common]
            [argumentica.btree-collection :as btree-collection]))

(defn create-stream-db-in-memory [id index-definitions]
  (assoc (stream/in-memory {:id id})
         :indexes (db-common/index-definitions-to-indexes (fn [_index-key]
                                                            (btree-collection/create-in-memory))
                                                          index-definitions)))

(defn create-stream-db-on-disk [id path index-definitions]
  (let [stream-db (assoc (stream/on-disk path {:id id})
                         :indexes (db-common/index-definitions-to-indexes (fn [_index-key]
                                                                            (btree-collection/create-in-memory))
                                                                          index-definitions))]
    (db-common/update-indexes-2! stream-db)
    stream-db))
