(ns argumentica.db.server-connection
  (:require [argumentica.db.server-btree-db :as server-connection]
            [flatland.useful.map :as map]
            [argumentica.db.common :as common]
            [argumentica.db.peer-index :as peer-index]
            [argumentica.index :as index]))

(defn- update-index-roots [server-connection]
  (update server-connection
          :indexes
          map/map-vals
          peer-index/update-root))

(defn- update-indexes [server-connection]
  (-> server-connection
      (update-index-roots)
      (common/update-indexes)))

(defn- index-definition-to-peer-indexes [index-definition client]
  (into {}
        (map (fn [[key eatcv-to-datoms]]
               [key
                (peer-index/create client key eatcv-to-datoms)])
             index-definition)))

(defrecord ServerConnection [client indexes]
  clojure.lang.IDeref
  (deref [this]
    {:indexes indexes
     :last-transaction-number (-> indexes vals first)}))

(defn create [client index-definition]
  (update-indexes (map->ServerConnection {:client client
                                          :indexes (index-definition-to-peer-indexes index-definition
                                                                                     client)})))
