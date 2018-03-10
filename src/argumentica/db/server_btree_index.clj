(ns argumentica.db.server-btree-index
  (:require (argumentica [index :as index]
                         [btree :as btree]
                         [btree-index :as btree-index])
            (argumentica.db [server-storage :as server-storage])))

(defn create [client index-key latest-root]
  (btree-index/->BtreeIndex (atom (btree/create-from-options :metadata-storage nil
                                                             :node-storage (server-storage/->ServerStorage client index-key)
                                                             :latest-root latest-root))))
