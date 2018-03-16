(ns argumentica.db.server-btree-index
  (:require (argumentica [index :as index]
                         [btree :as btree]
                         [btree-index :as btree-index])
            (argumentica.db [server-storage :as server-storage])))

(defn create [client index-key latest-root]
  (btree-index/->BtreeIndex (atom (btree/create-from-options :metadata-storage nil
                                                             :node-storage (server-storage/->ServerStorage client index-key)
                                                             :latest-root latest-root))))

(defn set-root [server-btree-index root]
  ;; We preserve the node storage to preserve the node cache
  
  (btree-index/->BtreeIndex (atom (btree/create-from-options :metadata-storage nil
                                                             :node-storage (-> server-btree-index btree-index/btree :node-storage)
                                                             :latest-root root))))
