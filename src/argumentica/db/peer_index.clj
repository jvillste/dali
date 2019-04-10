(ns argumentica.db.peer-index
  (:require [argumentica.index :as index]
            [argumentica.db.client :as client]
            [argumentica.sorted-set-index :as sorted-set-index]
            [argumentica.db.server-btree-index :as server-btree-index]
            [argumentica.db.server-btree-index :as server-btree-index]
            [argumentica.db.sorted-datom-set-branch :as sorted-datom-set-branch]))

(defrecord PeerIndex [local-index
                      remote-index])

(defmethod index/add!
  PeerIndex
  [this first-datom]
  (index/add! (get-in this [:local-index])
              first-datom))

(defmethod index/inclusive-subsequence
  PeerIndex
  [this first-datom]
  (concat (index/inclusive-subsequence (get-in this [:remote-index])
                                       first-datom)
          (index/inclusive-subsequence (get-in this [:local-index])
                                       first-datom)))

#_(defn create [client index-key eatcv-to-datoms]
  (let [latest-root (client/latest-root client
                                        index-key)]
    {:client client
     :index-key index-key
     :eatcv-to-datoms eatcv-to-datoms
     :index (map->PeerIndex {:local-index (sorted-set-index/create)
                             :remote-index (server-btree-index/create client
                                                                      index-key
                                                                      latest-root)})
     :last-indexed-transaction-number (-> latest-root :metadata :last-transaction-number)}))

(defn create [client index-definition base-transaction-number]
  (let [latest-root (client/latest-root client
                                        (:index-key index-definition))]
    (merge index-definition
           {:client client
            :index (sorted-datom-set-branch/create (server-btree-index/create client
                                                                              (:index-key index-definition)
                                                                              latest-root)
                                                   base-transaction-number
                                                   (:transaction-number-index index-definition)
                                                   (sorted-set-index/create))
            :last-indexed-transaction-number (-> latest-root :metadata :last-transaction-number)})))


(defn update-root [peer-index]
  (let [latest-root (client/latest-root (:client peer-index)
                                        (:index-key peer-index))
        previous-root (-> peer-index :index :remote-index :btree-index-atom deref :latest-root)]

    (if (= latest-root previous-root)
      peer-index
      (-> peer-index
          (update-in [:index :remote-index]
                     (fn [remote-btree-index]
                       (server-btree-index/set-root remote-btree-index
                                                    latest-root)))
          (assoc-in [:index :local-index]
                    (sorted-set-index/create))
          (assoc :last-indexed-transaction-number (-> latest-root :metadata :last-transaction-number))))))
