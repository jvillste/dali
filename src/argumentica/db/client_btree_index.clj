(ns argumentica.db.client-btree-index
  (:require [argumentica.db.sorted-datom-set-branch :as sorted-datom-set-branch]
            [argumentica.db.server-btree-index :as server-btree-index]))

(defrecord ClientBtreeIndex [sorted-datom-set-branch-atom])

(defn create []
  (->ClientBtreeIndex (atom (sorted-datom-set-branch/create (server-btree-index/create client
                                                                                       (:key index-definition)
                                                                                       latest-root)
                                                            (-> latest-root :metadata :last-transaction-number)
                                                            (:datom-transaction-number-index index-definition)
                                                            (sorted-set-index/create)))))
(util/defn-alias create )

(defmethod index/add!
  SortedDatomSetBranch
  [this value]
  (index/add! (:branch-datom-set this)
              value))
