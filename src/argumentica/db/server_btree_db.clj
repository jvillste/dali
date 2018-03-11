(ns argumentica.db.server-btree-db
  (:require (argumentica.db [common :as common]
                            [client :as client]
                            [server-btree-index :as server-btree-index])))

(defn create [client]
  (common/create :indexes {:eatcv {:index (server-btree-index/create client
                                                                     :eatcv
                                                                     (client/latest-root client
                                                                                         :eatcv))}}))


