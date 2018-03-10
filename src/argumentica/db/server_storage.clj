(ns argumentica.db.server-storage
  (:require (argumentica [storage :as storage])
            (argumentica.db [client :as client])))


(defrecord ServerStorage [client index-key])

(defmethod storage/get-from-storage!
  ServerStorage
  [this key]
  (client/get-from-node-storage (:client this)
                                (:index-key this)
                                key))


