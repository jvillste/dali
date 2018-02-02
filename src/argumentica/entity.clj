(ns argumentica.entity
    (:import [java.util UUID]))

(defn new-entity-id []
  (UUID/randomUUID))
