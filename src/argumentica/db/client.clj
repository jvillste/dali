(ns argumentica.db.client
  (:require ;; [cor.client :as client]
            [argumentica.db.server-api :as server-api]))

;; (client/define-multimethods-for-api-namespace argumentica.db.server-api)

;; (client/define-in-process-client InProcessClient *ns*)

;; (client/define-http-client HttpClient *ns*)

(defn latest-root [])
(defn transaction-log-subseq [])
(defn last-transaction-number [])
(defn transact [])
(defn get-from-node-storage [])
(defrecord InProcessClient [])

(comment
  (let [client (->HttpClient "http://localhost:9999/api")]
    (transaction-log-subseq client 0)))
