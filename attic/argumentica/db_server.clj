(ns argumentica.db-server
  (:require [argumentica.server :as server]
            [argumentica.log :as log]
            [argumentica.db :as db]
            [argumentica.command :as command]
            [net.ty.pipeline :as pipeline]
            [net.ty.channel  :as channel]))

(defn broadcast-other-channels [source-channel channel-group message]
  (doseq [destination-channel channel-group :when (not= destination-channel source-channel)]
    (channel/write-and-flush! destination-channel
                              message)))


(defn ^:public transact-over [channel-handler-context state-atom parent-references & statements]
  (log/write "transact-over called")
  (let [transaction-to-be-broadcasted (atom nil)
        new-state (swap! state-atom
                         update
                         :db
                         (fn [db]
                           (let [transaction (db/transaction-over db
                                                                  parent-references
                                                                  statements)]
                             (reset! transaction-to-be-broadcasted
                                     transaction)

                             (db/transact-over db
                                               (first parent-references)
                                               transaction))))]
    (log/write "broadcasting " @transaction-to-be-broadcasted)
    (broadcast-other-channels (channel/channel channel-handler-context)
                              (:channel-group @state-atom)
                              (pr-str ['transact @transaction-to-be-broadcasted])))
  
  nil)

(defn ^:public get-transaction-log [channel-handler-context state-atom]
  (or (get-in @state-atom
              [:db :transaction-log])
      []))

(defn ^:public quit [channel-handler-context state-atom]
  (channel/close! channel-handler-context)
  nil)

(defn handle-message [channel-handler-context state-atom message-string]
  (log/with-exception-logging
    (log/write "server got message: " (log/cut-string-to-max-length message-string
                                                                    300))
    (let [[call-id message] (read-string message-string)]
      (when-let [result (command/dispatch-command 'argumentica.db-server
                                                  message
                                                  channel-handler-context
                                                  state-atom)]
        
        (log/write "serever sends result: " (log/cut-string-to-max-length (pr-str result)
                                                                          300))
        
        (channel/write-and-flush! channel-handler-context
                                  [call-id result])))))

(declare stop)

(if (and (bound? #'stop)
         stop)
  (.sync (stop)))

(def stop
  (server/start-server 1337
                       (let [state-atom (atom {:db (db/create)
                                               :channel-group (channel/channel-group "clients")})]
                         (reify
                           pipeline/HandlerAdapter
                           (channel-read [this channel-handler-context message-string]
                             (handle-message channel-handler-context
                                             state-atom
                                             message-string))

                           pipeline/ChannelActive
                           (channel-active [this channel-handler-context]
                             (log/write "server got new connection")
                             (channel/add-to-group (:channel-group @state-atom)
                                                   (channel/channel channel-handler-context)))))))

(comment
  (stop))
