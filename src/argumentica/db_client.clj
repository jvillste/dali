(ns argumentica.db-client
  (:require [net.ty.channel  :as channel]
            [net.ty.pipeline :as pipeline]
            [argumentica.log :as log]
            [argumentica.client :as client]
            [clojure.core.async :as async]
            [argumentica.command :as command]
            [argumentica.db :as db]))

(def state-atom (let [state-atom (atom {:db (db/create)})]
                  (client/start-client 1337
                                       (reify
                                         pipeline/HandlerAdapter
                                         (channel-read [this channel-handler-context message-string]
                                           (log/with-exception-logging
                                             (let [[call-id result] (read-string message-string)]
                                               (if-let [result-channel (get-in @state-atom [:result-channels call-id])]
                                                 (async/>!! result-channel
                                                            result)
                                                 (do (log/write "client got message" (log/cut-string-to-max-length message-string
                                                                                                                   300))
                                                     (command/dispatch-command 'argumentica.db-client
                                                                               (read-string message-string) 
                                                                               channel-handler-context
                                                                               state-atom))))))

                                         pipeline/ChannelActive
                                         (channel-active [this channel-handler-context]
                                           (log/write "client connected")
                                           (swap! state-atom assoc :channel-handler-context channel-handler-context))
                                         
                                         pipeline/ChannelInactive
                                         (channel-inactive [this channel-handler-context]
                                           (log/write "client disconnected"))))
                  state-atom))

(defn call [state-atom command]
  (let [call-id (rand-int 1000)
        result-channel (async/chan)]
    (swap! state-atom
           assoc-in
           [:result-channels call-id]
           result-channel)

    (log/write "sending call with id " call-id)
    
    (channel/write-and-flush! (:channel-handler-context @state-atom)
                              (pr-str [call-id command]))
    result-channel))

(comment
  (async/go (let [result (async/<! (call state-atom [:get-transaction-log]))]
              (log/write "got result: " result)))
  (stop))
