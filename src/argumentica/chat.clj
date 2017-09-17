(ns argumentica.chat
  (:require [argumentica.server :as server]
            [argumentica.log :as log]
            [net.ty.channel  :as channel]
            [net.ty.pipeline :as pipeline]))

(declare stop)

(if (and (bound? #'stop)
         stop)
  (.sync (stop)))

(def stop (server/start-server 1337
                               (let [channel-group (channel/channel-group "clients")]
                                 (reify
                                   pipeline/HandlerAdapter
                                   (channel-read [this channel-handler-context message-string]
                                     (log/write  "server got message: " message-string)
                                     (if (= message-string "quit")
                                       (do (channel/write-and-flush! channel-handler-context
                                                                     "bye!")
                                           (channel/close! channel-handler-context))
                                       (let [src (channel/channel channel-handler-context)]
                                         (doseq [dst channel-group :when (not= dst src)]
                                           (channel/write-and-flush! dst message-string)))))

                                   pipeline/ChannelActive
                                   (channel-active [this channel-handler-context]
                                     (log/write "server got new connection")
                                     (channel/add-to-group channel-group (channel/channel channel-handler-context))
                                     (channel/write-and-flush! channel-handler-context "welcome to the chat"))))))

(comment
  (stop))
