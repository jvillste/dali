(ns argumentica.client
  (:require [net.tcp         :as tcp]
            [net.ty.channel  :as channel]
            [net.ty.pipeline :as pipeline]
            [argumentica.log :as log]))

(log/write "---------")

(def client-adapter
  (reify
    pipeline/HandlerAdapter
    (channel-read [this ctx msg]
      (log/write "server said: " msg)
      (channel/close! ctx))

    pipeline/ChannelActive
    (channel-active [this ctx]
      (channel/write-and-flush! ctx "hi and bye!"))))

(defn pipeline
  []
  (let [group (channel/channel-group "clients")]
    (pipeline/channel-initializer
     [(pipeline/line-based-frame-decoder)
      pipeline/string-decoder
      pipeline/string-encoder
      pipeline/line-frame-encoder
      (pipeline/make-handler-adapter client-adapter)])))


(-> (tcp/client "localhost" 1337 {:handler (pipeline)
                                  :so-keepalive true})
    (.sync)
    (.channel)
    (.closeFuture)
    (.sync))

(log/write "client closed")
