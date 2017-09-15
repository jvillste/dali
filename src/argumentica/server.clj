(ns argumentica.server
  (:require [net.tcp         :as tcp]
            [net.ty.channel  :as channel]
            [net.ty.pipeline :as pipeline]
            [argumentica.log :as log]))

(defn dispatch-command [message-string channel-handler-context state-atom api-namespace]
  (let [message (read message-string)
        [command & arguments] message]
    (log/write "handling " (pr-str message))
    (let [result (if-let [function-var (get (ns-publics api-namespace) (symbol (name command)))]
                   (if (:public (meta function-var))
                     (apply @function-var (concat [channel-handler-context state-atom] arguments))
                     (str "unknown command: " command))
                   (str "unknown command: " command))]

      (let [result-message (pr-str result)]
        (log/write "result " (subs result-message 0 (min (.length result-message)
                                                         300))))
      result)))

(defn call-handler [handler-key handlers channel-handler-context state-atom & arguments]
  (when-let [read (handler-key handlers)]
    (try (apply read
                channel-handler-context
                state-atom
                arguments)
         (catch Exception e
           (log/write "Exception in read handler" e)
           (.printStackTrace e *out*)
           (throw e)))))

(defn create-server-adapter
  [state-atom handlers]
  (reify
    pipeline/HandlerAdapter
    (channel-read [this channel-handler-context message-string]
      (call-handler :read
                    handlers
                    channel-handler-context
                    state-atom
                    message-string))

    pipeline/ChannelActive
    (channel-active [this channel-handler-context]
      (call-handler :new-channel-activated
                    handlers
                    channel-handler-context
                    state-atom))))

(defn create-pipeline
  [state-atom handlers]
  (pipeline/channel-initializer
   [(pipeline/line-based-frame-decoder)
    pipeline/string-decoder
    pipeline/string-encoder
    pipeline/line-frame-encoder
    (pipeline/read-timeout-handler 60)
    (pipeline/make-handler-adapter (create-server-adapter state-atom
                                                          handlers))]))

(defn start-server [port state-atom handlers]
  (tcp/server {:handler (create-pipeline state-atom handlers)}
              "localhost"
              port))


