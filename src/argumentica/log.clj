(ns argumentica.log
  (:require [clojure.core.async :as async]))

(defonce log-chan (async/chan))

(async/go-loop [message (async/<! log-chan)]
  (if message
    (do (println message)
        (recur (async/<! log-chan)))))

(defn write [& messages]
 (async/go (async/>! log-chan
                      (apply str messages))))
