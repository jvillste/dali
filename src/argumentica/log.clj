(ns argumentica.log
  (:require [clojure.core.async :as async]
            [clojure.stacktrace :as stacktrace]))

(defonce log-chan (async/chan))

(async/go-loop [message (async/<! log-chan)]
  (if message
    (do (println message)
        (recur (async/<! log-chan)))))

(defn write [& messages]
 (async/go (async/>! log-chan
                      (apply str messages))))

(defn cut-string-to-max-length [string max-length]
  (subs string 0 (min (.length string)
                      max-length)))

(defn log-data [data]
  (write (cut-string-to-max-length (pr-str data)
                                       300)))

(defmacro log-result [message & body]
  `(let [result# (do ~@body)]
     (write message (cut-string-to-max-length (pr-str result#)
                                                      300))
     result#))

(defmacro with-exception-logging [& body]
  `(try ~@body
        (catch Exception e#
          (write e#)
          (write (with-out-str (stacktrace/print-stack-trace e#)))
          (throw e#))))
