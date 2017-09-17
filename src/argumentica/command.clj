(ns argumentica.command)

(defn find-command-implementation [api-namespace command]
  (if-let [function-var (get (ns-publics api-namespace)
                             (symbol (name command)))]
    (if (:public (meta function-var))
      function-var)))

(defn dispatch-command [api-namespace message & additional-arguments]
  (let [[command & arguments] message]
    (if-let [function-var (find-command-implementation api-namespace
                                                       command)]
      (apply @function-var (concat additional-arguments
                                   arguments))
      (str "unknown command: " command))))
