(ns argumentica.graphliner
  (:require [datomic.api :as d]
            [datascript.core :as ds])
  (:use clojure.test))


;; datomic

(defn attribute [ident value-type cardinality & {:keys [identity fulltext] :or {identity false fulltext false}}]
  (-> {:db/id (d/tempid :db.part/db)
       :db/ident ident
       :db/valueType value-type
       :db/cardinality cardinality
       :db.install/_attribute :db.part/db}
      (cond-> identity
        (assoc :db/unique :db.unique/identity))
      (cond-> fulltext
        (assoc :db/fulltext true))))

(defn create-database []
  (let [db-uri "datomic:mem://argumentica"]
    (d/delete-database db-uri)
    (d/create-database db-uri)
    (let [conn (d/connect db-uri)]
      (d/transact
       conn
       [(attribute :argumentica/label
                   :db.type/string
                   :db.cardinality/one
                   :fulltext true)

        (attribute :argumentica.argument/premises
                   :db.type/ref
                   :db.cardinality/many)

        (attribute :argumentica.argument/conclusion
                   :db.type/ref
                   :db.cardinality/many)

        (attribute :argumentica.argument/opposes
                   :db.type/boolean
                   :db.cardinality/one)])
      conn)))


;; queries

(defn entities-by-label [db search]
  (d/q '[:find [?entity ...]
         :in $ ?search
         :where
         [(fulltext $ :argumentica/label ?search) [[?entity ?label]]]]
       db
       search))

(defn entity-by-label [db search]
  (d/q '[:find ?entity .
         :in $ ?search
         :where
         [?entity :argumentica/label ?search]]
       db
       search))

(defn entities [db]
  (d/q '[:find [?entity ...] 
         :in $
         :where
         [?entity :argumentica/label _]]
       db))

(defn value [db entity attribute]
  (d/q '[:find ?value .
         :in $ ?entity ?attribute
         :where
         [?entity ?attribute ?value]]
       db
       entity
       attribute))

(defn values [db entity attribute]
  (d/q '[:find [?value ...]
         :in $ ?entity ?attribute
         :where
         [?entity ?attribute ?value]]
       db
       entity
       attribute))

(defn attributes [db entity]
  (d/q '[:find [?attribute ...]
         :in $ ?entity
         :where
         [?entity ?attribute]]
       db
       entity))

;; adding

(defn set-label
  ([label]
   (set-label (d/tempid :db.part/user) label))

  ([id label]
   [{:db/id id
     :argumentica/label label}]))

(defn new-id []
  (d/tempid :db.part/user))

(defn add-conclusion [argument-id statement-id]
  [{:db/id argument-id
    :argumentica.argument/conclusion statement-id}])


(defn set-attribute-value [transaction entity-id attribute value]
  (let [updated-transaction (reduce (fn [updated-transaction statement-map]
                                      (if (= entity-id (:db/id statement-map))
                                        (conj updated-transaction
                                              (assoc statement-map
                                                     attribute value))
                                        (conj updated-transaction
                                              statement-map)))
                                    []
                                    transaction)]
    (if (= transaction updated-transaction)
      (conj transaction
            {:db/id entity-id
             attribute value})
      updated-transaction)))

(defn tempids [transaction]
  (->> transaction
       (map :db/id)
       (filter #(instance? datomic.db.DbId %))))

(defn ids-to-tempids [db transaction-tempids tempids]
  (reduce (fn [result tempid]
            (assoc result
                   (d/resolve-tempid db
                                     transaction-tempids
                                     tempid)
                   tempid))
          {}
          tempids))


(defn tempid-to-id [state tempid]
  (-> (filter (fn [[key value]]
                (= value tempid))
              (:ids-to-tempids-map state))
      (first)
      (first)))


(defn set-changes [state changes]
  (let [result (d/with (:db state)
                       changes)]
    (assoc state
           :changes changes
           :db-with-changes (:db-after result)
           :ids-to-tempids-map (ids-to-tempids (:db-after result)
                                               (:tempids result)
                                               (tempids changes)))))

#_(defn attribute-editor [view-context state entity-id attribute]
    (let [old-value (value (:db state)
                           entity-id
                           attribute)
          new-value (value (:db-with-changes state)
                           entity-id
                           attribute)
          changed-value-key [entity-id attribute]]
      (l/horizontally (-> (gui/call-view controls/text-editor [:editor changed-value-key])
                          (update-in [:state-overrides]
                                     assoc
                                     :text new-value)
                          (update-in [:constructor-overrides]
                                     assoc [:on-change :text]
                                     (fn [global-state new-value]
                                       (gui/apply-to-local-state global-state
                                                                 view-context
                                                                 (fn [state]
                                                                   (set-changes state
                                                                                (set-attribute-value (:changes state)
                                                                                                     (or (get (:ids-to-tempids-map state)
                                                                                                              entity-id)
                                                                                                         entity-id)
                                                                                                     attribute
                                                                                                     new-value)))))))
                      (when (not= old-value new-value)
                        (text "*" [255 0 0 255] 30)))))


#_(defn entity-view [view-context state entity]
    (l/vertically (attribute-editor view-context
                                    state
                                    entity
                                    :argumentica/label)
                  (l/margin 0 0 0 10
                            (l/vertically (for [attribute (attributes (:db-with-changes state) entity)]
                                            (let [ident (value (:db-with-changes state) attribute :db/ident)]
                                              (when (not= ident :argumentica/label)
                                                (l/horizontally (text ident)
                                                                (l/vertically (for [entity (values (:db-with-changes state) entity attribute)]
                                                                                (entity-view view-context state entity)))))
                                              #_(text ident)))))))


#_(defn statement-view [view-context state statement]
    (l/vertically (attribute-editor view-context
                                    state
                                    statement
                                    :argumentica/label)
                  (l/horizontally (l/vertically (text "+")
                                                (-> (button "Add")
                                                    (gui/on-mouse-clicked-with-view-context view-context
                                                                                            (fn [state event]
                                                                                              (set-changes state (concat (:changes state)
                                                                                                                         (add-conclusion (new-id) (:db/id statement)))))))) 
                                  (l/vertically (for [supporting-argument (:_argumentica.argument/conclusion statement)]
                                                  (l/vertically (attribute-editor view-context
                                                                                  state
                                                                                  supporting-argument
                                                                                  :argumentica/label)
                                                                (for [premise (:argumentica.argument/premises supporting-argument)]
                                                                  (statement-view state premise))))))))


#_(defn button [text-value]
    (layouts/->Box 10 [(drawable/->Rectangle 0
                                             0
                                             [0 200 200 1])
                       (text text-value)]))

#_(defn argumentica-root-view [view-context state]
    (l/vertically
     (gui/call-view autocompleter/autocompleter :completer-1
                    {:text-function (fn [id]
                                      (-> (d/entity (:db-with-changes state) id)
                                          (:argumentica/label)))
                     :query-function (fn [query]
                                       (entities-by-label (:db-with-changes state) query))
                     :selected-value (:root-entity state)
                     :on-select (fn [selection]
                                  (if-let [old-entity (if (string? selection)
                                                        (entity-by-label (:db-with-changes state)
                                                                         selection)
                                                        selection)]
                                    (gui/send-local-state-transformation view-context
                                                                         assoc :root-entity old-entity) 
                                    (gui/send-local-state-transformation view-context
                                                                         (fn [state]
                                                                           (let [id (d/tempid :db.part/user)
                                                                                 state-with-changes (set-changes state (concat (:changes state)
                                                                                                                               (set-label id selection)))]
                                                                             (assoc state-with-changes
                                                                                    :root-entity (tempid-to-id state-with-changes id)))))))}
                    [0])

     (when-let [root-entity (:root-entity state)]
       (statement-view view-context
                       state
                       root-entity))

     (-> (button "New")
         (gui/on-mouse-clicked-with-view-context view-context
                                                 (fn [state event]
                                                   (set-changes state (concat (:changes state)
                                                                              (set-label "New entity"))))))
     (-> (button "Save")
         (gui/on-mouse-clicked-with-view-context view-context
                                                 (fn [state event]
                                                   (d/transact (:conn state)
                                                               (:changes state))
                                                   (-> state
                                                       (assoc :db (d/db (:conn state)))
                                                       (set-changes [])))))
     (-> (button "Refresh")
         (gui/on-mouse-clicked-with-view-context view-context
                                                 (fn [state event]
                                                   (-> state
                                                       (assoc :db (d/db (:conn state)))
                                                       (set-changes (:changes state))))))
     (-> (button "Cancel")
         (gui/on-mouse-clicked-with-view-context view-context
                                                 (fn [state event]
                                                   (set-changes state []))))
     (text (vec (:changes state)))))

(defn argumentica-root [conn]
  (fn [view-context]
    {:local-state (-> {:conn conn
                       :db (d/db conn)}
                      (set-changes []))
     
     ;; :view #'argumentica-root-view
     }))

(def connection (let [connection (create-database)]
                  (d/transact connection
                              (let [child-id (d/tempid :db.part/user)]
                                [{:db/id child-id
                                  :argumentica/label "Child entity"}
                                 {:db/id (d/tempid :db.part/user)
                                  :argumentica/label "Parent"
                                  :argumentica/child child-id}]))
                  connection))

(d/transact connection
            [{:db/id (d/tempid :db.part/user)
              :argumentica/label "Child"}])

#_(entities-by-label (d/db connection) "child")

(defn start []
  #_(trace/with-trace
      (trace/log "start")
      (gui/start-control (argumentica-root connection)))
  
  
  #_(.start (Thread. (fn []
                       (trace/with-trace
                         (trace/untrace-ns 'flow-gl.gui.gui)
                         #_(trace/trace-var* 'flow-gl.gui.gui/set-focus-if-can-gain-focus)
                         #_(trace/trace-var* 'flow-gl.gui.gui/set-focus)
                         #_(trace/trace-var* 'flow-gl.gui.gui/resolve-size-dependent-view-calls)
                         #_(trace/trace-var 'flow-gl.gui.gui/apply-to-state)
                         #_(trace/trace-var 'flow-gl.gui.gui/add-layout-afterwards)
                         (trace/trace-ns 'argumentica.graphliner)
                         
                         (gui/start-control (argumentica-root connection))))))

  #_(.start (Thread. (fn []
                       (gui/start-control (argumentica-root connection)))))
  #_(.start (Thread. (fn []
                       (gui/start-control (argumentica-root connection)))))
  
  #_(.start (Thread. (fn []
                       (let [conn (create-database)]
                         (d/transact conn
                                     (add-argument (d/db conn) "argument 1" "conclusion 1" "premise 1" "premise 2"))
                         
                         (d/transact conn
                                     (add-argument (d/db conn) "argument 2" "conclusion 1" "premise 1" "premise 3"))

                         (d/transact conn
                                     (add-argument (d/db conn) "argument 3" "conclusion 2" "premise 4" "premise 5"))
                         (gui/start-control (argumentica-root conn))))))

  #_(profiler/with-profiler (gui/start-control argumentica-root)))

;; TODO

