(ns argumentica.graphliner
  (:require (flow-gl.gui [drawable :as drawable]
                         [layout :as layout]
                         [layouts :as layouts]
                         [layout-dsl :as l]
                         [controls :as controls]
                         [gui :as gui]
                         [events :as events]
                         [layoutable :as layoutable]
                         [transformer :as transformer])
            [flow-gl.gui.components.autocompleter :as autocompleter]
            [datomic.api :as d]
            (flow-gl.opengl.jogl [quad :as quad]
                                 [render-target :as render-target]
                                 [opengl :as opengl])
            (flow-gl.tools [profiler :as profiler]
                           [trace :as trace])
            (flow-gl.graphics [font :as font]))
  (:import [javax.media.opengl GL2])
  (:use flow-gl.utils
        clojure.test))


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
        
        (attribute :argumentica/child
                   :db.type/ref
                   :db.cardinality/many)])
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



(defn text
  ([value]
   (text value [255 255 255 255]))

  ([value color]
   (text value color 12))
  
  ([value color size]
   (drawable/->Text (str value)
                    (font/create "LiberationSans-Regular.ttf" size)
                    color)))

(defn argument-view [argument]
  (l/vertically (for [premise (:argumentica.argument/premises argument)]
                  (text (str "text: "(:argumentica.sentence/text premise))))
                (l/margin 10 0 10 0
                          (drawable/->Rectangle 10 2 [255 255 255 255]))
                (text (-> argument
                          :argumentica.argument/main-conclusion
                          :argumentica.sentence/text))))

(defn set-changes [state changes]
  (let [result (d/with (:db state)
                       changes)]
    (assoc state
           :changes changes
           :db-with-changes (:db-after result)
           :ids-to-tempids-map (ids-to-tempids (:db-after result)
                                               (:tempids result)
                                               (tempids changes)))))

(defn attribute-editor [view-context state entity-id attribute]
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
                                                                                                   :argumentica/label
                                                                                                   new-value)))))))
                    (when (not= old-value new-value)
                      (text "*" [255 0 0 255] 30)))))


(defn entity-view [view-context state entity]
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


(defn button [text-value]
  (layouts/->Box 10 [(drawable/->Rectangle 0
                                           0
                                           [0 200 200 1])
                     (text text-value)]))

(defn argumentica-root-view [view-context state]
  (trace/log "root-view")
  (l/vertically
   (gui/call-view autocompleter/autocompleter :completer-1
                  {:query-function (fn [query]
                                     (map (fn [id]
                                            (d/entity (:db-with-changes state) id))
                                          (entities-by-label (:db-with-changes state) query)))
                   :selected-value (:root-entity state)
                   :on-select (fn [selection]
                                (if-let [old-entity (if (string? selection)
                                                      (d/entity (:db-with-changes state)
                                                                (entity-by-label (:db-with-changes state) selection))
                                                      selection)]
                                  (do (trace/log "old entity" old-entity)
                                      (gui/send-local-state-transformation view-context
                                                                           assoc :root-entity old-entity)) 
                                  (gui/send-local-state-transformation view-context
                                                                       (fn [state]
                                                                         (let [id (d/tempid :db.part/user)
                                                                               state-with-changes (set-changes state (concat (:changes state)
                                                                                                                             (set-label id selection)))]
                                                                           (trace/log "adding new" selection)
                                                                           (assoc state-with-changes
                                                                                  :root-entity (d/entity (:db-with-changes state-with-changes)
                                                                                                         id)))))))}
                  [:argumentica/label
                   0])

   (for [entity (entities (:db-with-changes state))]
     (entity-view view-context
                  state
                  entity))
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
   (text (:changes state))))

(defn argumentica-root [conn]
  (fn [view-context]
    {:local-state (-> {:conn conn
                       :db (d/db conn)}
                      (set-changes []))
     
     :view #'argumentica-root-view}))

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
  (trace/with-trace
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

