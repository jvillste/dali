(ns argumentica.editor
  (:require (flow-gl.gui [drawable :as drawable]
                         [layout :as layout]
                         [layouts :as layouts]
                         [layout-dsl :as l]
                         [controls :as controls]
                         [gui :as gui]
                         [events :as events]
                         [layoutable :as layoutable]
                         [transformer :as transformer])
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

(defn attribute [ident value-type cardinality & {:keys [identity] :or {identity false}}]
  (-> {:db/id (d/tempid :db.part/db)
       :db/ident ident
       :db/valueType value-type
       :db/cardinality cardinality
       :db.install/_attribute :db.part/db}
      (cond-> identity
        (assoc :db/unique :db.unique/identity))))

(defn create-database []
  (let [db-uri "datomic:mem://argumentica"]
    (d/delete-database db-uri)
    (d/create-database db-uri)
    (let [conn (d/connect db-uri)]
      (d/transact
       conn
       [(attribute :argumentica.sentence/text
                   :db.type/string
                   :db.cardinality/one
                   :identity true)
        
        (attribute :argumentica.argument/premises
                   :db.type/ref
                   :db.cardinality/many)

        (attribute :argumentica.argument/main-conclusion
                   :db.type/ref
                   :db.cardinality/one)

        (attribute :argumentica.argument/title
                   :db.type/string
                   :db.cardinality/one)])
      conn)))


;; queries


(defn sentence-by-text [db text]
  (d/q '[:find ?sentence .
         :in $ ?text
         :where
         [?sentence :argumentica.sentence/text ?text]]
       db
       text))

(defn value [db entity attribute]
  (d/q '[:find ?value .
         :in $ ?entity ?attribute
         :where
         [?entity ?attribute ?value]]
       db
       entity
       attribute))

(defn main-conclusions [db]
  (d/q '[:find [?main-conclusion ...]
         :in $
         :where
         [_ :argumentica.argument/main-conclusion ?main-conclusion]]
       db))

;; adding

(defn add-sentence
  ([text]
   {:db/id (d/tempid :db.part/user)
    :argumentica.sentence/text text})

  ([id text]
   {:db/id id
    :argumentica.sentence/text text}))

(defn ensure-sentence [db text]
  (if-let [sentence (sentence-by-text db text)]
    [sentence []]
    (let [id (d/tempid :db.part/user)]
      [id [(add-sentence id text)]])))

(defn add-argument [db title main-conclusion & premises]
  (let [[main-conclusion-id main-conclusion-transaction] (ensure-sentence db main-conclusion)
        premise-data (map (fn [premise] (ensure-sentence db premise)) premises)
        premise-ids (map first premise-data)
        premise-transactions (mapcat second premise-data)]
    
    (concat main-conclusion-transaction
            premise-transactions
            [{:db/id (d/tempid :db.part/user)
              :argumentica.argument/main-conclusion main-conclusion-id
              :argumentica.argument/premises premise-ids
              :argumentica.argument/title title} ])))


(defn set-attribute-value [transaction entity-id attribute value]
  (println "setting" transaction entity-id attribute value)
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

(let [conn (create-database)
      tempid (d/tempid :db.part/user)
      transaction [{:db/id tempid
                    :argumentica.argument/title "Foo"}]
      result @(d/transact conn
                          transaction)]
  (println (tempids transaction))
  (println (instance? datomic.db.DbId tempid))
  (println (type tempid)  tempid (:tempids result))
  (println (d/resolve-tempid (:db-after result)
                             (:tempids result) tempid))
  (println (ids-to-tempids (:db-after result)
                           (:tempids result)
                           (tempids transaction)))
  
  #_(println "foo:" (let [db (d/db conn)
                          entity-id (first (main-conclusions db))]
                      (value db entity-id :argumentica.sentence/text)))
  #_(println "foo:" (main-conclusions (d/db conn))
             #_(-> (d/entity (d/db conn)
                             (sentence-by-text (d/db conn) "conclusion"))
                   :argumentica.argument/_main-conclusion
                   first
                   :argumentica.argument/premises
                   second
                   :argumentica.sentence/text)))




(defn text
  ([value]
   (text value [255 255 255 255]))

  ([value color]
   (text value color 15))
  
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

(defn attribute-editor [view-context db db-with-changes state entity-id attribute]
  (let [old-value (value db
                         entity-id
                         attribute)
        new-value (value db-with-changes
                         entity-id
                         attribute)
        changed-value-key [entity-id attribute]]
    (l/horizontally (-> (gui/call-view controls/text-editor [:editor changed-value-key])
                        (update-in [:state-overrides] assoc :text new-value)
                        (update-in [:constructor-overrides] assoc [:on-change :text] (fn [state new-value]
                                                                                       (gui/apply-to-local-state state
                                                                                                                 view-context
                                                                                                                 (fn [state]
                                                                                                                   (set-changes state
                                                                                                                                (set-attribute-value (:changes state)
                                                                                                                                                     (or (get (:ids-to-tempids-map state)
                                                                                                                                                              entity-id)
                                                                                                                                                         entity-id)
                                                                                                                                                     :argumentica.sentence/text
                                                                                                                                                     new-value)))))))
                    (when (not= old-value new-value)
                      (text "*" [255 0 0 255] 30)))))

(defn button [text-value]
  (layouts/->Box 10 [(drawable/->Rectangle 0
                                           0
                                           [0 200 200 1])
                     (text text-value)]))




(defn argumentica-root-view [view-context state]
  (l/vertically 
   (for [conclusion (main-conclusions (:db-with-changes state))]
     (attribute-editor view-context
                       (:db state)
                       (:db-with-changes state)
                       state
                       conclusion
                       :argumentica.sentence/text))
   (-> (button "New")
       (gui/on-mouse-clicked-with-view-context view-context
                                               (fn [state event]
                                                 (set-changes state (concat (:changes state)
                                                                            (add-argument (:db-with-changes state) "New argument" "conclusion"))))))
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
                              (add-argument (d/db connection) "argument 1" "conclusion 1" "premise 1" "premise 2"))
                  (d/transact connection
                              (add-argument (d/db connection) "argument 2" "conclusion 2" "premise 1" "premise 3"))
                  connection))

(defn start []
  #_(.start (Thread. (fn []
                       (trace/untrace-ns 'flow-gl.gui.gui)
                       (trace/trace-var* 'flow-gl.gui.gui/set-focus-if-can-gain-focus)
                       (trace/trace-var* 'flow-gl.gui.gui/set-focus)
                       #_(trace/trace-var* 'flow-gl.gui.gui/resolve-size-dependent-view-calls)
                       (trace/with-trace
                         (gui/start-control argumentica-root)))))

  #_(.start (Thread. (fn []
                       (gui/start-control (argumentica-root connection)))))
  (.start (Thread. (fn []
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
;; changing new arguents conclusion does not work because d/with generates a new entity id every time
;; use resolveTempId to build a translation table that is used to manipulate the transaction

