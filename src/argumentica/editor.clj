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


(let [conn (create-database)]
  (d/transact conn
              (add-argument (d/db conn) "argument 1" "conclusion" "premise 1" "premise 2"))
  (println "foo:" (let [db (d/db conn)
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
   (drawable/->Text (str value)
                    (font/create "LiberationSans-Regular.ttf" 15)
                    color)))

(defn argument-view [argument]
  (l/vertically (for [premise (:argumentica.argument/premises argument)]
                  (text (str "text: "(:argumentica.sentence/text premise))))
                (l/margin 10 0 10 0
                          (drawable/->Rectangle 10 2 [255 255 255 255]))
                (text (-> argument
                          :argumentica.argument/main-conclusion
                          :argumentica.sentence/text))))

(defn attribute-editor [view-context db state entity-id attribute]
  (let [value (value db
                     entity-id
                     attribute)
        changed-value-key [entity-id attribute]]
    (-> (gui/call-view controls/text-editor [:editor changed-value-key])
        (update-in [:state-overrides] assoc :text (or (get-in state [:changes changed-value-key])
                                                      value))
        (update-in [:constructor-overrides] assoc [:on-change :text] (fn [state new-value]
                                                                       (gui/apply-to-local-state state
                                                                                                 view-context
                                                                                                 assoc-in [:changes changed-value-key] new-value))))))

(defn button [text-value]
  (layouts/->Box 10 [(drawable/->Rectangle 0
                                           0
                                           [0 0.8 0.8 1])
                     (text text-value)]))


(defn changes-to-transaction [changes]
  (reduce (fn [transaction [[id attribute] value]]
            (conj transaction {:db/id id
                               attribute value}))
          []
          changes))

(defn argumentica-root-view [view-context state]
  (l/vertically (l/preferred (attribute-editor view-context
                                               (:db state)
                                               state
                                               (first (main-conclusions (:db state)))
                                               :argumentica.sentence/text))
                (-> (button "Save")
                    (gui/on-mouse-clicked-with-view-context view-context
                                                            (fn [state event]
                                                              (d/transact (:conn state)
                                                                          (changes-to-transaction (:changes state)))
                                                              (assoc state :changes {}
                                                                     :db (d/db (:conn state))))))
                (text (:changes state))))

(defn argumentica-root [conn]
  (fn [view-context]
    {:local-state {:conn conn
                   :changes {}
                   :db (d/db conn)}
     :view #'argumentica-root-view}))



(defn start []
  #_(.start (Thread. (fn []
                       (trace/untrace-ns 'flow-gl.gui.gui)
                       (trace/trace-var* 'flow-gl.gui.gui/set-focus-if-can-gain-focus)
                       (trace/trace-var* 'flow-gl.gui.gui/set-focus)
                       #_(trace/trace-var* 'flow-gl.gui.gui/resolve-size-dependent-view-calls)
                       (trace/with-trace
                         (gui/start-control argumentica-root)))))
  
  (.start (Thread. (fn []
                     (let [conn (create-database)]
                       (d/transact conn
                                   (add-argument (d/db conn) "argument 1" "conclusion 1" "premise 1" "premise 2"))
                       
                       (d/transact conn
                                   (add-argument (d/db conn) "argument 2" "conclusion 1" "premise 1" "premise 3"))

                       (d/transact conn
                                   (add-argument (d/db conn) "argument 3" "conclusion 2" "premise 4" "premise 5"))
                       (gui/start-control (argumentica-root conn))))))

  #_(profiler/with-profiler (gui/start-control argumentica-root)))


