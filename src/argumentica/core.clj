(ns argumentica.core
  (:require [datomic.api :as d]
            [flow-gl.graphics.font :as font]
            [flow-gl.gui.visuals :as visuals]
            [fungl.application :as application]
            [fungl.atom-registry :as atom-registry]
            [fungl.cache :as cache]
            [fungl.layouts :as layouts]))

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
  (println "foo:" (main-conclusions (d/db conn))
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
   (visuals/text color
                 (font/create "LiberationSans-Regular.ttf" 15)
                 (str value))))

(defn argument-view [argument]
  (layouts/vertically (for [premise (:argumentica.argument/premises argument)]
                        (text (str "text: "(:argumentica.sentence/text premise))))
                      (layouts/with-margins 10 0 10 0
                        (visuals/rectangle 10 2 [255 255 255 255]))
                      (text (-> argument
                                :argumentica.argument/main-conclusion
                                :argumentica.sentence/text))))


(defn argumentica-root-view [state-atom]
  (let [state (atom-registry/deref! state-atom)]
    (layouts/horizontally (layouts/vertically (for [conclusion-id (main-conclusions (d/db (:conn state)))]
                                                (let [conclusion (d/entity (d/db (:conn state))
                                                                           conclusion-id)]
                                                  (-> (text (:argumentica.sentence/text conclusion) (if (= (:selected-conclusion state)
                                                                                                           conclusion-id)
                                                                                                      [255 255 255 255]
                                                                                                      [100 100 100 255]))
                                                      (assoc :mouse-event-handler (fn [node event]
                                                                                    (when (= (:type event)
                                                                                             :mouse-clicked)
                                                                                      (swap! state-atom assoc :selected-conclusion conclusion-id))
                                                                                    event))))))
                          (when-let [selected-conclusion-id (:selected-conclusion state)]
                            (layouts/vertically (for [argument (:argumentica.argument/_main-conclusion (d/entity (d/db (:conn state))
                                                                                                                 selected-conclusion-id))]
                                                  (text (:argumentica.argument/title argument))))))))

(defn argumentica-root [conn]
  (cache/call! argumentica-root-view
               (atom-registry/get! :root {:create (fn [] {:conn conn
                                                          :selected-conclusion nil})}))
  
  #_(fn [view-context]
      {:local-state {:conn conn
                     :selected-conclusion nil}
       :handle-keyboard-event (fn [state event]
                                (cond
                                  #_(events/key-pressed? event :enter)
                                  #_(do (println "dec")
                                        (gui/apply-to-local-state state view-context update-in [:editor-count] dec))

                                  :default
                                  state))
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
                       
                       (application/start-window (fn [width height]
                                                   (application/do-layout (#'argumentica-root conn)
                                                                          width height)) 
                                                 :target-frame-rate 30)))))

  #_(profiler/with-profiler (gui/start-control argumentica-root)))


