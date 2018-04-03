(ns argumentica.ui.argument
  (:require [fungl.application :as application]
            (flow-gl.graphics [font :as font])
            (fungl [cache :as cache]
                   [layouts :as layouts]
                   [atom-registry :as atom-registry])
            (flow-gl.gui [layout :as layout]
                         [visuals :as visuals]
                         [animation :as animation]
                         [keyboard :as keyboard])
            (fungl.component [text-area :as text-area]
                             [text :as text]
                             [button :as button])
            (argumentica.db [client :as client]
                            [common :as db-common]
                            [server-api :as server-api]
                            [server-btree-db :as server-btree-db]
                            [client-db :as client-db])
            (argumentica [btree-db :as btree-db]))
  (:import [java.util UUID]))

(def entity-id  #uuid "adcba48b-b9a9-4c28-b1e3-3a97cb10cffb")
(def entity-id-2  #uuid "04776425-6078-41bf-af59-b9a113441368")

(defonce server-state-atom (let [server-state-atom (atom (server-api/create-state (btree-db/create-memory-btree-db)))]
                             (let [what-should-we-do-to-global-warming (UUID/randomUUID)
                                   reduce-coal-burning (UUID/randomUUID)
                                   what-should-we-do-to-poverty (UUID/randomUUID)]
                               (server-api/transact server-state-atom
                                                    [[what-should-we-do-to-global-warming :type :set :question]
                                                     [what-should-we-do-to-global-warming :text :set "What should we do to global warming?"]
                                                     [reduce-coal-burning :type :set :answer]
                                                     [reduce-coal-burning :text :set "Reduce coal burning"]
                                                     [reduce-coal-burning :question :set what-should-we-do-to-global-warming]
                                                     [what-should-we-do-to-poverty :type :set :question]
                                                     [what-should-we-do-to-poverty :text :set "What should we do to poverty?"]]))
                             server-state-atom))

(comment
  (:db @server-state-atom))

(def font (font/create "LiberationSans-Regular.ttf" #_20 38))
(def symbol-font (font/create "LiberationSans-Regular.ttf" #_20 58))

(defn button [message handler & arguments]
  (button/button (layouts/box 10
                              (visuals/rectangle [255 255 0 255] 30 30)
                              (text/text message [0 0 0 255] font))
                 handler
                 arguments))

(defn commit-button-handler [client-db-atom]
  (swap! client-db-atom client-db/commit))

(defn create-button-handler [node-type state-atom client-db-atom]
  (let [new-entity-id (UUID/randomUUID)]
    (swap! client-db-atom client-db/transact [[new-entity-id :type :set node-type]
                                              [new-entity-id :text :set (:prompt-value @state-atom)]]))
  (swap! state-atom assoc :prompt-value ""))

(defn refresh-button-handler [client-db-atom]
  (swap! client-db-atom client-db/refresh))


(defn paragraph [id text]
  (cache/call! text-area/text-area id
               {:color [0 0 0 255]
                :font  font}
               text
               nil))

(defn bare-text-editor [id text handle-text-change]
  (text-area/text-area id
                       {:color [0 0 0 255]
                        :font  font}
                       text
                       (fn [old-state new-state]
                         (if (not= (:text new-state) (:text old-state))
                           (assoc new-state :text (handle-text-change (:text old-state)
                                                                      (:text new-state))))
                         new-state)))

(defn text-editor [id text handle-text-change]
  (layouts/box 10
               (visuals/rectangle (if (:has-focus @(text-area/get-state-atom :area-1))
                                    [255 255 255 255]
                                    [200 200 200 255])
                                  30 30)
               (text-area/text-area id
                                    {:color [0 0 0 255]
                                     :font  font}
                                    text
                                    (fn [old-state new-state]
                                      (if (not= (:text new-state) (:text old-state))
                                        (assoc new-state :text (handle-text-change (:text old-state)
                                                                                   (:text new-state))))
                                      new-state))))

(defn property-editor [client-db-atom entity-id attribute]
  (text-editor [client-db-atom entity-id attribute]
               (str (client-db/value @client-db-atom
                                     entity-id
                                     attribute))
               (fn [old-text new-text]
                 (swap! client-db-atom client-db/transact [[entity-id attribute :set new-text]]))))

(def node-styles {:question {:symbol "?"
                             :color [120 170 255 255]}
                  :answer {:symbol "!"
                           :color [255 255 0 255]}})

(defn text [string]
  (text/text string
             [0 0 0 255]
             font))

(defn gain-focus-on-click [node event]
  (if (= (:type event)
         :mouse-clicked)
    (keyboard/set-focused-node! node))
  event)

(defn node-keyboard-event-handler [event node-id]
  (prn node-id event))

(defn node-view [node-id node-type node-text on-text-change]
  (layouts/box 15
               (visuals/rectangle (if (= (:focused-node-id @keyboard/state-atom)
                                         [:node-view node-id])
                                    [189 189 189 255]
                                    [229 229 229 255])
                                  60 60)
               (layouts/horizontally-2 {:margin 10
                                        :centered true}
                                       (layouts/with-margins 5 5 5 5
                                         (layouts/box 10
                                                      (assoc (visuals/rectangle (-> node-styles node-type :color)
                                                                                60 60)
                                                             :id [:node-view node-id]
                                                             :keyboard-event-handler [node-keyboard-event-handler node-id]
                                                             :mouse-event-handler [gain-focus-on-click])
                                                      
                                                      (layouts/with-maximum-size 70 70
                                                        (layouts/center (text/text (-> node-styles node-type :symbol)
                                                                                   [0 0 0 255]
                                                                                   symbol-font)))))

                                       (layouts/with-maximum-size 500 nil
                                         (bare-text-editor [:node-text node-id]
                                                           node-text
                                                           on-text-change)))))

(defn prompt [prompt-value handle-prompt-change]
  (text-editor :prompt
               (or prompt-value
                   "")
               (fn [old-text new-text]
                 (handle-prompt-change new-text)
                 new-text)))

(defn create-scene-graph [client-db-atom]
  (let [state-atom (atom-registry/get! [:state client-db-atom] {:create (fn [] {})})]
    (animation/swap-state! animation/set-wake-up 1000)
    
    (layouts/superimpose (visuals/rectangle [255 255 255 255]
                                            0 0)
                         (layouts/with-margins 20 20 20 20
                           (layouts/vertically-with-margin 30
                                                           #_(property-editor client-db-atom
                                                                              entity-id
                                                                              :name)

                                                           (prompt (:prompt-value @state-atom) (fn [new-prompt-value]
                                                                                                 (swap! state-atom assoc :prompt-value new-prompt-value)))

                                                           (cache/call! button "Create question" create-button-handler :question state-atom client-db-atom)
                                                           (cache/call! button "Create answer" create-button-handler :answer state-atom client-db-atom)
                                                            
                                                           (for [entity (map (fn [entity-id]
                                                                               (client-db/entity @client-db-atom
                                                                                                 entity-id))
                                                                             (concat (client-db/entities @client-db-atom
                                                                                                         :type
                                                                                                         :question)
                                                                                     (client-db/entities @client-db-atom
                                                                                                         :type
                                                                                                         :answer)))]
                                                             (node-view (:entity/id entity) (:type entity) (:text entity)
                                                                        (fn [old-text new-text]
                                                                          (swap! client-db-atom client-db/transact [[(:entity/id entity) :text :set new-text]])
                                                                          new-text))
                                                             )
                                                            
                                                           #_(node-view :id :question "What should we do to prevent global warming?")
                                                            
                                                           #_(node-view :id2 :answer "Reduce coal burning")

                                                           #_(layouts/box 10
                                                                          (visuals/rectangle (if (:has-focus @(text-area/get-state-atom :area-1))
                                                                                               [255 255 0 255]
                                                                                               [205 205 0 255])
                                                                                             30 30)
                                                                          (text-area/text-area :area-1
                                                                                               {:color [0 0 0 255]
                                                                                                :font  font}
                                                                                               (str (client-db/value @client-db-atom
                                                                                                                     entity-id
                                                                                                                     :name))
                                                                                               (fn [old-state new-state]
                                                                                                 (when (not= (:text new-state) (:text old-state))
                                                                                                   (swap! client-db-atom client-db/transact [[entity-id :name :set (:text new-state)]]))
                                                                                                 new-state)))

                                                           #_(paragraph :client "Uncommitted transaction")
                                                           #_(for [[index line] (map-indexed vector (client-db/transaction @client-db-atom))]
                                                               (paragraph [:client index] (pr-str line)))

                                                           (paragraph :server "Server")

                                                           (for [[index line] (map-indexed vector (server-api/transaction-log-subseq server-state-atom
                                                                                                                                     0))]
                                                             (paragraph [:client index] (pr-str line)))

                                                            
                                                           (cache/call! button "Commit" commit-button-handler client-db-atom)

                                                           #_(cache/call! button "Refresh" refresh-button-handler client-db-atom)))))
  
  #_(animation/swap-state! animation/start-if-not-running :animation)
  #_(let [x (animation/ping-pong 10 (animation/phase! :animation))]
      (-> (layouts/vertically (layouts/horizontally (assoc (visuals/rectangle [255 0 0 255]
                                                                              80 80 (* x 200) (* x 200))
                                                           :mouse-event-handler (fn [node event]
                                                                                  (when (= :mouse-clicked
                                                                                           (:type event))
                                                                                    (swap! clicks inc))
                                                                                  event))
                                                    (visuals/rectangle [0 255 255 255]
                                                                       80 80 200 200))
                              (layouts/horizontally (visuals/rectangle [0 0 255 255]
                                                                       80 80 200 200)
                                                    (visuals/rectangle [255 255 0 255]
                                                                       80 80 200 200))
                              (visuals/text (str (float x)))
                              (visuals/text (str "clicks:" @clicks)))
        
          (application/do-layout width height))))

(comment
  (with-bindings (application/create-event-handling-state)
    (create-scene-graph 100 100)))

(defn argument-demo []
  (let [client-db-atom (atom (client-db/create (client/->InProcessClient server-state-atom)))]
    (fn [width height]
      (-> (#'create-scene-graph client-db-atom)
          (application/do-layout width height)))))

(comment
  (server-api/transact server-state-atom
                       [[entity-id :friend :set entity-id-2]
                        [entity-id-2 :name :set "Baz"]])
  
  
  (-> (client-db/entity (client-db/create (client/->InProcessClient server-state-atom))
                        entity-id)
      :entity/id
      ;; :friend :name
      ))

(defn start []
  (application/start-window (argument-demo)))



