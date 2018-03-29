
(ns argumentica.ui.argument
  (:require [fungl.application :as application]
            (flow-gl.graphics [font :as font])
            (fungl [cache :as cache])
            (flow-gl.gui [layout :as layout]
                         [layouts :as layouts]
                         [visuals :as visuals]
                         [animation :as animation])
            (fungl.component [text-area :as text-area]
                             [text :as text]
                             [button :as button])
            (argumentica.db [client :as client]
                            [common :as db-common]
                            [server-api :as server-api]
                            [server-btree-db :as server-btree-db]
                            [client-db :as client-db])
            (argumentica [btree-db :as btree-db])))

(def entity-id  #uuid "adcba48b-b9a9-4c28-b1e3-3a97cb10cffb")

(defonce server-state-atom (let [server-state-atom (atom (server-api/create-state (btree-db/create-memory-btree-db)))]
                             (server-api/transact server-state-atom
                                                  [[entity-id :name :set "Foo"]])
                             server-state-atom))

(def font (font/create "LiberationSans-Regular.ttf" 33))

(defn button [message handler & arguments]
  (button/button (layouts/box 10
                              (visuals/rectangle [255 255 0 255] 30 30)
                              (text/text message [0 0 0 255] font))
                 handler
                 arguments))

(defn commit-button-handler [client-db-atom]
  (swap! client-db-atom client-db/commit))


(defn refresh-button-handler [client-db-atom]
  (swap! client-db-atom client-db/refresh))


(defn paragraph [id text]
  (cache/call! text-area/text-area id
               {:color [255 255 255 255]
                :font  font}
               text
               nil))

(defn text-editor [text handle-text-change]
  (layouts/box 10
               (visuals/rectangle (if (:has-focus @(text-area/get-state-atom :area-1))
                                    [255 255 255 255]
                                    [200 200 200 255])
                                  30 30)
               (text-area/text-area :area-1
                                    {:color [0 0 0 255]
                                     :font  font}
                                    text
                                    (fn [old-state new-state]
                                      (if (not= (:text new-state) (:text old-state))
                                        (assoc new-state :text (handle-text-change (:text old-state)
                                                                                   (:text new-state))))
                                      new-state))))

(defn property-editor [client-db-atom entity-id attribute]
  (text-editor (str (client-db/value @client-db-atom
                                     entity-id
                                     attribute))
               (fn [old-text new-text]
                 (swap! client-db-atom client-db/transact [[entity-id attribute :set new-text]]))))

(defn create-scene-graph [client-db-atom]
  (animation/swap-state! animation/set-wake-up 1000)
  
  (layouts/vertically (property-editor client-db-atom
                                       entity-id
                                       :name)

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


                      (paragraph :client "Client")
                      (for [[index line] (map-indexed vector (client-db/transaction @client-db-atom))]
                        (paragraph [:client index] (pr-str line)))

                      (paragraph :server "Server")

                      (for [[index line] (map-indexed vector (server-api/transaction-log-subseq server-state-atom
                                                                                                0))]
                        (paragraph [:client index] (pr-str line)))
                      
                      (cache/call! button "Commit" commit-button-handler client-db-atom)

                      (cache/call! button "Refresh" refresh-button-handler client-db-atom))
  
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


(defn start []
  (application/start-window (argument-demo)))



