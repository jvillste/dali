(ns argumentica.editor2
  (:require [fungl.application :as application]
            [clojure.java.io :as io]
            (fungl.component [text-area :as text-area])
            (fungl [layouts :as layouts]
                   [cache :as cache]
                   [handler :as handler]
                   [atom-registry :as atom-registry]
                   [value-registry :as value-registry])
            (flow-gl.graphics [font :as font])
            (flow-gl.gui
             [keyboard :as keyboard]
             [visuals :as visuals]
             [animation :as animation])

            [datomic.api :as d])
  (:use flow-gl.utils
        clojure.test))

(def font (font/create "LiberationSans-Regular.ttf" 30))




(defn create-demo-scene-graph [width height]

  #_(animation/swap-state! animation/set-wake-up 1000)

  (let [state-atom (atom-registry/get! :root {:create (fn []
                                                        {:text-1 (apply str
                                                                        (repeat 10 "foo bar "))
                                                         :text-2 "text 2"})})]
    (-> (layouts/with-margins 10 10 10 10
          (layouts/vertically
           (assoc (layouts/with-margins 0 0 20 0
                    (layouts/box 10
                                 (visuals/rectangle (if (:text-1-in-focus @state-atom)
                                                      [255 255 255 255]
                                                      [155 155 155 255]) 20 20)
                                 (text-area/text-area :area-1
                                                      {:color [0 0 0 255]
                                                       :font font}
                                                      (:text-1 @state-atom)
                                                      (value-registry/get-fn! ::text-1-handler [old-state new-state]
                                                                              (swap! state-atom assoc :text-1 (:text new-state))
                                                                              new-state))))
                  :keyboard-event-handler (value-registry/get-fn! ::text-1-keyboard-event-handler [event]
                                                                  (when (= :focus-gained
                                                                           (:type event))
                                                                    (prn "got focus")
                                                                    (swap! state-atom assoc :text-1-in-focus true))))
           
           (text-area/text-area :area-2
                                {:color [255 255 255 255]}
                                (:text-2 @state-atom)
                                (fn [old-state new-state]
                                  (swap! state-atom assoc :text-2 (:text new-state))
                                  new-state))
           
           #_(text (prn-str @state-atom))))
        (application/do-layout width height))))



(defn create-text-area-keyboard-event-handler-creator [x]
  (fn [event]
   [x event]))

(defn create-text-area-keyboard-event-handler [x]
  (cache/call!
   create-text-area-keyboard-event-handler-creator
   x))


(defn start []
  #_(with-bindings (cache/state-bindings)
      (= (create-text-area-keyboard-event-handler 1)
         (create-text-area-keyboard-event-handler 1)))

  #_(with-bindings (value-registry/state-bindings)
      (= (let [x 1]
           (value-registry/get! [:text-area-keyboard-event-handler x] {:create (fn []
                                                                                 (fn [event]
                                                                                   [x event]))}))
         (let [x 1]
           (value-registry/get! [:text-area-keyboard-event-handler x] {:create (fn []
                                                                                 (fn [event]
                                                                                   [x event]))}))))

  
  (application/start-window #'create-demo-scene-graph))
