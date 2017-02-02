(ns argumentica.chord
  (:require (fungl [application :as application]
                   [handler :as handler]
                   [cache :as cache]
                   [atom-registry :as atom-registry]
                   [value-registry :as value-registry]
                   [callable :as callable]
                   [layout :as layout]
                   [layouts :as layouts])
            (flow-gl.gui 
             [visuals :as visuals]
             [quad-renderer :as quad-renderer]
             [tiled-renderer :as tiled-renderer]
             [animation :as animation]
             [scene-graph :as scene-graph]
             [stateful :as stateful]
             [keyboard :as keyboard]
             [events :as events])
            (flow-gl.graphics [font :as font]))
  (:use clojure.test))

#_{:key-code 76, :shift false, :key :unknown, :alt false, :time 1485931426248, :type :key-released, :source :keyboard, :control false, :is-auto-repeat false, :character \l}

(defn remove-value [values value]
  (filter (fn [v]
            (not= v value))
          values))

(defn initialize [] {:keys-down []})

(defn handle-event [state event]
  (if (not (:is-auto-repeat event))
    (if (and (= :key-pressed (:type event))
             (not (:is-auto-repeat event)))
      (-> state
          (dissoc :chord)
          (assoc :recording true)
          (update-in [:keys-down] (fn [keys-down] (concat keys-down
                                                          [(:key-code event)]))))
      (if (= :key-released (:type event))
        (-> state
            (cond->
                (:recording state)
              (assoc :chord (:keys-down state))
              
              (not (:recording state))
              (dissoc :chord))
            (dissoc :recording)
            (update-in [:keys-down] remove-value (:key-code event)))
        state))
    (dissoc state :chord)))

(deftest handle-event-test
  (is (= '{:keys-down (1), :chord (2 1)}
         (-> (initialize)
             (handle-event {:key-code 1, :type :key-pressed})
             (handle-event {:key-code 2, :type :key-pressed})
             (handle-event {:key-code 2, :type :key-released}))))

  (is (= '{:keys-down ()}
         (-> (initialize)
             (handle-event {:key-code 1, :type :key-pressed})
             (handle-event {:key-code 2, :type :key-pressed})
             (handle-event {:key-code 2, :type :key-released})
             (handle-event {:key-code 1, :type :key-released}))))

  (is (= '{:keys-down ()}
         (-> (initialize)
             (handle-event {:key-code 1, :type :key-pressed})
             (handle-event {:key-code 2, :type :key-pressed})
             (handle-event {:key-code 2, :type :key-released})
             (handle-event {:key-code 1, :type :key-released})))))

(defn text
  ([value]
   (text value [255 255 255 255]))

  ([value color]
   (visuals/text color
                 (font/create "LiberationSans-Regular.ttf" 15)
                 (str value))))

(def key-codes-to-fingers {70 :left-2
                           68 :left-3
                           83 :left-4
                           65 :left-5

                           32 :right-1
                           74 :right-2
                           75 :right-3
                           76 :right-4
                           59 :right-5})
(defn button [number color]
  (layouts/with-minimum-size 30 0
    (layouts/box 10
                 (visuals/rectangle color 10 10)
                 (text number [0 0 0 255]))))



(defn finger-numbers [chord]
  (apply hash-map (flatten (map-indexed (fn [index finger]
                                          [finger index])
                                        chord))))

(deftest finger-numbers-test
  (is (= {:left-1 0, :right-2 1}
         (finger-numbers [:left-1 :right-2]))))

(defn finger-color [pressed]
  (if pressed
    [0 255 0 255]
    [0 100 0 255]))

(defn finger-button [number]
  (button (or number "")
          (finger-color number)))

(defn chord-view [chord]
  (let [finger-numbers (finger-numbers chord)]
    (layouts/horizontally (layouts/with-margins 0 10 0 0
                            (finger-button (:left-5 finger-numbers)))
                          (layouts/with-margins 0 10 0 0
                            (finger-button (:left-4 finger-numbers)))
                          (layouts/with-margins 0 10 0 0
                            (finger-button (:left-3 finger-numbers)))
                          (layouts/vertically (layouts/with-margins 0 10 10 0
                                                (finger-button (:left-2 finger-numbers)))
                                              (layouts/with-margins 0 10 0 0
                                                (finger-button (:left-1 finger-numbers))))
                          
                          (layouts/vertically (layouts/with-margins 0 10 10 0
                                                (finger-button (:right-2 finger-numbers)))
                                              (layouts/with-margins 0 10 0 0
                                                (finger-button (:right-1 finger-numbers))))
                          (layouts/horizontally (layouts/with-margins 0 10 0 0
                                                  (finger-button (:right-3 finger-numbers)))
                                                (layouts/with-margins 0 10 0 0
                                                  (finger-button (:right-4 finger-numbers)))
                                                (layouts/with-margins 0 10 0 0
                                                  (finger-button (:right-5 finger-numbers)))))))

(defn root-view [state-atom]
  (let [state @state-atom]
    (layouts/vertically (for [chord (reverse (:chords state))]
                          (layouts/with-margins 20 0 0 0
                            (chord-view (map key-codes-to-fingers chord))))
                        (layouts/with-margins 10 0 10 0
                          (assoc (visuals/rectangle [100 100 100 255] 0 0)
                                 :height 10))
                        (chord-view (map key-codes-to-fingers (-> state :chord-state :keys-down)))
                        #_(text @state-atom))))



(defn create-state-atom []
  (let [state-atom (atom {:chord-state (initialize)})]
    (keyboard/set-focused-event-handler! (fn [event]
                                           (swap! state-atom (fn [state]
                                                               (let [state (update state :chord-state handle-event event)]
                                                                 (if-let [chord (-> state :chord-state :chord)]
                                                                   (update state :chords (fn [chords]
                                                                                           (take 3 (conj chords chord))))
                                                                   state))))))
    state-atom))

(defn start []
  (application/start-window (fn [width height]
                              (application/do-layout (#'root-view (cache/call! create-state-atom))
                                                     width height))))
