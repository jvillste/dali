(ns argumentica.chord
  (:require (fungl [application :as application]
                   [handler :as handler]
                   [cache :as cache]
                   [atom-registry :as atom-registry]
                   [value-registry :as value-registry]
                   [callable :as callable]
                   [layout :as layout]
                   [layouts :as layouts])
            [flow-gl.profiling :as profiling]
            [clojure.set :as set]
            (fungl.component [text-area :as text-area])
            (flow-gl.gui
             [render-target-renderer :as render-target-renderer]
             [visuals :as visuals]
             [quad-renderer :as quad-renderer]
             [tiled-renderer :as tiled-renderer]
             [animation :as animation]
             [scene-graph :as scene-graph]
             [stateful :as stateful]
             [keyboard :as keyboard]
             [events :as events])
            (flow-gl.graphics [font :as font])
            (flow-gl.opengl.jogl [opengl :as opengl]))
  (:use clojure.test))


(def all-fingers [:left-5 :left-4 :left-3 :left-2 :left-1
                  :right-1 :right-2 :right-3 :right-4 :right-5])

(defn chord-commands [fixed-fingers-set & fingers-to-commands]
  (->> (for [[finger command] (map vector
                                   (filter (complement fixed-fingers-set)
                                           [:left-5 :left-4 :left-3 :left-2 :left-1
                                            :right-1 :right-2 :right-3 :right-4 :right-5])
                                   fingers-to-commands)]
         (if command
           [(conj fixed-fingers-set finger)
            (if (string? command)
              [#'text-area/insert-string command]
              command)]
           nil))
       (filter identity)
       (apply concat)
       (apply hash-map)))


(def finger-chords-to-commands
  (-> {
       
       #{:left-2 :left-3}
       [#'text-area/backward]
       
       #{:right-2 :right-3}
       [#'text-area/forward]
       
       }
      
      (conj (chord-commands #{} "a" "s" "e" "t" nil " " "n" "i" "o" "p")
            (chord-commands #{:left-5} "w" "x" "f" nil nil "q" "!" "(" "?")
            (chord-commands #{:left-4} "w" "d" "c" nil nil "j" "z" "(" "?")
            (chord-commands #{:left-3} "x" "d" "r" nil nil "y" "," "-" "'")
            (chord-commands #{:left-2} "f" "c" "r" nil nil "b" "v" "g" [#'text-area/delete-backward])
            (chord-commands #{:right-2} "q" "j" "y" "b" nil nil "h" "u" "m")
            (chord-commands #{:right-3} "!" "z" "," "v" nil nil "h" "l" "k")
            (chord-commands #{:right-4} "(" "." "-" "g" nil nil "u" "l" ";")
            (chord-commands #{:right-5} "?" ")" "'" [#'text-area/delete-backward] nil nil "m" "k" ";")
            
            (chord-commands #{:left-1} "A" "S" "E" "T"  nil "N" "I" "O" "P")
            (chord-commands #{:left-1 :left-5} "W" "X" "F" nil  nil "Q" nil nil nil)
            (chord-commands #{:left-1 :left-4} "W" "D" "C" nil nil "J" "Z" nil nil)
            (chord-commands #{:left-1 :left-3} "X" "D" "R" nil nil "Y" nil nil nil)
            (chord-commands #{:left-1 :left-2} "F" "C" "R" nil nil "B" "V" "G" nil)
            (chord-commands #{:left-1 :right-2} "Q" "J" "Y" "B" nil, "H" "U" "M" nil)
            (chord-commands #{:left-1 :right-3} nil "Z" nil "V" nil nil "H" "L" "K")
            (chord-commands #{:left-1 :right-4} nil nil nil "G" nil "U" "L" nil nil)
            (chord-commands #{:left-1 :right-5} nil nil nil nil nil "M" "K" nil nil)

            (chord-commands #{:left-2 :left-3} nil nil nil nil  [#'text-area/backward] [#'text-area/previous-row] [#'text-area/next-row] [#'text-area/forward])

            )))


#_{:key-code 76, :shift false, :key :unknown, :alt false, :time 1485931426248, :type :key-released, :source :keyboard, :control false, :is-auto-repeat false, :character \l}

(defn remove-value [values value]
  (filter (fn [v]
            (not= v value))
          values))

(defn initialize [] {:keys-down []
                     :lines ["foo" "bar"]})

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
  (is (= '{:keys-down (1), :chord (1 2)}
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

(def key-codes-to-fingers {32 :left-1
                           70 :left-2
                           68 :left-3
                           83 :left-4
                           65 :left-5

                           154 :right-1
                           75 :right-2
                           76 :right-3
                           59 :right-4
                           39 :right-5})

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


(def unpressed-finger-color [0 100 0 255])
(def unpressed-thumb-color [0 50 0 255])
(def pressed-finger-color [0 255 0 255])

(defn finger-color [pressed]
  (if pressed
    pressed-finger-color
    unpressed-finger-color))

(defn finger-button [number]
  (button (or number "")
          (finger-color number)))

(defn guide-button [pressed]
  (assoc (visuals/rectangle (finger-color pressed)
                            5 5)
         :width 10
         :height 10))

(defn left-hand-buttons [finger-numbers button-view margin]
  (layouts/horizontally-with-margin margin
                                    (button-view (:left-5 finger-numbers))
                                    (button-view (:left-4 finger-numbers))
                                    (button-view (:left-3 finger-numbers))
                                    (layouts/vertically (layouts/with-margins 0 margin margin 0
                                                          (button-view (:left-2 finger-numbers)))
                                                        (layouts/with-margins 0 margin 0 0
                                                          (button-view (:left-1 finger-numbers))))))

(defn right-hand-buttons [finger-numbers button-view margin]
  (layouts/horizontally-with-margin margin
                                    (layouts/vertically (layouts/with-margins 0 margin margin 0
                                                          (button-view (:right-2 finger-numbers)))
                                                        (layouts/with-margins 0 margin 0 0
                                                          (button-view (:right-1 finger-numbers))))
                                    (button-view (:right-3 finger-numbers))
                                    (button-view (:right-4 finger-numbers))
                                    (button-view (:right-5 finger-numbers))))

(defn both-hand-buttons [finger-numbers]
  (layouts/horizontally (left-hand-buttons finger-numbers finger-button 10)
                        (right-hand-buttons finger-numbers finger-button 10)))

(defn chord-view [chord]
  (both-hand-buttons (finger-numbers chord)))

(defn initialize-text-box-state []
  {:line-number 0
   :column-number 0
   :lines ["line 1"
           "line 2"]})

(defn text-box [state]
  (layouts/horizontally (text "text box")
                        (for [line (:lines state)]
                          (text line))))

(defn filter-chords-to-commands [predicate finger-chord-to-commands]
  (select-keys finger-chord-to-commands
               (->> (keys finger-chord-to-commands)
                    (filter predicate))))

(defn command-to-string [[command & arguments]]
  (if (= command
         #'text-area/insert-string)
    (first arguments)
    (str "(" (apply str
                    (interpose " "
                               (cons (:name (meta command))
                                     (map pr-str (filter identity arguments)))))
         
         
         ")")))

(defn guide [chords-to-commands]
  (layouts/vertically
   (for [[chord command] chords-to-commands]
     (layouts/with-margins 15 0 0 0
       (layouts/horizontally (layouts/horizontally (left-hand-buttons chord guide-button 5)
                                                   (right-hand-buttons chord guide-button 5))
                             (text (command-to-string command)))))))

(defn guide-string-color [finger-count invert]
  (let [value (* 255 (/ (dec finger-count)
                        5))
        value (max 50
                   (if invert
                     value
                     (- 255
                        value)))]
    [value value value 255]))

(defn finger-guide [chords-to-commands finger current-chord]
  (let [pressed ((apply hash-set current-chord) finger)]
    (layouts/box 5
                 (visuals/rectangle (if pressed
                                      pressed-finger-color
                                      (if (#{:left-1 :right-1} finger)
                                        unpressed-thumb-color
                                        unpressed-finger-color))
                                    10 10)
                 (layouts/with-maximum-size 150 nil
                   (apply layouts/flow
                          (for [[chord command] (->> (filter-chords-to-commands (fn [command-chord]
                                                                                  (contains? command-chord
                                                                                             finger))
                                                                                chords-to-commands)
                                                     (sort-by (fn [[chord command]]
                                                                [(count chord)
                                                                 (command-to-string command)])))]
                            (layouts/with-margins 5 5 0 0
                              (text (command-to-string command)
                                    (guide-string-color (count chord)
                                                        pressed)))))))))



(handler/def-handler-creator create-handle-rows [state-atom] [rows]
  (swap! state-atom assoc :rows rows))

(defn root-view [state-atom state]
  (layouts/with-margins 10 10 10 10
    (layouts/vertically-with-margin 10
                                    (layouts/with-maximum-size 200 nil
                                      (layouts/box 10
                                                   (visuals/rectangle [255 255 255 255]
                                                                      10 10)
                                                   (text-area/create-scene-graph (:text state)
                                                                                 (:index state)
                                                                                 {:color [0 0 0 255]}
                                                                                 (create-handle-rows state-atom))))
                                    #_(for [chord (reverse (:chords state))]
                                        (layouts/with-margins 20 0 0 0
                                          (chord-view (map key-codes-to-fingers chord))))
                                    
                                    #_(layouts/with-margins 10 0 10 0
                                        (assoc (visuals/rectangle [100 100 100 255] 0 0)
                                               :height 10))
                                    
                                    (let [chord (map key-codes-to-fingers (-> state :chord-state :keys-down))
                                          finger-chords-to-commands (filter-chords-to-commands (fn [command-chord]
                                                                                                 (set/subset? chord
                                                                                                              command-chord))
                                                                                               finger-chords-to-commands)]

                                      [#_(chord-view chord)
                                       (layouts/horizontally-with-margin 10
                                                                         (for [finger [:left-5
                                                                                       :left-4
                                                                                       :left-3
                                                                                       :left-2
                                                                                       :left-1
                                                                                       :right-1
                                                                                       :right-2
                                                                                       :right-3
                                                                                       :right-4
                                                                                       :right-5]]
                                                                           (finger-guide finger-chords-to-commands
                                                                                         finger
                                                                                         chord)))]))))

(defn cached-root-view [state-atom]
  (cache/call! root-view
               state-atom
               @state-atom))

(defn enter-text [state text]
  (-> state
      (update-in [:lines (:line-number state)]
                 (fn [line]
                   (str (apply str
                               (take (:column-number state)
                                     line))
                        text
                        (apply str
                               (drop (:column-number state)
                                     line)))))
      (update :column-number + (count text))))

(deftest enter-text-test
  (is (= {:line-number 0, :column-number 3, :lines ["fooline 1" "line 2"]}
         (enter-text {:line-number 0
                      :column-number 0
                      :lines ["line 1"
                              "line 2"]}
                     "foo")))

  (is (= {:line-number 0, :column-number 6, :lines ["linfooe 1" "line 2"]}
         (enter-text {:line-number 0
                      :column-number 3
                      :lines ["line 1"
                              "line 2"]}
                     "foo")))
  
  (is (= {:line-number 1, :column-number 6, :lines ["line 1" "linfooe 2"]}
         (enter-text {:line-number 1
                      :column-number 3
                      :lines ["line 1"
                              "line 2"]}
                     "foo"))))

(defn backspace [state]
  (if (< 0 (:column-number state))
    (-> state
        (update-in [:lines (:line-number state)]
                   (fn [line]
                     (str (apply str
                                 (take (dec (:column-number state))
                                       line))
                          (apply str
                                 (drop (:column-number state)
                                       line)))))
        (update :column-number dec))
    state))

(deftest backspace-test
  (is (= {:line-number 0, :column-number 0, :lines ["ine 1" "line 2"]}
         (backspace {:line-number 0
                     :column-number 1
                     :lines ["line 1"
                             "line 2"]}))))

(defn caret-right [state]
  (update state :column-number
          (fn [column-number]
            (min (count (get (:lines state)
                             (:line-number state)))
                 (inc column-number)))))

(def commands {#{:left-2} [enter-text "a"]
               #{:right-2} backspace})





(defn map-chord-to-command [chord]
  (let [fingers (apply hash-set (map key-codes-to-fingers
                                     chord))]
    (finger-chords-to-commands
     fingers)))


(defn create-state-atom []
  (let [state-atom (atom {:chord-state (initialize)
                          :text (apply str (repeat 10 "foo bar baz "))
                          :index 0})]
    (keyboard/set-focused-event-handler! (fn [event]
                                           (swap! state-atom (fn [state]
                                                               (let [state (update state :chord-state handle-event event)]
                                                                 (if-let [chord (-> state :chord-state :chord)]
                                                                   (let [state (update state :chords (fn [chords]
                                                                                                       (take 1 (conj chords chord))))]
                                                                     (if-let [[command & parameters] (map-chord-to-command chord)]
                                                                       (apply text-area/handle-command
                                                                              state
                                                                              (:rows state)
                                                                              command
                                                                              parameters)
                                                                       state))                                                                   
                                                                   state))))))
    state-atom))

(comment
  (with-bindings (application/create-event-handling-state)
    (let [state-atom (cache/call! create-state-atom)]
      (= (root-view state-atom @state-atom)
         (root-view state-atom @state-atom))
      #_(first (clojure.data/diff (root-view state-atom @state-atom)
                                  (root-view state-atom @state-atom))))))


(defn render [scene-graph gl]
  (let [quad-renderer-atom-1 (atom-registry/get! :quad-renderer-1 (quad-renderer/atom-specification gl))
        quad-renderer-atom-2 (atom-registry/get! :quad-renderer-2 (quad-renderer/atom-specification gl))
        render-target-renderer-atom (atom-registry/get! :render-target-renderer-1 (render-target-renderer/atom-specification gl))]

    (opengl/clear gl 0 0 0 1)
    (quad-renderer/render quad-renderer-atom-1 gl
                          (render-target-renderer/render render-target-renderer-atom gl scene-graph
                                                         (fn []
                                                           (println "render 2" (hash (assoc scene-graph
                                                                                            :x 0 :y 0)))
                                                           (opengl/clear gl 0 0 0 1)
                                                           (quad-renderer/render quad-renderer-atom-2 gl (assoc scene-graph
                                                                                                                :x 0 :y 0)))))))

(defn render-target-root-view [width height]
  (application/do-layout (#'cached-root-view (cache/call! create-state-atom))
                         width height)
  #_(assoc (application/do-layout (#'cached-root-view (cache/call! create-state-atom))
                                  width height)
           :render render
           ))

(comment
  (profiling/unprofile-ns 'fungl.application)
  (profiling/unprofile-ns 'flow-gl.gui.quad-renderer)
  (profiling/unprofile-ns 'flow-gl.gui.scene-graph)
  (profiling/unprofile-ns 'flow-gl.gui.render-target-renderer)
  )


(defn start []
  (application/start-window #'render-target-root-view))
