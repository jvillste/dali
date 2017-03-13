(ns argumentica.chord
  (:require (fungl [application :as application]
                   [handler :as handler]
                   [cache :as cache]
                   [atom-registry :as atom-registry]
                   [value-registry :as value-registry]
                   [callable :as callable]
                   [layout :as layout]
                   [layouts :as layouts])
            [clojure.set :as set]
            (fungl.component [text-area :as text-area])
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

(for [[a b] (map vector
                 [1 2 3]
                 [4 5 6])]
  [a b])

(defn two-finger-chords [first-finger & commands]
  (let [all-fingers [:left-5 :left-4 :left-3 :left-2 :left-1
                     :right-1 :right-2 :right-3 :right-4 :right-5]]
    (->> (for [[second-finger command] (map vector
                                            (filter (fn [finger] (not= finger first-finger))
                                                    all-fingers)
                                            commands)]
           (if command
             [#{first-finger second-finger}
              (if (string? command)
                [#'text-area/insert-string command]
                command)]
             nil))
         (filter identity)
         (apply concat)
         (apply hash-map))))



(def finger-chords-to-commands
  (-> {

       #{:left-5}
       [#'text-area/insert-string "a"]

       #{:left-4}
       [#'text-area/insert-string "s"]

       #{:left-3}
       [#'text-area/insert-string "e"]

       #{:left-2}
       [#'text-area/insert-string "t"]

       #{:right-2}
       [#'text-area/insert-string "n"]

       #{:right-3}
       [#'text-area/insert-string "i"]

       #{:right-4}
       [#'text-area/insert-string "o"]

       #{:right-5}
       [#'text-area/insert-string "p"]
       
       #{:left-2 :left-3}
       [#'text-area/backward]
       
       #{:right-2 :right-3}
       [#'text-area/forward]
       
       }
      
      (conj (two-finger-chords :left-5 "w" "x" "f" nil nil "q" "!" "(" "?")
            (two-finger-chords :left-4 "w" "d" "c" nil nil "j" "z" "(" "?")
            (two-finger-chords :left-3 "x" "d" "r" nil nil "y" "," "-" "'")
            (two-finger-chords :left-2 "f" "c" "r" nil nil "b" "v" "g" [#'text-area/delete-backward])
            (two-finger-chords :right-2 "q" "j" "y" "b" nil nil "h" "u" "m")
            (two-finger-chords :right-3 "!" "z" "," "v" nil nil "h" "l" "k")
            (two-finger-chords :right-4 "(" "." "-" "g" nil nil "u" "l" ";")
            (two-finger-chords :right-5 "?" ")" "'" [#'text-area/delete-backward] nil nil "m" "k" ";"))))

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
   (visuals/text-area color
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
    (layouts/with-minimum-size 100 100
      (layouts/box 5
                   (visuals/rectangle (if pressed
                                        pressed-finger-color
                                        (if (#{:left-1 :right-1} finger)
                                          unpressed-thumb-color
                                          unpressed-finger-color))
                                      10 10)
                   (layouts/vertically
                    (for [[chord command] (->> (filter-chords-to-commands (fn [command-chord]
                                                                            (contains? command-chord
                                                                                       finger))
                                                                          chords-to-commands)
                                               (sort-by (fn [[chord command]]
                                                          (count chord))))]
                      (layouts/with-margins 1 0 0 0
                        (text (command-to-string command)
                              (guide-string-color (count chord)
                                                  pressed)))))))))



(defn root-view [state-atom state]
  (layouts/with-margins 10 10 10 10
    (layouts/vertically-with-margin 10
                                    (layouts/box 10
                                                 (visuals/rectangle [255 255 255 255]
                                                                    10 10)
                                                 (text-area/create-scene-graph (:text state)
                                                                               (:index state)
                                                                               {:color [0 0 0 255]}
                                                                               (fn [rows]
                                                                                 (swap! state-atom assoc :rows rows))))
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
                          :text "foo bar baz"
                          :index 0})]
    (keyboard/set-focused-event-handler! (fn [event]
                                           (swap! state-atom (fn [state]
                                                               (let [state (update state :chord-state handle-event event)]
                                                                 (if-let [chord (-> state :chord-state :chord)]
                                                                   (let [state (update state :chords (fn [chords]
                                                                                                       (take 1 (conj chords chord))))]
                                                                     (if-let [[command & parameters] (map-chord-to-command chord)]
                                                                       (apply command
                                                                              state
                                                                              (:rows state)
                                                                              parameters)
                                                                       state))                                                                   
                                                                   state))))))
    state-atom))

(defn start []
  (application/start-window (fn [width height]
                              (application/do-layout (#'cached-root-view (cache/call! create-state-atom))
                                                     width height))))
