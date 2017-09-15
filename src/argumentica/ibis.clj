(ns argumentica.ibis
  (:require (flow-gl.gui [visuals :as visuals]
                         [animation :as animation]
                         [quad-renderer :as quad-renderer])
            (flow-gl.opengl.jogl [opengl :as opengl])
            (fungl [application :as application]
                   [layout :as layout]
                   [layouts :as layouts]
                   [atom-registry :as atom-registry])))

(defn node [text children type]
  {:text text
   :type type
   :children children})


(defn q [text & children]
  (node text children :question))

(defn i [text & children]
  (node text children :idea))

(defn p [text & children]
  (node text children :pro))

(defn c [text & children]
  (node text children :con))

(defn node-color [node-type]
  (case node-type
    :question [200 200 200 255]
    :idea [200 200 0 255]
    :pro [0 255 0 255]
    :con [255 100 100 255]
    [0 255 255 255]))

(defn node-icon [node-type]
  (visuals/rectangle (node-color node-type)
                     10 10
                     15 15))

(defn node-icon-mouse-event-handler [state-atom ibis-node scene-graph-node event]
  (when (and (= :mouse-pressed
                (:type event))
             (not (empty? (:children ibis-node))))
    (swap! state-atom update :closed-nodes
           (fn [closed-nodes]
             (if (contains? closed-nodes ibis-node)
               (disj closed-nodes ibis-node)
               (conj closed-nodes ibis-node)))))
  event)

(def text-color [0 0 0 255])

(defn node-view [state-atom node]
  (let [child-layout (-> (apply layouts/vertically (for [[index child] (map-indexed vector (:children node))]
                                                     (layouts/horizontally (layouts/with-margins 12 0 0 0
                                                                             (assoc (visuals/rectangle text-color
                                                                                                       0 0)
                                                                                    :width 7
                                                                                    :height 1))
                                                                           
                                                                           (node-view state-atom child))))
                         (application/do-layout java.lang.Integer/MAX_VALUE
                                                java.lang.Integer/MAX_VALUE))
        text-layout (-> (layouts/with-margins 3 0 5 0
                          (layouts/with-maximum-size 500 nil (visuals/text-area (:text node) text-color)))
                        (application/do-layout java.lang.Integer/MAX_VALUE
                                               java.lang.Integer/MAX_VALUE))
        show-children (and (:children node)
                           (not (contains? (:closed-nodes @state-atom) node)))]
    (layouts/vertically
     (layouts/horizontally
      (layouts/vertically (assoc (if (contains? (:closed-nodes @state-atom) node)
                                   (layouts/with-margins 1 1 0 0
                                     (layouts/box 4
                                                  (visuals/rectangle [150 150 150 255] 10 10)
                                                  (node-icon (:type node))))
                                   (layouts/with-margins 5 5 0 0
                                     (node-icon (:type node))))
                                 :mouse-event-handler [node-icon-mouse-event-handler state-atom node])
                          (when show-children
                            (layouts/with-margins 0 0 0 7
                              (assoc (visuals/rectangle text-color
                                                        0 0)
                                     :width 1
                                     :height (- (:height text-layout)
                                                20)))))
      text-layout)
     (when show-children
       (layouts/horizontally
        (layouts/with-margins -1 0 0 7
          (assoc (visuals/rectangle text-color
                                    0 0)
                 :width 1
                 :height (+ 14
                            (- (:height child-layout)
                               (:height (last (:children child-layout)))))))
        child-layout)))))


(defn render [scene-graph gl]
  (opengl/clear gl 1 1 1 1)
  (let [quad-renderer-atom (atom-registry/get! ::root-renderer (quad-renderer/atom-specification gl))]
    (quad-renderer/render quad-renderer-atom gl scene-graph)))

(defn create-scene-graph [state-atom width height]
  (animation/swap-state! animation/set-wake-up 10000)
  (-> (layouts/with-margins 10 10 10 10
        (node-view state-atom @(:node-atom @state-atom)))
      (application/do-layout width height)
      (assoc :render render)))

(defn create-state [node-atom]
  {:node-atom node-atom
   :closed-nodes #{}})

(defn visualize [node-atom]
  (application/start-window (partial create-scene-graph
                                     (atom (create-state node-atom)))))


(def node-atom (atom (q "How sustainable is photo voltaic solar power?")))

(defn start []
  (application/start-window (partial create-scene-graph
                                     (atom (create-state node-atom))))
  #_(.start (Thread. (fn []
                       (start-window)))))

(reset! node-atom
       (q "How sustainable is photo voltaic solar power?"
          (i "Manufacturing photo voltaic cells is an energy intensive process.")
          (i "Solar PV has actually increased energy use and greenhouse gas emissions instead of lowering them.")
          (i "By carefully selecting the location of the manufacturing and the installation of solar panels, the potential of solar power could be huge.")
          (i "polysilicon content in solar cells is the most energy-intensive component.")
          (i "the polysilicon content in solar cells has come down to 5.5-6.0 grams per watt peak (g/wp)")
          (i "greenhouse gas emissions have come down to around 30 grams of CO2-equivalents per kilwatt-hour of electricity generated (gCO2e/kWh)"
             (i "electricity generated by photovoltaic systems is 15 times less carbon-intensive than electricity generated by a natural gas plant (450 gCO2e/kWh)")
             (i "electricity generated by photovoltaic systems is at least 30 times less carbon-intensive than electricity generated by a coal plant (+1,000 gCO2e/kWh)")
             (i "The most-cited energy payback times (EPBT) for solar PV systems are between one and two years. "))
          (i "decline in costs accelerates sharply from 2009 onwards and it’s the consequence of moving almost the entire PV manufacturing industry from western countries to Asian countries")
          (i "manufacture of solar PV cells relies heavily on the use of electricity (for more than 95%)")
          (i "For the modules made in China, the carbon footprint is 72.2 and 69.2 gCO2e/kWh for mono-si and multi-si, respectively, while the energy payback times are 2.4 and 2.3 years.")
          (i "If solar modules manufactured in China are installed in Germany, then the carbon footprint increases to about 120 gCO2e/kWh for both mono- and multi-si — which makes solar PV only 3.75 times less carbon-intensive than natural gas, not 15 times.")
          (i "Almost all LCAs — including the one that deals with manufacturing in China — assume a solar insolation of 1,700 kilowatt-hour per square meter per year (kWh/m2/yr), typical of Southern Europe and the southwestern USA.")
          (i "these results are based on a solar PV lifespan of 30 years"
             (i "This might be over-optimistic, because the relocation of manufacturing to China has been associated with a decrease in the quality of PV solar panels."))
          (i "These results don’t include the energy required to ship the solar panels from China to Europe. ")
          (i "At high growth rates, the energy and CO2 savings made by the cumulative installed capacity of solar PV systems can be cancelled out by the energy use and CO2 emissions from the production of new installed capacity")))
