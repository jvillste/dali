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

(defn start []
  (application/start-window (partial create-scene-graph
                                     (atom  (create-state (atom (q "question 1 sf sfs dfsd fsd fds fdsf dsf sd fsdf sdf ss fssd f"
                                                                   (i "idea 1 1 sf sfs dfsd fsd fds fdsf dsf sd fsdf sdf ss fssd f"
                                                                      (p "pro 1")
                                                                      (c "con 1"
                                                                         (p "pro 2"))
                                                                      (c "con 2"))))))))
  #_(.start (Thread. (fn []
                       (start-window)))))
