(ns argumentica.ibis
  (:require (flow-gl.gui [visuals :as visuals]
                         [animation :as animation])
            (fungl [application :as application]
                   [layout :as layout]
                   [layouts :as layouts])))

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
    :question [255 255 255 255]
    :idea [255 255 0 255]
    :pro [0 255 0 255]
    :con [255 0 0 255]
    [0 255 255 255]))

(defn node-view [node]
  (layouts/vertically
   (visuals/text (:text node) (node-color (:type node)))
   (when (:children node)
     (layouts/horizontally (assoc (visuals/rectangle [255 255 255 255]
                                                     0 0)
                                  :width 1
                                  :height 10)
                           (apply layouts/vertically (for [child (:children node)]
                                                       (layouts/horizontally (layouts/with-margins 10 0 0 0
                                                                               (assoc (visuals/rectangle [255 255 255 255]
                                                                                                         0 0)
                                                                                      :width 10
                                                                                      :height 1))
                                                                             (node-view child))))))))


(defn create-scene-graph [width height]
  (animation/swap-state! animation/set-wake-up 1000)
  (-> (node-view (q "question 1"
                    (i "idea 1"
                       (p "pro 1")
                       (c "con 1"
                          (p "pro 2")))))
      (application/do-layout width height)))

(defn start []
  (application/start-window #'create-scene-graph)
  #_(.start (Thread. (fn []
                       (start-window)))))
