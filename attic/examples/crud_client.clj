(ns examples.crud-client
  (:gen-class)
  (:require [argumentica.db.client :as client]
            [argumentica.db.client-db :as client-db]
            [argumentica.db.server-btree-db :as server-btree-db]
            [clojure.java.io :as io]
            [examples.crud-server :as crud-server]
            [flow-gl.graphics.buffered-image :as buffered-image]
            [flow-gl.graphics.font :as font]
            [flow-gl.gui.animation :as animation]
            [flow-gl.gui.visuals :as visuals]
            [fungl.application :as application]
            [fungl.atom-registry :as atom-registry]
            [fungl.callable :as callable]
            [fungl.component.text :as text]
            [fungl.component.text-area :as text-area]
            [fungl.layouts :as layouts]
            [argumentica.db.server-api :as server-api])
  (:import java.util.UUID))

#_(declare server-state-atom)
(defonce server-state-atom (atom (server-api/create-state (-> (crud-server/create-in-memory-btree-db 21)
                                                              #_(crud-server/transact-titles "data/imdb/title.basics.tsv" (take 20))))))

(comment
  (:db @server-state-atom))

(def magnifying-glass (buffered-image/create-from-file (io/resource "magnifying-glass.png")))


(def font (font/create "LiberationSans-Regular.ttf" #_20 38))
(def symbol-font (font/create "LiberationSans-Regular.ttf" 20 #_58))


(defn text [string]
  (text/text string
             [0 0 0 255]
             font))

(defn button-mouse-event-handler [handler node event]
  (when (= :mouse-clicked
           (:type event))
    (callable/call handler))
  event)

(defn button [message handler]
  (-> (layouts/box 10
                   (visuals/rectangle [200 200 200 255] 30 30)
                   (text/text message [0 0 0 255] font))
      (assoc :mouse-event-handler [button-mouse-event-handler handler])))

(defn commit-button-handler [client-db-atom]
  (swap! client-db-atom client-db/commit))

(defn create-button-handler [client-db-atom]
  (swap! client-db-atom client-db/transact [[(UUID/randomUUID) :type :set :title]]))

(defn refresh-button-handler [client-db-atom]
  (swap! client-db-atom client-db/refresh))

(defn bare-text-editor [id text handle-text-change]
  (text-area/text-area-2 id
                         :style {:color [0 0 0 255]
                                 :font  font}
                         :text text
                         :on-text-change handle-text-change))

(defn text-editor [id text handle-text-change]
  (layouts/box 10
               (visuals/rectangle-2 :fill-color [255 255 255 255]
                                    :draw-color [200 200 200 255]
                                    :line-width 4
                                    :corner-arc-radius 30)
               (layouts/with-minimum-size 300 nil
                 (bare-text-editor id text handle-text-change))))



(defn property-view [id name value on-text-change]
  (layouts/horizontally-2 {:margin 10
                           :centered true}
                          (text/text name
                                     [0 0 0 255]
                                     font)
                          (layouts/with-maximum-size 500 nil
                            (text-editor id
                                         (or value "")
                                         on-text-change)))
  #_(layouts/box 15
                 (visuals/rectangle-2 :corner-arc-radius 60
                                      :fill-color [229 229 229 255])
                 (layouts/horizontally-2 {:margin 10
                                          :centered true}
                                         (text/text name
                                                    [0 0 0 255]
                                                    font)
                                         (layouts/with-maximum-size 500 nil
                                           (text-editor id
                                                        (or value "")
                                                        on-text-change)))))

(defn property-editor [id client-db-atom entity-id attribute]
  (property-view id
                 (name attribute)
                 (client-db/value @client-db-atom entity-id attribute)
                 (fn [new-text]
                   (swap! client-db-atom client-db/transact [[entity-id attribute :set new-text]]))))

(defn entity-editor [id client-db-atom entity-id]
  (layouts/box 15
               (visuals/rectangle-2 :corner-arc-radius 60
                                    :fill-color [229 229 229 255])
               (layouts/vertically-2 {}
                                     (property-editor (conj id :primaryTitle) client-db-atom entity-id :primaryTitle)
                                     #_(property-editor (conj id :address) client-db-atom entity-id :address))))

(defn loaded-nodes [client-db]
  (let [indexes (-> client-db :server-btree-db :indexes)]
    (into {} (for [index-key (keys indexes)]
               [index-key (-> indexes index-key :remote-index :index :btree-index-atom deref :nodes keys count)]))))

(defn entity-list [id client-db-atom]
  (let [state-atom (atom-registry/get! [:state client-db-atom]
                                       {:create (fn [] {:query ""})})]

    (animation/swap-state! animation/set-wake-up 2000)

    (layouts/superimpose (visuals/rectangle-2 :fill-color [255 255 255 255])
                         (layouts/with-margins 20 20 20 20
                           (layouts/vertically-2 {:margin 30}
                                                 (layouts/horizontally-2 {:margin 10}
                                                                         (assoc (visuals/image magnifying-glass) :width 60 :height 60)
                                                                         (text-editor (conj id :query-editor)
                                                                                      (:query @state-atom)
                                                                                      (fn [new-text] (swap! state-atom assoc :query new-text)))

                                                                         (button "Add" [create-button-handler client-db-atom])
                                                                         (button "Commit" [commit-button-handler client-db-atom])
                                                                         (button "Refresh" [refresh-button-handler client-db-atom]))

                                                 (for [entity-id (->> (client-db/entities @client-db-atom
                                                                                          :type
                                                                                          :title)
                                                                      (take 10)
                                                                      #_(filter (fn [entity-id]
                                                                                  (.contains (or (client-db/value @client-db-atom
                                                                                                                  entity-id
                                                                                                                  :name)
                                                                                                 "")
                                                                                             (:query @state-atom)))))]
                                                   (entity-editor (conj id entity-id) client-db-atom entity-id))

                                                 (layouts/vertically-2 {}
                                                                       (for [statement (client-db/transaction @client-db-atom)]
                                                                         (text/text (pr-str statement)
                                                                                    [0 0 0 255]
                                                                                    font)))
                                                 (text (pr-str (loaded-nodes @client-db-atom))))))))

(defn crud-client []
  (let [client-db-atom (atom (client-db/create #_(client/->HttpClient "http://localhost:4010/api")
                                               (client/->InProcessClient server-state-atom)
                                               crud-server/imdb-index-definition))]
    (fn [width height]
      (-> (#'entity-list [:entity-list-1] client-db-atom)
          (application/do-layout width height)))))



(comment
  (let [client (client/->HttpClient "http://localhost:4010/api")]
    (type (client/get-from-node-storage client :avtec "467092283B53F4DECB9CDB9483E4008E6F3E8BC9ED9B9566C06E3FEEB7F44ACD")))
  
  (let [client-db-atom (atom (client-db/create #_(client/->HttpClient "http://localhost:4010/api")
                                               (client/->InProcessClient server-state-atom)
                                               crud-server/imdb-index-definition))]

    #_(client-db/entities @client-db-atom
                          :type
                          :title)

    #_(loaded-nodes @client-db-atom)
    #_(-> @client-db-atom :server-btree-db :indexes :avtec :remote-index :index :btree-index-atom ;;deref :nodes keys count
          )
    #_(common/->Entity @client-db-atom
                       imdb/schema
                       "tt0000002"))

  (let [client-db-atom (atom (server-btree-db/create (client/->InProcessClient server-state-atom)
                                                     crud-server/imdb-index-definition))]

    )

  (let [client-db-atom (atom (client-db/create #_(client/->HttpClient "http://localhost:4010/api")
                                               (client/->InProcessClient server-state-atom)
                                               crud-server/imdb-index-definition))]

    #_(client-db/inclusive-subsequence @client-db-atom :eatcv [nil :primaryTitle nil nil nil])

    #_(client-db/inclusive-subsequence @client-db-atom :avtec [:primaryTitle nil nil nil nil])

    #_(client-db/inclusive-subsequence @client-db-atom :avtec [:type :title nil nil nil])
    #_(client-db/inclusive-subsequence @client-db-atom :full-text [:primaryTitle "the" nil nil nil])
    (-> @client-db-atom :local-db :indexes :eatcv #_:full-text)
    #_(db-common/datoms @client-db-atom :full-text [:primaryTitle "the" nil nil nil]))

  )

(defn start []
  (application/start-window (crud-client)))

(defn -main [& [url]]
  (application/start-window (crud-client)))
