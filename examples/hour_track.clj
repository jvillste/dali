(ns hour-track
  (:require [argumentica.btree-index :as btree-index]
            [argumentica.db.branch :as branch]
            [argumentica.db.client-db :as client-db]
            [argumentica.db.common :as common]
            [argumentica.db.file-transaction-log :as file-transaction-log]
            [argumentica.index :as index]
            [argumentica.sorted-map-transaction-log :as sorted-map-transaction-log]
            [argumentica.sorted-set-index :as sorted-set-index]
            [argumentica.transaction-log :as transaction-log]
            [clj-time.core :as clj-time]
            [clojure.java.io :as io]
            [clojure.set :as set]
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
            [me.raynes.fs :as fs])
  (:import java.util.UUID))

(defn create-directory-btree-db [base-path]
  (db-common/update-indexes-2!
   (db-common/db-from-index-definitions db-common/base-index-definitions
                                                                    (fn [index-key]
                                                                      (btree-index/create-directory-btree-index (str base-path "/" (name index-key))
                                                                                                                1001))
                                                                    (file-transaction-log/create (str base-path "/transaction-log")))))

(defn create-in-memory-btree-db []
  (db-common/db-from-index-definitions db-common/base-index-definitions
                                       (fn [_index-name]
                                         (btree-index/create-memory-btree-index 1001))
                                       (sorted-map-transaction-log/create)))

(defn create-in-memory-db []
  (db-common/db-from-index-definitions db-common/base-index-definitions
                                       sorted-set-index/creator
                                       (sorted-map-transaction-log/create)))



(def disk-db-directory "data/temp/hour-track")

(defn store-index-roots! [db]
  (let [last-transaction-number (transaction-log/last-transaction-number (:transaction-log db))]
    (doseq [index (vals (:indexes db))]
      (btree-index/store-root! (:index index)
                               last-transaction-number))))

(def schema
  {:year {}
   :month {}
   :day {}
   :type {}
   :log-lines {:reference? true
               :multivalued? true}
   :task-name {}
   :start-hour {}
   :start-minute {}
   :end-hour {}
   :end-minute {}})

(defn new-id []
  (UUID/randomUUID))

(defn create-day [id year month day]
  #{[id :type :set :day]
    [id :year :set year]
    [id :month :set month]
    [id :day :set day]
    [id :end-hour :set 16]
    [id :end-minute :set 0]})

(defn create-log-line [id start-hour start-minute]
  #{[id :type :set :log-line]
    [id :start-hour :set start-hour]
    [id :start-minute :set start-minute]})

(defn add-log-line [day start-hour start-minute]
  (let [log-line-id (new-id)]
    (set/union (create-log-line log-line-id start-hour start-minute)
               #{[day :log-lines :add log-line-id]})))

(defn generate-string [value]
  (let [w (java.io.StringWriter.)]
    (print-method value w)
    (.toString w)))

(comment
  (fs/mkdirs disk-db-directory)

  (let [db (create-directory-btree-db disk-db-directory)]
    (store-index-roots! db))

  (let [db (create-directory-btree-db disk-db-directory)]
    (db-common/transact! db #{[1 :name :set "Baz"]
                              [1 :name :set "Bar"]})
    (store-index-roots! db))

  (-> (directory-storage/create (str disk-db-directory "/eatcv/nodes"))
      (storage/get-edn-from-storage! "A8A6AD614CC164F3DE92E873870CCA71AB97DADFE245F84AED82B3189976B79E"))

  (let [db (create-directory-btree-db disk-db-directory)]
    (db-common/values db 1 :name))

  (let [db (create-directory-btree-db disk-db-directory)]
    (db-common/->Entity db schema 1))

  (do (fs/delete-dir (str disk-db-directory "/avtec"))
      (fs/delete-dir (str disk-db-directory "/eatcv"))
      (fs/delete-dir (str disk-db-directory "/transaction-log")))

  (let [transaction (let [day-id (new-id)]
                      (set/union (create-day day-id 2018 11 2)
                                 (add-log-line day-id 12 0)
                                 (add-log-line day-id 13 30)))]
    (let [db (create-in-memory-btree-db)]
      (db-common/transact! db transaction)

      (index/inclusive-subsequence (-> db :indexes :eatcv :index)
                                   nil)
      #_(map (partial db-common/->Entity db schema)
             (set (map first (index/inclusive-subsequence (-> db :indexes :eatcv :index)
                                                          nil))))))

  (let [db (create-directory-btree-db disk-db-directory)]
    (map (partial db-common/->Entity db schema)
         (db-common/entities db :type :day)))
  )

;; UI


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


(defn add-day! [db]
  (let [now (clj-time/now)]
    (db-common/transact! db
                         (create-day (new-id)
                                     (clj-time/year now)
                                     (clj-time/month now)
                                     (clj-time/day now)))))

(defn commit! [state-atom]
  (db-common/transact! (:db @state-atom) (set (db-common/squash-transaction-log (:transaction-log (:branch @state-atom)))))
  (swap! state-atom assoc :branch (branch/create (db-common/deref (:db @state-atom)))))

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

(defn day-button [day]
  (text (str (:day day)
             "."
             (:month day))))

(defn root-view [db]
  (let [state-atom (atom-registry/get! :root-state
                                       {:create (fn []
                                                  {:branch (branch/create (common/deref db))
                                                   :db db})})
        branch (:branch @state-atom)]
    (animation/swap-state! animation/set-wake-up 2000)

    (layouts/superimpose (visuals/rectangle-2 :fill-color [255 255 255 255])
                         (layouts/with-margins 20 20 20 20
                           (layouts/vertically-2 {:margin 30}
                                                 (layouts/horizontally-2 {:margin 10}
                                                                         (button "add day" [add-day! branch])
                                                                         (button "commit" [commit! state-atom]))
                                                 (apply layouts/horizontally-2 {:margin 10}
                                                        (map day-button
                                                             (map (partial db-common/->Entity branch schema)
                                                                  (db-common/entities branch :type :day)))))))))
(defn create-demo-db []
  (let [db (create-in-memory-btree-db)]
    (db-common/transact! db (let [day-id (new-id)]
                              (set/union (create-day day-id 2018 11 2)
                                         (add-log-line day-id 12 0)
                                         (add-log-line day-id 13 30))))
    db))

(defn demo []
     (fn [width height]
      (-> (#'root-view #_(create-demo-db)
                       #_(create-in-memory-btree-db)
                       (create-directory-btree-db disk-db-directory))
          (application/do-layout width height))))

(defn start []
  (application/start-window (demo)))
