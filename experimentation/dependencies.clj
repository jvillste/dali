(ns dependencies
  (:require [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as string]
            [clojure.test :refer [deftest is]]
            [clojure.tools.namespace.dependency :as dependency]
            [clojure.tools.namespace.find :as find]
            [clojure.tools.namespace.parse :as parse])
  (:import java.io.File))

(def all-kept-namespaces
  (concat ['flow-gl.tools.trace
           'flow-gl.debug
           'dev
           'flow-gl.gui.tiled-renderer]
          (parse/deps-from-ns-decl '(ns painta.core
                                      (:require
                                       [clojure.java.io :as io]
                                       [clojure.test :refer :all]
                                       [flow-gl.graphics.buffered-image :as buffered-image]
                                       [flow-gl.gui.keyboard :as keyboard]
                                       [flow-gl.gui.quad-renderer :as quad-renderer]
                                       [flow-gl.gui.render-target-renderer :as render-target-renderer]
                                       [flow-gl.gui.scene-graph :as scene-graph]
                                       [flow-gl.gui.visuals :as visuals]
                                       [flow-gl.opengl.jogl.opengl :as opengl]
                                       [flow-gl.opengl.jogl.quad :as quad]
                                       [flow-gl.opengl.jogl.render-target :as render-target]
                                       [flow-gl.opengl.jogl.texture :as texture]
                                       [flow-gl.opengl.jogl.window :as window]
                                       [flow-gl.profiling :as profiling]
                                       [fungl.application :as application]
                                       [fungl.atom-registry :as atom-registry]
                                       [fungl.cache :as cache]
                                       [fungl.callable :as callable]
                                       [fungl.layouts :as layouts])
                                      (:import java.awt.event.KeyEvent javax.swing.JFileChooser)))
          (parse/deps-from-ns-decl '(ns examples.crud-client
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
                                                [fungl.layouts :as layouts])
                                      (:import java.util.UUID)))))

(def all-kept-namespaces
  ['])

(defn- dependency-graph-for-directory [directory-name]
  (reduce (fn [graph ns-declaration]
            (reduce (fn [graph dependency]
                      (dependency/depend graph
                                         (second ns-declaration)
                                         dependency))
                    graph
                    (parse/deps-from-ns-decl ns-declaration)))
          (dependency/graph)
          (find/find-ns-decls-in-dir (File. directory-name))))

(defn namespaces-in-directory [directory-name]
  (set (map second (find/find-ns-decls-in-dir (File. directory-name)))))

(defn transitive-namespaces [directory-name namespaces]
  (let [dependency-graph (dependency-graph-for-directory directory-name)]
    (set (concat namespaces
                 (mapcat (partial dependency/transitive-dependencies
                                  dependency-graph)
                         namespaces)))))

(defn ns-file-name [ns]
  (str (-> (name ns)
           (string/replace #"-" "_")
           (string/replace #"\." "/"))
       ".clj"))

(deftest test-ns-file-name
  (is (= "flow_gl/data/zipper_list.clj"
         (ns-file-name 'flow-gl.data.zipper-list))))

(defn source-file-name-to-target-file-name [source-directory-name
                                             target-directory-name
                                             source-file-name]
  (str target-directory-name
       "/"
       (subs source-file-name
             (inc (count source-directory-name)))))

(deftest test-source-file-name-to-target-file-name
  (is (= "old/flow-gl/flow_gl/data/zipper_list.clj"
         (source-file-name-to-target-file-name "src"
                                                "old"
                                                "src/flow-gl/flow_gl/data/zipper_list.clj")))

  (is (= "clj-archive/flow-gl/flow_gl/data/zipper_list.clj"
         (source-file-name-to-target-file-name "src/clj"
                                                "clj-archive"
                                                "src/clj/flow-gl/flow_gl/data/zipper_list.clj"))))

(defn move-namespace-file [source-directory-name
                           target-directory-name
                           source-namespace]
  (let [source-file-name (str source-directory-name
                              "/"
                              (ns-file-name source-namespace))
        source-file (io/file source-file-name)
        target-file-name (source-file-name-to-target-file-name source-directory-name
                                                                 target-directory-name
                                                                 source-file-name)]

    (println "Moving " source-file-name " to " target-file-name)

    (if (and (.exists source-file)
             (not (.exists (io/file target-file-name))))
      (do
        (io/make-parents target-file-name)
        (io/copy (io/file source-file-name)
                 (io/file target-file-name))
        (io/delete-file (io/file source-file-name)))
      (println (str "No need to move " source-file-name)))))

(comment
  (namespaces-in-directory #_"src"
                           "/Users/jukka/nitor-src/cor")

  (transitive-namespaces "src"
                         all-kept-namespaces)

  (transitive-namespaces "src"
                         ['argumentica.hash-map-storage])

  (set/intersection (namespaces-in-directory "old")
                    (set/union (transitive-namespaces "src"
                                                      all-kept-namespaces)
                               (transitive-namespaces "old"
                                                      all-kept-namespaces)))
  
  (def unused-namespaces (set/difference (namespaces-in-directory "src")
                                         (transitive-namespaces "src"
                                                                all-kept-namespaces)))

  (doall (map (partial move-namespace-file
                       "src"
                       "old")
              unused-namespaces))

  (doall (map (partial move-namespace-file
                       "old"
                       "src")
              '#{fungl.component.text fungl.component.text-area}))

  (move-namespace-file "old"
                       "src"
                       'fungl.component.text-area)

  (move-namespace-file "src"
                       "old"
                       'argumentica.db-test)

  (move-namespace-file "test"
                       "old-test"
                       'argumentica.db-test)


  )
