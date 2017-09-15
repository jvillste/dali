(defproject argumentica "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha14"]
                 [com.datomic/datomic-free "0.9.5067"]
                 [flow-gl/flow-gl "1.0.0-SNAPSHOT"]
                 [org.clojure/math.combinatorics "0.1.4"]
                 [datascript "0.15.5"]
                 [com.google.guava/guava "21.0"]
                 [aysylu/loom "1.0.0"]
                 [spootnik/net "0.3.3-beta9"]
                 [org.clojure/core.async "0.3.443"]]
  :jvm-opts ["-XX:-OmitStackTraceInFastThrow"])
