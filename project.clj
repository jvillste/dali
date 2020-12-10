(defproject dali "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 #_[com.datomic/datomic-free "0.9.5697"]
                 [org.clojure/math.combinatorics "0.1.4"]
                 #_[datascript "0.15.5"]
                 #_[com.google.guava/guava "21.0"]
                 [aysylu/loom "1.0.2"]
                 [spootnik/net "0.3.3-beta14"]
                 [org.clojure/core.async "1.3.610"]
                 #_[org.clojure/algo.monads "0.1.6"]
                 [me.raynes/fs "1.4.6"]
                 [cor "0.1.0-SNAPSHOT"]
                 #_[iota "1.1.3"]
                 [net.cgrand/xforms "0.19.0"]
                 [kixi/stats "0.4.3"]
                 #_[prismatic/schema "1.1.7"]
                 [org.clojure/test.check "0.10.0-alpha2"]
                 [com.sleepycat/je "18.3.12" #_"7.5.11"]
                 [com.taoensso/nippy "2.14.0"]
                 [org.flatland/useful "0.11.5"]
                 [logga "0.1.0-SNAPSHOT"]
                 [prismatic/schema "1.1.12"]
                 [org.clojure/tools.trace "0.7.10"]
                 [org.clojure/tools.namespace "1.1.0"]
                 [com.taoensso/tufte "2.1.0"]]
  :test-paths ["src/clj" "test"]
  :jvm-opts ["-XX:-OmitStackTraceInFastThrow"]
  :resource-paths ["resources"]
  :aliases {"test" ["clean"]})
