(defproject argumentica "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [com.datomic/datomic-free "0.9.5661"]
                 [flow-gl/flow-gl "1.0.0-SNAPSHOT"]
                 [org.clojure/math.combinatorics "0.1.4"]
                 #_[datascript "0.15.5"]
                 #_[com.google.guava/guava "21.0"]
                 [aysylu/loom "1.0.0"]
                 #_[spootnik/net "0.3.3-beta9"]
                 [org.clojure/core.async "0.3.443"]
                 #_[org.clojure/algo.monads "0.1.6"]
                 [me.raynes/fs "1.4.6"]
                 #_[iota "1.1.3"]
                 #_[net.cgrand/xforms "0.15.0"]
                 #_[prismatic/schema "1.1.7"]
                 [org.clojure/test.check "0.10.0-alpha2"]
                 [com.sleepycat/je "7.5.11"]
                 [com.taoensso/nippy "2.13.0"]]
  :aot [argumentica.EdnComparator]
  :jvm-opts ["-XX:-OmitStackTraceInFastThrow"])
