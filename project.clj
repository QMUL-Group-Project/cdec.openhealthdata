(defproject cdec.health-analytics "0.1.1-SNAPSHOT"
  :description "Connected Digital Economy Catapult Open Health Data"
  :url "https://github.com/CDECatapult"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/tools.logging "0.2.6"]
                 [cascalog "1.10.1"]]
  :profiles {:dev {:dependencies [[midje "1.5.1"
                                   :exclusions [org.apache.httpcomponents/httpcore
                                                commons-io]]
                                  [org.apache.hadoop/hadoop-core "1.0.4"
                                   :exclusions [org.slf4j/slf4j-api
                                                commons-logging
                                                commons-codec
                                                org.slf4j/slf4j-log4j12
                                                log4j]]]}
             :provided {:dependencies [[org.apache.hadoop/hadoop-core "0.20.2-dev"]]}}
  :aot [cdec.health-analytics]
  :main cdec.health-analytics
  :uberjar-name "health-analytics.jar"
  :exclusions [org.apache.hadoop/hadoop-core
               org.clojure/clojure
               midje])
