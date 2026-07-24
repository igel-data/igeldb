(ns build
  (:require [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

(def lib 'com.igel-data/igeldb)
(def version (or (System/getenv "VERSION") "0.0.0-SNAPSHOT"))
(def class-dir "target/classes")
(def jar-file (format "target/%s-%s.jar" (name lib) version))
(def basis (delay (b/create-basis {:project "deps.edn"})))

(defn clean [_]
  (b/delete {:path "target"}))

(defn jar [_]
  (clean nil)
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis @basis
                :src-dirs ["src"]
                :pom-data [[:description
                            "An embeddable key-value store for the Clojure ecosystem"]
                           [:url "https://github.com/igel-data/igeldb"]
                           [:licenses
                            [:license
                             [:name "Eclipse Public License 2.0"]
                             [:url "https://www.eclipse.org/legal/epl-2.0/"]]]
                           [:scm
                            [:url "https://github.com/igel-data/igeldb"]
                            [:connection
                             "scm:git:https://github.com/igel-data/igeldb.git"]
                            [:developerConnection
                             "scm:git:ssh://git@github.com/igel-data/igeldb.git"]]]})
  (b/copy-dir {:src-dirs ["src"]
               :target-dir class-dir})
  (b/copy-file {:src "LICENSE"
                :target (str class-dir "/META-INF/LICENSE")})
  (b/jar {:class-dir class-dir
          :jar-file jar-file})
  {:jar-file jar-file})

(defn deploy [_]
  (dd/deploy {:installer :remote
              :artifact (b/resolve-path jar-file)
              :pom-file (b/pom-path {:lib lib
                                     :class-dir class-dir})}))
