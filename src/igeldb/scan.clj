(ns igeldb.scan
  (:require [igeldb.data :as data]))

(defn merge-results
  "Merge two ordered scan result sequences. Values from `higher-precedence`
  replace values with the same user key in `lower-precedence`."
  [higher-precedence lower-precedence]
  (loop [pairs (transient [])
         higher higher-precedence
         lower lower-precedence]
    (cond
      (and (empty? higher) (empty? lower))
      (persistent! pairs)

      (empty? lower)
      (reduce conj (persistent! pairs) higher)

      (empty? higher)
      (reduce conj (persistent! pairs) lower)

      :else
      (let [[higher-key higher-data] (first higher)
            [lower-key lower-data] (first lower)
            [updated higher-rest lower-rest]
            (cond
              (data/byte-array-equals? higher-key lower-key)
              [(conj! pairs [higher-key higher-data])
               (rest higher) (rest lower)]

              (data/byte-array-smaller? higher-key lower-key)
              [(conj! pairs [higher-key higher-data])
               (rest higher) lower]

              :else
              [(conj! pairs [lower-key lower-data])
               higher (rest lower)])]
        (recur updated higher-rest lower-rest)))))
