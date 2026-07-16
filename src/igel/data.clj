(ns igel.data
  (:import (java.util Comparator)))

(defrecord Data [value deleted?])

(defn new-data
  [^bytes value]
  (->Data value false))

(defn deleted-data
  []
  (->Data nil true))

(defn is-valid?
  [^Data data]
  (not (:deleted? data)))

;; A single, shared comparator instance. Bytes must be compared *unsigned*:
;; `aget` returns a signed Java byte (-128..127), so a raw `compare` would order
;; 0x80 before 0x01 and break key ordering for multi-byte UTF-8 or binary keys.
;; The whole LSM-tree relies on this ordering, so it is compared via
;; `Byte/compareUnsigned`.
(def ^Comparator ^:private byte-array-comparator-instance
  (reify Comparator
    (compare [_ a b]
      (loop [i 0]
        (if (< i (min (count a) (count b)))
          (let [cmp (Byte/compareUnsigned (aget ^bytes a i) (aget ^bytes b i))]
            (if (zero? cmp)
              (recur (inc i))
              cmp))
          (compare (count a) (count b)))))))

(defn byte-array-comparator
  "Return the shared unsigned byte-array comparator."
  ^Comparator []
  byte-array-comparator-instance)

(defn byte-array-smaller?
  "lhs < rhs"
  [^bytes lhs ^bytes rhs]
  (neg? (.compare byte-array-comparator-instance lhs rhs)))

(defn byte-array-smaller-or-equal?
  "lhs <= rhs"
  [^bytes lhs ^bytes rhs]
  (<= (.compare byte-array-comparator-instance lhs rhs) 0))

(defn byte-array-equals?
  [^bytes lhs ^bytes rhs]
  (zero? (.compare byte-array-comparator-instance lhs rhs)))
