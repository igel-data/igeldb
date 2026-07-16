(ns igel.data-test
  (:require [clojure.test :refer [deftest is testing]]
            [igel.data :as data]))

(defn- ->bytes
  [& ints]
  (byte-array (map unchecked-byte ints)))

(deftest unsigned-byte-ordering-test
  (testing "bytes >= 0x80 must order *after* smaller bytes (unsigned compare)"
    ;; 0x80 is negative as a signed Java byte, so a signed compare would place
    ;; it before 0x01 and break key ordering. It must be treated as 128.
    (is (data/byte-array-smaller? (->bytes 0x01) (->bytes 0x80)))
    (is (data/byte-array-smaller? (->bytes 0x7f) (->bytes 0x80)))
    (is (data/byte-array-smaller? (->bytes 0x80) (->bytes 0xff)))
    (is (not (data/byte-array-smaller? (->bytes 0x80) (->bytes 0x01)))))

  (testing "ordering by whole-array value with high bytes"
    (let [sorted (->> [(->bytes 0xff 0x00)
                       (->bytes 0x00 0x01)
                       (->bytes 0x80)
                       (->bytes 0x7f 0xff)
                       (->bytes 0x00)]
                      (sort (data/byte-array-comparator))
                      (map vec))]
      (is (= [[0x00]
              [0x00 0x01]
              [0x7f -1]     ;; 0xff as signed byte
              [-128]        ;; 0x80 as signed byte
              [-1 0x00]]    ;; 0xff 0x00
             sorted))))

  (testing "shared instance and prefix / equality semantics"
    (is (identical? (data/byte-array-comparator) (data/byte-array-comparator)))
    (is (data/byte-array-smaller? (->bytes 0x80) (->bytes 0x80 0x00)))
    (is (data/byte-array-equals? (->bytes 0x80 0xff) (->bytes 0x80 0xff)))
    (is (data/byte-array-smaller-or-equal? (->bytes 0x80) (->bytes 0x80)))))
