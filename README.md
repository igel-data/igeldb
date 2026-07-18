# IgelDB

Simple Key-Value Store based on LSM Tree

The written data is persisted into the disk. The memtable (write buffer on memory) is protected by WAL(write ahead logging).

## TODO
- ~~WAL~~
- Compaction
- Error handling
- Background ~~flush~~/compaction
- Concurrency
- Indexing

## Usage

```clojure
(require '[igeldb.core :as igel])

;; Generate a KVS with a config file
(def kvs (igel/gen-kvs "config.yaml"))

;; A key and a value as bytes
(def key1 (.getBytes "key1"))
(def val1 (.getBytes "val1"))

;; Write the key-value pair
(igel/write! kvs key1 val1)

;; Read the key-value pair
(igel/select kvs key1)
; -> #whidbey/bin "dmFsMQ"

;; with deserialization
(String. (igel/select kvs key1))
; -> "val1"

;; Write some pairs
(doseq [i (range 2 5)]
  (igel/write! kvs (.getBytes (str "key" i)) (.getBytes (str "val" i))))

;; Scan pairs with from-key(inclusive) and to-key(not-inclusive)
(def from-key (.getBytes "key0"))
(def to-key (.getBytes "key3"))
(igel/scan kvs from-key to-key)
; -> ([#whidbey/bin "a2V5MQ" #whidbey/bin "dmFsMQ"]
;     [#whidbey/bin "a2V5Mg" #whidbey/bin "dmFsMg"])

;; with deserialization
(map (fn [[k v]] [(String. k) (String. v)])
     (igel/scan kvs from-key to-key))
; -> (["key1" "val1"] ["key2" "val2"])

;; Delete a key
(igel/delete! kvs (.getBytes "key2"))

;; The key2 was deleted
(map (fn [[k v]] [(String. k) (String. v)])
     (igel/scan kvs from-key to-key))
; -> (["key1" "val1"])
```

## Configuration

FIXME: listing of parameters

## License

Copyright © 2023 Yuji Ito

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
