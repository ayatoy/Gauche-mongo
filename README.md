# Simple MongoDB driver for Gauche

Gauche 0.9.2, MongoDB 2.0.4

## Install

    $ git clone git://github.com/ayatoy/Gauche-mongo.git
    $ cd Gauche-mongo
    $ ./DIST gen
    $ ./configure
    $ [sudo] make install

## Example

    (use mongo)

    ;;; single server

    (mongo "mongodb://localhost/") ; => #<mongo #f>
    (mongo "mongodb://localhost:27017/") ; => #<mongo #f>
    ;; without scheme name
    (mongo "localhost:27017") ; => #<mongo #f>

    ;;; replica set

    (mongo "mongodb://localhost,localhost:27018/?replicaSet=foo") ; => #<mongo "foo">
    (define conn (mongo "localhost,localhost:27018/?replicaSet=foo"))

    ;;; admin command

    (mongo-admin conn '(("ping" . 1))) ; => (("ok" . 1.0))
    (mongo-admin conn '(("ping" . 1)) :slave #t) ; => (("ok" . 1.0))

    ;;; helper

    (mongo-ping conn) ; => (("ok" . 1.0))
    (mongo-ping conn :slave #t) ; => (("ok" . 1.0))

    ;;; database

    (mongo-database conn "test") ; => #<mongo-database "test">
    ;; when connection
    (define db (mongo "localhost/test"))

    ;;; command

    (mongo-command db '(("profile" . -1)))
    ; => (("was" . 0) ("slowms" . 100) ("ok" . 1.0))

    ;;; authenticate

    (mongo "user:pass@localhost/test")
    (mongo-auth db "user" "pass")
    (mongo-add-user db "new_user" "pass")
    (mongo-remove-user db "user")

    ;;; profiling

    (mongo-profiling-status db) ; => (("was" . 0) ("slowms" . 100) ("ok" . 1.0))
    (mongo-profiling-status db :slave #t)
    (mongo-get-profiling-level db) ; => 0
    (mongo-set-profiling-level db 2)
    (mongo-show-profiling db)

    ;;; collection

    (define coll (mongo-collection db "test_collection"))

    (mongo-insert1 coll `(("_id" . ,(bson-object-id))
                          ("name" . "ayatoy")
                          ("age" . 24)))
    ; => (("n" . 0) ("connectionId" . 2) ("err" . null) ("ok" . 1.0))

    (mongo-find1 coll '(("name" . "ayatoy")) :select '(("age" . 1)))
    ; => (("_id" . #<bson-object-id "4f85b49b4cfad10bea000000">) ("age" . 24))

    (mongo-insert coll (map (^[i] `(("i" . ,i))) (iota 100)))
    ; => (("n" . 0) ("connectionId" . 2) ("err" . null) ("ok" . 1.0))

    (define cursor (mongo-find coll '(("i" . (("$exists" . true))))))

    (mongo-cursor-next! cursor)
    ; => (("_id" . #<bson-object-id "4f85b5d6f888240579c3a64a">) ("i" . 0))

    (mongo-cursor-take! cursor 3)
    ; => ((("_id" . #<bson-object-id "4f85b5d6f888240579c3a64b">) ("i" . 1))
          (("_id" . #<bson-object-id "4f85b5d6f888240579c3a64c">) ("i" . 2))
          (("_id" . #<bson-object-id "4f85b5d6f888240579c3a64d">) ("i" . 3)))

    ;;; index

    (mongo-ensure-index coll "index_name" '(("name" . 1)) :unique #t)
    (mongo-show-indexes coll)
    (mongo-drop-index coll "index_name")
    (mongo-drop-indexes coll)
    (mongo-reindex coll)

    ;;; mapReduce

    (mongo-map-reduce coll
                      (bson-code "function() { emit(this.i, 1); }")
                      (bson-code "function(k, vals) { return 1; }")
                      :query '(("i" . (("$exists" . true))))
                      :out '(("inline" . 1)))

    ;;; DBRef

    (define ref (mongo-dbref "test_collection"
                             (bson-object-id "4f85b49b4cfad10bea000000")))
    (mongo-dbref-get db ref)
