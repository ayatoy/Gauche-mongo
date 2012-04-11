# Simple MongoDB driver for Gauche

Gauche 0.9.2, MongoDB 2.0.4

## Install

    $ git clone git://github.com/ayatoy/Gauche-mongo.git
    $ cd Gauche-mongo
    $ ./DIST gen
    $ ./configure
    $ [sudo] make install

## Example

    gosh> (use mongo)
    #<undef>
    gosh> (define conn (mongo "mongodb://localhost/"))
    conn
    gosh> (define db (mongo-database conn "test_db"))
    db
    gosh> (define coll (mongo-collection db "test_collection"))
    coll
    gosh> (mongo-insert1 coll `(("_id" . ,(bson-object-id))
                                ("name" . "ayatoy")
                                ("age" . 24)))
    (("n" . 0) ("connectionId" . 2) ("err" . null) ("ok" . 1.0))
    gosh> (mongo-find1 coll '(("name" . "ayatoy")) :select '(("age" . 1)))
    (("_id" . #<bson-object-id "4f85b49b4cfad10bea000000">) ("age" . 24))
    gosh> (mongo-insert coll (map (^[i] `(("i" . ,i))) (iota 100)))
    (("n" . 0) ("connectionId" . 2) ("err" . null) ("ok" . 1.0))
    gosh> (define cursor (mongo-find coll '(("i" . (("$exists" . true))))))
    cursor
    gosh> (mongo-cursor-next! cursor)
    (("_id" . #<bson-object-id "4f85b5d6f888240579c3a64a">) ("i" . 0))
    gosh> (mongo-cursor-take! cursor 3)
    ((("_id" . #<bson-object-id "4f85b5d6f888240579c3a64b">) ("i" . 1))
     (("_id" . #<bson-object-id "4f85b5d6f888240579c3a64c">) ("i" . 2))
     (("_id" . #<bson-object-id "4f85b5d6f888240579c3a64d">) ("i" . 3)))
