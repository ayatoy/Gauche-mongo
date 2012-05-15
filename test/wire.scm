(use gauche.test)
(use gauche.net)
(use mongo.util)
(use mongo.bson)

;;;; prep

(define socket (make-client-socket 'inet "localhost" 27017))

;;;; test

(test-start "wire")

(use mongo.wire)
(test-module 'mongo.wire)

;;;; constant

(test-section "constant")

(test* "MONGO_OP_REPLY" 1 MONGO_OP_REPLY)
(test* "MONGO_OP_MSG" 1000 MONGO_OP_MSG)
(test* "MONGO_OP_UPDATE" 2001 MONGO_OP_UPDATE)
(test* "MONGO_OP_INSERT" 2002 MONGO_OP_INSERT)
(test* "MONGO_OP_RESERVED" 2003 MONGO_OP_RESERVED)
(test* "MONGO_OP_QUERY" 2004 MONGO_OP_QUERY)
(test* "MONGO_OP_GETMORE" 2005 MONGO_OP_GETMORE)
(test* "MONGO_OP_DELETE" 2006 MONGO_OP_DELETE)
(test* "MONGO_OP_KILL_CURSORS" 2007 MONGO_OP_KILL_CURSORS)

;;;; update

(test-section "update")

(let1 m (mongo-message-update "foo.bar"
                              '(("key" . "value"))
                              '(("key" . "value"))
                              :upsert #t
                              :multi-update #t)
  (receive (m-size m-write) (write-mongo-message-update-prepare m)
    (test* "mongo-message-update" <mongo-message-update>
           (class-of m))
    (test* "mongo-message?" #t
           (mongo-message? m))
    (test* "mongo-message-update?" #t
           (mongo-message-update? m))
    (test* "mongo-message-op-code" 2001
           (mongo-message-op-code m))
    (test* "mongo-message-update size" 72
           m-size)
    (test* "write-mongo-message-update" #*"\x48\0\0\0\0\0\0\0\xff\xff\xff\xff\xd1\x07\0\0\0\0\0\0foo.bar\0\x03\0\0\0\x14\0\0\0\x02key\0\x06\0\0\0value\0\0\x14\0\0\0\x02key\0\x06\0\0\0value\0\0"
           (call-with-output-string m-write))))

;;;; insert

(test-section "insert")

(let1 m (mongo-message-insert "foo.bar"
                              '((("key" . "value")))
                              :continue-on-error #t)
  (receive (m-size m-write) (write-mongo-message-insert-prepare m)
    (test* "mongo-message-insert" <mongo-message-insert>
           (class-of m))
    (test* "mongo-message?" #t
           (mongo-message? m))
    (test* "mongo-message-insert?" #t
           (mongo-message-insert? m))
    (test* "mongo-message-op-code" 2002
           (mongo-message-op-code m))
    (test* "mongo-message-insert size" 48
           m-size)
    (test* "write-mongo-message-insert" #*"0\0\0\0\0\0\0\0\xff\xff\xff\xff\xd2\x07\0\0\x01\0\0\0foo.bar\0\x14\0\0\0\x02key\0\x06\0\0\0value\0\0"
           (call-with-output-string m-write))))

;;;; query

(test-section "query")

(let1 m (mongo-message-query "foo.bar"
                             '(("key" . "value")))
  (receive (m-size m-write) (write-mongo-message-query-prepare m)
    (test* "mongo-message-query" <mongo-message-query>
           (class-of m))
    (test* "mongo-message?" #t
           (mongo-message? m))
    (test* "mongo-message-query?" #t
           (mongo-message-query? m))
    (test* "mongo-message-op-code" 2004
           (mongo-message-op-code m))
    (test* "mongo-message-query size" 56
           m-size)
    (test* "write-mongo-message-query" #*"\x38\0\0\0\0\0\0\0\xff\xff\xff\xff\xd4\x07\0\0\0\0\0\0foo.bar\0\0\0\0\0\0\0\0\0\x14\0\0\0\x02key\0\x06\0\0\0value\0\0"
           (call-with-output-string m-write))))

;;;; getmore

(test-section "getmore")

(let1 m (mongo-message-getmore "foo.bar" 1234567890)
  (receive (m-size m-write) (write-mongo-message-getmore-prepare m)
    (test* "mongo-message-getmore" <mongo-message-getmore>
           (class-of m))
    (test* "mongo-message?" #t
           (mongo-message? m))
    (test* "mongo-message-getmore?" #t
           (mongo-message-getmore? m))
    (test* "mongo-message-op-code" 2005
           (mongo-message-op-code m))
    (test* "mongo-message-getmore size" 40
           m-size)
    (test* "write-mongo-message-getmore" #*"\x28\0\0\0\0\0\0\0\xff\xff\xff\xff\xd5\x07\0\0\0\0\0\0foo.bar\0\0\0\0\0\xd2\x02\x96\x49\0\0\0\0"
           (call-with-output-string m-write))))

;;;; delete

(test-section "delete")

(let1 m (mongo-message-delete "foo.bar" '(("key" . "value")) :single-remove #t)
  (receive (m-size m-write) (write-mongo-message-delete-prepare m)
    (test* "mongo-message-delete" <mongo-message-delete>
           (class-of m))
    (test* "mongo-message?" #t
           (mongo-message? m))
    (test* "mongo-message-delete?" #t
           (mongo-message-delete? m))
    (test* "mongo-message-op-code" 2006
           (mongo-message-op-code m))
    (test* "mongo-message-delete size" 52
           m-size)
    (test* "write-mongo-message-delete" #*"\x34\0\0\0\0\0\0\0\xff\xff\xff\xff\xd6\x07\0\0\0\0\0\0foo.bar\0\x01\0\0\0\x14\0\0\0\x02key\0\x06\0\0\0value\0\0"
           (call-with-output-string m-write))))

;;;; kill-cursors

(test-section "kill-cursors")

(let1 m (mongo-message-kill-cursors '(1235467890))
  (receive (m-size m-write) (write-mongo-message-kill-cursors-prepare m)
    (test* "mongo-message-kill-cursors" <mongo-message-kill-cursors>
           (class-of m))
    (test* "mongo-message?" #t
           (mongo-message? m))
    (test* "mongo-message-kill-cursors?" #t
           (mongo-message-kill-cursors? m))
    (test* "mongo-message-op-code" 2007
           (mongo-message-op-code m))
    (test* "mongo-message-kill-cursors size" 32
           m-size)
    (test* "write-mongo-message-kill-cursors" #*"\x20\0\0\0\0\0\0\0\xff\xff\xff\xff\xd7\x07\0\0\0\0\0\0\x01\0\0\0\x72\xbe\xa3\x49\0\0\0\0"
           (call-with-output-string m-write))))

;;;; msg

(test-section "msg")

(let1 m (mongo-message-msg "foo")
  (receive (m-size m-write) (write-mongo-message-msg-prepare m)
    (test* "mongo-message-msg" <mongo-message-msg>
           (class-of m))
    (test* "mongo-message?" #t
           (mongo-message? m))
    (test* "mongo-message-msg?" #t
           (mongo-message-msg? m))
    (test* "mongo-message-op-code" 1000
           (mongo-message-op-code m))
    (test* "mongo-message-msg size" 20
           m-size)
    (test* "write-mongo-message-msg" #*"\x14\0\0\0\0\0\0\0\xff\xff\xff\xff\xe8\x03\0\0foo\0"
           (call-with-output-string m-write))))

;;;; reply

(test-section "reply")

(let1 reply (mongo-message-request
             socket
             (mongo-message-query "admin.$cmd"
                                  '(("ping" . 1))
                                  :number-to-return 1))
  (test* "mongo-message-read-reply" <mongo-message-reply>
         (class-of reply))
  (test* "mongo-message?" #t
         (mongo-message? reply))
  (test* "mongo-message-reply?" #t
         (mongo-message-reply? reply))
  (test* "mongo-message-op-code" 1
         (mongo-message-op-code reply))
  (test* "mongo-message-response-to" 0
         (mongo-message-response-to reply))
  (test* "mongo-message-reply-response-flags" 8
         (mongo-message-reply-response-flags reply))
  (test* "mongo-message-reply-cursor-id" 0
         (mongo-message-reply-cursor-id reply))
  (test* "mongo-message-reply-starting-from" 0
         (mongo-message-reply-starting-from reply))
  (test* "mongo-message-reply-documents" #t
         (every mongo-ok? (mongo-message-reply-documents reply)))
  (test* "mongo-message-reply-document" #t
         (mongo-ok? (mongo-message-reply-document reply))))

(test-end)
