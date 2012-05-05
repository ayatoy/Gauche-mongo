(define-module mongo.node
  (use gauche.collection)
  (use gauche.net)
  (use gauche.record)
  (use gauche.threads)
  (use gauche.time)
  (use util.list)
  (use mongo.util)
  (use mongo.bson)
  (use mongo.wire)
  (export <mongo-request-error>
          mongo-request-error?
          <mongo-node>
          mongo-node
          mongo-node?
          mongo-node-address
          mongo-node-socket
          mongo-node-authed
          mongo-node-locking
          mongo-node-connect
          mongo-node-connect*
          mongo-node-connect?
          mongo-node-disconnect!
          mongo-node-available!
          mongo-node-request
          mongo-node-authed-put!
          mongo-node-authed-delete!
          mongo-node-find1
          mongo-node-find
          mongo-node-insert
          mongo-node-update
          mongo-node-delete
          mongo-node-command
          mongo-node-admin
          mongo-node-ping
          mongo-node-round-trip
          mongo-node-ismaster
          mongo-node-server-status
          mongo-node-replset-status
          mongo-node-show-databases
          mongo-node-drop-database
          mongo-node-create-collection
          mongo-node-show-collections
          mongo-node-drop-collection
          mongo-node-get-last-error
          mongo-node-reset-error
          mongo-node-profiling-status
          mongo-node-get-profiling-level
          mongo-node-set-profiling-level
          mongo-node-show-profiling
          mongo-node-ensure-index
          mongo-node-drop-index
          mongo-node-drop-indexes
          mongo-node-show-indexes
          mongo-node-reindex
          mongo-node-add-user
          mongo-node-remove-user
          mongo-node-auth
          mongo-node-auth-by-table
          mongo-node-reauth
          mongo-node-count
          mongo-node-distinct
          mongo-node-dbref-get
          mongo-node-map-reduce
          <mongo-cursor>
          mongo-cursor?
          mongo-cursor-locking
          mongo-cursor-exists?
          mongo-cursor-more!
          mongo-cursor-next!
          mongo-cursor-peek!
          mongo-cursor-take!
          mongo-cursor-all!
          mongo-cursor-kill))
(select-module mongo.node)

;;;; condition

(define-condition-type <mongo-request-error> <mongo-error>
  mongo-request-error?)

;;;; node

(define-record-type <mongo-node> make-mongo-node mongo-node?
  (address mongo-node-address)
  (socket  mongo-node-socket mongo-node-socket-set!)
  (authed  mongo-node-authed)
  (counter mongo-node-counter)
  (mutex   mongo-node-mutex))

(define-method write-object ((node <mongo-node>) oport)
  (format oport "#<mongo-node ~s ~s>"
          (socket-status (mongo-node-socket node))
          (mongo-address->string (mongo-node-address node))))

(define-syntax mongo-node-locking
  (syntax-rules ()
    [(_ node body ...)
     (with-locking-mutex-recursively (mongo-node-mutex node)
       (^[] body ...))]))

(define (mongo-node-connect address)
  (make-mongo-node address
                   (mongo-socket-connect address)
                   (make-hash-table 'equal?)
                   (make-counter (expt 2 32))
                   (make-mutex)))

(define (mongo-node-connect* address)
  (guard (e [(<mongo-connect-error> e) #f])
    (mongo-node-connect address)))

(define (mongo-node str)
  (mongo-node-connect (string->mongo-address str)))

(define (mongo-node-connect? node)
  (mongo-node-locking node
    (mongo-socket-connect? (mongo-node-socket node))))

(define (mongo-node-disconnect! node)
  (mongo-node-locking node
    (socket-close (mongo-node-socket node))))

(define (mongo-node-sync! node)
  (mongo-node-locking node
    (mongo-node-disconnect! node)
    (let1 socket (mongo-socket-connect (mongo-node-address node))
      (mongo-node-socket-set! node socket)
      ((mongo-node-counter node) -1)
      (mongo-node-reauth node))))

(define (mongo-node-available! node)
  (mongo-node-locking node
    (unless (mongo-node-connect? node)
      (mongo-node-sync! node))))

(define (mongo-node-request node message)
  (mongo-node-locking node
    (guard (e [(<mongo-wire-error> e)
               (mongo-node-disconnect! node)
               (raise e)])
      (mongo-message-request (mongo-node-socket node) message))))

(define (mongo-query-failure reply)
  (if (mongo-message-reply-query-failure? reply)
    (error <mongo-request-error> :reason reply
           (and-let* ([doc (mongo-message-reply-document reply)])
             (assoc-ref doc "$err")))
    reply))

(define (mongo-node-authed-put! node dn user pass)
  (hash-table-put! (mongo-node-authed node) (vector dn user) pass))

(define (mongo-node-authed-delete! node dn user)
  (hash-table-delete! (mongo-node-authed node) (vector dn user)))

;;;; CRUD

(define (mongo-node-find1 node dn cn query :key (number-to-skip 0)
                                                (return-field-selector #f)
                                                (slave-ok #f))
  (let1 message (mongo-message-query
                 (mongo-ns-compose dn cn)
                 query
                 :request-id ((mongo-node-counter node))
                 :number-to-skip number-to-skip
                 :number-to-return 1
                 :return-field-selector return-field-selector
                 :slave-ok slave-ok)
    (mongo-message-reply-document
     (mongo-query-failure (mongo-node-request node message)))))

(define (mongo-node-find node dn cn query :key (number-to-skip 0)
                                               (number-to-return 0)
                                               (return-field-selector #f)
                                               (tailable-cursor #f)
                                               (slave-ok #f)
                                               (oplog-replay #f)
                                               (no-cursor-timeout #f)
                                               (await-data #f)
                                               (exhaust #f)
                                               (partial #f))
  (let* ([message (mongo-message-query
                   (mongo-ns-compose dn cn)
                   query
                   :request-id ((mongo-node-counter node))
                   :number-to-skip number-to-skip
                   :number-to-return number-to-return
                   :return-field-selector return-field-selector
                   :tailable-cursor tailable-cursor
                   :slave-ok slave-ok
                   :oplog-replay oplog-replay
                   :no-cursor-timeout no-cursor-timeout
                   :await-data await-data
                   :exhaust exhaust
                   :partial partial)]
         [reply (mongo-query-failure (mongo-node-request node message))])
    (make-mongo-cursor node
                       message
                       reply
                       (mongo-message-reply-documents reply)
                       (make-mutex))))

(define (mongo-node-insert node dn cn docs :key (continue-on-error #f)
                                                (safe #f)
                                                fsync
                                                j
                                                w
                                                wtimeout)
  (let1 message (mongo-message-insert
                 (mongo-ns-compose dn cn)
                 docs
                 :request-id ((mongo-node-counter node))
                 :continue-on-error continue-on-error)
    (mongo-node-locking node
      (mongo-node-request node message)
      (when safe
        (mongo-node-get-last-error node
                                   dn
                                   :fsync fsync
                                   :j j
                                   :w w
                                   :wtimeout wtimeout)))))

(define (mongo-node-update node dn cn select update :key (upsert #f)
                                                         (multi-update #f)
                                                         (safe #f)
                                                         fsync
                                                         j
                                                         w
                                                         wtimeout)
  (let1 message (mongo-message-update
                 (mongo-ns-compose dn cn)
                 select
                 update
                 :request-id ((mongo-node-counter node))
                 :upsert upsert
                 :multi-update multi-update)
    (mongo-node-locking node
      (mongo-node-request node message)
      (when safe
        (mongo-node-get-last-error node
                                   dn
                                   :fsync fsync
                                   :j j
                                   :w w
                                   :wtimeout wtimeout)))))

(define (mongo-node-delete node dn cn select :key (single-remove #f)
                                                  (safe #f)
                                                  fsync
                                                  j
                                                  w
                                                  wtimeout)
  (let1 message (mongo-message-delete
                 (mongo-ns-compose dn cn)
                 select
                 :request-id ((mongo-node-counter node))
                 :single-remove single-remove)
    (mongo-node-locking node
      (mongo-node-request node message)
      (when safe
        (mongo-node-get-last-error node
                                   dn
                                   :fsync fsync
                                   :j j
                                   :w w
                                   :wtimeout wtimeout)))))

;;;; helper

(define (mongo-node-command node dn query)
  (rlet1 doc (mongo-node-find1 node dn "$cmd" query)
    (when (zero? (assoc-ref doc "ok"))
      (error <mongo-request-error> :reason doc (assoc-ref doc "errmsg")))))

(define (mongo-node-admin node query)
  (mongo-node-command node "admin" query))

(define (mongo-node-ping node)
  (mongo-node-admin node '(("ping" . 1))))

(define (mongo-node-round-trip node :optional (count 1))
  (let1 tr (time-this count (^[] (mongo-node-ping node)))
    (- (~ tr'real) (~ tr'user) (~ tr'sys))))

(define (mongo-node-ismaster node)
  (mongo-node-admin node '(("ismaster" . 1))))

(define (mongo-node-server-status node)
  (mongo-node-admin node '(("serverStatus" . 1))))

(define (mongo-node-replset-status node)
  (mongo-node-admin node '(("replSetGetStatus" . 1))))

(define (mongo-node-show-databases node)
  (mongo-node-admin node '(("listDatabases" . 1))))

(define (mongo-node-drop-database node dn)
  (mongo-node-command node dn '(("dropDatabase" . 1))))

(define (mongo-node-create-collection node dn cn :key capped
                                                      size
                                                      max)
  (mongo-node-command node
                      dn
                      `(("create" . ,cn)
                        ,@(bson-document-part "capped" capped)
                        ,@(bson-document-part "size" size)
                        ,@(bson-document-part "max" max))))

(define (mongo-node-show-collections node dn)
  (mongo-cursor-all! (mongo-node-find node dn "system.namespaces" '())))

(define (mongo-node-drop-collection node dn cn)
  (mongo-node-command node dn `(("drop" . ,cn))))

(define (mongo-node-get-last-error node dn :key fsync
                                                j
                                                w
                                                wtimeout)
  (rlet1 doc (mongo-node-command
              node
              dn
              `(("getLastError" . 1)
                ,@(bson-document-part "fsync" fsync)
                ,@(bson-document-part "j" j)
                ,@(bson-document-part "w" w)
                ,@(bson-document-part "wtimeout" wtimeout)))
    (if-let1 err (assoc-ref doc "err")
      (when (not (eq? 'null err))
        (error <mongo-request-error> :reason doc err)))))

(define (mongo-node-reset-error node dn)
  (mongo-node-command node dn '(("reseterror" . 1))))

;;;; index

(define (mongo-node-ensure-index node dn cn name spec :key unique
                                                           drop-dups
                                                           background
                                                           sparse
                                                           (safe #f)
                                                           fsync
                                                           j
                                                           w
                                                           wtimeout)
  (mongo-node-insert node
                     dn
                     "system.indexes"
                     `((("ns" . ,(mongo-ns-compose dn cn))
                        ("key" . ,spec)
                        ("name" . ,name)
                        ,@(bson-document-part "unique" unique)
                        ,@(bson-document-part "dropDups" drop-dups)
                        ,@(bson-document-part "background" background)
                        ,@(bson-document-part "sparse" sparse)))
                     :safe safe
                     :fsync fsync
                     :j j
                     :w w
                     :wtimeout wtimeout))

(define (mongo-node-show-indexes node dn cn)
  (mongo-cursor-all!
   (mongo-node-find node
                    dn
                    "system.indexes"
                    `(("ns" . ,(mongo-ns-compose dn cn))))))

(define (mongo-node-drop-index node dn cn name)
  (mongo-node-command node dn `(("dropIndexes" . ,cn) ("index" . ,name))))

(define (mongo-node-drop-indexes node dn cn)
  (mongo-node-command node dn `(("dropIndexes" . ,cn) ("index" . "*"))))

(define (mongo-node-reindex node dn cn)
  (mongo-node-command node dn `(("reIndex" . ,cn))))

;;;; profiling

(define (mongo-node-profiling-status node dn)
  (mongo-node-command node dn '(("profile" . -1))))

(define (mongo-node-get-profiling-level node dn)
  (assoc-ref (mongo-node-profiling-status node dn) "was"))

(define (mongo-node-set-profiling-level node dn level :key slowms)
  (mongo-node-command node
                      dn
                      `(("profile" . ,level)
                        ,@(bson-document-part "slowms" slowms))))

(define (mongo-node-show-profiling node dn)
  (mongo-cursor-all! (mongo-node-find node dn "system.profile" '())))

;;;; auth

(define (mongo-node-add-user node dn user pass :key read-only
                                                    (safe #f)
                                                    fsync
                                                    j
                                                    w
                                                    wtimeout)
  (mongo-node-update node
                     dn
                     "system.users"
                     `(("user" . ,user))
                     `(("$set" . (("pwd" . ,(mongo-user-digest-hexify user
                                                                      pass))
                                  ,@(bson-document-part "readOnly" read-only))))
                     :upsert #t
                     :safe safe
                     :fsync fsync
                     :j j
                     :w w
                     :wtimeout wtimeout))

(define (mongo-node-remove-user node dn user :key (safe #f)
                                                  fsync
                                                  j
                                                  w
                                                  wtimeout)
  (mongo-node-delete node
                     dn
                     "system.users"
                     `(("user" . ,user))
                     :safe safe
                     :fsync fsync
                     :j j
                     :w w
                     :wtimeout wtimeout))

(define (mongo-node-auth node dn user pass)
  (let1 nonce (alref (mongo-node-command node dn '(("getnonce" . 1))) "nonce")
    (begin0 (mongo-node-command
             node
             dn
             `(("authenticate" . 1)
               ("user"  . ,user)
               ("nonce" . ,nonce)
               ("key"   . ,(mongo-auth-digest-hexify user pass nonce))))
            (mongo-node-authed-put! node dn user pass))))

(define (mongo-node-auth-by-table node table :key (error #f)
                                                  (delete #t)
                                                  (ignore #f))
  (let1 authed (mongo-node-authed node)
    (hash-table-for-each
     table
     (^[vec pass]
       (guard (e [(<mongo-request-error> e)
                  (when delete (hash-table-delete! authed vec))
                  (when error (raise e))])
         (unless (and (hash-table-exists? authed vec) ignore)
           (mongo-node-auth node
                            (vector-ref vec 0)
                            (vector-ref vec 1)
                            pass)))))))

(define (mongo-node-reauth node)
  (mongo-node-auth-by-table node
                            (mongo-node-authed node)
                            :error #f
                            :delete #t
                            :ignore #f))

;;;; aggregation

(define (mongo-node-count node dn cn :key query fields limit skip)
  (mongo-node-command node
                      dn
                      `(("count" . ,cn)
                        ,@(bson-document-part "query" query)
                        ,@(bson-document-part "fields" fields)
                        ,@(bson-document-part "limit" limit)
                        ,@(bson-document-part "skip" skip))))

(define (mongo-node-distinct node dn cn key :key query)
  (mongo-node-command node
                      dn
                      `(("distinct" . ,cn)
                        ("key" . ,key)
                        ,@(bson-document-part "query" query))))

;;;; dbref

(define (mongo-node-dbref-get node dn ref :key (slave-ok #f))
  (mongo-node-find1 node
                    (or (assoc-ref ref "$db") dn)
                    (assoc-ref ref "$ref")
                    `(("_id" . ,(assoc-ref ref "$id")))
                    :slave-ok slave-ok))

;;;; map-reduce

(define (mongo-node-map-reduce node dn cn map reduce :key query
                                                          sort
                                                          limit
                                                          out
                                                          keeptemp
                                                          finalize
                                                          scope
                                                          js-mode
                                                          verbose)
  (mongo-node-command node
                      dn
                      `(("mapreduce" . ,cn)
                        ("map" . ,map)
                        ("reduce" . ,reduce)
                        ,@(bson-document-part "query" query)
                        ,@(bson-document-part "sort" sort)
                        ,@(bson-document-part "limit" limit)
                        ,@(bson-document-part "out" out)
                        ,@(bson-document-part "keeptemp" keeptemp)
                        ,@(bson-document-part "finalize" finalize)
                        ,@(bson-document-part "scope" scope)
                        ,@(bson-document-part "jsMode" js-mode)
                        ,@(bson-document-part "verbose" verbose))))

;;;; cursor

(define-record-type <mongo-cursor> make-mongo-cursor mongo-cursor?
  (node   mongo-cursor-node)
  (query  mongo-cursor-query)
  (reply  mongo-cursor-reply  mongo-cursor-reply-set!)
  (buffer mongo-cursor-buffer mongo-cursor-buffer-set!)
  (mutex  mongo-cursor-mutex))

(define-syntax mongo-cursor-locking
  (syntax-rules ()
    [(_ cursor body ...)
     (with-locking-mutex-recursively (mongo-cursor-mutex cursor)
       (^[] body ...))]))

(define (mongo-cursor-exists? cursor)
  (mongo-cursor-locking cursor
    (let1 reply (mongo-cursor-reply cursor)
      (not (or (= (mongo-message-reply-cursor-id reply) 0)
               (mongo-message-reply-cursor-not-found? reply))))))

(define (mongo-cursor-more! cursor)
  (mongo-cursor-locking cursor
    (and (mongo-cursor-exists? cursor)
         (let* ([node  (mongo-cursor-node cursor)]
                [reply (mongo-cursor-reply cursor)]
                [query (mongo-cursor-query cursor)]
                [ms (mongo-message-getmore
                     (mongo-message-query-full-collection-name query)
                     (mongo-message-reply-cursor-id reply)
                     :request-id ((mongo-node-counter node))
                     :number-to-return (mongo-message-query-number-to-return
                                        query))]
                [rep (mongo-query-failure (mongo-node-request node ms))]
                [docs (mongo-message-reply-documents rep)])
           (mongo-cursor-reply-set! cursor rep)
           (and (not (null? docs))
                (begin (mongo-cursor-buffer-set!
                        cursor (append (mongo-cursor-buffer cursor) docs))
                       #t))))))

(define (mongo-cursor-next! cursor)
  (mongo-cursor-locking cursor
    (let1 buffer (mongo-cursor-buffer cursor)
      (if (null? buffer)
        (and (mongo-cursor-more! cursor)
             (mongo-cursor-next! cursor))
        (begin (mongo-cursor-buffer-set! cursor (cdr buffer))
               (car buffer))))))

(define (mongo-cursor-peek! cursor)
  (mongo-cursor-locking cursor
    (let1 buffer (mongo-cursor-buffer cursor)
      (if (null? buffer)
        (and (mongo-cursor-more! cursor)
             (mongo-cursor-peek! cursor))
        (car buffer)))))

(define (mongo-cursor-take! cursor num)
  (mongo-cursor-locking cursor
    (let loop ([acc '()] [i 0])
      (if (> num i)
        (if-let1 doc (mongo-cursor-next! cursor)
          (loop (cons doc acc) (+ i 1))
          (reverse acc))
        (reverse acc)))))

(define (mongo-cursor-all! cursor)
  (mongo-cursor-locking cursor
    (let loop ([acc '()])
      (if-let1 doc (mongo-cursor-next! cursor)
        (loop (cons doc acc))
        (reverse acc)))))

(define (mongo-cursor-kill . cursors)
  (hash-table-for-each
   (rlet1 ht (make-hash-table 'eq?)
     (for-each (^[cursor]
                 (when (mongo-cursor-exists? cursor)
                   (hash-table-push! ht
                                     (mongo-cursor-node cursor)
                                     (mongo-message-reply-cursor-id
                                      (mongo-cursor-reply cursor)))))
               cursors))
   (^[node ids]
     (mongo-node-request node
                         (mongo-message-kill-cursors
                          ids
                          :request-id ((mongo-node-counter node)))))))
