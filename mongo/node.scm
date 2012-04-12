(define-module mongo.node
  (use gauche.collection)
  (use gauche.net)
  (use gauche.record)
  (use gauche.threads)
  (use gauche.time)
  (use rfc.md5)
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
          mongo-node-connect?
          mongo-node-disconnect!
          mongo-node-available!
          mongo-node-request
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
          mongo-node-auth-by-digest
          mongo-node-auth
          mongo-node-auth-put-by-digest!
          mongo-node-auth-put!
          mongo-node-auth-delete!
          mongo-node-reauth
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
          mongo-cursor-count
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
    (let1 socket (mongo-socket-connect (mongo-node-address node))
      (mongo-node-disconnect! node)
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
      (mongo-message-request (mongo-node-socket node)
                             message))))

(define (mongo-query-failure reply)
  (if (mongo-message-reply-query-failure? reply)
    (error <mongo-request-error> :reason #f
           (and-let* ([doc (mongo-message-reply-document reply)])
             (assoc-ref doc "$err")))
    reply))

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
                                                (fsync #f)
                                                (j #f)
                                                (w #f)
                                                (wtimeout #f))
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
                                                         (fsync #f)
                                                         (j #f)
                                                         (w #f)
                                                         (wtimeout #f))
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
                                                  (fsync #f)
                                                  (j #f)
                                                  (w #f)
                                                  (wtimeout #f))
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
      (error <mongo-request-error> :reason #f (assoc-ref doc "errmsg")))))

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

(define (mongo-node-create-collection node dn cn :key (capped #f)
                                                      (size #f)
                                                      (max #f))
  (mongo-node-command node
                      dn
                      `(("create" . ,cn)
                        ("capped" . ,(bson-boolean capped))
                        ("size" . ,(or size 'undefined))
                        ("max" . ,(or max 'undefined)))))

(define (mongo-node-show-collections node dn)
  (mongo-cursor-all! (mongo-node-find node dn "system.namespaces" '())))

(define (mongo-node-drop-collection node dn cn)
  (mongo-node-command node dn `(("drop" . ,cn))))

(define (mongo-node-get-last-error node dn :key (fsync #f)
                                                (j #f)
                                                (w #f)
                                                (wtimeout #f))
  (rlet1 doc (mongo-node-command
              node
              dn
              `(("getLastError" . 1)
                ,@(if fsync '(("fsync" . true)) '())
                ,@(if j '(("j" . true)) '())
                ,@(if w `(("w" . ,w)) '())
                ,@(if wtimeout `(("wtimeout" . ,wtimeout)) '())))
    (if-let1 err (assoc-ref doc "err")
      (when (not (eq? 'null err))
        (error <mongo-request-error> :reason #f err)))))

(define (mongo-node-reset-error node dn)
  (mongo-node-command node dn '(("reseterror" . 1))))

;;;; index

(define (mongo-node-ensure-index node dn cn name spec :key (unique #f)
                                                           (drop-dups #f)
                                                           (background #f)
                                                           (sparse #f)
                                                           (safe #f)
                                                           (fsync #f)
                                                           (j #f)
                                                           (w #f)
                                                           (wtimeout #f))
  (mongo-node-insert node
                     dn
                     "system.indexes"
                     `((("ns" . ,(mongo-ns-compose dn cn))
                        ("key" . ,spec)
                        ("name" . ,name)
                        ("unique" . ,(bson-boolean unique))
                        ("dropDups" . ,(bson-boolean drop-dups))
                        ("background" . ,(bson-boolean background))
                        ("sparse" . ,(bson-boolean sparse))))
                     :safe #t
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

(define (mongo-node-set-profiling-level node dn level :key (slowms #f))
  (mongo-node-command node
                      dn
                      (if slowms
                        `(("profile" . ,level) ("slowms" . ,slowms))
                        `(("profile" . ,level)))))

(define (mongo-node-show-profiling node dn)
  (mongo-cursor-all! (mongo-node-find node dn "system.profile" '())))

;;;; auth

(define (mongo-digest-hexify user pass)
  (digest-hexify (md5-digest-string (format "~a:mongo:~a" user pass))))

(define (mongo-node-auth-put-by-digest! node dn user digest)
  (hash-table-put! (mongo-node-authed node) (vector dn user) digest))

(define (mongo-node-auth-put! node dn user pass)
  (mongo-node-auth-put-by-digest! node dn user (mongo-digest-hexify user pass)))

(define (mongo-node-auth-delete! node dn user)
  (hash-table-delete! (mongo-node-authed node) (vector dn user)))

(define (mongo-node-add-user node dn user pass :key (read-only #f)
                                                    (safe #f)
                                                    (fsync #f)
                                                    (j #f)
                                                    (w #f)
                                                    (wtimeout #f))
  (mongo-node-update node
                     dn
                     "system.users"
                     `(("user" . ,user))
                     `(("$set" . (("readOnly" . ,(bson-boolean read-only))
                                  ("pwd" . ,(mongo-digest-hexify user pass)))))
                     :upsert #t
                     :safe safe
                     :fsync fsync
                     :j j
                     :w w
                     :wtimeout wtimeout))

(define (mongo-node-remove-user node dn user :key (safe #f)
                                                  (fsync #f)
                                                  (j #f)
                                                  (w #f)
                                                  (wtimeout #f))
  (begin0 (mongo-node-delete node
                             dn
                             "system.users"
                             `(("user" . ,user))
                             :safe safe
                             :fsync fsync
                             :j j
                             :w w
                             :wtimeout wtimeout)
          (hash-table-delete! (mongo-node-authed node)
                              (vector dn user))))

(define (mongo-node-auth-by-digest node dn user digest)
  (let1 nonce (assoc-ref (mongo-node-command node dn '(("getnonce" . 1)))
                         "nonce")
    (begin0 (mongo-node-command node
                                dn
                                `(("authenticate" . 1)
                                  ("user" . ,user)
                                  ("nonce" . ,nonce)
                                  ("key" . ,(digest-hexify
                                             (md5-digest-string
                                              (string-append nonce
                                                             user
                                                             digest))))))
            (mongo-node-auth-put-by-digest! node dn user digest))))

(define (mongo-node-auth node dn user pass)
  (mongo-node-auth-by-digest node dn user (mongo-digest-hexify user pass)))

(define (mongo-node-reauth node)
  (hash-table-map (mongo-node-authed node)
                  (^[vec digest]
                    (let1 user (vector-ref vec 1)
                      (cons user
                            (mongo-node-auth-by-digest node
                                                       (vector-ref vec 0)
                                                       user
                                                       digest))))))

;;;; aggregation

(define (mongo-node-distinct node dn cn key :optional (query #f))
  (mongo-node-command node
                      dn
                      `(("distinct" . ,cn)
                        ("key" . ,key)
                        ,@(if query `(("query" . ,query)) '()))))

;;;; dbref

(define (mongo-node-dbref-get node dn ref :key (slave-ok #f))
  (mongo-node-find1 node
                    (or (assoc-ref ref "$db") dn)
                    (assoc-ref ref "$ref")
                    `(("_id" . ,(assoc-ref ref "$id")))
                    :slave-ok slave-ok))

;;;; map-reduce

(define (mongo-node-map-reduce node dn cn map reduce :key (query #f)
                                                          (sort #f)
                                                          (limit #f)
                                                          (out #f)
                                                          (keeptemp #f)
                                                          (finalize #f)
                                                          (scope #f)
                                                          (js-mode #f)
                                                          (verbose #f))
  (mongo-node-command node
                      dn
                      `(("mapreduce" . ,cn)
                        ("map" . ,map)
                        ("reduce" . ,reduce)
                        ,@(if query `(("query" . ,query)) '())
                        ,@(if sort `(("sort" . ,sort)) '())
                        ,@(if limit `(("limit" . ,limit)) '())
                        ,@(if out `(("out" . ,out)) '())
                        ,@(if keeptemp `(("keeptemp" . ,keeptemp)) '())
                        ,@(if finalize `(("finalize" . ,finalize)) '())
                        ,@(if scope `(("scope" . ,scope)) '())
                        ,@(if js-mode `(("jsMode" . ,js-mode)) '())
                        ,@(if verbose `(("verbose" . ,verbose)) '()))))

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

(define (mongo-cursor-count cursor)
  (let* ([q  (mongo-cursor-query cursor)]
         [ns (mongo-ns-parse (mongo-message-query-full-collection-name q))])
    (floor->exact
     (assoc-ref
      (mongo-node-command
       (mongo-cursor-node cursor)
       (list-ref ns 0)
       `(("count" . ,(list-ref ns 1))
         ("query" . ,(mongo-message-query-query q))
         ("fields" . ,(or (mongo-message-query-return-field-selector q)
                          '()))))
      "n"))))

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
