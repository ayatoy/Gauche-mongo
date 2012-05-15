(define-module mongo.node
  (use gauche.collection)
  (use gauche.generator)
  (use gauche.net)
  (use gauche.record)
  (use gauche.threads)
  (use gauche.time)
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
          mongo-node-generate-id!
          mongo-node-reset-id!
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
          mongo-node-round-trip
          mongo-node-ismaster
          mongo-node-get-last-error
          mongo-node-reset-error
          mongo-node-auth
          mongo-node-auth-by-table
          mongo-node-reauth
          <mongo-cursor>
          mongo-cursor
          mongo-cursor?
          mongo-cursor-node
          mongo-cursor-query
          mongo-cursor-reply
          mongo-cursor-buffer
          mongo-cursor-locking
          mongo-cursor-initial?
          mongo-cursor-exists?
          mongo-cursor-rewind!
          mongo-cursor-more!
          mongo-cursor-next!
          mongo-cursor-peek!
          mongo-cursor-take!
          mongo-cursor-all!
          mongo-cursor-kill
          mongo-cursor->generator))

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

(define (mongo-node-generate-id! node)
  ((mongo-node-counter node)))

(define (mongo-node-reset-id! node)
  ((mongo-node-counter node) -1))

(define (mongo-node-sync! node)
  (mongo-node-locking node
    (mongo-node-disconnect! node)
    (let1 socket (mongo-socket-connect (mongo-node-address node))
      (mongo-node-socket-set! node socket)
      (mongo-node-reset-id! node)
      (mongo-node-reauth node))))

(define (mongo-node-available! node)
  (mongo-node-locking node
    (unless (mongo-node-connect? node)
      (mongo-node-sync! node))))

(define (mongo-node-request node message)
  (mongo-node-locking node
    (guard (e [(<mongo-wire-error> e) (mongo-node-disconnect! node) (raise e)])
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

;;;; basic

(define (mongo-node-find1 node dn cn query . opts)
  (mongo-message-reply-document
   (mongo-query-failure
    (mongo-node-request node (apply mongo-message-query
                                    (mongo-ns-compose dn cn)
                                    query
                                    :request-id (mongo-node-generate-id! node)
                                    :number-to-return 1
                                    opts)))))

(define (mongo-node-find node dn cn query . opts)
  (mongo-cursor node (apply mongo-message-query
                            (mongo-ns-compose dn cn)
                            query
                            :request-id (mongo-node-generate-id! node)
                            opts)))

(define (mongo-node-insert node dn cn docs :key (continue-on-error #f)
                                                (safe #f)
                                                (fsync (undefined))
                                                (j (undefined))
                                                (w (undefined))
                                                (wtimeout (undefined)))
  (let1 message (mongo-message-insert
                 (mongo-ns-compose dn cn)
                 docs
                 :request-id (mongo-node-generate-id! node)
                 :continue-on-error continue-on-error)
    (mongo-node-locking node
      (mongo-node-request node message)
      (when safe
        (mongo-node-get-last-error node
                                   dn
                                   :fsync fsync
                                   :j j
                                   :w w
                                   :wtimeout wtimeout
                                   :thrown #t)))))

(define (mongo-node-update node dn cn select update :key (upsert #f)
                                                         (multi-update #f)
                                                         (safe #f)
                                                         (fsync (undefined))
                                                         (j (undefined))
                                                         (w (undefined))
                                                         (wtimeout (undefined)))
  (let1 message (mongo-message-update
                 (mongo-ns-compose dn cn)
                 select
                 update
                 :request-id (mongo-node-generate-id! node)
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
                                   :wtimeout wtimeout
                                   :thrown #t)))))

(define (mongo-node-delete node dn cn select :key (single-remove #f)
                                                  (safe #f)
                                                  (fsync (undefined))
                                                  (j (undefined))
                                                  (w (undefined))
                                                  (wtimeout (undefined)))
  (let1 message (mongo-message-delete
                 (mongo-ns-compose dn cn)
                 select
                 :request-id (mongo-node-generate-id! node)
                 :single-remove single-remove)
    (mongo-node-locking node
      (mongo-node-request node message)
      (when safe
        (mongo-node-get-last-error node
                                   dn
                                   :fsync fsync
                                   :j j
                                   :w w
                                   :wtimeout wtimeout
                                   :thrown #t)))))

(define (mongo-node-command node dn query)
  (rlet1 doc (mongo-node-find1 node dn "$cmd" query)
    (unless (mongo-ok? doc)
      (error <mongo-request-error> :reason doc (assoc-ref doc "errmsg")))))

(define (mongo-node-admin node query)
  (mongo-node-command node "admin" query))

(define (mongo-node-round-trip node)
  (let1 tr (time-this 1 (^[] (mongo-node-admin node '(("ping" . 1)))))
    (- (~ tr'real) (~ tr'user) (~ tr'sys))))

(define (mongo-node-ismaster node)
  (mongo-node-admin node '(("ismaster" . 1))))

(define (mongo-node-get-last-error node dn :key (fsync (undefined))
                                                (j (undefined))
                                                (w (undefined))
                                                (wtimeout (undefined))
                                                (thrown #f))
  (rlet1 doc (mongo-node-command
              node
              dn
              `(("getLastError" . 1)
                ,@(bson-part "fsync" fsync)
                ,@(bson-part "j" j)
                ,@(bson-part "w" w)
                ,@(bson-part "wtimeout" wtimeout)))
    (when thrown
      (if-let1 err (assoc-ref doc "err")
        (when (not (eq? 'null err))
          (error <mongo-request-error> :reason doc err))))))

(define (mongo-node-reset-error node dn)
  (mongo-node-command node dn '(("resetError" . 1))))

(define (mongo-node-auth node dn user pass)
  (let1 nonce (assoc-ref (mongo-node-command node dn '(("getnonce" . 1)))
                         "nonce")
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
  (mongo-node-auth-by-table node (mongo-node-authed node)))

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

(define (mongo-cursor node query)
  (make-mongo-cursor node query #f '() (make-mutex)))

(define (mongo-cursor-initial? cursor)
  (mongo-cursor-locking cursor
    (and (not (mongo-cursor-reply cursor))
         (null? (mongo-cursor-buffer cursor)))))

(define (mongo-cursor-exists? cursor)
  (mongo-cursor-locking cursor
    (and-let* ([reply (mongo-cursor-reply cursor)])
      (and (not (= (mongo-message-reply-cursor-id reply) 0))
           (not (mongo-message-reply-cursor-not-found? reply))))))

(define (mongo-cursor-rewind! cursor)
  (mongo-cursor-locking cursor
    (and (not (mongo-cursor-initial? cursor))
         (begin (mongo-cursor-kill cursor)
                (mongo-message-request-id-set!
                 (mongo-cursor-query cursor)
                 (mongo-node-generate-id! (mongo-cursor-node cursor)))
                (mongo-cursor-reply-set! cursor #f)
                (mongo-cursor-buffer-set! cursor '())
                #t))))

(define (mongo-cursor-more! cursor)
  (mongo-cursor-locking cursor
    (if-let1 reply (mongo-cursor-reply cursor)
      (and (not (= 0 (mongo-message-reply-cursor-id reply)))
           (not (mongo-message-reply-cursor-not-found? reply))
           (let* ([node  (mongo-cursor-node cursor)]
                  [query (mongo-cursor-query cursor)]
                  [ms (mongo-message-getmore
                       (mongo-message-query-full-collection-name query)
                       (mongo-message-reply-cursor-id reply)
                       :request-id (mongo-node-generate-id! node)
                       :number-to-return (mongo-message-query-number-to-return
                                          query))]
                  [rep  (mongo-query-failure (mongo-node-request node ms))]
                  [docs (mongo-message-reply-documents rep)])
             (mongo-cursor-reply-set! cursor rep)
             (and (not (null? docs))
                  (begin (mongo-cursor-buffer-set!
                          cursor
                          (append (mongo-cursor-buffer cursor) docs))
                         #t))))
      (let1 rep (mongo-query-failure
                 (mongo-node-request (mongo-cursor-node cursor)
                                     (mongo-cursor-query cursor)))
        (mongo-cursor-reply-set! cursor rep)
        (mongo-cursor-buffer-set! cursor (mongo-message-reply-documents rep))
        #t))))

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
                 (when (and (not (mongo-cursor-initial? cursor))
                            (mongo-cursor-exists? cursor))
                   (hash-table-push! ht
                                     (mongo-cursor-node cursor)
                                     (mongo-message-reply-cursor-id
                                      (mongo-cursor-reply cursor)))))
               cursors))
   (^[node ids]
     (mongo-node-request node
                         (mongo-message-kill-cursors
                          ids
                          :request-id (mongo-node-generate-id! node))))))

(define (mongo-cursor->generator cursor)
  (generate
   (^[yield]
     (let loop ()
       (if-let1 doc (mongo-cursor-next! cursor)
         (begin (yield doc) (loop))
         (eof-object))))))
