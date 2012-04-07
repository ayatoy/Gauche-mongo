(define-module mongo.wire
  (use gauche.net)
  (use gauche.record)
  (use gauche.uvector)
  (use srfi-11)
  (use binary.io)
  (use util.match)
  (use mongo.util)
  (use mongo.bson)
  (export MONGO_OP_REPLY
          MONGO_OP_MSG
          MONGO_OP_UPDATE
          MONGO_OP_INSERT
          MONGO_OP_RESERVED
          MONGO_OP_QUERY
          MONGO_OP_GETMORE
          MONGO_OP_DELETE
          MONGO_OP_KILL_CURSORS
          <mongo-wire-error>
          <mongo-connect-error>
          <mongo-send-error>
          <mongo-recv-error>
          <mongo-exchange-error>
          mongo-socket-send
          mongo-socket-recv!
          mongo-socket-connect
          mongo-socket-connect?
          mongo-message-send
          mongo-message-recv
          mongo-message-request
          <mongo-message>
          mongo-message?
          mongo-message-length
          mongo-message-request-id
          mongo-message-response-to
          mongo-message-op-code
          <mongo-message-update>
          mongo-message-update?
          mongo-message-update-full-collction-name
          mongo-message-update-flags
          mongo-message-update-selector
          mongo-message-update-update
          write-mongo-message-update-prepare
          mongo-message-update
          <mongo-message-insert>
          mongo-message-insert?
          mongo-message-insert-flags
          mongo-message-insert-full-collection-name
          mongo-message-insert-documents
          write-mongo-message-insert-prepare
          mongo-message-insert
          <mongo-message-query>
          mongo-message-query?
          mongo-message-query-flags
          mongo-message-query-full-collection-name
          mongo-message-query-number-to-skip
          mongo-message-query-number-to-return
          mongo-message-query-query
          mongo-message-query-return-field-selector
          write-mongo-message-query-prepare
          mongo-message-query
          <mongo-message-getmore>
          mongo-message-getmore?
          mongo-message-getmore-full-collection-name
          mongo-message-getmore-number-to-return
          mongo-message-getmore-cursor-id
          write-mongo-message-getmore-prepare
          mongo-message-getmore
          <mongo-message-delete>
          mongo-message-delete?
          mongo-message-delete-full-collection-name
          mongo-message-delete-flags
          mongo-message-delete-selector
          write-mongo-message-delete-prepare
          mongo-message-delete
          <mongo-message-kill-cursors>
          mongo-message-kill-cursors?
          mongo-message-kill-cursors-number-of-cursor-ids
          mongo-message-kill-cursors-cursor-ids
          write-mongo-message-kill-cursors-prepare
          mongo-message-kill-cursors
          <mongo-message-msg>
          mongo-message-msg?
          mongo-message-msg-message
          write-mongo-message-msg-prepare
          mongo-message-msg
          <mongo-message-reply>
          mongo-message-reply?
          mongo-message-read-reply
          mongo-message-reply-response-flags
          mongo-message-reply-cursor-id
          mongo-message-reply-starting-from
          mongo-message-reply-number-returned
          mongo-message-reply-documents
          mongo-message-reply-cursor-not-found?
          mongo-message-reply-query-failure?
          mongo-message-reply-shard-config-stale?
          mongo-message-reply-await-capable?
          mongo-message-reply-document))
(select-module mongo.wire)

;;;; constant

(define-constant MONGO_OP_REPLY        1)
(define-constant MONGO_OP_MSG          1000)
(define-constant MONGO_OP_UPDATE       2001)
(define-constant MONGO_OP_INSERT       2002)
(define-constant MONGO_OP_RESERVED     2003)
(define-constant MONGO_OP_QUERY        2004)
(define-constant MONGO_OP_GETMORE      2005)
(define-constant MONGO_OP_DELETE       2006)
(define-constant MONGO_OP_KILL_CURSORS 2007)

(define-constant EMPTY_FDSET (make <sys-fdset>))

;;;; condition

(define-condition-type <mongo-wire-error> <mongo-error> #f)
(define-condition-type <mongo-connect-error> <mongo-wire-error> #f)
(define-condition-type <mongo-send-error> <mongo-wire-error> #f)
(define-condition-type <mongo-recv-error> <mongo-wire-error> #f)
(define-condition-type <mongo-exchange-error> <mongo-wire-error> #f)

;;;; io

(define (mongo-socket-send socket u8v)
  (guard (e [else (error <mongo-send-error> (~ e'message))])
    (let loop ([uv u8v] [size (uvector-size u8v)])
      (when (> size 0)
        (let1 sent (socket-send socket uv)
          (when (>= 0 sent)
            (error <mongo-send-error> "socket-send returned:" sent))
          (loop (u8vector-copy uv sent) (- size sent)))))))

(define (mongo-socket-recv! socket u8v)
  (guard (e [else (error <mongo-recv-error> (~ e'message))])
    (let loop ([uv u8v] [size (uvector-size u8v)] [recved 0])
      (when (> size 0)
        (let1 sent (socket-recv! socket uv)
          (when (>= 0 sent)
            (error <mongo-recv-error> "socket-recv! returned:" sent))
          (u8vector-copy! u8v recved uv 0 sent)
          (loop (u8vector-copy uv sent) (- size sent) (+ recved sent)))))))

(define (mongo-socket-connect address)
  (guard (e [(<system-error> e) (error <mongo-connect-error> (~ e'message))])
    (cond [(mongo-address-inet? address)
           (make-client-socket 'inet (~ address'host) (~ address'port))]
          [(mongo-address-unix? address)
           (make-client-socket 'unix (~ address'path))])))

(define (mongo-socket-connect? socket)
  (and (eq? 'connected (socket-status socket))
       (let1 fd (sys-fdset (socket-fd socket))
         (receive (n r w x)
             (guard (e [else (values 1 #f #f #f)])
               (sys-select fd EMPTY_FDSET EMPTY_FDSET 0))
           (= n 0)))))

(define (mongo-message-send socket ms)
  (mongo-socket-send
   socket
   (receive (size write-message)
       (cond [(mongo-message-update? ms)
              (write-mongo-message-update-prepare ms)]
             [(mongo-message-insert? ms)
              (write-mongo-message-insert-prepare ms)]
             [(mongo-message-query? ms)
              (write-mongo-message-query-prepare ms)]
             [(mongo-message-getmore? ms)
              (write-mongo-message-getmore-prepare ms)]
             [(mongo-message-delete? ms)
              (write-mongo-message-delete-prepare ms)]
             [(mongo-message-kill-cursors? ms)
              (write-mongo-message-kill-cursors-prepare ms)]
             [(mongo-message-msg? ms)
              (write-mongo-message-msg-prepare ms)])
     (rlet1 u8v (make-u8vector size)
       (call-with-output-uvector u8v write-message)))))

(define (mongo-message-recv socket)
  (call-with-input-uvector
      (let* ([len-vec (rlet1 u8v (make-u8vector BSON_INT32_SIZE)
                        (mongo-socket-recv! socket u8v))]
             [len (get-s32le len-vec 0)])
        (rlet1 u8v (make-u8vector len)
          (u8vector-copy! u8v 0 len-vec)
          (u8vector-copy! u8v 4 (rlet1 u8v (make-u8vector (- len 4))
                                  (mongo-socket-recv! socket u8v)))))
    mongo-message-read-reply))

(define (mongo-message-request socket ms)
  (mongo-message-send socket ms)
  (and (or (mongo-message-query? ms)
           (mongo-message-getmore? ms))
       (rlet1 reply (mongo-message-recv socket)
         (let ([request-id  (mongo-message-request-id ms)]
               [response-to (mongo-message-response-to reply)])
           (unless (= request-id response-to)
             (errorf <mongo-exchange-error>
                     "unexpected exchange: request-id: ~s, response-to: ~s"
                     request-id response-to))))))

;;;; message

(define-record-type <mongo-message>
  make-mongo-message
  mongo-message?
  (length      mongo-message-length mongo-message-length-set!)
  (request-id  mongo-message-request-id)
  (response-to mongo-message-response-to)
  (op-code     mongo-message-op-code))

(define (write-header ms oport)
  (write-bson-int32 (mongo-message-length ms) oport)
  (write-bson-int32 (mongo-message-request-id ms) oport)
  (write-bson-int32 (mongo-message-response-to ms) oport)
  (write-bson-int32 (mongo-message-op-code ms) oport))

;;;; update

(define-record-type (<mongo-message-update> <mongo-message>)
  make-mongo-message-update
  mongo-message-update?
  (zero                 mongo-message-update-zero)
  (full-collection-name mongo-message-update-full-collction-name)
  (flags                mongo-message-update-flags)
  (selector             mongo-message-update-selector)
  (update               mongo-message-update-update))

(define (write-mongo-message-update-prepare ms)
  (let-values ([(col-size write-col)
                (write-bson-cstring-prepare
                 (mongo-message-update-full-collction-name ms))]
               [(docs-size write-docs)
                (prepare-fold
                 write-bson-document-prepare
                 (list (mongo-message-update-selector ms)
                       (mongo-message-update-update ms)))])
    (let1 size (+ 24 col-size docs-size)
      (mongo-message-length-set! ms size)
      (values size
              (^[oport]
                (write-header ms oport)
                (write-bson-int32 BSON_ZERO oport)
                (write-col oport)
                (write-bson-int32 (mongo-message-update-flags ms) oport)
                (write-docs oport))))))

(define (mongo-message-update collection selector update :key (request-id 0)
                                                              (response-to -1)
                                                              (upsert #f)
                                                              (multi-update #f))
  (make-mongo-message-update #f
                             request-id
                             response-to
                             MONGO_OP_UPDATE
                             BSON_ZERO
                             collection
                             (+ (if upsert       1 0)
                                (if multi-update 2 0))
                             selector
                             update))

;;;; insert

(define-record-type (<mongo-message-insert> <mongo-message>)
  make-mongo-message-insert
  mongo-message-insert?
  (flags                mongo-message-insert-flags)
  (full-collection-name mongo-message-insert-full-collection-name)
  (documents            mongo-message-insert-documents))

(define (write-mongo-message-insert-prepare ms)
  (let-values ([(col-size write-col)
                (write-bson-cstring-prepare
                 (mongo-message-insert-full-collection-name ms))]
               [(docs-size write-docs)
                (prepare-fold
                 write-bson-document-prepare
                 (mongo-message-insert-documents ms))])
    (let1 size (+ 20 col-size docs-size)
      (mongo-message-length-set! ms size)
      (values size
              (^[oport]
                (write-header ms oport)
                (write-bson-int32 (mongo-message-insert-flags ms) oport)
                (write-col oport)
                (write-docs oport))))))

(define (mongo-message-insert collection documents :key (request-id 0)
                                                        (response-to -1)
                                                        (continue-on-error #f))
  (make-mongo-message-insert #f
                             request-id
                             response-to
                             MONGO_OP_INSERT
                             (if continue-on-error 1 0)
                             collection
                             documents))

;;;; query

(define-record-type (<mongo-message-query> <mongo-message>)
  make-mongo-message-query
  mongo-message-query?
  (flags                 mongo-message-query-flags)
  (full-collection-name  mongo-message-query-full-collection-name)
  (number-to-skip        mongo-message-query-number-to-skip)
  (number-to-return      mongo-message-query-number-to-return)
  (query                 mongo-message-query-query)
  (return-field-selector mongo-message-query-return-field-selector))

(define (write-mongo-message-query-prepare ms)
  (let-values ([(col-size write-col)
                (write-bson-cstring-prepare
                 (mongo-message-query-full-collection-name ms))]
               [(docs-size write-docs)
                (prepare-fold
                 (match-lambda
                   [(? list? x) (write-bson-document-prepare x)]
                   [#f (values 0 (^[x] (undefined)))])
                 (list (mongo-message-query-query ms)
                       (mongo-message-query-return-field-selector ms)))])
    (let1 size (+ 28 col-size docs-size)
      (mongo-message-length-set! ms size)
      (values size
              (^[oport]
                (write-header ms oport)
                (write-bson-int32 (mongo-message-query-flags ms) oport)
                (write-col oport)
                (write-bson-int32 (mongo-message-query-number-to-skip ms) oport)
                (write-bson-int32 (mongo-message-query-number-to-return ms)
                                  oport)
                (write-docs oport))))))

(define (mongo-message-query collection query :key (request-id 0)
                                                   (response-to -1)
                                                   (number-to-skip 0)
                                                   (number-to-return 0)
                                                   (return-field-selector #f)
                                                   (tailable-cursor #f)
                                                   (slave-ok #f)
                                                   (oplog-replay #f)
                                                   (no-cursor-timeout #f)
                                                   (await-data #f)
                                                   (exhaust #f)
                                                   (partial #f))
  (make-mongo-message-query #f
                            request-id
                            response-to
                            MONGO_OP_QUERY
                            (+ (if tailable-cursor    2 0)
                               (if slave-ok           4 0)
                               (if oplog-replay       8 0)
                               (if no-cursor-timeout 16 0)
                               (if await-data        32 0)
                               (if exhaust           64 0)
                               (if partial          128 0))
                            collection
                            number-to-skip
                            number-to-return
                            query
                            return-field-selector))

;;;; getmore

(define-record-type (<mongo-message-getmore> <mongo-message>)
  make-mongo-message-getmore
  mongo-message-getmore?
  (zero                 mongo-message-getmore-zero)
  (full-collection-name mongo-message-getmore-full-collection-name)
  (number-to-return     mongo-message-getmore-number-to-return)
  (cursor-id            mongo-message-getmore-cursor-id))

(define (write-mongo-message-getmore-prepare ms)
  (receive (col-size write-col)
      (write-bson-cstring-prepare
       (mongo-message-getmore-full-collection-name ms))
    (let1 size (+ 32 col-size)
      (mongo-message-length-set! ms size)
      (values size
              (^[oport]
                (write-header ms oport)
                (write-bson-int32 BSON_ZERO oport)
                (write-col oport)
                (write-bson-int32 (mongo-message-getmore-number-to-return ms)
                                  oport)
                (write-bson-int64 (mongo-message-getmore-cursor-id ms)
                                  oport))))))

(define (mongo-message-getmore collection cursor-id :key (request-id 0)
                                                         (response-to -1)
                                                         (number-to-return 0))
  (make-mongo-message-getmore #f
                              request-id
                              response-to
                              MONGO_OP_GETMORE
                              BSON_ZERO
                              collection
                              number-to-return
                              cursor-id))

;;;; delete

(define-record-type (<mongo-message-delete> <mongo-message>)
  make-mongo-message-delete
  mongo-message-delete?
  (zero                 mongo-message-delete-zero)
  (full-collection-name mongo-message-delete-full-collection-name)
  (flags                mongo-message-delete-flags)
  (selector             mongo-message-delete-selector))

(define (write-mongo-message-delete-prepare ms)
  (let-values ([(col-size write-col)
                (write-bson-cstring-prepare
                 (mongo-message-delete-full-collection-name ms))]
               [(sel-size write-sel)
                (write-bson-document-prepare
                 (mongo-message-delete-selector ms))])
    (let1 size (+ 24 col-size sel-size)
      (mongo-message-length-set! ms size)
      (values size
              (^[oport]
                (write-header ms oport)
                (write-bson-int32 BSON_ZERO oport)
                (write-col oport)
                (write-bson-int32 (mongo-message-delete-flags ms) oport)
                (write-sel oport))))))

(define (mongo-message-delete collection selector :key (request-id 0)
                                                       (response-to -1)
                                                       (single-remove #f))
  (make-mongo-message-delete #f
                             request-id
                             response-to
                             MONGO_OP_DELETE
                             BSON_ZERO
                             collection
                             (if single-remove 1 0)
                             selector))

;;;; kill cursors

(define-record-type (<mongo-message-kill-cursors> <mongo-message>)
  make-mongo-message-kill-cursors
  mongo-message-kill-cursors?
  (zero                 mongo-message-kill-cursors-zero)
  (number-of-cursor-ids mongo-message-kill-cursors-number-of-cursor-ids)
  (cursor-ids           mongo-message-kill-cursors-cursor-ids))

(define (write-mongo-message-kill-cursors-prepare ms)
  (receive (ids-size write-ids)
      (prepare-fold
       write-bson-int64-prepare
       (mongo-message-kill-cursors-cursor-ids ms))
    (let1 size (+ 24 ids-size)
      (mongo-message-length-set! ms size)
      (values size
              (^[oport]
                (write-header ms oport)
                (write-bson-int32 BSON_ZERO oport)
                (write-bson-int32
                 (mongo-message-kill-cursors-number-of-cursor-ids ms)
                 oport)
                (write-ids oport))))))

(define (mongo-message-kill-cursors cursor-ids :key (request-id 0)
                                                    (response-to -1))
  (make-mongo-message-kill-cursors #f
                                   request-id
                                   response-to
                                   MONGO_OP_KILL_CURSORS
                                   BSON_ZERO
                                   (length cursor-ids)
                                   cursor-ids))

;;;; msg

(define-record-type (<mongo-message-msg> <mongo-message>)
  make-mongo-message-msg
  mongo-message-msg?
  (message mongo-message-msg-message))

(define (write-mongo-message-msg-prepare ms)
  (receive (m-size write-m)
      (write-bson-cstring-prepare (mongo-message-msg-message ms))
    (let1 size (+ 16 m-size)
      (mongo-message-length-set! ms size)
      (values size
              (^[oport]
                (write-header ms oport)
                (write-m oport))))))

(define (mongo-message-msg message :key (request-id 0)
                                        (response-to -1))
  (make-mongo-message-msg #f
                          request-id
                          response-to
                          MONGO_OP_MSG
                          message))

;;;; reply

(define-record-type (<mongo-message-reply> <mongo-message>)
  make-mongo-message-reply
  mongo-message-reply?
  (response-flags  mongo-message-reply-response-flags)
  (cursor-id       mongo-message-reply-cursor-id)
  (starting-from   mongo-message-reply-starting-from)
  (number-returned mongo-message-reply-number-returned)
  (documents       mongo-message-reply-documents))

(define (mongo-message-read-reply iport)
  (let* ([message-length  (read-bson-int32 iport)]
         [request-id      (read-bson-int32 iport)]
         [response-to     (read-bson-int32 iport)]
         [op-code         (read-bson-int32 iport)]
         [response-flags  (read-bson-int32 iport)]
         [cursor-id       (read-bson-int64 iport)]
         [starting-from   (read-bson-int32 iport)]
         [number-returned (read-bson-int32 iport)])
    (make-mongo-message-reply message-length
                              request-id
                              response-to
                              op-code
                              response-flags
                              cursor-id
                              starting-from
                              number-returned
                              (let loop ([docs '()] [i 0])
                                (if (> number-returned i)
                                  (loop (cons (read-bson-document iport) docs)
                                        (+ i 1))
                                  (reverse docs))))))

(define (mongo-message-reply-cursor-not-found? reply)
  (logbit? 0 (mongo-message-reply-response-flags reply)))

(define (mongo-message-reply-query-failure? reply)
  (logbit? 1 (mongo-message-reply-response-flags reply)))

(define (mongo-message-reply-shard-config-stale? reply)
  (logbit? 2 (mongo-message-reply-response-flags reply)))

(define (mongo-message-reply-await-capable? reply)
  (logbit? 3 (mongo-message-reply-response-flags reply)))

(define (mongo-message-reply-document reply)
  (match (mongo-message-reply-documents reply)
    [((and ((_ . _) ...) doc)) doc]
    [_ #f]))
