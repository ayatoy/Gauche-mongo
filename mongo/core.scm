(define-module mongo.core
  (use gauche.collection)
  (use gauche.record)
  (use gauche.threads)
  (use gauche.time)
  (use util.list)
  (use util.match)
  (use mongo.util)
  (use mongo.bson)
  (use mongo.wire)
  (use mongo.node)
  (export <mongo>
          mongo
          mongo?
          mongo-master
          mongo-slave
          mongo-hosts
          mongo-name
          mongo-timeout
          mongo-timeout-set!
          mongo-locking
          mongo-replica-set?
          mongo-connect?
          mongo-disconnect!
          mongo-sync!
          mongo-available!
          mongo-ref
          mongo-admin
          mongo-ping
          mongo-ismaster
          mongo-server-status
          mongo-replset-status
          mongo-show-databases
          <mongo-database>
          mongo-database
          mongo-database?
          mongo-database-server
          mongo-database-name
          mongo-command
          mongo-drop-database
          mongo-get-last-error
          mongo-reset-error
          mongo-show-collections
          mongo-profiling-status
          mongo-get-profiling-level
          mongo-set-profiling-level
          mongo-show-profiling
          mongo-auth
          mongo-add-user
          mongo-remove-user
          <mongo-collection>
          mongo-collection
          mongo-collection?
          mongo-collection-database
          mongo-collection-name
          mongo-fullname
          mongo-create-collection
          mongo-drop-collection
          mongo-find1
          mongo-find
          mongo-insert1
          mongo-insert
          mongo-update
          mongo-delete
          mongo-ensure-index
          mongo-show-indexes
          mongo-drop-index
          mongo-drop-indexes
          mongo-reindex
          mongo-distinct
          mongo-map-reduce
          mongo-dbref?
          mongo-dbref
          mongo-dbref-get))
(select-module mongo.core)

;;;; constant

(define-constant MONGO_CONNECT_TIMEOUT 5000)
(define-constant MONGO_CHECK_INTERVAL #e5e8)

;;;; mongo

(define-record-type <mongo> make-mongo mongo?
  (master  mongo-master  mongo-master-set!)
  (slave   mongo-slave   mongo-slave-set!)
  (hosts   mongo-hosts   mongo-hosts-set!)
  (name    mongo-name)
  (timeout mongo-timeout mongo-timeout-set!)
  (mutex   mongo-mutex))

(define-method write-object ((m <mongo>) oport)
  (format oport "#<mongo ~s>" (mongo-name m)))

(define-syntax mongo-locking
  (syntax-rules ()
    [(_ m body ...)
     (with-locking-mutex-recursively (mongo-mutex m)
       (^[] body ...))]))

(define (mongo-single? m)
  (not (mongo-name m)))

(define (mongo-replica-set? m)
  (not (not (mongo-name m))))

(define (mongo-fetch-hosts name seeds timeout-limit)
  (let loop ([addrs seeds])
    (if (null? addrs)
      (if (>= (current-millisecond) timeout-limit)
        (error <mongo-error> :reason #f "could not connect to server")
        (begin (sys-nanosleep MONGO_CHECK_INTERVAL)
               (loop seeds)))
      (or (and-let* ([node (mongo-node-connect* (car addrs))]
                     [stat (mongo-node-ismaster node)]
                     [(mongo-node-disconnect! node)]
                     [(equal? (assoc-ref stat "setName") name)]
                     [hosts (assoc-ref stat "hosts")])
            (map string->mongo-address hosts))
          (loop (cdr addrs))))))

(define (mongo-fetch-nodes name seeds timeout-limit)
  (define (nearer! c1 c2)
    (if (and (vector? c1) (vector? c2))
      (if (> (vector-ref c2 1) (vector-ref c1 1))
        (begin (mongo-node-disconnect! (vector-ref c2 0)) c1)
        (begin (mongo-node-disconnect! (vector-ref c1 0)) c2))
      (or c1 c2)))
  (let loop ([addrs seeds] [master #f] [slave #f])
    (if (null? addrs)
      (if master
        (values (vector-ref master 0) (and slave (vector-ref slave 0)))
        (begin (when slave (mongo-node-disconnect! (vector-ref slave 0)))
               (if (>= (current-millisecond) timeout-limit)
                 (error <mongo-error> :reason #f "could not connect to master")
                 (begin (sys-nanosleep MONGO_CHECK_INTERVAL)
                        (loop seeds #f #f)))))
      (if-let1 node (mongo-node-connect* (car addrs))
        (let ([time (mongo-node-round-trip node)]
              [stat (mongo-node-ismaster node)])
          (if (equal? (assoc-ref stat "setName") name)
            (if (bson-true? (assoc-ref stat "ismaster"))
              (loop (cdr addrs) (nearer! master (vector node time)) slave)
              (loop (cdr addrs) master (nearer! slave (vector node time))))
            (begin (mongo-node-disconnect! node)
                   (loop (cdr addrs) master slave))))
        (loop (cdr addrs) master slave)))))

(define (mongo-connect name seeds timeout)
  (let* ([limit (+ (current-millisecond) timeout)]
         [hosts (if name (mongo-fetch-hosts name seeds limit) seeds)])
    (receive (master slave) (mongo-fetch-nodes name hosts limit)
      (make-mongo master slave hosts name timeout (make-mutex)))))

(define (mongo-connect? m)
  (mongo-locking m
    (and (mongo-node-connect? (mongo-master m))
         (if-let1 slave (mongo-slave m)
           (mongo-node-connect? slave)
           (mongo-single? m)))))

(define (mongo-disconnect! m)
  (mongo-locking m
    (if-let1 slave (mongo-slave m)
      (mongo-node-disconnect! slave))
    (mongo-node-disconnect! (mongo-master m))))

(define (mongo-sync! m)
  (mongo-locking m
    (mongo-disconnect! m)
    (let* ([name   (mongo-name m)]
           [seeds  (mongo-hosts m)]
           [limit  (+ (current-millisecond) (mongo-timeout m))]
           [authed (mongo-node-authed (mongo-master m))]
           [hosts  (if name (mongo-fetch-hosts name seeds limit) seeds)])
      (receive (master slave) (mongo-fetch-nodes name hosts limit)
        (mongo-master-set! m master)
        (mongo-slave-set! m slave)
        (mongo-hosts-set! m hosts)
        (mongo-node-auth-by-table master authed)
        (when slave (mongo-node-auth-by-table slave authed))))))

(define (mongo-available! m)
  (mongo-locking m
    (unless (mongo-connect? m)
      (mongo-sync! m))))

(define (mongo-ref m :key (slave #f))
  (mongo-locking m
    (if (and slave (mongo-replica-set? m))
      (or (mongo-slave m) (mongo-master m))
      (mongo-master m))))

(define (mongo uri)
  (define (param-ref alist str) (assoc-ref alist str #f string-ci=?))
  (receive (user pass addrs db params) (mongo-uri-parse uri)
    (let1 m (mongo-connect (param-ref params "replicaSet")
                           addrs
                           (or (param-ref params "connectTimeoutMS")
                               MONGO_CONNECT_TIMEOUT))
      (rlet1 x (if db (mongo-database m db) m)
        (when (and user pass)
          (guard (e [(<mongo-request-error> e)
                     (mongo-disconnect! m)
                     (raise e)])
            (mongo-auth (mongo-database m (or db "admin"))
                        user
                        pass)))))))

(define (mongo-admin m query :key (slave #f))
  (mongo-available! m)
  (mongo-node-admin (mongo-ref m :slave slave) query))

(define (mongo-ping m :key (slave #f))
  (mongo-available! m)
  (mongo-node-ping (mongo-ref m :slave slave)))

(define (mongo-ismaster m :key (slave #f))
  (mongo-available! m)
  (mongo-node-ismaster (mongo-ref m :slave slave)))

(define (mongo-server-status m :key (slave #f))
  (mongo-available! m)
  (mongo-node-server-status (mongo-ref m :slave slave)))

(define (mongo-replset-status m :key (slave #f))
  (mongo-available! m)
  (mongo-node-replset-status (mongo-ref m :slave slave)))

(define (mongo-show-databases m :key (slave #f))
  (mongo-available! m)
  (mongo-node-show-databases (mongo-ref m :slave slave)))

;;;; database

(define-record-type <mongo-database> make-mongo-database mongo-database?
  (server mongo-database-server mongo-database-server-set!)
  (name   mongo-database-name   mongo-database-name-set!))

(define-method write-object ((db <mongo-database>) oport)
  (format oport "#<mongo-database ~s>" (mongo-database-name db)))

(define (mongo-database m dn)
  (make-mongo-database m dn))

(define (mongo-command db query :key (slave #f))
  (let1 m (mongo-database-server db)
    (mongo-available! m)
    (mongo-node-command (mongo-ref m :slave slave)
                        (mongo-database-name db)
                        query)))

(define (mongo-drop-database db)
  (let1 m (mongo-database-server db)
    (mongo-available! m)
    (mongo-node-drop-database (mongo-ref m :slave #f)
                              (mongo-database-name db))))

(define (mongo-get-last-error db :key (slave #f)
                                      fsync
                                      j
                                      w
                                      wtimeout)
  (let1 m (mongo-database-server db)
    (mongo-available! m)
    (mongo-node-get-last-error (mongo-ref m :slave slave)
                               (mongo-database-name db)
                               :fsync fsync
                               :j j
                               :w w
                               :wtimeout wtimeout)))

(define (mongo-reset-error db :key (slave #f))
  (let1 m (mongo-database-server db)
    (mongo-available! m)
    (mongo-node-reset-error (mongo-ref m :slave slave)
                            (mongo-database-name db))))

(define (mongo-show-collections db)
  (let1 m (mongo-database-server db)
    (mongo-available! m)
    (mongo-node-show-collections (mongo-ref m :slave #f)
                                 (mongo-database-name db))))

(define (mongo-profiling-status db :key (slave #f))
  (let1 m (mongo-database-server db)
    (mongo-available! m)
    (mongo-node-profiling-status (mongo-ref m :slave slave)
                                 (mongo-database-name db))))

(define (mongo-get-profiling-level db :key (slave #f))
  (let1 m (mongo-database-server db)
    (mongo-available! m)
    (mongo-node-get-profiling-level (mongo-ref m :slave slave)
                                    (mongo-database-name db))))

(define (mongo-set-profiling-level db level :key (slave #f) slowms)
  (let1 m (mongo-database-server db)
    (mongo-available! m)
    (mongo-node-set-profiling-level (mongo-ref m :slave slave)
                                    (mongo-database-name db)
                                    level
                                    :slowms slowms)))

(define (mongo-show-profiling db :key (slave #f))
  (let1 m (mongo-database-server db)
    (mongo-available! m)
    (mongo-node-show-profiling (mongo-ref m :slave slave)
                               (mongo-database-name db))))

(define (mongo-auth db user pass)
  (let ([m  (mongo-database-server db)]
        [dn (mongo-database-name db)])
    (mongo-available! m)
    (begin0 (mongo-node-auth (mongo-master m) dn user pass)
            (if-let1 slave (mongo-slave m)
              (mongo-node-auth slave dn user pass)))))

(define (mongo-add-user db user pass :key read-only
                                          (safe #f)
                                          fsync
                                          j
                                          w
                                          wtimeout)
  (let1 m (mongo-database-server db)
    (mongo-available! m)
    (mongo-node-add-user (mongo-ref m :slave #f)
                         (mongo-database-name db)
                         user
                         pass
                         :read-only read-only
                         :safe safe
                         :fsync fsync
                         :j j
                         :w w
                         :wtimeout wtimeout)))

(define (mongo-remove-user db user :key (safe #f)
                                        fsync
                                        j
                                        w
                                        wtimeout)
  (let1 m (mongo-database-server db)
    (mongo-available! m)
    (mongo-node-remove-user (mongo-ref m :slave #f)
                            (mongo-database-name db)
                            user
                            :safe safe
                            :fsync fsync
                            :j j
                            :w w
                            :wtimeout wtimeout)))

;;;; collection

(define-record-type <mongo-collection> make-mongo-collection mongo-collection?
  (database mongo-collection-database mongo-collection-database-set!)
  (name     mongo-collection-name     mongo-collection-name-set!))

(define-method write-object ((col <mongo-collection>) oport)
  (format oport "#<mongo-collection ~s>" (mongo-fullname col)))

(define (mongo-collection db cn)
  (make-mongo-collection db cn))

(define (mongo-fullname col)
  (mongo-ns-compose (mongo-database-name (mongo-collection-database col))
                    (mongo-collection-name col)))

(define (mongo-create-collection col :key capped
                                          size
                                          max)
  (let* ([db (mongo-collection-database col)]
         [m  (mongo-database-server db)])
    (mongo-available! m)
    (mongo-node-create-collection (mongo-ref m :slave #f)
                                  (mongo-database-name db)
                                  (mongo-collection-name col)
                                  :capped capped
                                  :size size
                                  :max max)))

(define (mongo-drop-collection col)
  (let* ([db (mongo-collection-database col)]
         [m  (mongo-database-server db)])
    (mongo-available! m)
    (mongo-node-drop-collection (mongo-ref m :slave #f)
                                (mongo-database-name db)
                                (mongo-collection-name col))))

(define (mongo-find1 col query :key (slave #f)
                                    (select #f)
                                    (skip 0))
  (let* ([db (mongo-collection-database col)]
         [m  (mongo-database-server db)])
    (mongo-available! m)
    (mongo-node-find1 (mongo-ref m :slave slave)
                      (mongo-database-name db)
                      (mongo-collection-name col)
                      query
                      :number-to-skip skip
                      :return-field-selector select
                      :slave-ok slave)))

(define (mongo-find col query :key (slave #f)
                                   (select #f)
                                   (skip 0)
                                   (limit #f)
                                   (number-to-return 0)
                                   (tailable-cursor #f)
                                   (oplog-replay #f)
                                   (no-cursor-timeout #f)
                                   (await-data #f)
                                   (exhaust #f)
                                   (partial #f))
  (let* ([db (mongo-collection-database col)]
         [m  (mongo-database-server db)])
    (mongo-available! m)
    (mongo-node-find (mongo-ref m :slave slave)
                     (mongo-database-name db)
                     (mongo-collection-name col)
                     query
                     :number-to-skip skip
                     :number-to-return (or (and limit (* -1 (abs limit)))
                                           number-to-return)
                     :return-field-selector select
                     :tailable-cursor tailable-cursor
                     :slave-ok slave
                     :oplog-replay oplog-replay
                     :no-cursor-timeout no-cursor-timeout
                     :await-data await-data
                     :exhaust exhaust
                     :partial partial)))

(define (mongo-insert1 col doc :key (continue #t)
                                    (safe #f)
                                    fsync
                                    j
                                    w
                                    wtimeout)
  (let* ([db (mongo-collection-database col)]
         [m  (mongo-database-server db)])
    (mongo-available! m)
    (mongo-node-insert (mongo-ref m :slave #f)
                       (mongo-database-name db)
                       (mongo-collection-name col)
                       (list doc)
                       :continue-on-error continue
                       :safe safe
                       :fsync fsync
                       :j j
                       :w w
                       :wtimeout wtimeout)))

(define (mongo-insert col docs :key (continue #t)
                                    (safe #f)
                                    fsync
                                    j
                                    w
                                    wtimeout)
  (let* ([db (mongo-collection-database col)]
         [m  (mongo-database-server db)])
    (mongo-available! m)
    (mongo-node-insert (mongo-ref m :slave #f)
                       (mongo-database-name db)
                       (mongo-collection-name col)
                       docs
                       :continue-on-error continue
                       :safe safe
                       :fsync fsync
                       :j j
                       :w w
                       :wtimeout wtimeout)))

(define (mongo-update col query update :key (upsert #f)
                                            (single #f)
                                            (safe #f)
                                            fsync
                                            j
                                            w
                                            wtimeout)
  (let* ([db (mongo-collection-database col)]
         [m  (mongo-database-server db)])
    (mongo-available! m)
    (mongo-node-update (mongo-ref m :slave #f)
                       (mongo-database-name db)
                       (mongo-collection-name col)
                       query
                       update
                       :upsert upsert
                       :multi-update (not single)
                       :safe safe
                       :fsync fsync
                       :j j
                       :w w
                       :wtimeout wtimeout)))

(define (mongo-delete col query :key (single #f)
                                     (safe #f)
                                     fsync
                                     j
                                     w
                                     wtimeout)
  (let* ([db (mongo-collection-database col)]
         [m  (mongo-database-server db)])
    (mongo-available! m)
    (mongo-node-delete (mongo-ref m :slave #f)
                       (mongo-database-name db)
                       (mongo-collection-name col)
                       query
                       :single-remove single
                       :safe safe
                       :fsync fsync
                       :j j
                       :w w
                       :wtimeout wtimeout)))

(define (mongo-ensure-index col name spec :key unique
                                               drop-dups
                                               background
                                               sparse
                                               (safe #f)
                                               fsync
                                               j
                                               w
                                               wtimeout)
  (let* ([db (mongo-collection-database col)]
         [m  (mongo-database-server db)])
    (mongo-available! m)
    (mongo-node-ensure-index (mongo-ref m :slave #f)
                             (mongo-database-name db)
                             (mongo-collection-name col)
                             name
                             spec
                             :unique unique
                             :drop-dups drop-dups
                             :background background
                             :sparse sparse
                             :safe safe
                             :fsync fsync
                             :j j
                             :w w
                             :wtimeout wtimeout)))

(define (mongo-show-indexes col)
  (let* ([db (mongo-collection-database col)]
         [m  (mongo-database-server db)])
    (mongo-available! m)
    (mongo-node-show-indexes (mongo-ref m :slave #f)
                             (mongo-database-name db)
                             (mongo-collection-name col))))

(define (mongo-drop-index col name)
  (let* ([db (mongo-collection-database col)]
         [m  (mongo-database-server db)])
    (mongo-available! m)
    (mongo-node-drop-index (mongo-ref m :slave #f)
                           (mongo-database-name db)
                           (mongo-collection-name col)
                           name)))

(define (mongo-drop-indexes col)
  (let* ([db (mongo-collection-database col)]
         [m  (mongo-database-server db)])
    (mongo-available! m)
    (mongo-node-drop-indexes (mongo-ref m :slave #f)
                             (mongo-database-name db)
                             (mongo-collection-name col))))

(define (mongo-reindex col)
  (let* ([db (mongo-collection-database col)]
         [m  (mongo-database-server db)])
    (mongo-available! m)
    (mongo-node-reindex (mongo-ref m :slave #f)
                        (mongo-database-name db)
                        (mongo-collection-name col))))

(define (mongo-distinct col key :key query)
  (let* ([db (mongo-collection-database col)]
         [m  (mongo-database-server db)])
    (mongo-available! m)
    (mongo-node-distinct (mongo-ref m :slave #f)
                         (mongo-database-name db)
                         (mongo-collection-name col)
                         key
                         :query query)))

(define (mongo-map-reduce col map reduce :key query
                                              sort
                                              limit
                                              out
                                              keeptemp
                                              finalize
                                              scope
                                              js-mode
                                              verbose)
  (let* ([db (mongo-collection-database col)]
         [m  (mongo-database-server db)])
    (mongo-available! m)
    (mongo-node-map-reduce (mongo-ref m :slave #f)
                           (mongo-database-name db)
                           (mongo-collection-name col)
                           map
                           reduce
                           :query query
                           :sort sort
                           :limit limit
                           :out out
                           :keeptemp keeptemp
                           :finalize finalize
                           :scope scope
                           :js-mode js-mode
                           :verbose verbose)))

;;;; dbref

(define (mongo-dbref? ref)
  (match ref
    [(or (("$ref" . _) ("$id" . _)) (("$ref" . _) ("$id" . _) ("$db" . _))) #t]
    [_ #f]))

(define (mongo-dbref cn id :optional dn)
  `(("$ref" . ,cn)
    ("$id" . ,id)
    ,@(bson-document-part "$db" dn)))

(define (mongo-dbref-get db ref :key (slave #f))
  (let1 m (mongo-database-server db)
    (mongo-available! m)
    (mongo-node-dbref-get (mongo-ref m :slave slave)
                          (mongo-database-name db)
                          ref
                          :slave-ok slave)))
