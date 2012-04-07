(use gauche.test)
(use gauche.threads)
(use gauche.uvector)
(use binary.io)
(use srfi-1)
(use mongo.util)
(use mongo.bson)

(define (thread-map proc xs)
  (map thread-join!
       (map (^[x] (thread-start! (make-thread (^[] (proc x)))))
            xs)))

(define (ok? doc)
  (not (zero? (alref doc "ok"))))

(define (test-insert n)
  (rlet1 id (bson-object-id)
    (mongo-node-insert *node* *dn* *cn*
                       (map (^[i] (% "test-id" id
                                     "flag" 'null
                                     "i" i))
                            (iota n))
                       :safe #t
                       :w 1)))

(define (test-docs . kv)
  (mongo-cursor-all! (mongo-node-find *node* *dn* *cn* (apply % kv))))

(define (test-insert1 doc . opts)
  (apply mongo-node-insert *node* *dn* *cn* (list doc) opts))

(define (test-update select . opts)
  (apply mongo-node-update
         *node* *dn* *cn*
         select
         (% "$set" (% "flag" (bson-true)))
         opts))

(define (test-find1 query . opts)
  (apply mongo-node-find1 *node* *dn* *cn* query opts))

(define (test-delete select . opts)
  (apply mongo-node-delete *node* *dn* *cn* select opts))

(define *host* "localhost:27017")
(define *address* (string->mongo-address *host*))
(define *dn* "gauche_mongo_test_database")
(define *cn* "gauche_mongo_test_collection")
(define *index-name* "gauche_mongo_test_index")
(define *user* "gauche_mongo_test_user")
(define *pass* "gauche_mongo_test_pass")
(define *node* #f)
(define *doc* `(("int32" . 2147483647)
                ("int64" . 2147483648)
                ("double" .  5.05)
                ("string" . "foo")
                ("null" . ,(bson-null))
                ("false" . ,(bson-false))
                ("true" . ,(bson-true))
                ("undefined" . ,(bson-undefined))
                ("min-key" . ,(bson-min))
                ("max-key" . ,(bson-max))
                ("bin-generic" . ,(bson-binary 'generic (u8vector 0)))
                ("bin-function" . ,(bson-binary 'function (u8vector 0)))
                ("bin-old" . ,(bson-binary 'old (u8vector 0)))
                ("bin-uuid" . ,(bson-binary 'uuid (u8vector 0)))
                ("bin-md5" . ,(bson-binary 'md5 (u8vector 0)))
                ("bin-udef" . ,(bson-binary 'user-defined (u8vector 0)))
                ("datetime" . ,(bson-datetime))
                ("timestamp" . ,(bson-timestamp))
                ("object-id" . ,(bson-object-id))
                ("regexp" . ,(bson-regexp "foo" "bar"))
                ("dbpointer" . ,(bson-dbpointer "foo.bar" (bson-object-id)))
                ("code" . ,(bson-code "foo"))
                ("code/scope" . ,(bson-code/scope "foo" '(("bar" . "baz"))))
                ("symbol" . ,(bson-symbol "foo"))
                ("document" . (("foo" . "foo") ("bar" . "bar")))
                ("array" . ,(vector "foo" "bar" "baz"))))

;;;; test

(test-start "connection")

(use mongo.wire)
(test-module 'mongo.wire)

(use mongo.node)
(test-module 'mongo.node)

;;;; connect

(test-section "connect")

(test* "mongo-node-connect" #t
       (begin (set! *node* (mongo-node-connect *address*))
              (and (mongo-node? *node*)
                   (mongo-node-connect? *node*))))

(test* "mongo-node-disconnect!" #f
       (begin (mongo-node-disconnect! *node*)
              (mongo-node-connect? *node*)))

(test* "mongo-node-available!" #t
       (begin (mongo-node-available! *node*)
              (mongo-node-connect? *node*)))

;;;; insert

(test-section "insert")

(test* "mongo-node-insert" (undefined)
       (test-insert1 *doc*))

(test* "mongo-node-insert invariance" #t
       (let* ([id (bson-object-id)]
              [doc (alset *doc* "_id" id)])
         (test-insert1 doc)
         (equal? doc (mongo-node-find1 *node* *dn* *cn* (% "_id" id)))))

(test* "mongo-node-insert safe" #t
       (ok? (test-insert1 *doc* :safe #t)))

(test* "mongo-node-insert journal" #t
       (ok? (test-insert1 *doc* :safe #t :j #t)))

(test* "mongo-node-insert fsync" #t
       (let1 res (test-insert1 *doc* :safe #t :fsync #t)
         (and (ok? res)
              (number? (alref res "waited")))))

(test* "mongo-node-insert w" #t
       (let1 res (test-insert1 *doc* :safe #t :w 1)
         (and (ok? res)
              (number? (alref res "wtime")))))

(test* "mongo-node-insert thread safe" #t
       (every ok? (thread-map (^[i] (test-insert1 *doc* :safe #t :w 1))
                              (iota 100))))

(test* "mongo-node-insert continue-on-error #f" 2
       (let ([id  (bson-object-id)]
             [tid (bson-object-id)])
         (guard (e [(<mongo-request-error> e)
                    (length (test-docs "test-id" tid))])
           (mongo-node-insert
            *node* *dn* *cn*
            (list (% "test-id" tid "_id" (bson-object-id))
                  (% "test-id" tid "_id" id)
                  (% "test-id" tid "_id" id)
                  (% "test-id" tid "_id" (bson-object-id)))
            :continue-on-error #f :safe #t :w 1))))

(test* "mongo-node-insert continue-on-error #t" 3
       (let ([id  (bson-object-id)]
             [tid (bson-object-id)])
         (guard (e [(<mongo-request-error> e)
                    (length (test-docs "test-id" tid))])
           (mongo-node-insert
            *node* *dn* *cn*
            (list (% "test-id" tid "_id" (bson-object-id))
                  (% "test-id" tid "_id" id)
                  (% "test-id" tid "_id" id)
                  (% "test-id" tid "_id" (bson-object-id)))
            :continue-on-error #t :safe #t :w 1))))

;;;; update

(test-section "update")

(test* "mongo-node-update multi-update #t" 2
       (let1 id (test-insert 2)
         (test-update (% "test-id" id) :multi-update #t :safe #t :w 1)
         (length (test-docs "test-id" id "flag" (bson-true)))))

(test* "mongo-node-update multi-update #f" 1
       (let1 id (test-insert 2)
         (test-update (% "test-id" id) :multi-update #f :safe #t :w 1)
         (length (test-docs "test-id" id "flag" (bson-true)))))

(test* "mongo-node-update upsert #t" 3
       (let1 id (test-insert 2)
         (test-update (% "test-id" id "flag" "foo") :upsert #t :safe #t :w 1)
         (length (test-docs "test-id" id))))

(test* "mongo-node-update upsert #f" 2
       (let1 id (test-insert 2)
         (test-update (% "test-id" id "flag" "foo") :upsert #f :safe #t :w 1)
         (length (test-docs "test-id" id))))

(test* "mongo-node-update" (undefined)
       (test-update (% "test-id" (test-insert 2))))

(test* "mongo-node-update safe" 'true
       (let1 res (test-update (% "test-id" (test-insert 2)) :safe #t)
         (and (ok? res)
              (alref res "updatedExisting"))))

(test* "mongo-node-update journal" 'true
       (let1 res (test-update (% "test-id" (test-insert 2)) :safe #t :j #t)
         (and (ok? res)
              (alref res "updatedExisting"))))

(test* "mongo-node-update fsync" #t
       (let1 res (test-update (% "test-id" (test-insert 2)) :safe #t :fsync #t)
         (and (ok? res)
              (number? (alref res "waited")))))

(test* "mongo-node-update w" #t
       (let1 res (test-update (% "test-id" (test-insert 2)) :safe #t :w 1)
         (and (ok? res)
              (number? (alref res "wtime")))))

(test* "mongo-node-update thread safe" #t
       (let1 id (test-insert 100)
         (every (^[res doc] (and (ok? res) (alref doc "flag")))
                (thread-map
                 (^[i] (test-update (% "test-id" id "i" i) :safe #t :w 1))
                 (iota 100))
                (test-docs "test-id" id))))

;;;; find1

(test-section "find1")

(test* "mongo-node-find1" 0
       (let1 id (test-insert 2)
         (alref (test-find1
                 (% "query" (% "test-id" id "i" (% "$exists" (bson-true)))
                    "orderby" (% "i" 1)))
                "i")))

(test* "mongo-node-find1 number-to-skip" 1
       (let1 id (test-insert 2)
         (alref (test-find1
                 (% "query" (% "test-id" id "i" (% "$exists" (bson-true)))
                    "orderby" (% "i" 1))
                 :number-to-skip 1)
                "i")))

(test* "mongo-node-find1 return-field-selector" #t
       (let1 id (test-insert 1)
         (let1 doc (test-find1
                    (% "query" (% "test-id" id "i" (% "$exists" (bson-true)))
                       "orderby" (% "i" 1))
                    :return-field-selector (% "i" 1))
           (and (bson-object-id? (alref doc "_id"))
                (number? (alref doc "i"))
                (not (alref doc "flag"))
                (not (alref doc "test-id"))))))

(test* "mongo-node-find1 thread safe" 4950
       (let1 id (test-insert 100)
         (fold (^[doc r] (+ (alref doc "i") r))
               0
               (thread-map (^[i] (test-find1 (% "test-id" id "i" i)))
                           (iota 100)))))

;;;; find

(test-section "find")

(test* "mongo-node-find thread safe" 4950
       (let1 id (test-insert 100)
         (fold (^[cur r] (+ (alref (mongo-cursor-next! cur) "i") r))
               0
               (thread-map
                (^[i] (mongo-node-find *node* *dn* *cn* (% "test-id" id "i" i)))
                (iota 100)))))

(test* "mongo-cursor-next! thread safe" 4950
       (let* ([id (test-insert 100)]
              [cr (mongo-node-find *node* *dn* *cn* (% "test-id" id))])
         (fold (^[doc r] (+ (alref doc "i") r))
               0
               (thread-map (^[i] (mongo-cursor-next! cr))
                           (iota 100)))))

(test* "mongo-cursor-take! thread safe" 4950
       (let* ([id (test-insert 100)]
              [cr (mongo-node-find *node* *dn* *cn* (% "test-id" id))])
         (fold (^[docs r] (+ (fold (^[doc r] (+ (alref doc "i") r)) 0 docs) r))
               0
               (thread-map (^[i] (mongo-cursor-take! cr 5))
                           (iota 20)))))

(test* "mongo-cursor-all!" 100
       (let1 id (test-insert 100)
         (length (mongo-cursor-all!
                  (mongo-node-find *node* *dn* *cn* (% "test-id" id))))))

(test* "mongo-cursor-count" 100
       (let1 id (test-insert 100)
         (mongo-cursor-count
          (mongo-node-find *node* *dn* *cn* (% "test-id" id)))))

;;;; delete

(test-section "delete")

(test* "mongo-node-delete" (undefined)
       (test-delete (% "test-id" (test-insert 1))))

(test* "mongo-node-delete safe" #t
       (ok? (test-delete (% "test-id" (test-insert 1)) :safe #t)))

(test* "mongo-node-delete journal" #t
       (ok? (test-delete (% "test-id" (test-insert 1)) :safe #t :j #t)))

(test* "mongo-node-delete fsync" #t
       (let1 res (test-delete (% "test-id" (test-insert 1)) :safe #t :fsync #t)
         (and (ok? res)
              (number? (alref res "waited")))))

(test* "mongo-node-delete w" #t
       (let1 res (test-delete (% "test-id" (test-insert 1)) :safe #t :w 1)
           (and (ok? res)
                (number? (alref res "wtime")))))

(test* "mongo-node-delete single-remove #t" 1
       (let1 id (test-insert 2)
         (test-delete (% "test-id" id) :single-remove #t :safe #t :w 1)
         (length (test-docs "test-id" id))))

(test* "mongo-node-delete single-remove #f" 0
       (let1 id (test-insert 2)
         (test-delete (% "test-id" id) :single-remove #f :safe #t :w 1)
         (length (test-docs "test-id" id))))

(test* "mongo-node-delete thread safe" #t
       (let1 id (test-insert 100)
         (and (every ok?
                     (thread-map
                      (^[i] (test-delete (% "test-id" id "i" i) :safe #t :w 1))
                      (iota 100)))
              (= 0 (length (test-docs "test-id" id))))))

;;;; helper

(test-section "helper")

(test* "mongo-node-command" #t
       (ok? (mongo-node-command *node* "admin" (% "ping" 1))))

(test* "mongo-node-admin" #t
       (ok? (mongo-node-admin *node* (% "ping" 1))))

(test* "mongo-node-ping" #t
       (ok? (mongo-node-ping *node*)))

(test* "mongo-node-round-trip" #t
       (number? (mongo-node-round-trip *node*)))

(test* "mongo-node-ismaster" #t
       (let1 res (mongo-node-ismaster *node*)
         (and (ok? res)
              (bson-true? (alref res "ismaster")))))

(test* "mongo-node-server-status" #t
       (let1 res (mongo-node-server-status *node*)
         (and (ok? res)
              (string? (alref res "host"))
              (string? (alref res "version")))))

(test* "mongo-node-replset-status" #t
       (guard (e [(<mongo-request-error> e) #t])
         (mongo-node-replset-status *node*)))

(test* "mongo-node-show-databases" #t
       (let1 res (mongo-node-show-databases *node*)
         (and (ok? res)
              (vector? (alref res "databases")))))

(test* "mongo-node-drop-database" #t
       (ok? (mongo-node-drop-database *node* *dn*)))

(test* "mongo-node-create-collection" #t
       (ok? (mongo-node-create-collection
             *node* *dn* "create_collection_test_collection")))

(test* "mongo-node-show-collections" #t
       (every (^[doc] (string? (alref doc "name")))
              (mongo-node-show-collections *node* *dn*)))

(test* "mongo-node-drop-collection" #t
       (ok? (mongo-node-drop-collection
             *node* *dn* "create_collection_test_collection")))

(test* "mongo-node-get-last-error" #t
       (ok? (mongo-node-get-last-error *node* *dn*)))

(test* "mongo-node-reset-error" #t
       (ok? (mongo-node-reset-error *node* *dn*)))

(test* "mongo-node-ensure-index" #t
       (ok? (mongo-node-ensure-index *node* *dn* *cn* *index-name* (% "i" 1))))

(test* "mongo-node-show-indexes" #t
       (every (^[doc] (string? (alref doc "name")))
              (mongo-node-show-indexes *node* *dn* *cn*)))

(test* "mongo-node-drop-index" #t
       (ok? (mongo-node-drop-index *node* *dn* *cn* *index-name*)))

(test* "mongo-node-drop-indexes" #t
       (ok? (mongo-node-drop-indexes *node* *dn* *cn*)))

(test* "mongo-node-reindex" #t
       (ok? (mongo-node-reindex *node* *dn* *cn*)))

(test* "mongo-node-profiling-status" #t
       (ok? (mongo-node-profiling-status *node* *dn*)))

(test* "mongo-node-get-profiling-level" #t
       (number? (mongo-node-get-profiling-level *node* *dn*)))

(test* "mongo-node-set-profiling-level" #t
       (ok? (mongo-node-set-profiling-level *node* *dn* 2)))

(test* "mongo-node-show-profiling" #t
       (list? (mongo-node-show-profiling *node* *dn*)))

(test* "mongo-node-add-user" #t
       (ok? (mongo-node-add-user *node* *dn* *user* *pass* :safe #t)))

(test* "mongo-node-auth" #t
       (ok? (mongo-node-auth *node* *dn* *user* *pass*)))

(test* "mongo-node-reauth" #t
       (every (^[pair] (ok? (cdr pair)))
              (mongo-node-reauth *node*)))

(test* "mongo-node-remove-user" #t
       (ok? (mongo-node-remove-user *node* *dn* *user* :safe #t)))

(test* "mongo-node-distinct" #t
       (let1 doc (mongo-node-distinct *node* *dn* *cn* "_id")
         (and (ok? doc)
              (vector? (alref doc "values")))))

(test* "mongo-node-dbref-get" #t
       (let* ([oid (bson-object-id)]
              [ref (% "$ref" *cn* "$id" oid "$db" *dn*)]
              [doc (% "_id" oid "foo" "bar")])
         (and (ok? (mongo-node-insert *node* *dn* *cn* (list doc):safe #t :w 1))
              (equal? doc (mongo-node-dbref-get *node* "foo" ref)))))

(test-end)
