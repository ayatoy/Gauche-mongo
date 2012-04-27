(use gauche.test)
(use gauche.threads)
(use gauche.uvector)
(use binary.io)
(use srfi-1)
(use mongo.util)
(use mongo.bson)
(use mongo.wire)
(use mongo.node)

(define (thread-map proc xs)
  (map thread-join!
       (map (^[x] (thread-start! (make-thread (^[] (proc x)))))
            xs)))

(define (ok? doc)
  (not (zero? (alref doc "ok"))))

(define *hosts* '("localhost:27017"
                  "localhost:27018"
                  "localhost:27019"
                  "localhost:27020"))
(define *rn* "gauche_mongo_test_replica_set")
(define *dn* "gauche_mongo_test_database")
(define *cn* "gauche_mongo_test_collection")
(define *in* "gauche_mongo_test_index")
(define *user* "gauche_mongo_test_user")
(define *pass* "gauche_mongo_test_pass")
(define *single* #f)
(define *single-db* #f)
(define *single-col* #f)
(define *rs* #f)
(define *rs-db* #f)
(define *rs-col* #f)

;;;; test

(test-start "core")

(use mongo.core)
(test-module 'mongo.core)

;;;; single

(test-section "single")

(test* "mongo" #t
       (begin (set! *single* (mongo "localhost:27017")) #t))

(test* "mongo?" #t
       (mongo? *single*))

(test* "mongo-master" #t
       (mongo-node? (mongo-master *single*)))

(test* "mongo-slave" #f
       (mongo-slave *single*))

(test* "mongo-hosts" #t
       (every mongo-address-inet? (mongo-hosts *single*)))

(test* "mongo-name" #f
       (mongo-name *single*))

(test* "mongo-timeout" 5000
       (mongo-timeout *single*))

(test* "mongo-timeout-set!" 10000
       (begin (mongo-timeout-set! *single* 10000)
              (mongo-timeout *single*)))

(test* "mongo-locking" 10100
       (begin (thread-map
               (^[i] (mongo-locking *single*
                       (mongo-timeout-set! *single*
                                           (+ (mongo-timeout *single*) 1))))
               (iota 100))
              (mongo-timeout *single*)))

(test* "mongo-replica-set?" #f
       (mongo-replica-set? *single*))

(test* "mongo-connect?" #t
       (mongo-connect? *single*))

(test* "mongo-disconnect!" #t
       (and (mongo-connect? *single*)
            (begin (mongo-disconnect! *single*) #t)
            (not (mongo-connect? *single*))))

(test* "mongo-sync!" #t
       (and (not (mongo-connect? *single*))
            (begin (mongo-sync! *single*) #t)
            (mongo-connect? *single*)))

(test* "mongo-available!" #t
       (and (begin (mongo-disconnect! *single*) #t)
            (not (mongo-connect? *single*))
            (begin (mongo-available! *single*) #t)
            (mongo-connect? *single*)))

(test* "mongo-ref" #t
       (eq? (mongo-ref *single* :slave #f)
            (mongo-ref *single* :slave #t)))

(test* "mongo-admin" #t
       (ok? (mongo-admin *single* (% "ping" 1))))

(test* "mongo-ping" #t
       (ok? (mongo-ping *single*)))

(test* "mongo-ismaster" #t
       (ok? (mongo-ismaster *single*)))

(test* "mongo-server-status" #t
       (ok? (mongo-server-status *single*)))

(test* "mongo-replset-status" (test-error <mongo-request-error>)
       (mongo-replset-status *single*))

(test* "mongo-show-databases" #t
       (ok? (mongo-show-databases *single*)))

(test* "mongo-database" #t
       (begin (set! *single-db* (mongo-database *single* *dn*)) #t))

(test* "mongo-database?" #t
       (mongo-database? *single-db*))

(test* "mongo-database-server" #t
       (mongo? (mongo-database-server *single-db*)))

(test* "mongo-database-name" *dn*
       (mongo-database-name *single-db*))

(test* "mongo-command" #t
       (ok? (mongo-command *single-db* (% "ping" 1))))

(test* "mongo-drop-database" #t
       (ok? (mongo-drop-database *single-db*)))

(test* "mongo-get-last-error" #t
       (ok? (mongo-get-last-error *single-db*)))

(test* "mongo-reset-error" #t
       (ok? (mongo-reset-error *single-db*)))

(test* "mongo-show-collections" #t
       (list? (mongo-show-collections *single-db*)))

(test* "mongo-profiling-status" #t
       (ok? (mongo-profiling-status *single-db*)))

(test* "mongo-get-profiling-level" #t
       (number? (mongo-get-profiling-level *single-db*)))

(test* "mongo-set-profiling-level" #t
       (ok? (mongo-set-profiling-level *single-db* 1)))

(test* "mongo-show-profiling" #t
       (list? (mongo-show-profiling *single-db*)))

(test* "mongo-auth" (test-error <mongo-request-error>)
       (mongo-auth *single-db* *user* *pass*))

(test* "mongo-add-user" #t
       (ok? (mongo-add-user *single-db* *user* *pass* :safe #t)))

(test* "mongo-remove-user" #t
       (ok? (mongo-remove-user *single-db* *user* :safe #t)))

(test* "mongo-collection" #t
       (begin (set! *single-col* (mongo-collection *single-db* *cn*)) #t))

(test* "mongo-collection-database" #t
       (mongo-database? (mongo-collection-database *single-col*)))

(test* "mongo-collection-name" *cn*
       (mongo-collection-name *single-col*))

(test* "mongo-create-collection" #t
       (ok? (mongo-create-collection *single-col*)))

(test* "mongo-drop-collection" #t
       (ok? (mongo-drop-collection *single-col*)))

(test* "mongo-insert1" #t
       (ok? (mongo-insert1 *single-col*
                           (% "x" "foo")
                           :safe #t)))

(test* "mongo-insert" #t
       (ok? (mongo-insert *single-col*
                          (list (% "x" "bar") (% "x" "baz"))
                          :safe #t)))

(test* "mongo-find1" "foo"
       (alref (mongo-find1 *single-col* (% "x" "foo")) "x"))

(test* "mongo-find" #t
       (mongo-cursor? (mongo-find *single-col* '())))

(test* "mongo-update" #t
       (ok? (mongo-update *single-col*
                          (% "x" "FOO")
                          (% "$set" (% "x" "Foo"))
                          :safe #t)))

(test* "mongo-delete" #t
       (ok? (mongo-delete *single-col*
                          (% "x" (% "$exists" 'true))
                          :safe #t)))

(test* "mongo-ensure-index" #t
       (ok? (mongo-ensure-index *single-col* *in* (% "x" 1) :safe #t)))

(test* "mongo-show-indexes" #t
       (list? (mongo-show-indexes *single-col*)))

(test* "mongo-drop-index" #t
       (ok? (mongo-drop-index *single-col* *in*)))

(test* "mongo-drop-indexes" #t
       (ok? (mongo-drop-indexes *single-col*)))

(test* "mongo-reindex" #t
       (ok? (mongo-reindex *single-col*)))

(test* "mongo-distinct" #t
       (ok? (mongo-distinct *single-col* "x")))

(test* "mongo-map-reduce" #t
       (ok? (mongo-map-reduce *single-col*
                              (bson-code "function() { emit(this.i, 1); }")
                              (bson-code "function(k, vals) { return 1; }")
                              :out (% "inline" 1))))

(test* "mongo-dbref? 1" #t
       (mongo-dbref? '(("$ref" . "foo") ("$id" . "bar"))))

(test* "mongo-dbref? 2" #t
       (mongo-dbref? '(("$ref" . "foo") ("$id" . "bar") ("$db" . "baz"))))

(test* "mongo-dbref 1" '(("$ref" . "foo") ("$id" . "bar"))
       (mongo-dbref "foo" "bar"))

(test* "mongo-dbref 2" '(("$ref" . "foo") ("$id" . "bar") ("$db" . "baz"))
       (mongo-dbref "foo" "bar" "baz"))

(test* "mongo-dbref-get" #f
       (mongo-dbref-get *single-db* (mongo-dbref "foo" "bar" "baz")))

;;;; replica-set

(test-section "replica-set")

(test* "mongo" #t
       (begin (set! *rs* (mongo "localhost:27018,localhost:27019,localhost:27010/?replicaset=gauche_mongo_test_replica_set")) #t))

(test* "mongo?" #t
       (mongo? *rs*))

(test* "mongo-master" #t
       (mongo-node? (mongo-master *rs*)))

(test* "mongo-slave" #t
       (mongo-node? (mongo-slave *rs*)))

(test* "mongo-hosts" #t
       (every mongo-address-inet? (mongo-hosts *rs*)))

(test* "mongo-name" *rn*
       (mongo-name *rs*))

(test* "mongo-timeout" 5000
       (mongo-timeout *rs*))

(test* "mongo-timeout-set!" 10000
       (begin (mongo-timeout-set! *rs* 10000)
              (mongo-timeout *rs*)))

(test* "mongo-locking" 10100
       (begin (thread-map
               (^[i] (mongo-locking *rs*
                       (mongo-timeout-set! *rs*
                                           (+ (mongo-timeout *rs*) 1))))
               (iota 100))
              (mongo-timeout *rs*)))

(test* "mongo-replica-set?" #t
       (mongo-replica-set? *rs*))

(test* "mongo-connect?" #t
       (mongo-connect? *rs*))

(test* "mongo-disconnect!" #t
       (and (mongo-connect? *rs*)
            (begin (mongo-disconnect! *rs*) #t)
            (not (mongo-connect? *rs*))))

(test* "mongo-sync!" #t
       (and (not (mongo-connect? *rs*))
            (begin (mongo-sync! *rs*) #t)
            (mongo-connect? *rs*)))

(test* "mongo-available!" #t
       (and (begin (mongo-disconnect! *rs*) #t)
            (not (mongo-connect? *rs*))
            (begin (mongo-available! *rs*) #t)
            (mongo-connect? *rs*)))

(test* "mongo-ref" #f
       (eq? (mongo-ref *rs* :slave #f)
            (mongo-ref *rs* :slave #t)))

(test* "mongo-admin" #t
       (ok? (mongo-admin *rs* (% "ping" 1))))

(test* "mongo-ping" #t
       (ok? (mongo-ping *rs*)))

(test* "mongo-ismaster" #t
       (ok? (mongo-ismaster *rs*)))

(test* "mongo-server-status" #t
       (ok? (mongo-server-status *rs*)))

(test* "mongo-replset-status" #t
       (ok? (mongo-replset-status *rs*)))

(test* "mongo-show-databases" #t
       (ok? (mongo-show-databases *rs*)))

(test* "mongo-database" #t
       (begin (set! *rs-db* (mongo-database *rs* *dn*)) #t))

(test* "mongo-database?" #t
       (mongo-database? *rs-db*))

(test* "mongo-database-server" #t
       (mongo? (mongo-database-server *rs-db*)))

(test* "mongo-database-name" *dn*
       (mongo-database-name *rs-db*))

(test* "mongo-command" #t
       (ok? (mongo-command *rs-db* (% "ping" 1))))

(test* "mongo-drop-database" #t
       (ok? (mongo-drop-database *rs-db*)))

(test* "mongo-get-last-error" #t
       (ok? (mongo-get-last-error *rs-db*)))

(test* "mongo-reset-error" #t
       (ok? (mongo-reset-error *rs-db*)))

(test* "mongo-show-collections" #t
       (list? (mongo-show-collections *rs-db*)))

(test* "mongo-profiling-status" #t
       (ok? (mongo-profiling-status *rs-db*)))

(test* "mongo-get-profiling-level" #t
       (number? (mongo-get-profiling-level *rs-db*)))

(test* "mongo-set-profiling-level" #t
       (ok? (mongo-set-profiling-level *rs-db* 1)))

(test* "mongo-show-profiling" #t
       (list? (mongo-show-profiling *rs-db*)))

(test* "mongo-auth" (test-error <mongo-request-error>)
       (mongo-auth *rs-db* *user* *pass*))

(test* "mongo-add-user" #t
       (ok? (mongo-add-user *rs-db* *user* *pass* :safe #t)))

(test* "mongo-remove-user" #t
       (ok? (mongo-remove-user *rs-db* *user* :safe #t)))

(test* "mongo-collection" #t
       (begin (set! *rs-col* (mongo-collection *rs-db* *cn*)) #t))

(test* "mongo-collection-database" #t
       (mongo-database? (mongo-collection-database *rs-col*)))

(test* "mongo-collection-name" *cn*
       (mongo-collection-name *rs-col*))

(test* "mongo-create-collection" #t
       (ok? (mongo-create-collection *rs-col*)))

(test* "mongo-drop-collection" #t
       (ok? (mongo-drop-collection *rs-col*)))

(test* "mongo-insert1" #t
       (ok? (mongo-insert1 *rs-col*
                           (% "x" "foo")
                           :safe #t)))

(test* "mongo-insert" #t
       (ok? (mongo-insert *rs-col*
                          (list (% "x" "bar") (% "x" "baz"))
                          :safe #t)))

(test* "mongo-find1" "foo"
       (alref (mongo-find1 *rs-col* (% "x" "foo") :slave #f) "x"))

(test* "mongo-find" #t
       (mongo-cursor? (mongo-find *rs-col* '())))

(test* "mongo-update" #t
       (ok? (mongo-update *rs-col*
                          (% "x" "FOO")
                          (% "$set" (% "x" "Foo"))
                          :safe #t)))

(test* "mongo-delete" #t
       (ok? (mongo-delete *rs-col*
                          (% "x" (% "$exists" 'true))
                          :safe #t)))

(test* "mongo-ensure-index" #t
       (ok? (mongo-ensure-index *rs-col* *in* (% "x" 1) :safe #t)))

(test* "mongo-show-indexes" #t
       (list? (mongo-show-indexes *rs-col*)))

(test* "mongo-drop-index" #t
       (ok? (mongo-drop-index *rs-col* *in*)))

(test* "mongo-drop-indexes" #t
       (ok? (mongo-drop-indexes *rs-col*)))

(test* "mongo-reindex" #t
       (ok? (mongo-reindex *rs-col*)))

(test* "mongo-distinct" #t
       (ok? (mongo-distinct *rs-col* "x")))

(test* "mongo-map-reduce" #t
       (ok? (mongo-map-reduce *rs-col*
                              (bson-code "function() { emit(this.i, 1); }")
                              (bson-code "function(k, vals) { return 1; }")
                              :out (% "inline" 1))))

(test* "mongo-dbref? 1" #t
       (mongo-dbref? '(("$ref" . "foo") ("$id" . "bar"))))

(test* "mongo-dbref? 2" #t
       (mongo-dbref? '(("$ref" . "foo") ("$id" . "bar") ("$db" . "baz"))))

(test* "mongo-dbref 1" '(("$ref" . "foo") ("$id" . "bar"))
       (mongo-dbref "foo" "bar"))

(test* "mongo-dbref 2" '(("$ref" . "foo") ("$id" . "bar") ("$db" . "baz"))
       (mongo-dbref "foo" "bar" "baz"))

(test* "mongo-dbref-get" #f
       (mongo-dbref-get *rs-db* (mongo-dbref "foo" "bar" "baz")))

(test-end)
