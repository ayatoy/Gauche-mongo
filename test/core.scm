(use gauche.test)
(use gauche.threads)
(use gauche.uvector)
(use binary.io)
(use mongo.util)
(use mongo.bson)
(use mongo.wire)
(use mongo.node)

;;;; prep

(define *hosts*  '("localhost"))
(define *hosts1* '("localhost:27018" "localhost:27019" "localhost:27020"))
(define *hosts2* '("localhost:27021" "localhost:27022" "localhost:27023"))
(define *rs*     "gauche_mongo_test_replica_set")
(define *dn*     "gauche_mongo_test_database")
(define *cn*     "gauche_mongo_test_collection")
(define *index*  "gauche_mongo_test_index")
(define *user*   "gauche_mongo_test_user")
(define *pass*   "gauche_mongo_test_pass")
(define *s*      #f)
(define *s-db*   #f)
(define *s-col*  #f)
(define *r*      #f)
(define *r-db*   #f)
(define *r-col*  #f)

(define (mt-map proc xs)
  (map thread-join!
       (map (^[x] (thread-start! (make-thread (^[] (proc x))))) xs)))

;;;; test

(test-start "core")

(use mongo.core)
(test-module 'mongo.core)

;;;; single

(test-section "single")

(test* "mongo" <mongo>
       (begin (set! *s* (mongo "localhost:27017"))
              (class-of *s*)))

(test* "mongo?" #t
       (mongo? *s*))

(test* "mongo-master" #t
       (mongo-node? (mongo-master *s*)))

(test* "mongo-slave" #f
       (mongo-slave *s*))

(test* "mongo-hosts" #t
       (every mongo-address-inet? (mongo-hosts *s*)))

(test* "mongo-name" #f
       (mongo-name *s*))

(test* "mongo-timeout" 5000
       (mongo-timeout *s*))

(test* "mongo-timeout-set!" 10000
       (begin (mongo-timeout-set! *s* 10000)
              (mongo-timeout *s*)))

(test* "mongo-locking" 10100
       (begin (mt-map
               (^[i] (mongo-locking *s*
                       (mongo-timeout-set! *s*
                                           (+ (mongo-timeout *s*) 1))))
               (iota 100))
              (mongo-timeout *s*)))

(test* "mongo-single?" #t
       (mongo-single? *s*))

(test* "mongo-replica-set?" #f
       (mongo-replica-set? *s*))

(test* "mongo-connect?" #t
       (mongo-connect? *s*))

(test* "mongo-disconnect!" #t
       (begin (mongo-disconnect! *s*) #t))

(test* "mongo-connect?" #f
       (mongo-connect? *s*))

(test* "mongo-sync!" #t
       (begin (mongo-sync! *s*) #t))

(test* "mongo-connect?" #t
       (mongo-connect? *s*))

(test* "mongo-disconnect!" #t
       (begin (mongo-disconnect! *s*) #t))

(test* "mongo-connect?" #f
       (mongo-connect? *s*))

(test* "mongo-available!" #t
       (begin (mongo-available! *s*) #t))

(test* "mongo-connect?" #t
       (mongo-connect? *s*))

(test* "mongo-ref" #t
       (eq? (mongo-ref *s* :slave #f)
            (mongo-ref *s* :slave #t)))

(test* "mongo-admin" #t
       (mongo-ok? (mongo-admin *s* '(("ping" . 1)))))

(test* "mongo-ping" #t
       (mongo-ok? (mongo-ping *s*)))

(test* "mongo-ismaster" #t
       (mongo-ok? (mongo-ismaster *s*)))

(test* "mongo-server-status" #t
       (mongo-ok? (mongo-server-status *s*)))

(test* "mongo-replset-status" (test-error <mongo-request-error>)
       (mongo-ok? (mongo-replset-status *s*)))

(test* "mongo-show-databases" #t
       (mongo-ok? (mongo-show-databases *s*)))

(test* "mongo-database" <mongo-database>
       (begin (set! *s-db* (mongo-database *s* *dn*))
              (class-of *s-db*)))

(test* "mongo-database validation" (test-error <mongo-validation-error>)
       (mongo-database *s* "$foo"))

(test* "mongo-database?" #t
       (mongo-database? *s-db*))

(test* "mongo-database-server" #t
       (mongo? (mongo-database-server *s-db*)))

(test* "mongo-database-name" *dn*
       (mongo-database-name *s-db*))

(test* "mongo-command" #t
       (mongo-ok? (mongo-command *s-db* '(("ping" . 1)))))

(test* "mongo-drop-database" #t
       (mongo-ok? (mongo-drop-database *s-db*)))

(test* "mongo-get-last-error" #t
       (mongo-ok? (mongo-get-last-error *s-db*)))

(test* "mongo-reset-error" #t
       (mongo-ok? (mongo-reset-error *s-db*)))

(test* "mongo-show-collections" #t
       (every bson-document? (mongo-show-collections *s-db*)))

(test* "mongo-profiling-status" #t
       (mongo-ok? (mongo-profiling-status *s-db*)))

(test* "mongo-get-profiling-level" #t
       (number? (mongo-get-profiling-level *s-db*)))

(test* "mongo-set-profiling-level" #t
       (mongo-ok? (mongo-set-profiling-level *s-db* 1)))

(test* "mongo-show-profiling" #t
       (every bson-document? (mongo-show-profiling *s-db*)))

(test* "mongo-auth" (test-error <mongo-request-error>)
       (mongo-auth *s-db* *user* *pass*))

(test* "mongo-add-user" #t
       (mongo-ok? (mongo-add-user *s-db* *user* *pass* :safe #t)))

(test* "mongo-remove-user" #t
       (mongo-ok? (mongo-remove-user *s-db* *user* :safe #t)))

(test* "mongo-add-function" #t
       (mongo-ok? (mongo-add-function
                   *s-db*
                   "func"
                   (bson-code "function(x){return x;}")
                   :safe #t)))

(test* "mongo-eval" "foo"
       (mongo-eval *s-db* "func('foo')"))

(test* "mongo-remove-function" #t
       (mongo-ok? (mongo-remove-function *s-db* "func" :safe #t)))

(test* "mongo-eval" (test-error <mongo-request-error>)
       (mongo-eval *s-db* "func('foo')"))

(test* "mongo-collection" <mongo-collection>
       (begin (set! *s-col* (mongo-collection *s-db* *cn*))
              (class-of *s-col*)))

(test* "mongo-collection validation" (test-error <mongo-validation-error>)
       (mongo-collection *s-db* "$foo"))

(test* "mongo-collection-database" #t
       (mongo-database? (mongo-collection-database *s-col*)))

(test* "mongo-collection-name" *cn*
       (mongo-collection-name *s-col*))

(test* "mongo-create-collection" #t
       (mongo-ok? (mongo-create-collection *s-col*)))

(test* "mongo-drop-collection" #t
       (mongo-ok? (mongo-drop-collection *s-col*)))

(test* "mongo-insert1" #t
       (mongo-ok? (mongo-insert1 *s-col* '(("x" . "foo")) :safe #t)))

(test* "mongo-insert" #t
       (mongo-ok? (mongo-insert *s-col*
                                '((("x" . "bar")) (("x" . "baz")))
                                :safe #t)))

(test* "mongo-find1" "foo"
       (assoc-ref (mongo-find1 *s-col* '(("x" . "foo")))
                  "x"))

(test* "mongo-find" #t
       (every bson-document?
              (mongo-find *s-col* '(("x" . (("$exists" . true)))))))

(test* "mongo-find" #t
       (mongo-cursor? (mongo-find *s-col* '(("x" . (("$exists" . true))))
                                  :cursor #t)))

(test* "mongo-find" #t
       (every bson-document?
              (mongo-cursor-all!
               (mongo-find *s-col* '(("x" . (("$exists" . true))))
                           :cursor #t))))

(test* "mongo-update1" #t
       (mongo-ok? (mongo-update1 *s-col*
                                 '(("x" . "FOO"))
                                 '(("$set" . (("x" . "Foo"))))
                                 :safe #t)))

(test* "mongo-update" #t
       (mongo-ok? (mongo-update *s-col*
                                '(("x" . "FOO"))
                                '(("$set" . (("x" . "Foo"))))
                                :safe #t)))

(test* "mongo-upsert1" #t
       (mongo-ok? (mongo-upsert1 *s-col*
                                 '(("x" . "FOO"))
                                 '(("$set" . (("x" . "Foo"))))
                                 :safe #t)))

(test* "mongo-upsert" #t
       (mongo-ok? (mongo-upsert *s-col*
                                '(("x" . "FOO"))
                                '(("$set" . (("x" . "Foo"))))
                                :safe #t)))

(test* "mongo-delete1" #t
       (mongo-ok? (mongo-delete1 *s-col*
                                 '(("x" . (("$exists" . true))))
                                 :safe #t)))

(test* "mongo-delete" #t
       (mongo-ok? (mongo-delete *s-col*
                                '(("x" . (("$exists" . true))))
                                :safe #t)))

(test* "mongo-save" #t
       (mongo-ok? (mongo-save *s-col*
                              `(("x" . "foo"))
                              :safe #t)))

(test* "mongo-ensure-index" #t
       (mongo-ok?
        (mongo-ensure-index *s-col* '(("x" . 1)) :name *index* :safe #t)))

(test* "mongo-show-indexes" #t
       (every bson-document? (mongo-show-indexes *s-col*)))

(test* "mongo-drop-index" #t
       (mongo-ok? (mongo-drop-index *s-col* *index*)))

(test* "mongo-drop-indexes" #t
       (mongo-ok? (mongo-drop-indexes *s-col*)))

(test* "mongo-reindex" #t
       (mongo-ok? (mongo-reindex *s-col*)))

(test* "mongo-count" #t
       (number? (mongo-count *s-col*)))

(test* "mongo-distinct" #t
       (vector? (mongo-distinct *s-col* "x")))

(test* "mongo-group" #t
       (vector? (mongo-group *s-col*
                             '()
                             (bson-code "function(doc,acc){acc.cnt++;}")
                             '(("cnt" . 0)))))

(test* "mongo-map-reduce" #t
       (mongo-ok?
        (mongo-map-reduce *s-col*
                          (bson-code "function() { emit(this.i, 1); }")
                          (bson-code "function(k, vals) { return 1; }")
                          :out '(("inline" . 1)))))

(test* "mongo-dbref? 1" #t
       (mongo-dbref? '(("$ref" . "foo") ("$id" . "bar"))))

(test* "mongo-dbref? 2" #t
       (mongo-dbref? '(("$ref" . "foo") ("$id" . "bar") ("$db" . "baz"))))

(test* "mongo-dbref 1" '(("$ref" . "foo") ("$id" . "bar"))
       (mongo-dbref "foo" "bar"))

(test* "mongo-dbref 2" '(("$ref" . "foo") ("$id" . "bar") ("$db" . "baz"))
       (mongo-dbref "foo" "bar" "baz"))

(test* "mongo-dbref-get" #f
       (mongo-dbref-get *s-db* (mongo-dbref "foo" "bar" "baz")))

;;;; replica-set

(test-section "replica-set")

(test* "mongo" <mongo>
       (begin (set! *r* (mongo "localhost:27018,localhost:27019,localhost:27010/?replicaset=gauche_mongo_test_replica_set"))
              (class-of *r*)))

(test* "mongo?" #t
       (mongo? *r*))

(test* "mongo-master" #t
       (mongo-node? (mongo-master *r*)))

(test* "mongo-slave" #t
       (mongo-node? (mongo-slave *r*)))

(test* "mongo-hosts" #t
       (every mongo-address-inet? (mongo-hosts *r*)))

(test* "mongo-name" *rs*
       (mongo-name *r*))

(test* "mongo-timeout" 5000
       (mongo-timeout *r*))

(test* "mongo-timeout-set!" 10000
       (begin (mongo-timeout-set! *r* 10000)
              (mongo-timeout *r*)))

(test* "mongo-locking" 10100
       (begin (mt-map
               (^[i] (mongo-locking *r*
                       (mongo-timeout-set! *r*
                                           (+ (mongo-timeout *r*) 1))))
               (iota 100))
              (mongo-timeout *r*)))

(test* "mongo-single?" #f
       (mongo-single? *r*))

(test* "mongo-replica-set?" #t
       (mongo-replica-set? *r*))

(test* "mongo-connect?" #t
       (mongo-connect? *r*))

(test* "mongo-disconnect!" #t
       (begin (mongo-disconnect! *r*) #t))

(test* "mongo-connect?" #f
       (mongo-connect? *r*))

(test* "mongo-sync!" #t
       (begin (mongo-sync! *r*) #t))

(test* "mongo-connect?" #t
       (mongo-connect? *r*))

(test* "mongo-disconnect!" #t
       (begin (mongo-disconnect! *r*) #t))

(test* "mongo-connect?" #f
       (mongo-connect? *r*))

(test* "mongo-available!" #t
       (begin (mongo-available! *r*) #t))

(test* "mongo-connect?" #t
       (mongo-connect? *r*))

(test* "mongo-ref" #f
       (eq? (mongo-ref *r* :slave #f)
            (mongo-ref *r* :slave #t)))

(test* "mongo-admin" #t
       (mongo-ok? (mongo-admin *r* '(("ping" . 1)))))

(test* "mongo-ping" #t
       (mongo-ok? (mongo-ping *r*)))

(test* "mongo-ismaster" #t
       (mongo-ok? (mongo-ismaster *r*)))

(test* "mongo-server-status" #t
       (mongo-ok? (mongo-server-status *r*)))

(test* "mongo-replset-status" #t
       (mongo-ok? (mongo-replset-status *r*)))

(test* "mongo-show-databases" #t
       (mongo-ok? (mongo-show-databases *r*)))

(test* "mongo-database" <mongo-database>
       (begin (set! *r-db* (mongo-database *r* *dn*))
              (class-of *r-db*)))

(test* "mongo-database validation" (test-error <mongo-validation-error>)
       (mongo-database *r* "$foo"))

(test* "mongo-database?" #t
       (mongo-database? *r-db*))

(test* "mongo-database-server" #t
       (mongo? (mongo-database-server *r-db*)))

(test* "mongo-database-name" *dn*
       (mongo-database-name *r-db*))

(test* "mongo-command" #t
       (mongo-ok? (mongo-command *r-db* '(("ping" . 1)))))

(test* "mongo-drop-database" #t
       (mongo-ok? (mongo-drop-database *r-db*)))

(test* "mongo-get-last-error" #t
       (mongo-ok? (mongo-get-last-error *r-db*)))

(test* "mongo-reset-error" #t
       (mongo-ok? (mongo-reset-error *r-db*)))

(test* "mongo-show-collections" #t
       (every bson-document? (mongo-show-collections *r-db*)))

(test* "mongo-profiling-status" #t
       (mongo-ok? (mongo-profiling-status *r-db*)))

(test* "mongo-get-profiling-level" #t
       (number? (mongo-get-profiling-level *r-db*)))

(test* "mongo-set-profiling-level" #t
       (mongo-ok? (mongo-set-profiling-level *r-db* 1)))

(test* "mongo-show-profiling" #t
       (every bson-document? (mongo-show-profiling *r-db*)))

(test* "mongo-auth" (test-error <mongo-request-error>)
       (mongo-auth *r-db* *user* *pass*))

(test* "mongo-add-user" #t
       (mongo-ok? (mongo-add-user *r-db* *user* *pass* :safe #t)))

(test* "mongo-remove-user" #t
       (mongo-ok? (mongo-remove-user *r-db* *user* :safe #t)))

(test* "mongo-add-function" #t
       (mongo-ok? (mongo-add-function
                   *r-db*
                   "func"
                   (bson-code "function(x){return x;}")
                   :safe #t)))

(test* "mongo-eval" "foo"
       (mongo-eval *r-db* "func('foo')"))

(test* "mongo-remove-function" #t
       (mongo-ok? (mongo-remove-function *r-db* "func" :safe #t)))

(test* "mongo-eval" (test-error <mongo-request-error>)
       (mongo-eval *r-db* "func('foo')"))

(test* "mongo-collection" <mongo-collection>
       (begin (set! *r-col* (mongo-collection *r-db* *cn*))
              (class-of *r-col*)))

(test* "mongo-collection validation" (test-error <mongo-validation-error>)
       (mongo-collection *r-db* "$foo"))

(test* "mongo-collection-database" #t
       (mongo-database? (mongo-collection-database *r-col*)))

(test* "mongo-collection-name" *cn*
       (mongo-collection-name *r-col*))

(test* "mongo-create-collection" #t
       (mongo-ok? (mongo-create-collection *r-col*)))

(test* "mongo-drop-collection" #t
       (mongo-ok? (mongo-drop-collection *r-col*)))

(test* "mongo-insert1" #t
       (mongo-ok? (mongo-insert1 *r-col* '(("x" . "foo")) :safe #t)))

(test* "mongo-insert" #t
       (mongo-ok? (mongo-insert *r-col*
                                '((("x" . "bar")) (("x" . "baz")))
                                :safe #t)))

(test* "mongo-find1" "foo"
       (assoc-ref (mongo-find1 *r-col* '(("x" . "foo")) :slave #f)
                  "x"))

(test* "mongo-find" #t
       (every bson-document?
              (mongo-find *r-col* '(("x" . (("$exists" . true)))))))

(test* "mongo-find" #t
       (mongo-cursor? (mongo-find *r-col* '(("x" . (("$exists" . true))))
                                  :cursor #t)))

(test* "mongo-find" #t
       (every bson-document?
              (mongo-cursor-all!
               (mongo-find *r-col* '(("x" . (("$exists" . true))))
                           :cursor #t))))

(test* "mongo-update1" #t
       (mongo-ok? (mongo-update1 *r-col*
                                 '(("x" . "FOO"))
                                 '(("$set" . (("x" . "Foo"))))
                                 :safe #t)))

(test* "mongo-update" #t
       (mongo-ok? (mongo-update *r-col*
                                '(("x" . "FOO"))
                                '(("$set" . (("x" . "Foo"))))
                                :safe #t)))

(test* "mongo-upsert1" #t
       (mongo-ok? (mongo-upsert1 *r-col*
                                 '(("x" . "FOO"))
                                 '(("$set" . (("x" . "Foo"))))
                                 :safe #t)))

(test* "mongo-upsert" #t
       (mongo-ok? (mongo-upsert *r-col*
                                '(("x" . "FOO"))
                                '(("$set" . (("x" . "Foo"))))
                                :safe #t)))

(test* "mongo-delete1" #t
       (mongo-ok? (mongo-delete1 *r-col*
                                 '(("x" . (("$exists" . true))))
                                 :safe #t)))

(test* "mongo-delete" #t
       (mongo-ok? (mongo-delete *r-col*
                                '(("x" . (("$exists" . true))))
                                :safe #t)))

(test* "mongo-save" #t
       (mongo-ok? (mongo-save *r-col*
                              `(("x" . "foo"))
                              :safe #t)))

(test* "mongo-ensure-index" #t
       (mongo-ok?
        (mongo-ensure-index *r-col* '(("x" . 1)) :name *index* :safe #t)))

(test* "mongo-show-indexes" #t
       (every bson-document? (mongo-show-indexes *r-col*)))

(test* "mongo-drop-index" #t
       (mongo-ok? (mongo-drop-index *r-col* *index*)))

(test* "mongo-drop-indexes" #t
       (mongo-ok? (mongo-drop-indexes *r-col*)))

(test* "mongo-reindex" #t
       (mongo-ok? (mongo-reindex *r-col*)))

(test* "mongo-count" #t
       (number? (mongo-count *r-col*)))

(test* "mongo-distinct" #t
       (vector? (mongo-distinct *r-col* "x")))

(test* "mongo-group" #t
       (vector? (mongo-group *r-col*
                             '()
                             (bson-code "function(doc,acc){acc.cnt++;}")
                             '(("cnt" . 0)))))

(test* "mongo-map-reduce" #t
       (mongo-ok?
        (mongo-map-reduce *r-col*
                          (bson-code "function() { emit(this.i, 1); }")
                          (bson-code "function(k, vals) { return 1; }")
                          :out '(("inline" . 1)))))

(test* "mongo-dbref? 1" #t
       (mongo-dbref? '(("$ref" . "foo") ("$id" . "bar"))))

(test* "mongo-dbref? 2" #t
       (mongo-dbref? '(("$ref" . "foo") ("$id" . "bar") ("$db" . "baz"))))

(test* "mongo-dbref 1" '(("$ref" . "foo") ("$id" . "bar"))
       (mongo-dbref "foo" "bar"))

(test* "mongo-dbref 2" '(("$ref" . "foo") ("$id" . "bar") ("$db" . "baz"))
       (mongo-dbref "foo" "bar" "baz"))

(test* "mongo-dbref-get" #f
       (mongo-dbref-get *r-db* (mongo-dbref "foo" "bar" "baz")))

(test-end)
