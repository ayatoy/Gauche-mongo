(use gauche.test)
(use gauche.threads)
(use gauche.uvector)
(use binary.io)
(use mongo.util)
(use mongo.bson)
(use mongo.wire)

;;;; prep

(define *host*  "localhost")
(define *host1* "localhost:65535")
(define *dn*    "gauche_mongo_test_database")
(define *cn*    "gauche_mongo_test_collection")
(define *user*  "gauche_mongo_test_user")
(define *pass*  "gauche_mongo_test_pass")
(define *node*  #f)
(define *doc* `(("int32"        . 2147483647)
                ("int64"        . 2147483648)
                ("double"       . 5.05)
                ("string"       . "foo")
                ("null"         . ,(bson-null))
                ("false"        . ,(bson-false))
                ("true"         . ,(bson-true))
                ("undefined"    . ,(bson-undefined))
                ("min-key"      . ,(bson-min))
                ("max-key"      . ,(bson-max))
                ("bin-generic"  . ,(bson-binary 'generic (u8vector 0)))
                ("bin-function" . ,(bson-binary 'function (u8vector 0)))
                ("bin-old"      . ,(bson-binary 'old (u8vector 0)))
                ("bin-uuid"     . ,(bson-binary 'uuid (u8vector 0)))
                ("bin-md5"      . ,(bson-binary 'md5 (u8vector 0)))
                ("bin-udef"     . ,(bson-binary 'user-defined (u8vector 0)))
                ("datetime"     . ,(bson-datetime))
                ("timestamp"    . ,(bson-timestamp))
                ("object-id"    . ,(bson-object-id))
                ("regexp"       . ,(bson-regexp "foo" "bar"))
                ("dbpointer"    . ,(bson-dbpointer "foo.bar" (bson-object-id)))
                ("code"         . ,(bson-code "foo"))
                ("code/scope"   . ,(bson-code/scope "foo" '(("bar" . "baz"))))
                ("symbol"       . ,(bson-symbol "foo"))
                ("document"     . (("foo" . "foo") ("bar" . "bar")))
                ("array"        . ,(vector "foo" "bar" "baz"))))

(define (mt-map proc xs)
  (map thread-join!
       (map (^[x] (thread-start! (make-thread (^[] (proc x))))) xs)))

(define (test-find1 query . opts)
  (apply mongo-node-find1 *node* *dn* *cn* query opts))

(define (test-find query . opts)
  (mongo-cursor-all! (apply mongo-node-find *node* *dn* *cn* query opts)))

(define (test-insert n . opts)
  (rlet1 id (bson-object-id)
    (apply mongo-node-insert *node* *dn* *cn*
           (map (^[i] `(("test-id" . ,id) ("flag" . null) ("i" . ,i))) (iota n))
           opts)))

(define (test-insert1 doc . opts)
  (apply mongo-node-insert *node* *dn* *cn* (list doc) opts))

(define (test-update select . opts)
  (apply mongo-node-update *node* *dn* *cn*
         select '(("$set" . (("flag" . true)))) opts))

(define (test-delete select . opts)
  (apply mongo-node-delete *node* *dn* *cn* select opts))

;;;; test

(test-start "node")

(use mongo.node)
(test-module 'mongo.node)

;;;; connect

(test-section "connect")

(test* "mongo-node-connect" <mongo-node>
       (begin (set! *node* (mongo-node-connect (string->mongo-address *host*)))
              (class-of *node*)))

(test* "mongo-node-connect" (test-error <mongo-connect-error>)
       (mongo-node-connect (string->mongo-address *host1*)))

(test* "mongo-node-connect*" <mongo-node>
       (class-of (mongo-node-connect* (string->mongo-address *host*))))

(test* "mongo-node-connect*" #f
       (mongo-node-connect* (string->mongo-address *host1*)))

(test* "mongo-node" <mongo-node>
       (class-of (mongo-node *host*)))

(test* "mongo-node?" #t
       (mongo-node? *node*))

(test* "mongo-node-connect?" #t
       (mongo-node-connect? *node*))

(test* "mongo-node-disconnect!" #t
       (mongo-node-disconnect! *node*))

(test* "mongo-node-connect?" #f
       (mongo-node-connect? *node*))

(test* "mongo-node-available!" (undefined)
       (begin (mongo-node-available! *node*)
              (undefined)))

(test* "mongo-node-connect?" #t
       (mongo-node-connect? *node*))

;;;; insert

(test-section "insert")

(test* "mongo-node-insert" (undefined)
       (mongo-node-insert *node* *dn* *cn* (list *doc*)))

(test* "mongo-node-insert invariance" #t
       (let* ([id  (bson-object-id)]
              [doc `(("_id" . ,id) ,@*doc*)])
         (mongo-node-insert *node* *dn* *cn* (list doc) :safe #t)
         (equal? doc (mongo-node-find1 *node* *dn* *cn* `(("_id" . ,id))))))

(test* "mongo-node-insert safe" #t
       (mongo-ok?
        (mongo-node-insert *node* *dn* *cn* (list *doc*) :safe #t)))

(test* "mongo-node-insert journal" #t
       (mongo-ok?
        (mongo-node-insert *node* *dn* *cn* (list *doc*) :safe #t :j #t)))

(test* "mongo-node-insert fsync" #t
       (mongo-ok?
        (mongo-node-insert *node* *dn* *cn* (list *doc*) :safe #t :fsync #t)))

(test* "mongo-node-insert w" #t
       (mongo-ok?
        (mongo-node-insert *node* *dn* *cn* (list *doc*) :safe #t :w 1)))

(test* "mongo-node-insert mt" #t
       (every mongo-ok?
              (mt-map (^[i] (mongo-node-insert *node* *dn* *cn* (list *doc*)
                                               :safe #t :w 1))
                      (iota 100))))

(test* "mongo-node-insert continue-on-error #f" 2
       (let ([id  (bson-object-id)]
             [tid (bson-object-id)])
         (guard (e [(<mongo-request-error> e)
                    (length (test-find `(("test-id" . ,tid))))])
           (mongo-node-insert
            *node* *dn* *cn*
            (list `(("test-id" . ,tid) ("_id" . ,(bson-object-id)))
                  `(("test-id" . ,tid) ("_id" . ,id))
                  `(("test-id" . ,tid) ("_id" . ,id))
                  `(("test-id" . ,tid) ("_id" . ,(bson-object-id))))
            :continue-on-error #f
            :safe #t
            :w 1))))

(test* "mongo-node-insert continue-on-error #t" 3
       (let ([id  (bson-object-id)]
             [tid (bson-object-id)])
         (guard (e [(<mongo-request-error> e)
                    (length (test-find `(("test-id" . ,tid))))])
           (mongo-node-insert
            *node* *dn* *cn*
            (list `(("test-id" . ,tid) ("_id" . ,(bson-object-id)))
                  `(("test-id" . ,tid) ("_id" . ,id))
                  `(("test-id" . ,tid) ("_id" . ,id))
                  `(("test-id" . ,tid) ("_id" . ,(bson-object-id))))
            :continue-on-error #t
            :safe #t
            :w 1))))

;;;; update

(test-section "update")

(test* "mongo-node-update multi-update #t" 2
       (let1 id (test-insert 2)
         (test-update `(("test-id" .  ,id)) :multi-update #t :safe #t :w 1)
         (length (test-find `(("test-id" . ,id) ("flag" . ,(bson-true)))))))

(test* "mongo-node-update multi-update #f" 1
       (let1 id (test-insert 2)
         (test-update `(("test-id" .  ,id)) :multi-update #f :safe #t :w 1)
         (length (test-find `(("test-id" . ,id) ("flag" . ,(bson-true)))))))

(test* "mongo-node-update upsert #t" 3
       (let1 id (test-insert 2)
         (test-update `(("test-id" . ,id) ("flag" . "foo")) :upsert #t :safe #t :w 1)
         (length (test-find `(("test-id" . ,id))))))

(test* "mongo-node-update upsert #f" 2
       (let1 id (test-insert 2)
         (test-update `(("test-id" . ,id) ("flag" . "foo")) :upsert #f :safe #t :w 1)
         (length (test-find `(("test-id" . ,id))))))

(test* "mongo-node-update" (undefined)
       (test-update `(("test-id" . ,(test-insert 2)))))

(test* "mongo-node-update safe" #t
       (mongo-ok?
        (test-update `(("test-id" . ,(test-insert 2))) :safe #t)))

(test* "mongo-node-update journal" #t
       (mongo-ok?
        (test-update `(("test-id" . ,(test-insert 2))) :safe #t :j #t)))

(test* "mongo-node-update fsync" #t
       (mongo-ok?
        (test-update `(("test-id" . ,(test-insert 2))) :safe #t :fsync #t)))

(test* "mongo-node-update w" #t
       (mongo-ok?
        (test-update `(("test-id" . ,(test-insert 2))) :safe #t :w 1)))

(test* "mongo-node-update mt" #t
       (let1 id (test-insert 100)
         (every (^[res doc] (and (mongo-ok? res)
                                 (bson-true? (assoc-ref doc "flag"))))
                (mt-map (^[i] (test-update `(("test-id" . ,id) ("i". ,i))
                                           :safe #t :w 1))
                        (iota 100))
                (test-find `(("test-id" . ,id))))))

;;;; find1

(test-section "find1")

(test* "mongo-node-find1 orderby" 0
       (let1 id (test-insert 2)
         (assoc-ref (test-find1 `(("test-id" . ,id)
                                  ("i" . (("$exists" . ,(bson-true)))))
                                :orderby `(("i" . 1)))
                    "i")))

(test* "mongo-node-find1 orderby number-to-skip" 1
       (let1 id (test-insert 2)
         (assoc-ref (test-find1 `(("test-id" . ,id)
                                  ("i" . (("$exists" . ,(bson-true)))))
                                :orderby `(("i" . 1))
                                :number-to-skip 1)
                    "i")))

(test* "mongo-node-find1 orderby return-field-selector" #t
       (let1 id (test-insert 1)
         (let1 doc (test-find1 `(("test-id" . ,id)
                                 ("i" . (("$exists" . ,(bson-true)))))
                               :orderby `(("i" . 1))
                               :return-field-selector `(("i" . 1)))
           (and (bson-object-id? (assoc-ref doc "_id"))
                (number? (assoc-ref doc "i"))
                (not (assoc-ref doc "flag"))
                (not (assoc-ref doc "test-id"))))))

(test* "mongo-node-find1 mt" 4950
       (let1 id (test-insert 100)
         (fold (^[doc r] (+ (assoc-ref doc "i") r))
               0
               (mt-map (^[i] (test-find1 `(("test-id" . ,id) ("i" . ,i))))
                       (iota 100)))))

;;;; delete

(test-section "delete")

(test* "mongo-node-delete" (undefined)
       (test-delete `(("test-id" . ,(test-insert 1)))))

(test* "mongo-node-delete safe" #t
       (mongo-ok?
        (test-delete `(("test-id" . ,(test-insert 1))) :safe #t)))

(test* "mongo-node-delete journal" #t
       (mongo-ok?
        (test-delete `(("test-id" . ,(test-insert 1))) :safe #t :j #t)))

(test* "mongo-node-delete fsync" #t
       (mongo-ok?
        (test-delete `(("test-id" . ,(test-insert 1))) :safe #t :fsync #t)))

(test* "mongo-node-delete w" #t
       (mongo-ok?
        (test-delete `(("test-id" . ,(test-insert 1))) :safe #t :w 1)))

(test* "mongo-node-delete single-remove #t" 1
       (let1 id (test-insert 2)
         (test-delete `(("test-id" . ,id)) :single-remove #t :safe #t :w 1)
         (length (test-find `(("test-id" . ,id))))))

(test* "mongo-node-delete single-remove #f" 0
       (let1 id (test-insert 2)
         (test-delete `(("test-id" . ,id)) :single-remove #f :safe #t :w 1)
         (length (test-find `(("test-id" . ,id))))))

(test* "mongo-node-delete mt" #t
       (let1 id (test-insert 100)
         (and (every mongo-ok?
                     (mt-map (^[i] (test-delete `(("test-id" . ,id)
                                                  ("i" . ,i))
                                                :safe #t
                                                :w 1))
                             (iota 100)))
              (= 0 (length (test-find `(("test-id" . ,id))))))))

;;;; find

(test-section "find")

(let* ([id (test-insert 100 :safe #t)]
       [c  (mongo-node-find *node* *dn* *cn*
                            `(("test-id" . ,id))
                            :number-to-return 2)])
  (test* "mongo-cursor" <mongo-cursor>
         (class-of c))
  (test* "mongo-cursor?" #t
         (mongo-cursor? c))
  (test* "mongo-cursor-initial?" #t
         (mongo-cursor-initial? c))
  (test* "mongo-cursor-next!" #t
         (bson-document? (mongo-cursor-next! c)))
  (test* "mongo-cursor-initial?" #f
         (mongo-cursor-initial? c))
  (test* "mongo-cursor-take!" 5
         (length (mongo-cursor-take! c 5)))
  (test* "mongo-cursor-peek!" #t
         (bson-document? (mongo-cursor-peek! c)))
  (test* "mongo-cursor-exists?" #t
         (mongo-cursor-exists? c))
  (test* "mongo-cursor-all!" 94
         (length (mongo-cursor-all! c)))
  (test* "mongo-cursor-next!" #f
         (mongo-cursor-next! c))
  (test* "mongo-cursor-take!" '()
         (mongo-cursor-take! c 5))
  (test* "mongo-cursor-all!" '()
         (mongo-cursor-all! c))
  (test* "mongo-cursor-exists?" #f
         (mongo-cursor-exists? c))
  (test* "mongo-cursor-rewind!" #t
         (mongo-cursor-rewind! c))
  (test* "mongo-cursor-initial?" #t
         (mongo-cursor-initial? c))
  (test* "mongo-cursor-rewind!" #f
         (mongo-cursor-rewind! c))
  (test* "mongo-cursor-all!" 100
         (length (mongo-cursor-all! c))))

(let1 c (mongo-node-find *node* *dn* *cn*
                         '(("foobarbaz" . "foobarbaz"))
                         :number-to-return 2)
  (test* "mongo-cursor" <mongo-cursor>
         (class-of c))
  (test* "mongo-cursor?" #t
         (mongo-cursor? c))
  (test* "mongo-cursor-initial?" #t
         (mongo-cursor-initial? c))
  (test* "mongo-cursor-next!" #f
         (mongo-cursor-next! c))
  (test* "mongo-cursor-initial?" #f
         (mongo-cursor-initial? c))
  (test* "mongo-cursor-next!" #f
         (mongo-cursor-next! c))
  (test* "mongo-cursor-rewind!" #t
         (mongo-cursor-rewind! c))
  (test* "mongo-cursor-initial?" #t
         (mongo-cursor-initial? c))
  (test* "mongo-cursor-rewind!" #f
         (mongo-cursor-rewind! c))
  (test* "mongo-cursor-take!" '()
         (mongo-cursor-take! c 5))
  (test* "mongo-cursor-rewind!" #t
         (mongo-cursor-rewind! c))
  (test* "mongo-cursor-all!" '()
         (mongo-cursor-all! c))
  (test* "mongo-cursor-rewind!" #t
         (mongo-cursor-rewind! c))
  (test* "mongo-cursor-peek!" #f
         (mongo-cursor-peek! c))
  (test* "mongo-cursor-exists?" #f
         (mongo-cursor-exists? c)))

(test* "mongo-node-find mt" 4950
       (let1 id (test-insert 100)
         (apply + (mt-map (^[c] (assoc-ref (mongo-cursor-next! c) "i"))
                          (map (^[i] (mongo-node-find *node* *dn* *cn*
                                                      `(("test-id" . ,id)
                                                        ("i" . ,i))))
                               (iota 100))))))

(test* "mongo-cursor-next! mt" 4950
       (let* ([id (test-insert 100)]
              [cr (mongo-node-find *node* *dn* *cn* `(("test-id" . ,id)))])
         (apply + (mt-map (^[i] (assoc-ref (mongo-cursor-next! cr) "i"))
                          (iota 100)))))

(test* "mongo-cursor-take! mt" 4950
       (let* ([id (test-insert 100)]
              [cr (mongo-node-find *node* *dn* *cn* `(("test-id" . ,id)))])
         (fold (^[docs r]
                 (+ (fold (^[doc r] (+ (assoc-ref doc "i") r)) 0 docs) r))
               0
               (mt-map (^[i] (mongo-cursor-take! cr 5))
                       (iota 20)))))

(test* "mongo-cursor->generator" 100
       (let* ([id (test-insert 100)]
              [g  (mongo-cursor->generator
                   (mongo-node-find *node* *dn* *cn*
                                    `(("test-id" . ,id))
                                    :number-to-return 3))])
         (length (let loop ([acc '()] [doc (g)])
                   (if (eof-object? doc)
                     acc
                     (loop (if (equal? id (assoc-ref doc "test-id"))
                             (cons doc acc)
                             acc)
                           (g)))))))

;;;; helper

(test-section "helper")

(test* "mongo-node-command" #t
       (mongo-ok? (mongo-node-command *node* "admin" '(("ping" . 1)))))

(test* "mongo-node-command" (test-error <mongo-request-error>)
       (mongo-ok? (mongo-node-command *node* "admin" '(("foo" . 1)))))

(test* "mongo-node-admin" #t
       (mongo-ok? (mongo-node-admin *node* '(("ping" . 1)))))

(test* "mongo-node-round-trip" #t
       (number? (mongo-node-round-trip *node*)))

(test* "mongo-node-ismaster" #t
       (mongo-ok? (mongo-node-ismaster *node*)))

(test* "mongo-node-get-last-error" #t
       (mongo-ok? (mongo-node-get-last-error *node* *dn*)))

(test* "mongo-node-reset-error" #t
       (mongo-ok? (mongo-node-reset-error *node* *dn*)))

(test* "mongo-node-auth" (test-error <mongo-request-error>)
       (mongo-ok? (mongo-node-auth *node* *dn* *user* *pass*)))

(test* "mongo-node-reauth" (undefined)
       (mongo-node-reauth *node*))

(test-end)
