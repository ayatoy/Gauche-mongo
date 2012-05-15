(use gauche.test)

;;;; test

(test-start "util")

(use mongo.util)
(test-module 'mongo.util)

;;;; mutex

(test-section "mutex")

;;;; u8vector

(test-section "u8vector")

;;;; counter

(test-section "counter")

;;;; uri

(test-section "uri")

(test* "mongo-ns-compose" "foo.bar.baz"
       (mongo-ns-compose "foo" "bar" "baz"))
(test* "mongo-ns-compose exception" (test-error)
       (mongo-ns-compose "foo" 'bar "baz"))

(test* "mongo-ns-parse" '("foo" "bar.baz")
       (mongo-ns-parse "foo.bar.baz"))
(test* "mongo-ns-parse exception 1" (test-error)
       (mongo-ns-parse ".bar"))
(test* "mongo-ns-parse exception 2" (test-error)
       (mongo-ns-parse "foo."))

(let1 addr (string->mongo-address "foo.bar.baz")
  (test* "string->mongo-address inet 1" #t
         (mongo-address-inet? addr))
  (test* "mongo-address-inet-host 1" "foo.bar.baz"
         (mongo-address-inet-host addr))
  (test* "mongo-address-inet-port 1" 27017
         (mongo-address-inet-port addr))
  (test* "mongo-address->string inet 1" "foo.bar.baz:27017"
         (mongo-address->string addr)))

(let1 addr (string->mongo-address "foo.bar.baz:27018")
  (test* "string->mongo-address inet 2" #t
         (mongo-address-inet? addr))
  (test* "mongo-address-inet-host 2" "foo.bar.baz"
         (mongo-address-inet-host addr))
  (test* "mongo-address-inet-port 2" 27018
         (mongo-address-inet-port addr))
  (test* "mongo-address->string inet 2" "foo.bar.baz:27018"
         (mongo-address->string addr)))

(let1 addr (string->mongo-address "/foo/bar/baz")
  (test* "string->mongo-address unix 1" #t
         (mongo-address-unix? addr))
  (test* "mongo-address-unix-path 1" "/foo/bar/baz"
         (mongo-address-unix-path addr))
  (test* "mongo-address->string unix 1" "/foo/bar/baz"
         (mongo-address->string addr)))

(let1 addr (string->mongo-address "/foo/bar/baz:12345")
  (test* "string->mongo-address unix 2" #t
         (mongo-address-unix? addr))
  (test* "mongo-address-unix-path 2" "/foo/bar/baz"
         (mongo-address-unix-path addr))
  (test* "mongo-address->string unix 2" "/foo/bar/baz"
         (mongo-address->string addr)))

(test* "mongo-uri-parse 1" '("user"
                             "pass"
                             ("/foo/bar/baz"
                              "foo.bar.baz:27017"
                              "foo.bar.baz:27017"
                              "foo.bar.baz:27018")
                             "db"
                             (("key1" . "value1")
                              ("key2" . "value2")))
       (receive (user pass addrs db params)
           (mongo-uri-parse "mongodb://user:pass@/foo/bar/baz:0,foo.bar.baz,\
                             foo.bar.baz:27017,foo.bar.baz:27018/\
                             db?key1=value1&key2=value2")
         (list user pass (map mongo-address->string addrs) db params)))

(test* "mongo-uri-parse 2" '(#f #f ("localhost:27017") #f ())
       (receive (user pass addrs db params)
           (mongo-uri-parse "mongodb://localhost/")
         (list user pass (map mongo-address->string addrs) db params)))

(test* "mongo-uri-parse 3" '(#f #f ("localhost:27017") #f ())
       (receive (user pass addrs db params)
           (mongo-uri-parse "mongodb://localhost")
         (list user pass (map mongo-address->string addrs) db params)))

(test* "mongo-uri-parse 4" '(#f #f ("/foo/bar/baz") #f ())
       (receive (user pass addrs db params)
           (mongo-uri-parse "mongodb:///foo/bar/baz:0/")
         (list user pass (map mongo-address->string addrs) db params)))

(test* "mongo-uri-parse 5" '(#f #f ("/foo/bar/baz") #f (("key" . "value")))
       (receive (user pass addrs db params)
           (mongo-uri-parse "mongodb:///foo/bar/baz:0/?key=value")
         (list user pass (map mongo-address->string addrs) db params)))

(test* "mongo-uri-parse 6" '(#f #f ("localhost:27017") "foo" ())
       (receive (user pass addrs db params)
           (mongo-uri-parse "localhost/foo")
         (list user pass (map mongo-address->string addrs) db params)))

;;;; digest

(test-section "digest")

(test* "mongo-user-digest-hexify" "3563025c1e89c7ad43fb63fcbcf1c3c6"
       (mongo-user-digest-hexify "foo" "bar"))

(test* "mongo-auth-digest-hexify" "68031908da80a027a2ab1e1d15056cd8"
       (mongo-auth-digest-hexify "foo" "bar" "baz"))

;;;; validation

(test* "mongo-validate-database-name 1" "foo"
       (mongo-validate-database-name "foo"))

(test* "mongo-validate-database-name 2" (test-error <mongo-validation-error>)
       (mongo-validate-database-name "foo bar"))

(test* "mongo-validate-database-name 3" (test-error <mongo-validation-error>)
       (mongo-validate-database-name "foo.bar"))

(test* "mongo-validate-database-name 4" (test-error <mongo-validation-error>)
       (mongo-validate-database-name "$foo"))

(test* "mongo-validate-database-name 5" (test-error <mongo-validation-error>)
       (mongo-validate-database-name "foo/bar"))

(test* "mongo-validate-database-name 6" (test-error <mongo-validation-error>)
       (mongo-validate-database-name "foo\\bar"))

(test* "mongo-validate-database-name 7" (test-error <mongo-validation-error>)
       (mongo-validate-database-name ""))

(test* "mongo-validate-collection-name 1" "foo"
       (mongo-validate-collection-name "foo"))

(test* "mongo-validate-collection-name 2" (test-error <mongo-validation-error>)
       (mongo-validate-collection-name ""))

(test* "mongo-validate-collection-name 3" (test-error <mongo-validation-error>)
       (mongo-validate-collection-name ".."))

(test* "mongo-validate-collection-name 4" (test-error <mongo-validation-error>)
       (mongo-validate-collection-name "$foo"))

(test* "mongo-validate-collection-name 5" "$cmd"
       (mongo-validate-collection-name "$cmd"))

(test* "mongo-validate-collection-name 6" "oplog.$main"
       (mongo-validate-collection-name "oplog.$main"))

(test* "mongo-validate-collection-name 7" (test-error <mongo-validation-error>)
       (mongo-validate-collection-name ".foo"))

(test* "mongo-validate-collection-name 8" (test-error <mongo-validation-error>)
       (mongo-validate-collection-name "foo."))

;;;; misc

(test* "mongo-ok? 1" #t
       (mongo-ok? '(("ok" . 1))))

(test* "mongo-ok? 2" #t
       (mongo-ok? '(("ok" . 1.0))))

(test* "mongo-ok? 3" #t
       (mongo-ok? '(("ok" . true))))

(test* "mongo-ok? 4" #f
       (mongo-ok? '(("ok" . false))))

(test* "mongo-generate-index-name" "key1_1_key2_-1"
       (mongo-generate-index-name '(("key1" . 1) ("key2" . -1))))

(test-end)
