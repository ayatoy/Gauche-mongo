(use gauche.test)

;;;; test

(test-start "util")

(use mongo.util)
(test-module 'mongo.util)

;;;; alist

(test-section "alist")

(test* "alist" '(("key1" . "value1") ("key2" . "value2"))
       (alist "key1" "value1" "key2" "value2"))

(test* "alref 1" "value2"
       (alref (alist "key1" "value1"
                     "key2" "value2")
              "key2"))

(test* "alref 2" "value3"
       (alref (alist "key1" "value1"
                     "key2" (alist "key3" "value3")
                     "key4" "value4")
              "key2"
              "key3"))

(test* "alref 3" #f
       (alref (alist "key1" "value1"
                     "key2" "value2")
              "key3"))

(test* "alref 4" #f
       (alref (alist "key1" "value1"
                     "key2" "value2"
                     "key4" "value4")
              "key2"
              "key3"))

(test* "alset" '(("key1" . "value4") ("key2" . "value2") ("key3" . "value5"))
       (alset (alist "key1" "value1"
                     "key2" "value2"
                     "key3" "value3")
              "key1" "value4"
              "key3" "value5"))

(test* "alcut" '(("key3" . "value3"))
       (alcut (alist "key1" "value1"
                     "key2" "value2"
                     "key3" "value3")
              "key1"
              "key2"))
;;;; mutex

;;(test-section "mutex")

;;;; u8vector

;;(test-section "u8vector")

;;;; counter

;;(test-section "counter")

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

;;;; clock

(test-section "clock")

(let* ([i 0]
       [clock (mongo-clock (^[] (inc! i)) #e3e8)])
  (test* "mongo-clock" <mongo-clock> (class-of clock))
  (test* "mongo-clock?" #t (mongo-clock? clock))
  (test* "mongo-clock-worker" #t
         (begin (mongo-clock-start! clock)
                (mongo-clock-stop! clock)
                (sys-sleep 1)
                (> i 0))))

(test-end)
