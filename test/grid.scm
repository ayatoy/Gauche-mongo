(use gauche.test)
(use gauche.threads)
(use mongo.util)
(use mongo.bson)
(use mongo.wire)
(use mongo.node)
(use mongo.core)

;;;; prep

(define *dn*   "gauche_mongo_test_database")
(define *db*   (mongo-database (mongo "localhost") *dn*))
(define *fn*   "gauche_mongo_test_file")
(define *grid* #f)

(define (mt-map proc xs)
  (map thread-join!
       (map (^[x] (thread-start! (make-thread (^[] (proc x))))) xs)))

(define (input-to-string in)
  (call-with-output-string
    (^[out] (let loop ([c (read-char in)])
              (when (not (eof-object? c))
                (write-char c out)
                (loop (read-char in)))))))

;;;; test

(test-start "grid")

(use mongo.grid)
(test-module 'mongo.grid)

(test* "mongo-grid" <mongo-grid>
       (begin (set! *grid* (mongo-grid *db*))
              (mongo-delete (mongo-grid-files *grid*) '() :safe #t)
              (mongo-delete (mongo-grid-chunks *grid*) '() :safe #t)
              (class-of *grid*)))

(test* "mongo-grid?" #t
       (mongo-grid? *grid*))

(test* "mongo-grid-database" *db*
       (mongo-grid-database *grid*))

(test* "mongo-grid-prefix" "fs"
       (mongo-grid-prefix *grid*))

(test* "mongo-grid-files" #t
       (mongo-collection? (mongo-grid-files *grid*)))

(test* "mongo-grid-chunks" #t
       (mongo-collection? (mongo-grid-chunks *grid*)))

(test* "call-with-mongo-grid-output-port" #t
       (call-with-mongo-grid-output-port
        *grid*
        *fn*
        (^[out] (format out "version1") #t)
        :chunk-size 2))

(test* "call-with-mongo-grid-input-port" "version1"
       (call-with-mongo-grid-input-port *grid* *fn* input-to-string))

(test* "call-with-mongo-grid-output-port" #t
       (call-with-mongo-grid-output-port
        *grid*
        *fn*
        (^[out] (format out "version2") #t)
        :chunk-size 2))

(test* "call-with-mongo-grid-input-port" "version2"
       (call-with-mongo-grid-input-port *grid* *fn* input-to-string))

(test* "call-with-mongo-grid-input-port" "version1"
       (call-with-mongo-grid-input-port *grid* *fn* input-to-string :old 1))

(test* "call-with-mongo-grid-output-port" #t
       (call-with-mongo-grid-output-port
        *grid*
        *fn*
        (^[out] (format out "version3") #t)
        :chunk-size 2
        :retain 2))

(test* "call-with-mongo-grid-input-port" "version3"
       (call-with-mongo-grid-input-port *grid* *fn* input-to-string :old 0))

(test* "call-with-mongo-grid-input-port" "version2"
       (call-with-mongo-grid-input-port *grid* *fn* input-to-string :old 1))

(test* "call-with-mongo-grid-input-port" (test-error <mongo-grid-error>)
       (call-with-mongo-grid-input-port *grid* *fn* input-to-string :old 2))

(test* "mongo-grid-delete" (test-error <mongo-grid-error>)
       (begin (mongo-grid-delete *grid* *fn*)
              (call-with-mongo-grid-input-port *grid* *fn* input-to-string)))

(test* "call-with-mongo-grid-input-port" (test-error <mongo-grid-error>)
       (call-with-mongo-grid-input-port *grid* *fn* input-to-string))

(test* "call-with-mongo-grid-output-port" #t
       (call-with-mongo-grid-output-port *grid* "empty" (^[out] #t)))

(test* "call-with-mongo-grid-input-port" ""
       (call-with-mongo-grid-input-port *grid* "empty" input-to-string))

(test* "call-with-mongo-grid-output-port" #t
       (begin (mt-map (^[i] (call-with-mongo-grid-output-port
                                 *grid*
                                 *fn*
                                 (^[out] (format out "foobarbaz"))
                                 :chunk-size 3))
                          (iota 50))
              #t))

(test* "call-with-mongo-grid-input-port" #t
       (every (^[str] (= 9 (string-length str)))
              (mt-map (^[i] (call-with-mongo-grid-input-port
                                 *grid*
                                 *fn*
                                 input-to-string
                                 :old i))
                          (iota 50))))

(test* "call-with-mongo-grid-input-port" (test-error <mongo-grid-error>)
       (call-with-mongo-grid-input-port *grid* *fn* input-to-string :old 50))

(test* "call-with-mongo-grid-output-port" #t
       (begin (call-with-mongo-grid-output-port
               *grid*
               *fn*
               (^[out] (mt-map (^[i] (format out "foo")) (iota 50)))
               :chunk-size 3
               :retain #f)
              #t))

(test* "call-with-mongo-grid-input-port" 150
       (string-length (call-with-mongo-grid-input-port *grid*
                                                       *fn*
                                                       input-to-string)))

(test-end)
