(use gauche.test)
(use gauche.uvector)
(use binary.io)
(use srfi-1)
(use srfi-42)

(define (invariance x w r)
  (call-with-input-string
      (call-with-output-string (^[o] (w x o)))
    (^[i] (r i))))

(define (invariance? x w r)
  (equal? x (invariance x w r)))

(define (pick-uniformly-step start stop num-pick)
  (let1 step (/ (abs (- start stop)) num-pick)
    (if (and (integer? start) (integer? stop))
      (round step)
      step)))

(define (nums-pick-uniformly start stop num-pick)
  (list-ec (: n start stop (pick-uniformly-step start stop num-pick))
           n))

;;;; test

(test-start "bson")

(use mongo.bson)
(test-module 'mongo.bson)

;;;; int32

(test-section "int32")

(test* "bson-int32? supremum 1" #f
       (bson-int32? (expt 2 31)))

(test* "bson-int32? supremum 2" #t
       (bson-int32? (- (expt 2 31) 1)))

(test* "bson-int32? infimum 1" #f
       (bson-int32? (- (* -1 (expt 2 31)) 1)))

(test* "bson-int32? infimum 2" #t
       (bson-int32? (* -1 (expt 2 31))))

(test* "bson-int32 invariance" #t
       (every (^[i] (invariance? i write-bson-int32 read-bson-int32))
              (nums-pick-uniformly (* -1 (expt 2 31)) (expt 2 31) 1000)))

(test* "write-bson-int32-prepare size" 4
       (receive (s w) (write-bson-int32-prepare 1) s))

(test* "write-bson-int32 supremum 1" (test-error)
       (invariance? (expt 2 31) write-bson-int32 read-bson-int32))

(test* "write-bson-int32 supremum 2" #t
       (invariance? (- (expt 2 31) 1) write-bson-int32 read-bson-int32))

(test* "write-bson-int32 infimum 1" (test-error)
       (invariance? (- (* -1 (expt 2 31)) 1) write-bson-int32 read-bson-int32))

(test* "write-bson-int32 infimum 2" #t
       (invariance? (* -1 (expt 2 31)) write-bson-int32 read-bson-int32))

(test* "read-bson-int32 exception" (test-error <bson-read-error>)
       (call-with-input-string "abc" (^[in] (read-bson-int32 in))))

;;;; int64

(test-section "int64")

(test* "bson-int64? supremum 1" #f
       (bson-int64? (expt 2 63)))

(test* "bson-int64? supremum 2" #t
       (bson-int64? (- (expt 2 63) 1)))

(test* "bson-int64? infimum 1" #f
       (bson-int64? (- (* -1 (expt 2 63)) 1)))

(test* "bson-int64? infimum 2" #t
       (bson-int64? (* -1 (expt 2 63))))

(test* "bson-int64 invariance" #t
       (every (^[i] (invariance? i write-bson-int64 read-bson-int64))
              (nums-pick-uniformly (* -1 (expt 2 63)) (expt 2 63) 1000)))

(test* "write-bson-int64-prepare size" 8
       (receive (s w) (write-bson-int64-prepare 1) s))

(test* "write-bson-int64 supremum 1" (test-error)
       (invariance? (expt 2 63) write-bson-int64 read-bson-int64))

(test* "write-bson-int64 supremum 2" #t
       (invariance? (- (expt 2 63) 1) write-bson-int64 read-bson-int64))

(test* "write-bson-int64 infimum 1" (test-error)
       (invariance? (- (* -1 (expt 2 63)) 1) write-bson-int64 read-bson-int64))

(test* "write-bson-int64 infimum 2" #t
       (invariance? (* -1 (expt 2 63)) write-bson-int64 read-bson-int64))

(test* "read-bson-int64 exception" (test-error <bson-read-error>)
       (call-with-input-string "abcdefg" (^[in] (read-bson-int64 in))))

;;;; double

(test-section "double")

(test* "bson-double invariance" #t
       (every (^[d] (invariance? d write-bson-double read-bson-double))
              (nums-pick-uniformly (* 2.225074 (expt 10 -308))
                                   (* 1.797693 (expt 10 308))
                                   1000)))

(test* "bson-double size" 8
       (receive (s w) (write-bson-double-prepare 1) s))

(test* "read-bson-double exception" (test-error <bson-read-error>)
       (call-with-input-string "abcdefg" (^[in] (read-bson-double in))))

;;;; string

(test-section "string")

(test* "bson-string invariance" #t
         (invariance? "foo\x00bar" write-bson-string read-bson-string))

(let* ([str  "foo\x00bar"] [size (string-size str)])
  (test* "write-bson-string-prepare size" (+ 4 size 1)
         (receive (s w) (write-bson-string-prepare str) s)))

(test* "bson-string empty" #t
       (invariance? "" write-bson-string read-bson-string))

(test* "write-bson-string incomplete exception" (test-error <bson-write-error>)
       (invariance? #*"foo" write-bson-string read-bson-string))

(test* "read-bson-string exception 1" (test-error <bson-read-error>)
       (call-with-input-string (call-with-output-string
                                 (^[out] (write-s32 1 out 'litte-endian)))
         (^[in] (read-bson-string in))))

(test* "read-bson-string exception 2" (test-error <bson-read-error>)
       (call-with-input-string (call-with-output-string
                                 (^[out]
                                   (write-s32 255 out 'litte-endian)
                                   (display "foo" out)
                                   (write-byte #x00 out)))
         (^[in] (read-bson-string in))))

(test* "read-bson-string exception 3" (test-error <bson-read-error>)
       (call-with-input-string (call-with-output-string
                                 (^[out]
                                   (write-s32 2 out 'litte-endian)
                                   (display "ab" out)))
         (^[in] (read-bson-string in))))

;;;; cstring

(test-section "cstring")

(test* "bson-cstring invariance" #t
         (invariance? "foo" write-bson-cstring read-bson-cstring))

(let* ([str  "foo"]
       [size (string-size str)])
  (test* "write-bson-cstring-prepare size" (+ size 1)
         (receive (s w) (write-bson-cstring-prepare str) s)))

(test* "bson-cstring empty" #t
       (invariance? "" write-bson-cstring read-bson-cstring))

(test* "write-bson-cstring exception 1" (test-error <bson-write-error>)
       (invariance? "foo\x00bar" write-bson-cstring read-bson-cstring))

(test* "write-bson-cstring exception 2" (test-error <bson-write-error>)
       (invariance? "\x00foobar" write-bson-cstring read-bson-cstring))

(test* "write-bson-cstring exception 3" (test-error <bson-write-error>)
       (invariance? "foobar\x00" write-bson-cstring read-bson-cstring))

(test* "read-bson-cstring exception" (test-error <bson-read-error>)
       (call-with-input-string "foo" (^[in] (read-bson-string in))))

;;;; null

(test-section "null")

(test* "bson-null?" #t (bson-null? 'null))
(test* "bson-null" 'null (bson-null))

;;;; boolean

(test-section "boolean")

(test* "bson-false?" #t (bson-false? 'false))
(test* "bson-true?" #t (bson-true? 'true))
(test* "bson-boolean? false" #t (bson-boolean? 'false))
(test* "bson-boolean? true" #t (bson-boolean? 'true))
(test* "bson-false" 'false (bson-false))
(test* "bson-true" 'true (bson-true))
(test* "bson-boolean false" 'false (bson-boolean #f))
(test* "bson-boolean true" 'true (bson-boolean #t))

(test* "bson-false invariance" #t
       (invariance? (bson-false) write-bson-boolean read-bson-boolean))

(test* "bson-true invariance" #t
       (invariance? (bson-true) write-bson-boolean read-bson-boolean))

(test* "write-bson-boolean exception" (test-error <bson-write-error>)
       (invariance? 'foo write-bson-boolean read-bson-boolean))

(test* "read-bson-boolean exception" (test-error <bson-read-error>)
       (call-with-input-string (call-with-output-string
                                 (^[out] (write-byte #x02 out)))
         (^[in] (read-bson-boolean in))))

;;;; undefined

(test-section "undefined")

(test* "bson-undefined?" #t (bson-undefined? 'undefined))
(test* "bson-undefined" 'undefined (bson-undefined))

;;;; min/max

(test-section "min/max")

(test* "bson-min?" #t (bson-min? 'min))
(test* "bson-max?" #t (bson-max? 'max))
(test* "bson-min" 'min (bson-min))
(test* "bson-max" 'max (bson-max))

;;;; binary

(test-section "binary")

(let* ([uv  (u8vector 0)]
       [bin (bson-binary uv)])
  (test* "bson-binary constructor generic 1" #t
         (bson-binary? bin))
  (test* "bson-binary-type generic 1" 'generic
         (bson-binary-type bin))
  (test* "bson-binary-bytes generic 1" uv
         (bson-binary-bytes bin))
  (test* "write-bson-binary-prepare size generic 1" (+ 4 1 (uvector-size uv))
         (receive (s w) (write-bson-binary-prepare bin) s))
  (test* "bson-binary invariance generic 1" #t
         (invariance? bin write-bson-binary read-bson-binary))
  (test* "bson-binary object-equal? generic 1" #t
         (equal? bin (bson-binary (u8vector 0)))))

(let* ([uv (u8vector 0)]
       [bin (bson-binary 'generic uv)])
  (test* "bson-binary constructor generic 2" #t
         (bson-binary? bin))
  (test* "bson-binary-type generic 2" 'generic
         (bson-binary-type bin))
  (test* "bson-binary-bytes generic 2" uv
         (bson-binary-bytes bin))
  (test* "write-bson-binary-prepare size generic 2" (+ 4 1 (uvector-size uv))
         (receive (s w) (write-bson-binary-prepare bin) s))
  (test* "bson-binary invariance generic 2" #t
         (invariance? bin write-bson-binary read-bson-binary))
  (test* "bson-binary object-equal? generic 2" #t
         (equal? bin (bson-binary 'generic (u8vector 0)))))

(let* ([uv (u8vector 0)]
       [bin (bson-binary 'function uv)])
  (test* "bson-binary constructor function" #t
         (bson-binary? bin))
  (test* "bson-binary-type function" 'function
         (bson-binary-type bin))
  (test* "bson-binary-bytes function" uv
         (bson-binary-bytes bin))
  (test* "write-bson-binary-prepare size function" (+ 4 1 (uvector-size uv))
         (receive (s w) (write-bson-binary-prepare bin) s))
  (test* "bson-binary invariance function" #t
         (invariance? bin write-bson-binary read-bson-binary))
  (test* "bson-binary object-equal? function" #t
         (equal? bin (bson-binary 'function (u8vector 0)))))

(let* ([uv (u8vector 0)]
       [bin (bson-binary 'old uv)])
  (test* "bson-binary constructor old" #t
         (bson-binary? bin))
  (test* "bson-binary-type old" 'old
         (bson-binary-type bin))
  (test* "bson-binary-bytes old" uv
         (bson-binary-bytes bin))
  (test* "write-bson-binary-prepare size old" (+ 4 1 (uvector-size uv))
         (receive (s w) (write-bson-binary-prepare bin) s))
  (test* "bson-binary invariance old" #t
         (invariance? bin write-bson-binary read-bson-binary))
  (test* "bson-binary object-equal? old" #t
         (equal? bin (bson-binary 'old (u8vector 0)))))

(let* ([uv (u8vector 0)]
       [bin (bson-binary 'uuid uv)])
  (test* "bson-binary constructor uuid" #t
         (bson-binary? bin))
  (test* "bson-binary-type uuid" 'uuid
         (bson-binary-type bin))
  (test* "bson-binary-bytes uuid" uv
         (bson-binary-bytes bin))
  (test* "write-bson-binary-prepare size uuid" (+ 4 1 (uvector-size uv))
         (receive (s w) (write-bson-binary-prepare bin) s))
  (test* "bson-binary invariance uuid" #t
         (invariance? bin write-bson-binary read-bson-binary))
  (test* "bson-binary object-equal? uuid" #t
         (equal? bin (bson-binary 'uuid (u8vector 0)))))

(let* ([uv (u8vector 0)]
       [bin (bson-binary 'md5 uv)])
  (test* "bson-binary constructor md5" #t
         (bson-binary? bin))
  (test* "bson-binary-type md5" 'md5
         (bson-binary-type bin))
  (test* "bson-binary-bytes md5" uv
         (bson-binary-bytes bin))
  (test* "write-bson-binary-prepare size md5" (+ 4 1 (uvector-size uv))
         (receive (s w) (write-bson-binary-prepare bin) s))
  (test* "bson-binary invariance md5" #t
         (invariance? bin write-bson-binary read-bson-binary))
  (test* "bson-binary object-equal? md5" #t
         (equal? bin (bson-binary 'md5 (u8vector 0)))))

(let* ([uv (u8vector 0)]
       [bin (bson-binary 'user-defined uv)])
  (test* "bson-binary constructor user-defined" #t
         (bson-binary? bin))
  (test* "bson-binary-type user-defined" 'user-defined
         (bson-binary-type bin))
  (test* "bson-binary-bytes user-defined" uv
         (bson-binary-bytes bin))
  (test* "write-bson-binary-prepare size user-defined" (+ 4 1 (uvector-size uv))
         (receive (s w) (write-bson-binary-prepare bin) s))
  (test* "bson-binary invariance user-defined" #t
         (invariance? bin write-bson-binary read-bson-binary))
  (test* "bson-binary object-equal? user-defined" #t
         (equal? bin (bson-binary 'user-defined (u8vector 0)))))

(test* "bson-binary constructor exception" (test-error <bson-construct-error>)
       (bson-binary 'foo (u8vector 0)))

(test* "read-bson-binary subtype exception" (test-error <bson-read-error>)
       (call-with-input-string (call-with-output-string
                                 (^[out]
                                   (write-s32 1 out 'litte-endian)
                                   (write-byte #xFF out)
                                   (write-block (u8vector 0) out)))
         (^[in] (read-bson-binary in))))

(test* "read-bson-binary eof exception" (test-error <bson-read-error>)
       (call-with-input-string (call-with-output-string
                                 (^[out]
                                   (write-s32 1 out 'litte-endian)
                                   (write-byte #x00 out)
                                   (write-block (u8vector) out)))
         (^[in] (read-bson-binary in))))

(test* "read-bson-binary size exception" (test-error <bson-read-error>)
       (call-with-input-string (call-with-output-string
                                 (^[out]
                                   (write-s32 2 out 'litte-endian)
                                   (write-byte #x00 out)
                                   (write-block (u8vector 123) out)))
         (^[in] (read-bson-binary in))))

;;;; code

(test-section "code")

(test* "bson-code constructor" #t
       (bson-code? (bson-code "foo")))

(test* "bson-code-string" "foo"
       (bson-code-string (bson-code "foo")))

(test* "bson-code invariance" #t
       (invariance? (bson-code "foo") write-bson-code read-bson-code))

(test* "write-bson-code-prepare size" 8
       (receive (s w) (write-bson-code-prepare (bson-code "foo")) s))

(test* "bson-code object-equal?" #t
       (equal? (bson-code "foo") (bson-code "foo")))

;;;; code/scope

(test-section "code/scope")

(test* "bson-code/scope constructor" #t
       (bson-code/scope? (bson-code/scope "foo" '(("bar" . "baz")))))

(test* "bson-code/scope-string" "foo"
       (bson-code/scope-string (bson-code/scope "foo" '(("bar" . "baz")))))

(test* "bson-code/scope-scope" '(("bar" . "baz"))
       (bson-code/scope-scope (bson-code/scope "foo" '(("bar" . "baz")))))

(test* "bson-code/scope invariance" #t
       (invariance? (bson-code/scope "foo" '(("bar" . "baz")))
                    write-bson-code/scope
                    read-bson-code/scope))

(test* "write-bson-code/scope-prepare size" 30
       (receive (s w) (write-bson-code/scope-prepare
                       (bson-code/scope "foo" '(("bar" . "baz"))))
         s))

(test* "bson-code/scope object-equal?" #t
       (equal? (bson-code/scope "foo" '(("bar" . "baz")))
               (bson-code/scope "foo" '(("bar" . "baz")))))

;;;; object-id

(test-section "object-id")

(test* "bson-object-id constructor" #t
       (bson-object-id? (bson-object-id)))

(test* "bson-object-id <string> constructor" #t
       (bson-object-id? (bson-object-id "4f4cd3da4cfad11445000008")))

(test* "bson-object-id <string> constructor exception"
       (test-error <bson-construct-error>)
       (bson-object-id "4f4cd3da4cfad1144500000x"))

(test* "bson-object-id <u8vector> constructor" #t
       (bson-object-id? (bson-object-id (u8vector 0 0 0 0 0 0 0 0 0 0 0 0))))

(test* "bson-object-id <u8vector> constructor exception"
       (test-error <bson-construct-error>)
       (bson-object-id (u8vector 0)))

(test* "bson-object-id <integer> constructor" #t
       (bson-object-id? (bson-object-id 0 0 0 0)))

(test* "bson-object-id constructor time supremum 1" #t
       (bson-object-id? (bson-object-id (- (expt 2 31) 1) 0 0 0)))
(test* "bson-object-id constructor time supremum 2"
       (test-error <bson-construct-error>)
       (bson-object-id (expt 2 31) 0 0 0))
(test* "bson-object-id constructor time infimum 1" #t
       (bson-object-id? (bson-object-id (* -1 (expt 2 31)) 0 0 0)))
(test* "bson-object-id constructor time infimum 2"
       (test-error <bson-construct-error>)
       (bson-object-id (- (* -1 (expt 2 31)) 1) 0 0 0))

(test* "bson-object-id constructor machine supremum 1" #t
       (bson-object-id? (bson-object-id 0 (- (expt 2 24) 1) 0 0)))
(test* "bson-object-id constructor machine supremum 2"
       (test-error <bson-construct-error>)
       (bson-object-id 0 (expt 2 24) 0 0))
(test* "bson-object-id constructor machine infimum 1" #t
       (bson-object-id? (bson-object-id 0 0 0 0)))
(test* "bson-object-id constructor machine infimum 2"
       (test-error <bson-construct-error>)
       (bson-object-id 0 -1 0 0))

(test* "bson-object-id constructor pid supremum 1" #t
       (bson-object-id? (bson-object-id 0 0 (- (expt 2 16) 1) 0)))
(test* "bson-object-id constructor pid supremum 2"
       (test-error <bson-construct-error>)
       (bson-object-id 0 0 (expt 2 16) 0))
(test* "bson-object-id constructor pid infimum 1" #t
       (bson-object-id? (bson-object-id 0 0 0 0)))
(test* "bson-object-id constructor pid infimum 2"
       (test-error <bson-construct-error>)
       (bson-object-id 0 0 -1 0))

(test* "bson-object-id constructor inc supremum 1" #t
       (bson-object-id? (bson-object-id 0 0 0 (- (expt 2 24) 1))))
(test* "bson-object-id constructor inc supremum 2"
       (test-error <bson-construct-error>)
       (bson-object-id 0 0 0 (expt 2 24)))
(test* "bson-object-id constructor inc infimum 1" #t
       (bson-object-id? (bson-object-id 0 0 0 0)))
(test* "bson-object-id constructor inc infimum 2"
       (test-error <bson-construct-error>)
       (bson-object-id 0 0 0 -1))

(let* ([time (sys-time)]
       [machine (bson-object-id-generate-machine)]
       [pid (bson-object-id-generate-pid)]
       [inc (bson-object-id-generate-inc)]
       [oid (bson-object-id time machine pid inc)])
  (test* "write-bson-object-id-prepare size" 12
         (receive (s w) (write-bson-object-id-prepare oid) s))
  (test* "bson-object-id-time" time (bson-object-id-time oid))
  (test* "bson-object-id-machine" machine (bson-object-id-machine oid))
  (test* "bson-object-id-pid" pid (bson-object-id-pid oid))
  (test* "bson-object-id-inc" inc (bson-object-id-inc oid))
  (test* "bson-object-id invariance" #t
         (invariance? oid write-bson-object-id read-bson-object-id))
  (test* "bson-object-id object-equal?" #t
         (equal? oid (bson-object-id time machine pid inc))))

;;;; datetime

(test-section "datetime")

(test* "bson-datetime constructor" #t
       (bson-datetime? (bson-datetime)))

(let* ([ms 123]
       [dt (bson-datetime ms)])
  (test* "bson-datetime <integer> constructor" #t
         (bson-datetime? dt))
  (test* "bson-datetime-millisecond" ms
         (bson-datetime-millisecond dt))
  (test* "bson-datetime invariance" #t
         (invariance? dt write-bson-datetime read-bson-datetime))
  (test* "bson-datetime object-equal?" #t
         (equal? dt (bson-datetime ms))))

(test* "bson-datetime constructor supremum 1" #t
       (bson-datetime? (bson-datetime (- (expt 2 63) 1))))
(test* "bson-datetime constructor supremum 2" (test-error <bson-construct-error>)
       (bson-datetime (expt 2 63)))
(test* "bson-datetime constructor infimum 1" #t
       (bson-datetime? (bson-datetime (* -1 (expt 2 63)))))
(test* "bson-datetime constructor infimum 2" (test-error <bson-construct-error>)
       (bson-datetime (- (* -1 (expt 2 63)) 1)))

;;;; regexp

(test-section "regexp")

(let* ([pat "foo"]
       [opt "bar"]
       [rx (bson-regexp pat opt)])
  (test* "bson-regexp constructor" #t
         (bson-regexp? rx))
  (test* "bson-regexp-pattern" pat
         (bson-regexp-pattern rx))
  (test* "bson-regexp-options" opt
         (bson-regexp-options rx))
  (test* "bson-regexp invariance" #t
         (invariance? rx write-bson-regexp read-bson-regexp))
  (test* "bson-regexp object-equal?" #t
         (equal? rx (bson-regexp pat opt))))

;;;; dbpointer

(test-section "dbpointer")

(let* ([ns "foo.bar"]
       [oid (bson-object-id)]
       [dbp (bson-dbpointer ns oid)])
  (test* "bson-dbpointer constructor" #t
         (bson-dbpointer? dbp))
  (test* "bson-dbpointer-namespace" ns
         (bson-dbpointer-namespace dbp))
  (test* "bson-dbpointer-object-id" oid
         (bson-dbpointer-object-id dbp))
  (test* "bson-dbpointer invariance" #t
         (invariance? dbp write-bson-dbpointer read-bson-dbpointer))
  (test* "bson-dbpointer object-equal?" #t
         (equal? dbp (bson-dbpointer ns oid))))

;;;; symbol

(test-section "symbol")

(let* ([str "foo"]
       [sym (bson-symbol str)])
  (test* "bson-symbol constructor" #t
         (bson-symbol? sym))
  (test* "bson-symbol-string" str
         (bson-symbol-string sym))
  (test* "bson-symbol invariance" #t
         (invariance? sym write-bson-symbol read-bson-symbol))
  (test* "bson-symbol object-equal?" #t
         (equal? sym (bson-symbol str))))

;;;; timestamp

(test-section "timestamp")

(test* "bson-timestamp constructor" #t
       (bson-timestamp? (bson-timestamp)))

(test* "bson-timestamp <integer> constructor 1" #t
       (bson-timestamp? (bson-timestamp (sys-time))))

(test* "bson-timestamp <integer> constructor 2" #t
       (bson-timestamp? (bson-timestamp (sys-time) 1)))

(test* "bson-timestamp constructor time supremum 1" #t
       (bson-timestamp? (bson-timestamp (- (expt 2 31) 1) 0)))
(test* "bson-timestamp constructor time supremum 2"
       (test-error <bson-construct-error>)
       (bson-timestamp (expt 2 31) 0))
(test* "bson-timestamp constructor time infimum 1" #t
       (bson-timestamp? (bson-timestamp (* -1 (expt 2 31)) 0)))
(test* "bson-timestamp constructor time infimum 2"
       (test-error <bson-construct-error>)
       (bson-timestamp (- (* -1 (expt 2 31)) 1) 0))

(test* "bson-timestamp constructor inc supremum 1" #t
       (bson-timestamp? (bson-timestamp 0 (- (expt 2 32) 1))))
(test* "bson-timestamp constructor inc supremum 2"
       (test-error <bson-construct-error>)
       (bson-timestamp (expt 2 32) 0))
(test* "bson-timestamp constructor inc infimum 1" #t
       (bson-timestamp? (bson-timestamp 0 0)))
(test* "bson-timestamp constructor time infimum 2"
       (test-error <bson-construct-error>)
       (bson-timestamp 0 -1))

(let* ([time (sys-time)]
       [inc (bson-timestamp-generate-inc)]
       [ts (bson-timestamp time inc)])
  (test* "bson-timestamp-time" time (bson-timestamp-time ts))
  (test* "bson-timestamp-inc" inc (bson-timestamp-inc ts))
  (test* "bson-timestamp invariance" #t
         (invariance? ts write-bson-timestamp read-bson-timestamp))
  (test* "bson-timestamp object-equal?" #t
         (equal? ts (bson-timestamp time inc))))

;;;; document

(test-section "document")

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

(test* "bson-document constructor" #t
       (bson-document? *doc*))

(test* "bson-document invariance" #t
       (invariance? *doc* write-bson-document read-bson-document))

(test* "write-bson-document exception" (test-error <bson-write-error>)
       (invariance? '(("foo" . foo)) write-bson-document read-bson-document))

(test* "read-bson-document exception 1" (test-error <bson-read-error>)
       (call-with-input-string "\x12\0\0\0\x13foo\0\x04\0\0\0bar\0\0"
         (^[in] (read-bson-document in))))

(test* "read-bson-document exception 2" (test-error <bson-read-error>)
       (call-with-input-string "\x12\0\0\0"
         (^[in] (read-bson-document in))))

;;;; array

(test-section "array")

(test* "bson-array invariance" #t
       (invariance? (vector "foo" "bar" "baz")
                    write-bson-array
                    read-bson-array))

(test-end)
