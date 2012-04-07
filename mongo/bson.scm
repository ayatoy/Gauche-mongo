(define-module mongo.bson
  (use gauche.collection)
  (use gauche.record)
  (use gauche.sequence)
  (use gauche.threads)
  (use gauche.uvector)
  (use srfi-11)
  (use srfi-19)
  (use binary.io)
  (use rfc.md5)
  (use util.match)
  (use mongo.util)
  (export BSON_ZERO
          BSON_INT32_MIN
          BSON_INT32_MAX
          BSON_INT64_MIN
          BSON_INT64_MAX
          BSON_ZERO_SIZE
          BSON_INT32_SIZE
          BSON_INT64_SIZE
          BSON_DOUBLE_SIZE
          BSON_OBJECT_ID_SIZE
          BSON_SUBTYPE_SIZE
          BSON_BOOLEAN_SIZE
          BSON_FALSE
          BSON_TRUE
          BSON_SUBTYPE_GENERIC
          BSON_SUBTYPE_FUNCTION
          BSON_SUBTYPE_OLD
          BSON_SUBTYPE_UUID
          BSON_SUBTYPE_MD5
          BSON_SUBTYPE_USER_DEFINED
          BSON_TYPE_DOUBLE
          BSON_TYPE_STRING
          BSON_TYPE_DOCUMENT
          BSON_TYPE_ARRAY
          BSON_TYPE_BINARY
          BSON_TYPE_UNDEFINED
          BSON_TYPE_OBJECT_ID
          BSON_TYPE_BOOLEAN
          BSON_TYPE_DATETIME
          BSON_TYPE_NULL
          BSON_TYPE_REGEXP
          BSON_TYPE_DBPOINTER
          BSON_TYPE_CODE
          BSON_TYPE_SYMBOL
          BSON_TYPE_CODE/SCOPE
          BSON_TYPE_INT32
          BSON_TYPE_TIMESTAMP
          BSON_TYPE_INT64
          BSON_TYPE_MIN_KEY
          BSON_TYPE_MAX_KEY
          <bson-error>
          bson-error?
          <bson-read-error>
          bson-read-error?
          <bson-write-error>
          bson-write-error?
          <bson-construct-error>
          bson-construct-error?
          prepare-fold
          bson-int32?
          write-bson-int32-prepare
          write-bson-int32
          read-bson-int32
          bson-int64?
          write-bson-int64-prepare
          write-bson-int64
          read-bson-int64
          bson-double?
          write-bson-double-prepare
          write-bson-double
          read-bson-double
          bson-string?
          write-bson-string-prepare
          write-bson-string
          read-bson-string
          bson-cstring?
          write-bson-cstring-prepare
          write-bson-cstring
          read-bson-cstring
          bson-null?
          bson-null
          bson-false?
          bson-true?
          bson-boolean?
          bson-false
          bson-true
          bson-boolean
          write-bson-boolean-prepare
          write-bson-boolean
          read-bson-boolean
          bson-undefined?
          bson-undefined
          bson-min?
          bson-max?
          bson-min
          bson-max
          <bson-binary>
          bson-binary?
          bson-binary
          bson-binary-type
          bson-binary-bytes
          write-bson-binary-prepare
          write-bson-binary
          read-bson-binary
          <bson-code>
          bson-code?
          bson-code
          bson-code-string
          write-bson-code-prepare
          write-bson-code
          read-bson-code
          <bson-code/scope>
          bson-code/scope?
          bson-code/scope
          bson-code/scope-string
          bson-code/scope-scope
          write-bson-code/scope-prepare
          write-bson-code/scope
          read-bson-code/scope
          <bson-object-id>
          bson-object-id?
          bson-object-id
          bson-object-id-bytes
          bson-object-id-time
          bson-object-id-machine
          bson-object-id-pid
          bson-object-id-inc
          bson-object-id-generate-machine
          bson-object-id-generate-pid
          bson-object-id-generate-inc
          write-bson-object-id-prepare
          write-bson-object-id
          read-bson-object-id
          <bson-datetime>
          bson-datetime?
          bson-datetime
          bson-datetime-int64
          bson-datetime-millisecond
          write-bson-datetime-prepare
          write-bson-datetime
          read-bson-datetime
          <bson-regexp>
          bson-regexp?
          bson-regexp
          bson-regexp-pattern
          bson-regexp-options
          write-bson-regexp-prepare
          write-bson-regexp
          read-bson-regexp
          <bson-dbpointer>
          bson-dbpointer?
          bson-dbpointer
          bson-dbpointer-namespace
          bson-dbpointer-object-id
          write-bson-dbpointer-prepare
          write-bson-dbpointer
          read-bson-dbpointer
          <bson-symbol>
          bson-symbol?
          bson-symbol
          bson-symbol-string
          write-bson-symbol-prepare
          write-bson-symbol
          read-bson-symbol
          <bson-timestamp>
          bson-timestamp?
          bson-timestamp
          bson-timestamp-int64
          bson-timestamp-time
          bson-timestamp-inc
          bson-timestamp-generate-inc
          write-bson-timestamp-prepare
          write-bson-timestamp
          read-bson-timestamp
          bson-document?
          write-bson-document-prepare
          write-bson-document
          read-bson-document
          write-bson-array-prepare
          write-bson-array
          read-bson-array))
(select-module mongo.bson)

;;;; constant

(define-constant BSON_ZERO 0)

(define-constant BSON_INT32_MIN -2147483648)
(define-constant BSON_INT32_MAX 2147483647)
(define-constant BSON_INT64_MIN -9223372036854775808)
(define-constant BSON_INT64_MAX 9223372036854775807)

(define-constant BSON_ZERO_SIZE      1)
(define-constant BSON_INT32_SIZE     4)
(define-constant BSON_INT64_SIZE     8)
(define-constant BSON_DOUBLE_SIZE    8)
(define-constant BSON_OBJECT_ID_SIZE 12)
(define-constant BSON_SUBTYPE_SIZE   1)
(define-constant BSON_BOOLEAN_SIZE   1)

(define-constant BSON_FALSE #x00)
(define-constant BSON_TRUE  #x01)

(define-constant BSON_SUBTYPE_GENERIC      #x00)
(define-constant BSON_SUBTYPE_FUNCTION     #x01)
(define-constant BSON_SUBTYPE_OLD          #x02)
(define-constant BSON_SUBTYPE_UUID         #x03)
(define-constant BSON_SUBTYPE_MD5          #x05)
(define-constant BSON_SUBTYPE_USER_DEFINED #x80)

(define-constant BSON_TYPE_DOUBLE     #x01)
(define-constant BSON_TYPE_STRING     #x02)
(define-constant BSON_TYPE_DOCUMENT   #x03)
(define-constant BSON_TYPE_ARRAY      #x04)
(define-constant BSON_TYPE_BINARY     #x05)
(define-constant BSON_TYPE_UNDEFINED  #x06)
(define-constant BSON_TYPE_OBJECT_ID  #x07)
(define-constant BSON_TYPE_BOOLEAN    #x08)
(define-constant BSON_TYPE_DATETIME   #x09)
(define-constant BSON_TYPE_NULL       #x0A)
(define-constant BSON_TYPE_REGEXP     #x0B)
(define-constant BSON_TYPE_DBPOINTER  #x0C)
(define-constant BSON_TYPE_CODE       #x0D)
(define-constant BSON_TYPE_SYMBOL     #x0E)
(define-constant BSON_TYPE_CODE/SCOPE #x0F)
(define-constant BSON_TYPE_INT32      #x10)
(define-constant BSON_TYPE_TIMESTAMP  #x11)
(define-constant BSON_TYPE_INT64      #x12)
(define-constant BSON_TYPE_MIN_KEY    #xFF)
(define-constant BSON_TYPE_MAX_KEY    #x7F)

;;;; condition

(define-condition-type <bson-error> <mongo-error>
  bson-error?)

(define-condition-type <bson-read-error> <bson-error>
  bson-read-error?)

(define-condition-type <bson-write-error> <bson-error>
  bson-write-error?)

(define-condition-type <bson-construct-error> <bson-error>
  bson-construct-error?)

;;;; util

(define-syntax define-writer
  (syntax-rules ()
    [(_ writer-name prepare-name)
     (define (writer-name x oport)
       ((values-ref (prepare-name x) 1) oport))]))

(define (empty-prepare x)
  (values 0 (^[oport] (undefined))))

(define (prepare-fold proc vals)
  (fold2 (^[val vals-size write-vals]
           (receive (val-size write-val) (proc val)
             (values (+ vals-size val-size)
                     (^[oport]
                       (write-vals oport)
                       (write-val oport)))))
         0
         (^[oport] (undefined))
         vals))

(define (read-nbyte-to-u8vector n iport)
  (let* ([u8v (make-u8vector n)]
         [res (read-block! u8v iport)])
    (cond
     [(eof-object? res)
      (error <bson-read-error> :reason #f "unexpected EOF")]
     [(not (= n res))
      (errorf <bson-read-error> :reason #f "expected ~s byte: ~s" n res)]
     [else u8v])))

;;;; int32

(define (bson-int32? obj)
  (and (integer? obj)
       (<= BSON_INT32_MIN obj BSON_INT32_MAX)))

(define (write-bson-int32-prepare int)
  (values BSON_INT32_SIZE
          (^[oport] (write-s32 int oport 'little-endian))))

(define-writer write-bson-int32 write-bson-int32-prepare)

(define (read-bson-int32 iport)
  (let1 int (read-s32 iport 'little-endian)
    (if (eof-object? int)
      (error <bson-read-error> :reason #f "unexpected EOF")
      int)))

;;;; int64

(define (bson-int64? obj)
  (and (integer? obj)
       (<= BSON_INT64_MIN obj BSON_INT64_MAX)))

(define (write-bson-int64-prepare int)
  (values BSON_INT64_SIZE
          (^[oport] (write-s64 int oport 'little-endian))))

(define-writer write-bson-int64 write-bson-int64-prepare)

(define (read-bson-int64 iport)
  (let1 int (read-s64 iport 'little-endian)
    (if (eof-object? int)
      (error <bson-read-error> :reason #f "unexpected EOF")
      int)))

;;;; double

(define (bson-double? obj)
  (and (real? obj)
       (finite? obj)
       (not (nan? obj))))

(define (write-bson-double-prepare num)
  (values BSON_DOUBLE_SIZE
          (^[oport] (write-f64 num oport 'little-endian))))

(define-writer write-bson-double write-bson-double-prepare)

(define (read-bson-double iport)
  (let1 num (read-f64 iport 'little-endian)
    (if (eof-object? num)
      (error <bson-read-error> :reason #f "unexpected EOF")
      num)))

;;;; string

(define (bson-string? obj)
  (and (string? obj)
       (not (string-incomplete? obj))))

(define (write-bson-string-prepare str)
  (let1 size (+ (string-size str) 1)
    (values (+ BSON_INT32_SIZE size)
            (^[oport]
              (write-bson-int32 size oport)
              (if (string-incomplete? str)
                (error <bson-write-error> :reason #f
                       "expected complete string:" str)
                (write-block (string->u8vector str) oport))
              (write-byte BSON_ZERO oport)))))

(define-writer write-bson-string write-bson-string-prepare)

(define (read-bson-string iport)
  (let1 str-size (- (read-bson-int32 iport) 1)
    (begin0 (let1 str (read-block str-size iport)
              (or (and-let* ([(not (eof-object? str))]
                             [(= (string-size str) str-size)]
                             [strc (string-incomplete->complete str)])
                    strc)
                  (error <bson-read-error> :reason #f "unexpected input:" str)))
            (let1 zero (read-byte iport)
              (or (and (not (eof-object? zero))
                       (= zero BSON_ZERO))
                  (error <bson-read-error> :reason #f "expected 0:" zero))))))

;;;; cstring

(define (bson-cstring? obj)
  (and (string? obj)
       (not (string-incomplete? obj))))

(define (write-bson-cstring-prepare str)
  (let1 size (string-size str)
    (values (+ size 1)
            (^[oport]
              (call-with-input-string str
                (^[str-iport]
                  (let loop ([i size] [byte (read-byte str-iport)])
                    (cond [(>= 0 i)
                           (write-byte BSON_ZERO oport)]
                          [(= BSON_ZERO byte)
                           (error <bson-write-error> :reason #f
                                  "unexpected byte:" byte)]
                          [(eof-object? byte)
                           (error <bson-write-error> :reason #f
                                  "unexpected EOF")]
                          [else
                           (write-byte byte oport)
                           (loop (- i 1) (read-byte str-iport))]))))))))

(define-writer write-bson-cstring write-bson-cstring-prepare)

(define (read-bson-cstring iport)
  (call-with-output-string
    (^[str-oport]
      (let loop ([byte (read-byte iport)])
        (cond [(eof-object? byte)
               (error <bson-read-error> :reason #f "unexpected EOF")]
              [(not (= byte BSON_ZERO))
               (write-byte byte str-oport)
               (loop (read-byte iport))])))))

;;;; null

(define (bson-null? obj) (eq? 'null obj))
(define (bson-null) 'null)

;;;; boolean

(define (bson-false? obj) (eq? 'false obj))
(define (bson-true? obj) (eq? 'true obj))
(define (bson-boolean? obj) (or (eq? 'false obj) (eq? 'true obj)))
(define (bson-false) 'false)
(define (bson-true) 'true)
(define (bson-boolean obj) (if obj 'true 'false))

(define (write-bson-boolean-prepare bson-bool)
  (values BSON_BOOLEAN_SIZE
          (^[oport]
            (write-byte
             (cond [(eq? bson-bool 'false) BSON_FALSE]
                   [(eq? bson-bool 'true)  BSON_TRUE]
                   [else (error <bson-write-error> :reason #f
                                "unexpected symbol:" bson-bool)])
             oport))))

(define-writer write-bson-boolean write-bson-boolean-prepare)

(define (read-bson-boolean iport)
  (let1 byte (read-byte iport)
    (cond [(= byte BSON_FALSE) 'false]
          [(= byte BSON_TRUE)  'true]
          [else (error <bson-read-error> :reason #f "unexpected byte:" byte)])))

;;;; undefined

(define (bson-undefined? obj) (eq? 'undefined obj))
(define (bson-undefined) 'undefined)

;;;; min/max

(define (bson-min? obj) (eq? 'min obj))
(define (bson-max? obj) (eq? 'max obj))
(define (bson-min) 'min)
(define (bson-max) 'max)

;;;; binary

(define-record-type <bson-binary> make-bson-binary bson-binary?
  (type  bson-binary-type)
  (bytes bson-binary-bytes))

(define (write-bson-binary-prepare bson-bin)
  (let* ([bytes-u8v      (bson-binary-bytes bson-bin)]
         [bytes-u8v-size (uvector-size bytes-u8v)])
    (values (+ BSON_INT32_SIZE BSON_SUBTYPE_SIZE bytes-u8v-size)
            (^[oport]
              (write-bson-int32 bytes-u8v-size oport)
              (write-byte (let1 type (bson-binary-type bson-bin)
                            (cond [(eq? type 'generic)
                                   BSON_SUBTYPE_GENERIC]
                                  [(eq? type 'function)
                                   BSON_SUBTYPE_FUNCTION]
                                  [(eq? type 'old)
                                   BSON_SUBTYPE_OLD]
                                  [(eq? type 'uuid)
                                   BSON_SUBTYPE_UUID]
                                  [(eq? type 'md5)
                                   BSON_SUBTYPE_MD5]
                                  [(eq? type 'user-defined)
                                   BSON_SUBTYPE_USER_DEFINED]
                                  [else (error <bson-write-error> :reason #f
                                               "unexpected type:" type)]))
                          oport)
              (write-block bytes-u8v oport)))))

(define-writer write-bson-binary write-bson-binary-prepare)

(define (read-bson-binary iport)
  (let ([bytes-size (read-bson-int32 iport)]
        [subtype    (read-byte iport)])
    (make-bson-binary
     (cond [(= subtype BSON_SUBTYPE_GENERIC)      'generic]
           [(= subtype BSON_SUBTYPE_FUNCTION)     'function]
           [(= subtype BSON_SUBTYPE_OLD)          'old]
           [(= subtype BSON_SUBTYPE_UUID)         'uuid]
           [(= subtype BSON_SUBTYPE_MD5)          'md5]
           [(= subtype BSON_SUBTYPE_USER_DEFINED) 'user-defined]
           [else (error <bson-read-error> :reason #f
                        "unexpected byte:" subtype)])
     (read-nbyte-to-u8vector bytes-size iport))))

(define-method object-equal? ((bin1 <bson-binary>) (bin2 <bson-binary>))
  (and (eq? (bson-binary-type bin1) (bson-binary-type bin2))
       (equal? (bson-binary-bytes bin1) (bson-binary-bytes bin2))))

(define-method write-object ((bin <bson-binary>) oport)
  (format oport "#<bson-binary ~a ~a>"
          (bson-binary-type bin)
          (uvector-size (bson-binary-bytes bin))))

(define-method bson-binary ((u8v <u8vector>))
  (make-bson-binary 'generic u8v))

(define-method bson-binary ((type <symbol>) (u8v <u8vector>))
  (case type
    [(generic function old uuid md5 user-defined) (make-bson-binary type u8v)]
    [else (error <bson-construct-error> :reason #f "unexpected type:" type)]))

;;;; code

(define-record-type <bson-code> make-bson-code bson-code?
  (string bson-code-string))

(define (write-bson-code-prepare code)
  (write-bson-string-prepare (bson-code-string code)))

(define-writer write-bson-code write-bson-code-prepare)

(define (read-bson-code iport)
  (make-bson-code (read-bson-string iport)))

(define-method object-equal? ((code1 <bson-code>) (code2 <bson-code>))
  (string=? (bson-code-string code1) (bson-code-string code2)))

(define (bson-code str)
  (make-bson-code str))

;;;; code/scope

(define-record-type <bson-code/scope> make-bson-code/scope bson-code/scope?
  (string bson-code/scope-string)
  (scope  bson-code/scope-scope))

(define (write-bson-code/scope-prepare code/s)
  (let-values ([(str-size write-str)
                (write-bson-string-prepare
                 (bson-code/scope-string code/s))]
               [(scp-size write-scp)
                (write-bson-document-prepare
                 (bson-code/scope-scope code/s))])
    (let1 total-size (+ BSON_INT32_SIZE str-size scp-size)
      (values total-size
              (^[oport]
                (write-bson-int32 total-size oport)
                (write-str oport)
                (write-scp oport))))))

(define-writer write-bson-code/scope write-bson-code/scope-prepare)

(define (read-bson-code/scope iport)
  (read-bson-int32 iport)
  (make-bson-code/scope (read-bson-string iport) (read-bson-document iport)))

(define-method object-equal? ((c/s1 <bson-code/scope>) (c/s2 <bson-code/scope>))
  (and (string=? (bson-code/scope-string c/s1) (bson-code/scope-string c/s2))
       (equal? (bson-code/scope-scope c/s1) (bson-code/scope-scope c/s2))))

(define-method bson-code/scope ((str <string>) (scope <list>))
  (unless (bson-document? scope)
    (error <bson-construct-error> :reason #f "unexpected type:" scope))
  (make-bson-code/scope str scope))

(define-method bson-code ((str <string>) (scope <list>))
  (bson-code/scope str scope))

;;;; object-id

(define-record-type <bson-object-id> make-bson-object-id bson-object-id?
  (bytes bson-object-id-bytes))

(define (write-bson-object-id-prepare oid)
  (values BSON_OBJECT_ID_SIZE
          (^[oport] (write-block (bson-object-id-bytes oid) oport))))

(define-writer write-bson-object-id write-bson-object-id-prepare)

(define (read-bson-object-id iport)
  (make-bson-object-id (read-nbyte-to-u8vector BSON_OBJECT_ID_SIZE iport)))

(define-method object-equal? ((oid1 <bson-object-id>) (oid2 <bson-object-id>))
  (equal? (bson-object-id-bytes oid1) (bson-object-id-bytes oid2)))

(define-method write-object ((oid <bson-object-id>) oport)
  (format oport "#<bson-object-id ~s>"
          (call-with-output-string
            (^[out] (for-each (^[n] (format out "~2,'0x" n))
                              (bson-object-id-bytes oid))))))

(define (bson-object-id-time oid)
  (get-s32be (bson-object-id-bytes oid) 0))

(define (bson-object-id-machine oid)
  (u8vector-sub-bytes-to-integer (bson-object-id-bytes oid) 4 7))

(define (bson-object-id-pid oid)
  (get-u16be (bson-object-id-bytes oid) 7))

(define (bson-object-id-inc oid)
  (u8vector-sub-bytes-to-integer (bson-object-id-bytes oid) 9 12))

(define bson-object-id-generate-inc (make-counter (expt 2 24)))

(define (bson-object-id-generate-machine)
  (u8vector-sub-bytes-to-integer
   (string->u8vector (md5-digest-string (sys-gethostname))) 0 3))

(define (bson-object-id-generate-pid)
  (modulo (sys-getpid) 65536))

(define (bson-object-id t m p i)
  (unless (<= -2147483648 t 2147483647)
    (error <bson-construct-error> :reason #f "argument out of range:" t))
  (unless (<= 0 m 16777215)
    (error <bson-construct-error> :reason #f "argument out of range:" m))
  (unless (<= 0 p 65535)
    (error <bson-construct-error> :reason #f "argument out of range:" p))
  (unless (<= 0 i 16777215)
    (error <bson-construct-error> :reason #f "argument out of range:" i))
  (make-bson-object-id
   (rlet1 uv (make-u8vector BSON_OBJECT_ID_SIZE)
     (put-s32be! uv 0 t)
     (integer-to-u8vector-sub-bytes! m uv 4 7)
     (put-u16be! uv 7 p)
     (integer-to-u8vector-sub-bytes! i uv 9 12))))

(define-method bson-object-id ()
  (bson-object-id
   (sys-time)
   (bson-object-id-generate-machine)
   (bson-object-id-generate-pid)
   (bson-object-id-generate-inc)))

(define-method bson-object-id ((str <string>))
  (unless (= (string-length str) 24)
    (error <bson-construct-error> :reason #f "unexpected length:" str))
  (make-bson-object-id
   (rlet1 u8v (make-u8vector BSON_OBJECT_ID_SIZE)
     (call-with-input-string str
       (^[str-iport]
         (let loop ([i 0])
           (when (> BSON_OBJECT_ID_SIZE i)
             (let* ([s (read-block 2 str-iport)]
                    [n (string->number s 16)])
               (unless (integer? n)
                 (error <bson-construct-error> :reason #f
                        "unexpected input:" s))
               (u8vector-set! u8v i n))
             (loop (+ i 1)))))))))

(define-method bson-object-id ((u8v <u8vector>))
  (unless (= (u8vector-length u8v) BSON_OBJECT_ID_SIZE)
    (error <bson-construct-error> :reason #f "unexpected length:" u8v))
  (make-bson-object-id u8v))

;;;; datetime

(define-record-type <bson-datetime> make-bson-datetime bson-datetime?
  (int64 bson-datetime-int64))

(define (write-bson-datetime-prepare dtime)
  (write-bson-int64-prepare (bson-datetime-int64 dtime)))

(define-writer write-bson-datetime write-bson-datetime-prepare)

(define (read-bson-datetime iport)
  (make-bson-datetime (read-bson-int64 iport)))

(define-method object-equal? ((dtime1 <bson-datetime>) (dtime2 <bson-datetime>))
  (= (bson-datetime-int64 dtime1) (bson-datetime-int64 dtime2)))

(define-method write-object ((dtime <bson-datetime>) oport)
  (format oport "#<bson-datetime ~a>" (bson-datetime-int64 dtime)))

(define (bson-datetime-millisecond dtime)
  (bson-datetime-int64 dtime))

(define (bson-datetime ms)
  (unless (<= BSON_INT64_MIN ms BSON_INT64_MAX)
    (error <bson-construct-error> :reason #f "argument out of range" ms))
  (make-bson-datetime ms))

(define-method bson-datetime ()
  (bson-datetime (current-millisecond)))

;;;; regexp

(define-record-type <bson-regexp> make-bson-regexp bson-regexp?
  (pattern bson-regexp-pattern)
  (options bson-regexp-options))

(define (write-bson-regexp-prepare bson-rx)
  (let-values ([(pattern-size write-pattern)
                (write-bson-cstring-prepare (bson-regexp-pattern bson-rx))]
               [(options-size write-options)
                (write-bson-cstring-prepare (bson-regexp-options bson-rx))])
    (values (+ pattern-size options-size)
            (^[oport]
              (write-pattern oport)
              (write-options oport)))))

(define-writer write-bson-regexp write-bson-regexp-prepare)

(define (read-bson-regexp iport)
  (make-bson-regexp (read-bson-cstring iport) (read-bson-cstring iport)))

(define-method object-equal? ((brx1 <bson-regexp>) (brx2 <bson-regexp>))
  (and (string=? (bson-regexp-pattern brx1) (bson-regexp-pattern brx2))
       (string=? (bson-regexp-options brx1) (bson-regexp-options brx2))))

(define-method bson-regexp ((pattern <string>) (options <string>))
  (make-bson-regexp pattern options))

;;;; dbpointer

(define-record-type <bson-dbpointer> make-bson-dbpointer bson-dbpointer?
  (namespace bson-dbpointer-namespace)
  (object-id bson-dbpointer-object-id))

(define (write-bson-dbpointer-prepare dp)
  (let-values ([(ns-size write-ns)
                (write-bson-string-prepare (bson-dbpointer-namespace dp))]
               [(oid-size write-oid)
                (write-bson-object-id-prepare (bson-dbpointer-object-id dp))])
    (values (+ ns-size oid-size)
            (^[oport]
              (write-ns oport)
              (write-oid oport)))))

(define-writer write-bson-dbpointer write-bson-dbpointer-prepare)

(define (read-bson-dbpointer iport)
  (make-bson-dbpointer
   (read-bson-string iport)
   (make-bson-object-id (read-nbyte-to-u8vector BSON_OBJECT_ID_SIZE iport))))

(define-method object-equal? ((dp1 <bson-dbpointer>) (dp2 <bson-dbpointer>))
  (and (string=? (bson-dbpointer-namespace dp1) (bson-dbpointer-namespace dp2))
       (equal? (bson-dbpointer-object-id dp1) (bson-dbpointer-object-id dp2))))

(define-method bson-dbpointer ((ns <string>) (oid <bson-object-id>))
  (make-bson-dbpointer ns oid))

;;;; symbol

(define-record-type <bson-symbol> make-bson-symbol bson-symbol?
  (string bson-symbol-string))

(define (write-bson-symbol-prepare b-sym)
  (write-bson-string-prepare (bson-symbol-string b-sym)))

(define-writer write-bson-symbol write-bson-symbol-prepare)

(define (read-bson-symbol iport)
  (make-bson-symbol (read-bson-string iport)))

(define-method object-equal? ((b-sym1 <bson-symbol>) (b-sym2 <bson-symbol>))
  (string=? (bson-symbol-string b-sym1) (bson-symbol-string b-sym2)))

(define (bson-symbol str)
  (make-bson-symbol str))

;;;; timestamp

(define-record-type <bson-timestamp> make-bson-timestamp bson-timestamp?
  (int64 bson-timestamp-int64))

(define (write-bson-timestamp-prepare ts)
  (write-bson-int64-prepare (bson-timestamp-int64 ts)))

(define-writer write-bson-timestamp write-bson-timestamp-prepare)

(define (read-bson-timestamp iport)
  (make-bson-timestamp (read-bson-int64 iport)))

(define-method object-equal? ((ts1 <bson-timestamp>) (ts2 <bson-timestamp>))
  (= (bson-timestamp-int64 ts1) (bson-timestamp-int64 ts2)))

(define-method write-object ((ts <bson-timestamp>) oport)
  (format oport "#<bson-timestamp ~a ~a>"
          (bson-timestamp-time ts)
          (bson-timestamp-inc ts)))

(define (bson-timestamp-time ts)
  (let1 uv (make-u8vector BSON_INT64_SIZE)
    (put-s64be! uv 0 (bson-timestamp-int64 ts))
    (get-s32be uv 0)))

(define (bson-timestamp-inc ts)
  (let1 uv (make-u8vector BSON_INT64_SIZE)
    (put-s64be! uv 0 (bson-timestamp-int64 ts))
    (get-u32be uv 4)))

(define bson-timestamp-generate-inc (make-counter (expt 2 32)))

(define (bson-timestamp t i)
  (unless (<= -2147483648 t 2147483647)
    (error <bson-construct-error> :reason #f "argument out of range" t))
  (unless (<= 0 i 4294967295)
    (error <bson-construct-error> :reason #f "argument out of range" i))
  (make-bson-timestamp (logior (ash t 32) i)))

(define-method bson-timestamp (t)
  (bson-timestamp t (bson-timestamp-generate-inc)))

(define-method bson-timestamp ()
  (bson-timestamp (sys-time) (bson-timestamp-generate-inc)))

;;;; document

(define (bson-document? x)
  (match x
    [((_ . _) ...) #t]
    [_ #f]))

(define (write-bson-element-prepare element)
  (let1 element-val (cdr element)
    (let*-values
        ([(type prepare)
          (cond [(bson-int32? element-val)
                 (values BSON_TYPE_INT32 write-bson-int32-prepare)]
                [(bson-int64? element-val)
                 (values BSON_TYPE_INT64 write-bson-int64-prepare)]
                [(bson-double? element-val)
                 (values BSON_TYPE_DOUBLE write-bson-double-prepare)]
                [(bson-string? element-val)
                 (values BSON_TYPE_STRING write-bson-string-prepare)]
                [(bson-null? element-val)
                 (values BSON_TYPE_NULL empty-prepare)]
                [(bson-boolean? element-val)
                 (values BSON_TYPE_BOOLEAN write-bson-boolean-prepare)]
                [(bson-undefined? element-val)
                 (values BSON_TYPE_UNDEFINED empty-prepare)]
                [(bson-min? element-val)
                 (values BSON_TYPE_MIN_KEY empty-prepare)]
                [(bson-max? element-val)
                 (values BSON_TYPE_MAX_KEY empty-prepare)]
                [(bson-binary? element-val)
                 (values BSON_TYPE_BINARY write-bson-binary-prepare)]
                [(bson-datetime? element-val)
                 (values BSON_TYPE_DATETIME write-bson-datetime-prepare)]
                [(bson-timestamp? element-val)
                 (values BSON_TYPE_TIMESTAMP write-bson-timestamp-prepare)]
                [(bson-object-id? element-val)
                 (values BSON_TYPE_OBJECT_ID write-bson-object-id-prepare)]
                [(bson-regexp? element-val)
                 (values BSON_TYPE_REGEXP write-bson-regexp-prepare)]
                [(bson-dbpointer? element-val)
                 (values BSON_TYPE_DBPOINTER write-bson-dbpointer-prepare)]
                [(bson-code? element-val)
                 (values BSON_TYPE_CODE write-bson-code-prepare)]
                [(bson-code/scope? element-val)
                 (values BSON_TYPE_CODE/SCOPE write-bson-code/scope-prepare)]
                [(bson-symbol? element-val)
                 (values BSON_TYPE_SYMBOL write-bson-symbol-prepare)]
                [(bson-document? element-val)
                 (values BSON_TYPE_DOCUMENT write-bson-document-prepare)]
                [(bson-array? element-val)
                 (values BSON_TYPE_ARRAY write-bson-array-prepare)]
                [else
                 (error <bson-write-error> :reason #f
                        "unexpected type:" element-val)])]
         [(key-size write-key) (write-bson-cstring-prepare (car element))]
         [(val-size write-val) (prepare element-val)])
      (values (+ 1 key-size val-size)
              (^[oport]
                (write-byte type oport)
                (write-key oport)
                (write-val oport))))))

(define (write-bson-document-prepare bson-doc)
  (receive (elements-size write-elements)
      (fold2 (^[element elements-size write-elements]
               (receive (element-size write-element)
                   (write-bson-element-prepare element)
                 (values (+ elements-size element-size)
                         (^[oport]
                           (write-elements oport)
                           (write-element oport)))))
             0
             (^[oport] (undefined))
             bson-doc)
    (let1 total-size (+ BSON_INT32_SIZE elements-size BSON_ZERO_SIZE)
      (values total-size
              (^[oport]
                (write-bson-int32 total-size oport)
                (write-elements oport)
                (write-byte BSON_ZERO oport))))))

(define-writer write-bson-document write-bson-document-prepare)

(define (read-bson-element iport)
  (let1 type (read-byte iport)
    (cond [(eof-object? type)
           (error <bson-read-error> :reason #f "unexpected EOF")]
          [(= type BSON_ZERO)
           #f]
          [else
           (cons (read-bson-cstring iport)
                 (cond [(= type BSON_TYPE_DOUBLE)
                        (read-bson-double iport)]
                       [(= type BSON_TYPE_STRING)
                        (read-bson-string iport)]
                       [(= type BSON_TYPE_DOCUMENT)
                        (read-bson-document iport)]
                       [(= type BSON_TYPE_ARRAY)
                        (read-bson-array iport)]
                       [(= type BSON_TYPE_BINARY)
                        (read-bson-binary iport)]
                       [(= type BSON_TYPE_UNDEFINED)
                        (bson-undefined)]
                       [(= type BSON_TYPE_OBJECT_ID)
                        (read-bson-object-id iport)]
                       [(= type BSON_TYPE_BOOLEAN)
                        (read-bson-boolean iport)]
                       [(= type BSON_TYPE_DATETIME)
                        (read-bson-datetime iport)]
                       [(= type BSON_TYPE_NULL)
                        (bson-null)]
                       [(= type BSON_TYPE_REGEXP)
                        (read-bson-regexp iport)]
                       [(= type BSON_TYPE_DBPOINTER)
                        (read-bson-dbpointer iport)]
                       [(= type BSON_TYPE_CODE)
                        (read-bson-code iport)]
                       [(= type BSON_TYPE_SYMBOL)
                        (read-bson-symbol iport)]
                       [(= type BSON_TYPE_CODE/SCOPE)
                        (read-bson-code/scope iport)]
                       [(= type BSON_TYPE_INT32)
                        (read-bson-int32 iport)]
                       [(= type BSON_TYPE_TIMESTAMP)
                        (read-bson-timestamp iport)]
                       [(= type BSON_TYPE_INT64)
                        (read-bson-int64 iport)]
                       [(= type BSON_TYPE_MIN_KEY)
                        (bson-min)]
                       [(= type BSON_TYPE_MAX_KEY)
                        (bson-max)]
                       [else
                        (error <bson-read-error> :reason #f
                               "unexpected byte:" type)]))])))

(define (read-bson-document iport)
  (read-bson-int32 iport)
  (let loop ([elements '()])
    (if-let1 element (read-bson-element iport)
      (loop (cons element elements))
      (reverse elements))))

;;;; array

(define (bson-array? x) (vector? x))

(define (write-bson-array-prepare vec)
  (write-bson-document-prepare
   (map-with-index (^[i v] (cons (number->string i) v)) vec)))

(define-writer write-bson-array write-bson-array-prepare)

(define (read-bson-array iport)
  (let1 doc (read-bson-document iport)
    (rlet1 v (make-vector (length doc))
      (for-each (^[p] (vector-set! v (string->number (car p)) (cdr p))) doc))))
