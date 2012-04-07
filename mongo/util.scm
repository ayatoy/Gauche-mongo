(define-module mongo.util
  (use gauche.record)
  (use gauche.threads)
  (use gauche.uvector)
  (use gauche.vport)
  (use parser.peg)
  (use srfi-19)
  (use util.list)
  (use util.match)
  (export <mongo-error>
          mongo-error?
          mongo-error-reason
          <mongo-parse-error>
          mongo-parse-error?
          alist
          %
          alref
          alset
          alcut
          mutex-lock-recursively!
          mutex-unlock-recursively!
          with-locking-mutex-recursively
          integer-to-u8vector-sub-bytes!
          u8vector-sub-bytes-to-integer
          call-with-input-uvector
          call-with-output-uvector
          make-counter
          mongo-ns-compose
          mongo-ns-parse
          <mongo-address-inet>
          make-mongo-address-inet
          mongo-address-inet?
          mongo-address-inet-host
          mongo-address-inet-port
          <mongo-address-unix>
          make-mongo-address-unix
          mongo-address-unix?
          mongo-address-unix-path
          mongo-address->string
          string->mongo-address
          mongo-uri-parse
          <mongo-clock>
          mongo-clock?
          mongo-clock-thunk
          mongo-clock-thunk-set!
          mongo-clock-interval
          mongo-clock-interval-set!
          mongo-clock
          mongo-clock-start!
          mongo-clock-stop!
          current-millisecond))
(select-module mongo.util)

;;;; condition

(define-condition-type <mongo-error> <error>
  mongo-error?
  (reason mongo-error-reason))

(define-condition-type <mongo-parse-error> <mongo-error>
  mongo-parse-error?)

;;;; alist

(define (alist . kvs)
  (reverse (fold (^[kv r] (cons (cons (car kv) (cadr kv)) r))
                 '()
                 (slices kvs 2))))

(define % alist)

(define (alref alist . ks)
  (fold (^[k alist] (and (list? alist)
                         (assoc-ref alist k)))
        alist
        ks))

(define (alset1 alist k v)
  (let loop ([alist alist] [acc '()] [e #f])
    (if (null? alist)
        (reverse (or (and e acc)
                     (cons (cons k v) acc)))
        (let* ([x  (car alist)]
               [k1 (car x)]
               [p  (equal? k k1)])
          (loop (cdr alist)
                (cons (cons k1 (if p v (cdr x)))
                      acc)
                (or e p))))))

(define (alset alist . kvs)
  (fold (^[kv r] (apply alset1 r kv))
        alist
        (slices kvs 2)))

(define (alcut alist . ks)
  (reverse (fold (^[kv r] (let1 k (car kv)
                            (if (any (^[x] (equal? k x)) ks)
                              r
                              (cons kv r))))
                 '()
                 alist)))

;;;; mutex

(define (mutex-lock-recursively! mutex)
  (if (eq? (mutex-state mutex) (current-thread))
    (let1 n (mutex-specific mutex)
      (mutex-specific-set! mutex (+ n 1)))
    (begin
      (mutex-lock! mutex)
      (mutex-specific-set! mutex 0))))

(define (mutex-unlock-recursively! mutex)
  (let1 n (mutex-specific mutex)
    (if (= n 0)
      (mutex-unlock! mutex)
      (mutex-specific-set! mutex (- n 1)))))

(define (with-locking-mutex-recursively mutex thunk)
  (dynamic-wind
      (^[] (mutex-lock-recursively! mutex))
      thunk
      (^[] (mutex-unlock-recursively! mutex))))

;;;; uvector

(define (integer-to-u8vector-sub-bytes! n uv start end)
  (let loop ([n n] [i start])
    (when (< i end)
      (receive (q r) (quotient&remainder n (expt 2 (* 8 (- end i 1))))
        (u8vector-set! uv i (if (< 255 q) 255 q))
        (loop r (+ i 1))))))

(define (u8vector-sub-bytes-to-integer uv start end)
  (let loop ([n 0] [i start])
    (if (< i end)
      (loop (+ n (ash (u8vector-ref uv i) (* 8 (- end i 1))))
            (+ i 1))
      n)))

(define (call-with-input-uvector uvector proc)
  (let1 port (open-input-uvector uvector)
    (unwind-protect
      (proc port)
      (close-input-port port))))

(define (call-with-output-uvector uvector proc)
  (let1 port (open-output-uvector uvector)
    (unwind-protect
      (proc port)
      (close-output-port port))))

;;;; counter

(define (make-counter cycles)
  (let ([count -1] [mutex (make-mutex)])
    (lambda args
      (match args
        [(x) (with-locking-mutex mutex (^[] (set! count x)))]
        [()  (with-locking-mutex mutex
               (^[] (rlet1 new (modulo (+ count 1) cycles)
                      (set! count new))))]))))

;;;; namespace

(define (mongo-ns-compose . args)
  (call-with-output-string
    (^[out] (fold (^[x d]
                    (unless (string? x)
                      (error <mongo-error> "string required, but got" x))
                    (format out "~a~a" d x)
                    ".")
                  ""
                  args))))

(define (mongo-ns-parse str)
  (guard (e [(<parse-error> e)
             (error <mongo-parse-error> :reason e
                    (condition-ref e 'message))])
    (peg-parse-string ($do [db  ($many1 ($one-of #[^.]))]
                           [col ($seq ($char #\.) ($many1 ($one-of #[^])))]
                           ($return (list (list->string db)
                                          (list->string col))))
                      str)))

;;;; uri

(define $mongo-uri-user
  ($many1 ($one-of #[^@,:])))

(define $mongo-uri-pass
  ($many1 ($one-of #[^@,])))

(define $mongo-uri-auth
  ($optional ($try ($do [user $mongo-uri-user]
                        [($char #\:)]
                        [pass $mongo-uri-pass]
                        [($char #\@)]
                        ($return (list (list->string user)
                                       (list->string pass)))))))

(define-record-type <mongo-address-inet>
  make-mongo-address-inet
  mongo-address-inet?
  (host mongo-address-inet-host)
  (port mongo-address-inet-port))

(define-method write-object ((obj <mongo-address-inet>) out)
  (format out "#<mongo-address-inet ~s>" (mongo-address->string obj)))

(define-method mongo-address->string ((obj <mongo-address-inet>))
  (format "~a:~a"
          (mongo-address-inet-host obj)
          (mongo-address-inet-port obj)))

(define-record-type <mongo-address-unix>
  make-mongo-address-unix
  mongo-address-unix?
  (path mongo-address-unix-path))

(define-method write-object ((obj <mongo-address-unix>) out)
  (format out "#<mongo-address-unix ~s>" (mongo-address->string obj)))

(define-method mongo-address->string ((obj <mongo-address-unix>))
  (mongo-address-unix-path obj))

(define $mongo-uri-address-inet
  ($do [host ($many1 ($one-of #[^,:/]))]
       [port ($optional ($seq ($char #\:) ($many1 digit)))]
       ($return (make-mongo-address-inet
                 (list->string host)
                 (if port (x->integer (list->string port)) 27017)))))

(define $mongo-uri-address-unix
  ($do [root ($char #\/)]
       [rest ($many1 ($one-of #[^,:\x00]))]
       [port ($optional ($seq ($char #\:) ($many1 digit)))]
       ($return (make-mongo-address-unix (list->string (cons root rest))))))

(define $mongo-uri-address
  ($or ($try $mongo-uri-address-unix) ($try $mongo-uri-address-inet)))

(define $mongo-uri-addresses
  ($sep-by $mongo-uri-address ($char #\,)))

(define (string->mongo-address str)
  (guard (e [(<parse-error> e)
             (error <mongo-parse-error> :reason e
                    (condition-ref e 'message))])
    (peg-parse-string $mongo-uri-address str)))

(define %param-char ($one-of #[^=&]))

(define $mongo-uri-parameter
  ($do [key ($many1 %param-char)]
       [($char #\=)]
       [val ($many1 %param-char)]
       ($return (cons (list->string key)
                      (list->string val)))))

(define $mongo-uri-parameters
  ($optional ($seq ($char #\?)
                   ($sep-by $mongo-uri-parameter ($char #\&)))
             '()))

(define $mongo-uri-path
  ($optional ($do [db ($seq ($char #\/) ($many ($one-of #[^?])))]
                  [ps $mongo-uri-parameters]
                  ($return (list (and (not (null? db))
                                      (list->string db))
                                 ps)))
             '(#f ())))

(define $mongo-uri
  ($do [($string "mongodb://")]
       [auth $mongo-uri-auth]
       [addrs $mongo-uri-addresses]
       [path  $mongo-uri-path]
       ($return (list auth addrs path))))

(define (mongo-uri-parse str)
  (match (guard (e [(<parse-error> e)
                    (error <mongo-parse-error> :reason e
                           (condition-ref e 'message))])
           (peg-parse-string $mongo-uri str))
    [(auth addrs (db params))
     (match auth
       [(user pass) (values user pass addrs db params)]
       [#f          (values #f   #f   addrs db params)])]))

;;;; clock

(define-record-type <mongo-clock> make-mongo-clock mongo-clock?
  (thunk    mongo-clock-thunk    mongo-clock-thunk-set!)
  (interval mongo-clock-interval mongo-clock-interval-set!)
  (thread   mongo-clock-thread   mongo-clock-thread-set!))

(define (mongo-clock-worker cl)
  (make-thread
   (^[] (let loop ()
          ((mongo-clock-thunk cl))
          (sys-nanosleep (mongo-clock-interval cl))
          (loop)))))

(define (mongo-clock thunk :optional (interval #e5e9))
  (rlet1 cl (make-mongo-clock thunk interval #f)
    (mongo-clock-thread-set! cl (mongo-clock-worker cl))))

(define (mongo-clock-start! cl)
  (let* ([t (mongo-clock-thread cl)]
         [s (thread-state t)])
    (cond [(eq? 'new s) (thread-start! t) #t]
          [(eq? 'runnable s) #f]
          [(eq? 'stopped s) (thread-cont! t) #t]
          [(eq? 'terminated s)
           (mongo-clock-thread-set! cl (thread-start! (mongo-clock-worker cl)))
           #t])))

(define (mongo-clock-stop! cl)
  (let1 t (mongo-clock-thread cl)
    (and (eq? 'runnable (thread-state t))
         (begin (thread-terminate! t) #t))))

;;;; milisecond

(define (current-millisecond)
  (let1 t (current-time)
    (+ (* (time-second t) 1000)
       (quotient (time-nanosecond t) 1000000))))
