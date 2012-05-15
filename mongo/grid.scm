(define-module mongo.grid
  (use gauche.record)
  (use gauche.uvector)
  (use gauche.vport)
  (use mongo.util)
  (use mongo.bson)
  (use mongo.wire)
  (use mongo.node)
  (use mongo.core)
  (export <mongo-grid-error>
          mongo-grid-error?
          <mongo-grid>
          mongo-grid?
          mongo-grid
          mongo-grid-database
          mongo-grid-prefix
          mongo-grid-files
          mongo-grid-chunks
          mongo-grid-delete
          <mongo-grid-input-port>
          open-mongo-grid-input-port
          call-with-mongo-grid-input-port
          <mongo-grid-output-port>
          open-mongo-grid-output-port
          call-with-mongo-grid-output-port))

(select-module mongo.grid)

;;;; constant

(define-constant MONGO_GRID_CHUNK_SIZE   (* 256 1024))
(define-constant MONGO_GRID_CONTENT_TYPE "binary/octet-stream")

;;;; condition

(define-condition-type <mongo-grid-error> <mongo-error>
  mongo-grid-error?)

;;;; grid

(define-record-type <mongo-grid> make-mongo-grid mongo-grid?
  (database mongo-grid-database)
  (prefix   mongo-grid-prefix)
  (files    mongo-grid-files)
  (chunks   mongo-grid-chunks))

(define-method write-object ((grid <mongo-grid>) oport)
  (format oport "#<mongo-grid ~s ~s>"
          (mongo-fullname (mongo-grid-files grid))
          (mongo-fullname (mongo-grid-chunks grid))))

(define (mongo-grid db :optional (prefix "fs"))
  (let ([files  (mongo-collection db (mongo-ns-compose prefix "files"))]
        [chunks (mongo-collection db (mongo-ns-compose prefix "chunks"))])
    (mongo-ensure-index files '(("filename" . 1) ("uploadDate" . -1)))
    (mongo-ensure-index chunks '(("files_id" . 1) ("n" . 1)) :unique #t)
    (make-mongo-grid db prefix files chunks)))

(define (mongo-grid-find grid name :key (id #f) (old 0))
  (mongo-find1 (mongo-grid-files grid)
               (if id `(("_id" . ,id)) `(("filename" . ,name)))
               :skip (if id 0 old)
               :sort '(("uploadDate" . -1))))

(define (mongo-grid-delete grid name :key (id #f))
  (let ([files  (mongo-grid-files grid)]
        [chunks (mongo-grid-chunks grid)])
    (for-each (^[file]
                (let1 fid (assoc-ref file "_id")
                  (mongo-delete files `(("_id" . ,fid)))
                  (mongo-delete chunks `(("files_id" . ,fid)))))
              (mongo-find files
                          (if id `(("_id" . ,id)) `(("filename" . ,name)))
                          :select '(("_id" . 1))))))

(define-class <mongo-grid-port-base> ()
  ((id           :init-keyword :id)
   (name         :init-keyword :name)
   (length       :init-keyword :length)
   (chunk-size   :init-keyword :chunk-size)
   (upload-date  :init-keyword :upload-date)
   (md5          :init-keyword :md5)
   (content-type :init-keyword :content-type)
   (aliases      :init-keyword :aliases)
   (metadata     :init-keyword :metadata)))

;;;; input

(define-class <mongo-grid-input-port>
  (<mongo-grid-port-base> <buffered-input-port>)
  ())

(define (open-mongo-grid-input-port grid name :key (id #f) (old 0))
  (if-let1 file (mongo-grid-find grid name :id id :old old)
    (let* ([fid    (assoc-ref file "_id")]
           [csize  (assoc-ref file "chunkSize")]
           [cursor (mongo-find (mongo-grid-chunks grid)
                               `(("files_id" . ,fid))
                               :select '(("data" . 1))
                               :sort '(("n" . 1))
                               :cursor #t)])
      (make <mongo-grid-input-port>
        :id           fid
        :name         (assoc-ref file "filename")
        :length       (assoc-ref file "length")
        :chunk-size   csize
        :upload-date  (assoc-ref file "uploadDate")
        :md5          (assoc-ref file "md5")
        :content-type (assoc-ref file "contentType")
        :aliases      (assoc-ref file "aliases")
        :metadata     (assoc-ref file "metadata")
        :buffer-size  csize
        :fill  (^[target]
                 (if-let1 chunk (mongo-cursor-next! cursor)
                   (let1 data (bson-binary-bytes (assoc-ref chunk "data"))
                     (u8vector-copy! target 0 data)
                     (uvector-size data))
                   0))
        :close (^[] (mongo-cursor-kill cursor))))
    (error <mongo-grid-error> :reason #f "couldn't open input file")))

(define (call-with-mongo-grid-input-port grid name proc . opts)
  (let1 port (apply open-mongo-grid-input-port grid name opts)
    (unwind-protect
      (proc port)
      (close-input-port port))))

;;;; output

(define-class <mongo-grid-output-port>
  (<mongo-grid-port-base> <buffered-output-port>)
  ((n :init-keyword :n)))

(define-method slot-unbound ((c <class>) (p <mongo-grid-port-base>) s)
  (undefined))

(define (mongo-grid-insert grid port)
  (mongo-insert1 (mongo-grid-files grid)
                 `(("_id"         . ,(slot-ref port 'id))
                   ("length"      . ,(slot-ref port 'length))
                   ("chunkSize"   . ,(slot-ref port 'chunk-size))
                   ("uploadDate"  . ,(slot-ref port 'upload-date))
                   ("md5"         . ,(slot-ref port 'md5))
                   ,@(bson-part "filename"    (slot-ref port 'name))
                   ,@(bson-part "contentType" (slot-ref port 'content-type))
                   ,@(bson-part "aliases"     (slot-ref port 'aliases))
                   ,@(bson-part "metadata"    (slot-ref port 'metadata)))))

(define (mongo-grid-md5 grid port)
  (assoc-ref (mongo-command (mongo-grid-database grid)
                            `(("filemd5" . ,(slot-ref port 'id))
                              ("root"    . ,(mongo-grid-prefix grid))))
             "md5"))

(define (mongo-grid-save-chunk! grid port u8v)
  (rlet1 size (uvector-size u8v)
    (let1 n (+ (slot-ref port 'n) 1)
      (mongo-insert1 (mongo-grid-chunks grid)
                     `(("files_id" . ,(slot-ref port 'id))
                       ("n"        . ,n)
                       ("data"     . ,(bson-binary u8v))))
      (slot-set! port 'length (+ (slot-ref port 'length) size))
      (slot-set! port 'n n))))

(define (mongo-grid-retain grid port retain)
  (when (not (eq? retain #t))
    (let ([files  (mongo-grid-files grid)]
          [chunks (mongo-grid-chunks grid)])
      (for-each (^[file]
                  (let1 fid (assoc-ref file "_id")
                    (mongo-delete files `(("_id" . ,fid)))
                    (mongo-delete chunks `(("files_id" . ,fid)))))
                (mongo-find
                 files
                 `(("filename" . ,(slot-ref port 'name))
                   ("_id"      . (("$ne" . ,(slot-ref port 'id)))))
                 :select '(("_id" . 1))
                 :skip (if (or (eq? retain #f) (> 2 retain)) 0 (- retain 1))
                 :sort '(("uploadDate" . -1)))))))

(define (mongo-grid-finalize! grid port retain)
  (flush port)
  (when (= 0 (slot-ref port 'length))
    (mongo-grid-save-chunk! grid port '#u8()))
  (slot-set! port 'upload-date (bson-datetime))
  (slot-set! port 'md5 (mongo-grid-md5 grid port))
  (mongo-grid-insert grid port)
  (mongo-grid-retain grid port retain))

(define (open-mongo-grid-output-port grid name
                                     :key (id #f)
                                          (chunk-size MONGO_GRID_CHUNK_SIZE)
                                          (content-type MONGO_GRID_CONTENT_TYPE)
                                          (aliases (undefined))
                                          (metadata (undefined))
                                          (retain #t))
  (when (and id (mongo-grid-find grid name :id id))
    (error <mongo-grid-error> :reason #f "couldn't open output file"))
  (rlet1 port (make <mongo-grid-output-port>
                :id           (or id (bson-object-id))
                :name         name
                :length       0
                :chunk-size   chunk-size
                :content-type content-type
                :aliases      aliases
                :metadata     metadata
                :n            -1
                :buffer-size  chunk-size)
    (slot-set! port 'flush (^[v x] (mongo-grid-save-chunk! grid port v)))
    (slot-set! port 'close (^[] (mongo-grid-finalize! grid port retain)))))

(define (call-with-mongo-grid-output-port grid name proc . opts)
  (let1 port (apply open-mongo-grid-output-port grid name opts)
    (unwind-protect
      (proc port)
      (close-output-port port))))
