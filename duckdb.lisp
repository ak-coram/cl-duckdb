;;;; duckdb.lisp

(in-package #:duckdb)

(defmacro with-all-float-traps-masked (&rest body)
  `(float-features:with-float-traps-masked (:underflow
                                            :overflow
                                            :inexact
                                            :invalid
                                            :divide-by-zero
                                            :denormalized-operand)
     ,@body))

(define-condition duckdb-error (simple-error)
  ((database :initarg :database :accessor database)
   (statement :initarg :statement :accessor statement)
   (appender :initarg :appender :accessor appender)
   (arrow :initarg :arrow :accessor arrow)
   (error-message :initarg :error-message
                  :accessor error-message))
  (:report (lambda (condition stream)
             (format stream "~A" (error-message condition)))))

;;; Databases

(defclass database ()
  ((handle :accessor handle)
   (path :accessor path)
   (threads :accessor threads)
   (worker-pool :accessor worker-pool)))

(defmethod initialize-instance :after
    ((instance database) &key path threads)
  (with-foreign-object (p-database 'duckdb-api:duckdb-database)
    (with-foreign-object (p-error-message '(:pointer :string))
      (duckdb-api:with-config
          (config (when threads
                    (list "threads" #+bordeaux-threads threads
                                    #-bordeaux-threads 1
                          "external_threads" #+bordeaux-threads threads
                                             #-bordeaux-threads 1)))
        (let (;; prefer duckdb-open-ext over duckdb-open for error message
              (result (duckdb-api:duckdb-open-ext path
                                                  p-database
                                                  config
                                                  p-error-message)))
          (if (eql result :duckdb-success)
              (let* ((handle (mem-ref p-database 'duckdb-api:duckdb-database))
                     (pool (when (and #+bordeaux-threads t
                                      #-bordeaux-threads nil
                                      threads
                                      (< 1 threads))
                             (duckdb-api:start-worker-pool handle (1- threads)))))
                (duckdb-api:add-static-table-replacement-scan handle)
                (setf (handle instance) handle
                      (path instance) path
                      (threads instance) threads
                      (worker-pool instance) pool))
              (error 'duckdb-error
                     :error-message (duckdb-api:get-message p-error-message))))))))

(defvar *threads*
  #-ECL nil
  #+ECL (progn #+bordeaux-threads t #-bordeaux-threads 1)
  "Default value for the number of threads when opening a new database.

NIL: DuckDB internal thread management is used. Use SQL pragmas to
adjust the defaults (e.g. PRAGMA threads=4).

N (where N > 0): DuckDB internal thread management is turned off. When
N is larger than one and the implementation supports threads, a thread
pool of size N-1 is created and used for parallel query execution. The
thread pool is shut down when the database is closed.

T: See above, but the DuckDB defaults are used to determine N. This is
used by default on ECL if it's built with support for threading.")

(defmacro with-threads (threads &body body)
  `(let ((*threads* ,threads))
     ,@body))

(defvar *default-thread-count* nil
  "This value will be initialized using a temporary in-memory connection
to DuckDB with internal thread-management to retrieve the default
value.")

(defvar *sql-null-return-value* nil
  "This value will be returned for SQL NULL values in query
results. Defaults to NIL.")

(defun open-database (&key (path ":memory:") (threads *threads*))
  "Opens and returns database for PATH with \":memory:\" as default.

See CLOSE-DATABASE for cleanup and *THREADS* for the supported values
for THREADS."
  (make-instance 'database
                 :path path
                 :threads
                 (when (and *default-thread-count* threads)
                   (etypecase threads
                     ((integer 1) threads)
                     (boolean *default-thread-count*)))))

(defun close-database (database)
  "Does resource cleanup for DATABASE, also see OPEN-DATABASE."
  (with-foreign-object (p-database 'duckdb-api:duckdb-database)
    (duckdb-api:stop-worker-pool (worker-pool database))
    (setf (mem-ref p-database 'duckdb-api:duckdb-database)
          (handle database))
    (duckdb-api:duckdb-close p-database)))

(defmacro with-open-database ((database-var &key path threads) &body body)
  "Opens database for PATH, binds it to DATABASE-VAR.
The database is closed after BODY is evaluated."
  `(let ((,database-var (open-database ,@(when path
                                           `(:path ,path))
                                       ,@(when threads
                                           `(:threads ,threads)))))
     (unwind-protect (progn ,@body)
       (close-database ,database-var))))

;;; Connections

(defclass connection ()
  ((database :accessor database)
   (handle :accessor handle)))

(defmethod initialize-instance :after
    ((instance connection) &key database)
  (with-foreign-object (p-connection 'duckdb-api:duckdb-connection)
    (let ((result (duckdb-api:duckdb-connect (handle database)
                                             p-connection)))
      (if (eql result :duckdb-success)
          (let ((handle (mem-ref p-connection
                                 'duckdb-api:duckdb-connection)))
            (duckdb-api:register-static-table-function handle)
            (setf (database instance) database
                  (handle instance) handle))
          (error 'duckdb-error :database database)))))

(defun connect (database)
  "Opens and returns new connection to DATABASE.
See DISCONNECT for cleanup."
  (make-instance 'connection :database database))

(defun disconnect (connection)
  "Does resource cleanup for CONNECTION, also see CONNECT."
  (with-foreign-object (p-connection 'duckdb-api:duckdb-connection)
    (setf (mem-ref p-connection 'duckdb-api:duckdb-connection)
          (handle connection))
    (duckdb-api:duckdb-disconnect p-connection)))

(defmacro with-open-connection ((connection-var database) &body body)
  `(let ((,connection-var (connect ,database)))
     (unwind-protect (progn ,@body)
       (disconnect ,connection-var))))

(defvar *connection* nil
  "Used to refer to the current database connection.
It is intended to make interactive use more convenient and is used as
the default connection for functions where CONNECTION is an optional
or a keyword parameter.")

(defun initialize-default-connection (&rest args &key &allow-other-keys)
  "Connects to database for PATH and sets the value of *CONNECTION*.
Also see DISCONNECT-DEFAULT-CONNECTION for cleanup."
  (setf *connection* (connect (apply #'open-database args))))

(defun disconnect-default-connection ()
  "Disconnects *CONNECTION* and also closes the related database.
Also see INITIALIZE-DEFAULT-CONNECTION for initializing *CONNECTION*."
  (when *connection*
    (let ((database (slot-value *connection* 'database)))
      (disconnect *connection*)
      (close-database database)
      (setf *connection* nil))))

(defmacro with-default-connection ((database) &body body)
  "Connects to DATABASE and dynamically binds *CONNECTION*.
The connection is disconnected after BODY is evaluated."
  `(let ((*connection* (connect ,database)))
     (unwind-protect (progn ,@body)
       (disconnect *connection*))))

(defmacro with-transient-connection (&body body)
  "Connects to new in-memory database and dynamically binds *CONNECTION*.
The connection and the database are cleaned up after BODY is evaluated."
  (alexandria:with-gensyms (database)
    `(let ((,database (open-database)))
       (unwind-protect (with-default-connection (,database)
                         ,@body)
         (close-database ,database)))))

;;; Statements

(defclass statement ()
  ((connection :initarg :connection :accessor connection)
   (query :initarg :query)
   (parameter-count :initarg :parameter-count :accessor parameter-count)
   (parameter-types :initarg :parameter-types :accessor parameter-types)
   (handle :accessor handle :initarg :handle)))

(defun prepare (query &key (connection *connection*))
  (with-foreign-object (p-statement 'duckdb-api:duckdb-prepared-statement)
    (with-foreign-string (p-query query)
      (let* ((result (duckdb-api:duckdb-prepare (handle connection)
                                                p-query
                                                p-statement))
             (statement (mem-ref p-statement
                                 'duckdb-api:duckdb-prepared-statement)))
        (if (eql result :duckdb-success)
            (let* ((parameter-count (duckdb-api:duckdb-nparams statement))
                   (parameter-types
                     (loop :for i :from 1 :to parameter-count
                           :collect (duckdb-api:duckdb-param-type statement i))))
              (make-instance 'statement
                             :connection connection
                             :query query
                             :handle statement
                             :parameter-count parameter-count
                             :parameter-types parameter-types))
            (error 'duckdb-error
                   :database (database connection)
                   :error-message
                   (duckdb-api:duckdb-prepare-error statement)))))))

(defun destroy-statement (statement)
  (with-foreign-object (p-statement 'duckdb-api:duckdb-prepared-statement)
    (setf (mem-ref p-statement 'duckdb-api:duckdb-prepared-statement)
          (handle statement))
    (duckdb-api:duckdb-destroy-prepare p-statement)))

(defmacro with-statement
    ((statement-var query &key connection) &body body)
  `(let ((,statement-var (prepare ,query
                                  ,@(when connection
                                      `(:connection ,connection)))))
     (unwind-protect (progn ,@body)
       (destroy-statement ,statement-var))))

;;; Queries

(defclass result ()
  ((connection :accessor connection :initarg :connection)
   (statement :accessor statement :initarg :statement)
   (handle :accessor handle :initarg :handle)))

(defun make-result (connection statement p-result)
  (make-instance 'result
                 :connection connection
                 :statement statement
                 :handle p-result))

(defmacro without-interrupts (&body body)
  `(#+sbcl sb-sys:without-interrupts
    #+openmcl ccl:without-interrupts
    #+ecl mp:without-interrupts
    #-(or sbcl openmcl ecl) progn
    ,@body))

(defmacro %with-execute ((result-var statement) &body body)
  `(let ((connection (connection ,statement))
         finished)
     (with-foreign-objects ((p-pending-result 'duckdb-api:duckdb-pending-result)
                            (,result-var '(:struct duckdb-api:duckdb-result)))
       ;; TODO: interrupt proof this unwind-protect
       (unwind-protect
            (progn
              (if (eql (duckdb-api:duckdb-pending-prepared (handle ,statement) p-pending-result)
                       :duckdb-success)
                  (loop :until
                        (without-interrupts
                          (setq finished
                                (duckdb-api:duckdb-pending-execution-is-finished
                                 (duckdb-api:duckdb-pending-execute-task
                                  (mem-ref p-pending-result 'duckdb-api:duckdb-pending-result))))))
                  (setq finished t))
              (if (eql (duckdb-api:duckdb-execute-pending
                        (mem-ref p-pending-result 'duckdb-api:duckdb-pending-result)
                        ,result-var)
                       :duckdb-success)
                  (locally ,@body)
                  (let ((error-message (duckdb-api:duckdb-result-error ,result-var)))
                    (error 'duckdb-error
                           :database (database connection)
                           :statement ,statement
                           :error-message error-message))))
         (unless finished (duckdb-api:duckdb-interrupt (handle connection)))
         (duckdb-api:duckdb-destroy-pending p-pending-result)))))

(defun execute (statement)
  "Runs STATEMENT and returns RESULT instance.
DESTROY-RESULT must be called on the returned value for resource
cleanup."
  (%with-execute (p-result statement)
    (let ((p-result-1 (foreign-alloc '(:struct duckdb-api:duckdb-result))))
      (setf (mem-ref p-result-1 '(:struct duckdb-api:duckdb-result))
            (mem-ref p-result '(:struct duckdb-api:duckdb-result)))
      (make-result connection statement p-result-1))))

(defun perform (statement)
  "Same as EXECUTE, but doesn't return any results and needs no cleanup."
  (%with-execute (p-result statement)
    (duckdb-api:duckdb-destroy-result p-result)))

(defun destroy-result (result)
  (let ((p-result (handle result)))
    (duckdb-api:duckdb-destroy-result p-result)
    (foreign-free p-result)))

(defmacro with-execute ((result-var statement) &body body)
  `(let ((,result-var (execute ,statement)))
     (unwind-protect (progn ,@body)
       (destroy-result ,result-var))))

(declaim (inline validity-row-is-valid))

(defun validity-row-is-valid (validity i)
  (declare (cffi:foreign-pointer validity)
           ((integer 0 #.array-dimension-limit) i)
           (optimize speed))
  (oddp (ash (cffi:mem-aref validity :uint8 (ash i -3))
             (- (ldb (byte 3 0) i)))))

(defmacro do-specialized-vector
    ((index-var chunk-size)
     (array-element-type sql-null-return-value validity)
     &body body)
  "Iterate with INDEX-VAR from 0 below CHUNK-SIZE and collect into a Lisp vector.

The result is masked according to VALIDITY. Try to specialize the result vector
with ARRAY-ELEMENT-TYPE. Fallback to T if SQL-NULL-RETURN-VALUE is not of
ARRAY-ELEMENT-TYPE. ARRAY-ELEMENT-TYPE is not evaluated."
  `(cond ((cffi:null-pointer-p ,validity)
          (let ((result (make-array ,chunk-size :element-type ',array-element-type)))
            #+nil (declare (optimize speed))
            (loop :for ,index-var :below ,chunk-size
                  :do (setf (aref result ,index-var)
                            (locally ,@body)))
            result))
         ;; We can still specialize if there is a validity mask, but
         ;; SQL-NULL-RETURN-VALUE happens to be of ARRAY-ELEMENT-TYPE.
         ;; This case is only needed if ARRAY-ELEMENT-TYPE is not T.
         ,@ (unless (eql array-element-type t)
              `(((typep ,sql-null-return-value ',array-element-type)
                 (let ((result (make-array ,chunk-size :element-type ',array-element-type)))
                   (loop :for ,index-var :below ,chunk-size
                         :do (setf (aref result i)
                                   (if (validity-row-is-valid ,validity ,index-var)
                                       (locally ,@body)
                                       ,sql-null-return-value)))
                   result))))
         (t (let ((result (make-array ,chunk-size)))
              (loop :for ,index-var :below ,chunk-size
                    :do (setf (aref result ,index-var)
                              (if (validity-row-is-valid ,validity ,index-var)
                                  (locally ,@body)
                                  ,sql-null-return-value)))
              result))))

(defgeneric translate-vector (vector-type sql-null-return-value vector chunk-size)
  (:documentation
   "Translate a DuckDB VECTOR of VECTOR-TYPE into a Lisp vector.

The results are passed to `aggregate-vectors'."))

(defmethod translate-vector ((type duckdb-api:duckdb-logical-list)
                             sql-null-return-value vector chunk-size)
  (let ((child (translate-vector
                (duckdb-api:duckdb-logical-list-child type) sql-null-return-value
                (duckdb-api:duckdb-list-vector-get-child vector)
                (duckdb-api:duckdb-list-vector-get-size vector)))
        (p-data (duckdb-api:duckdb-vector-get-data vector))
        (validity (duckdb-api:duckdb-vector-get-validity vector)))
    (do-specialized-vector (i chunk-size) (t sql-null-return-value validity)
      (destructuring-bind (offset length)
          (cffi:mem-aref p-data '(:struct duckdb-api::duckdb-list) i)
        (loop :for j :from offset :below (+ offset length)
              :collect (aref child j))))))

(defmethod translate-vector ((type duckdb-api:duckdb-logical-struct)
                             sql-null-return-value vector chunk-size)
  (let* ((validity (duckdb-api:duckdb-vector-get-validity vector))
         (child-results
           (loop :for child-index :from 0
                 :for (name . vector-type)
                   :in (duckdb-api:duckdb-logical-struct-fields type)
                 :for child-vector
                   := (duckdb-api:duckdb-struct-vector-get-child vector child-index)
                 :collect
                 (cons name (translate-vector vector-type sql-null-return-value
                                              child-vector chunk-size)))))
    (do-specialized-vector (i chunk-size) (t sql-null-return-value validity)
      (loop :for (name . child-vector) :in child-results
            :collect (cons name (aref child-vector i))))))

(defmethod translate-vector ((type duckdb-api:duckdb-logical-union)
                             sql-null-return-value vector chunk-size)
  (map 'vector
       (lambda (v) (if v (cdr (nth (cdar v) (cdr v))) sql-null-return-value))
       (translate-vector (duckdb-api::make-duckdb-logical-struct
                          :fields
                          ;; add value selector tag to struct members
                          (cons '("" . :duckdb-utinyint)
                                (duckdb-api:duckdb-logical-union-fields type)))
                         nil vector chunk-size)))

(defmethod translate-vector ((type duckdb-api:duckdb-logical-map)
                             sql-null-return-value vector chunk-size)
  (let* ((child-types (duckdb-api:duckdb-logical-map-fields type))
         (internal (translate-vector
                    (duckdb-api::make-duckdb-logical-list
                     :child
                     (duckdb-api::make-duckdb-logical-struct
                      :fields `((k . ,(car child-types))
                                (v . ,(cadr child-types)))))
                    nil vector chunk-size)))
    (map 'vector
         (lambda (v)
           (if v (loop :for entry :in v
                       :collect (cons (alexandria:assoc-value entry 'k)
                                      (alexandria:assoc-value entry 'v)))
               sql-null-return-value))
         internal)))

(defmacro define-specialized-translate-vector (vector-type array-element-type)
  `(defmethod translate-vector
       ((vector-type (eql ,vector-type)) sql-null-return-value vector chunk-size)
     (let ((p-data (duckdb-api:duckdb-vector-get-data vector))
           (validity (duckdb-api:duckdb-vector-get-validity vector)))
       (do-specialized-vector (i chunk-size)
           (,array-element-type sql-null-return-value validity)
         (cffi:mem-aref p-data ',(duckdb-api:get-ffi-type vector-type) i)))))

(define-specialized-translate-vector :duckdb-boolean boolean)
(define-specialized-translate-vector :duckdb-tinyint (signed-byte 8))
(define-specialized-translate-vector :duckdb-smallint (signed-byte 16))
(define-specialized-translate-vector :duckdb-integer (signed-byte 32))
(define-specialized-translate-vector :duckdb-bigint (signed-byte 64))
(define-specialized-translate-vector :duckdb-utinyint (unsigned-byte 8))
(define-specialized-translate-vector :duckdb-usmallint (unsigned-byte 16))
(define-specialized-translate-vector :duckdb-uinteger (unsigned-byte 32))
(define-specialized-translate-vector :duckdb-ubigint (unsigned-byte 64))
(define-specialized-translate-vector :duckdb-float single-float)
(define-specialized-translate-vector :duckdb-double double-float)

(macrolet ((define-unspecialized-translate-vectors ()
             `(progn
                ,@(loop :for type :in
                        '(:duckdb-varchar :duckdb-hugeint :duckdb-uhugeint
                          :duckdb-varint :duckdb-blob :duckdb-date :duckdb-time
                          :duckdb-timestamp :duckdb-timestamp-s :duckdb-timestamp-ms :duckdb-timestamp-ns
                          :duckdb-interval :duckdb-uuid :duckdb-timestamp-tz)
                        :collect `(define-specialized-translate-vector ,type t)))))
  (define-unspecialized-translate-vectors))

(defmethod translate-vector ((vector-type duckdb-api:duckdb-logical-decimal)
                             sql-null-return-value vector chunk-size)
  (map 'vector
       (lambda (v)
         (if v (* v (expt 10 (- (duckdb-api:duckdb-logical-decimal-scale vector-type))))
             sql-null-return-value))
       (translate-vector (duckdb-api:duckdb-logical-decimal-internal vector-type)
                         nil vector chunk-size)))

(defmethod translate-vector ((vector-type duckdb-api:duckdb-logical-enum)
                             sql-null-return-value vector chunk-size)
  (let ((alist (cons (cons -1 sql-null-return-value) (duckdb-api:duckdb-logical-enum-alist vector-type))))
    (map 'vector (lambda (v) (alexandria:assoc-value alist v))
         (translate-vector (duckdb-api:duckdb-logical-enum-internal vector-type)
                           -1 vector chunk-size))))

(defmethod translate-vector ((vector-type (eql :duckdb-bit)) sql-null-return-value vector chunk-size)
  (map 'vector
       (lambda (v)
         (if v (loop :with unused-bit-count := (aref v 0)
                     :with octet-count := (1- (length v))
                     :with bit-count := (- (* octet-count 8) unused-bit-count)
                     :with bits := (make-array (list bit-count) :element-type 'bit)
                     :for i :below bit-count
                     :for j := (+ i unused-bit-count)
                     :for octet-index := (floor j 8)
                     :for octet := (aref v (1+ octet-index))
                     :for octet-bit-index := (- 7 (mod j 8))
                     :do (setf (aref bits i) (ldb (byte 1 octet-bit-index) octet))
                     :finally (return bits))
             sql-null-return-value))
       (translate-vector :duckdb-blob nil vector chunk-size)))

(defgeneric aggregate-vectors (vector-type sql-null-return-value results)
  (:documentation
   "Aggregate RESULTS from calling `translate-vector' on DuckDB vectors in the same
column into a single vector."))

(defmethod aggregate-vectors (vector-type sql-null-return-value results)
  (declare (ignore vector-type sql-null-return-value))
  (if results
      (if (cdr results)
          (let ((element-type (array-element-type (car results))))
            ;; If any array is unspecialized, the result should also be unspecialized
            (loop :for vector :in (cdr results)
                  :for type := (array-element-type vector)
                  :do (setq element-type
                            (if (equal type element-type)
                                element-type
                                t)))
            (apply #'concatenate `(vector ,element-type) results))
          ;; No need to concatenate if there's only one result vector
          (car results))
      (vector)))

(defun translate-chunk (result-alist column-types chunk sql-null-return-value)
  (let ((column-count (duckdb-api:duckdb-data-chunk-get-column-count chunk))
        (chunk-size (duckdb-api:duckdb-data-chunk-get-size chunk)))
    (loop :for column-index :below column-count
          :for entry :in result-alist
          :for vector-type :in column-types
          :for vector := (duckdb-api:duckdb-data-chunk-get-vector chunk column-index)
          :do (push (translate-vector vector-type sql-null-return-value vector chunk-size)
                    (cdr entry)))
    chunk-size))

(defun translate-result (result &optional sql-null-return-value)
  (let* ((p-result (handle result))
         (column-count (duckdb-api:duckdb-column-count p-result))
         (result-alist
           (loop :for column-index :below column-count
                 :collect (list (duckdb-api:duckdb-column-name p-result column-index))))
         (column-types
           (loop :for column-index :below column-count
                 :collect (duckdb-api:get-result-type p-result column-index)))
         (row-count
           (loop :for n
                   := (duckdb-api:with-data-chunk (chunk p-result)
                        (if (null-pointer-p chunk)
                            (alexandria:when-let
                                (error-message (duckdb-api:duckdb-result-error
                                                p-result))
                              (destroy-result result)
                              (error 'duckdb-error
                                     :database (database (connection result))
                                     :statement (statement result)
                                     :error-message error-message))
                            (translate-chunk result-alist column-types chunk sql-null-return-value)))
                 :while n :sum n)))
    (values (loop :for (column-name . results) :in result-alist
                  :for vector-type :in column-types
                  :collect (cons column-name
                                 (aggregate-vectors vector-type sql-null-return-value
                                                    (nreverse results))))
            column-types row-count)))

(defun assert-parameter-count (statement values)
  (let ((statement-parameter-count (parameter-count statement))
        (binding-value-count (length values)))
    (unless (eql statement-parameter-count
                 binding-value-count)
      (error 'duckdb-error
             :database (database (connection statement))
             :statement statement
             :error-message
             (format nil "Failed to bind ~d value~:p to ~d parameter~:p."
                     (length values)
                     (parameter-count statement))))))

(defun rational-to-string (x n)
  (multiple-value-bind (i r) (truncate x)
    (let ((s (format nil "~d.~d" i (truncate (* (abs r) (expt 10 n))))))
      (string-right-trim '(#\0) s))))

(defmacro generate-parameter-binding-dispatch ()
  "Generates dispatch for parameter bindings.
This macro captures variables from the surrounding scope of
BIND-PARAMETERS intentionally and is only used to make parameter
binding a bit more concise. It is not intended for any other use."
  (let ((parameter-binding-types
          '(;; No null type in DuckDB, dispatch based on parameter
            ;; value type
            (:_ null (duckdb-api:duckdb-bind-null statement-handle i))
            ;; Handle keywords for boolean and NULL
            (:_ keyword
             (ecase value
               (:null (duckdb-api:duckdb-bind-null statement-handle i))
               (:false (duckdb-api:duckdb-bind-boolean statement-handle i nil))
               (:true (duckdb-api:duckdb-bind-boolean statement-handle i t))))
            (:duckdb-boolean boolean
             (case value
               (:false (duckdb-api:duckdb-bind-boolean statement-handle i nil))
               ;; :TRUE is treated as T in the clause below
               (t (duckdb-api:duckdb-bind-boolean statement-handle i value))))
            ((:duckdb-varchar :duckdb-enum) string
             (duckdb-api:duckdb-bind-varchar statement-handle i value))
            (:duckdb-blob (vector (unsigned-byte 8))
             (let ((length (length value)))
               (with-foreign-array (ptr value `(:array :uint8 ,length))
                 (duckdb-api:duckdb-bind-blob statement-handle i ptr length))))
            (:duckdb-float single-float
             (duckdb-api:duckdb-bind-float statement-handle i value))
            (:duckdb-double double-float
             (duckdb-api:duckdb-bind-double statement-handle i value))
            ;; Use max decimal width to bind rationals as varchar,
            ;; don't include :duckdb-decimal here as the CL type is
            ;; preferable to determine the the best way to bind
            ;; decimal values (preferably using one of the integer
            ;; binding functions).
            (:_ ratio (let ((s (rational-to-string value 38)))
                        (duckdb-api:duckdb-bind-varchar statement-handle i s)))
            ;; 8-bit integers
            (:duckdb-tinyint (integer -128 127)
             (duckdb-api:duckdb-bind-int8 statement-handle i value))
            (:duckdb-utinyint (integer 0 255)
             (duckdb-api:duckdb-bind-uint8 statement-handle i value))
            ;; 16-bit integers
            (:duckdb-smallint (integer -32768 32767)
             (duckdb-api:duckdb-bind-int16 statement-handle i value))
            (:duckdb-usmallint (integer 0 65535)
             (duckdb-api:duckdb-bind-uint16 statement-handle i value))
            ;; 32-bit integers
            (:duckdb-integer (integer -2147483648 2147483647)
             (duckdb-api:duckdb-bind-int32 statement-handle i value))
            (:duckdb-uinteger (integer 0 4294967295)
             (duckdb-api:duckdb-bind-uint32 statement-handle i value))
            ;; 64-bit integers
            (:duckdb-bigint (integer -9223372036854775808 9223372036854775807)
             (duckdb-api:duckdb-bind-int64 statement-handle i value))
            (:duckdb-ubigint (integer 0 18446744073709551615)
             (duckdb-api:duckdb-bind-uint64 statement-handle i value))
            ;; hugeint
            (:duckdb-hugeint (integer
                              -170141183460469231731687303715884105727
                              170141183460469231731687303715884105727)
             (duckdb-api:duckdb-bind-hugeint statement-handle i value))
            (:duckdb-uhugeint (integer
                               0
                               340282366920938463463374607431768211455)
             (duckdb-api:duckdb-bind-uhugeint statement-handle i value))
            (:duckdb-date local-time:date
             (duckdb-api:duckdb-bind-date statement-handle i value))
            (:duckdb-timestamp local-time:timestamp
             (duckdb-api:duckdb-bind-timestamp statement-handle i value))
            (:duckdb-time local-time-duration:duration
             (duckdb-api:duckdb-bind-time statement-handle i value))
            (:duckdb-uuid fuuid:uuid
             (let ((s (fuuid:to-string value)))
               (duckdb-api:duckdb-bind-varchar statement-handle i s))))))
    `(case duckdb-type
       ,@(loop :for (type _ binding-form) :in parameter-binding-types
               :unless (eql type :_)    ; :_ is used to skip
                 :collect `(,type
                            ;; Handle binding NULL with the exception
                            ;; of NIL being bound as FALSE for
                            ;; booleans:
                            (if (or ,(unless (eql type :duckdb-boolean)
                                       `(null value))
                                    (eql value :null))
                                (duckdb-api:duckdb-bind-null statement-handle i)
                                ,binding-form)))
       ;; In some cases such as "SELECT ?" the type can not be
       ;; determined in advance by DuckDB, so we use the type of the
       ;; parameter value to bind it.
       (t (etypecase value
            ,@(loop :for (_ cl-type binding-form) :in parameter-binding-types
                    :collect `(,cl-type ,binding-form)))))))

(defun bind-parameters (statement values)
  (assert-parameter-count statement values)
  (let ((parameter-count (parameter-count statement))
        (parameter-types (parameter-types statement))
        (statement-handle (handle statement)))
    (unless (eql (duckdb-api:duckdb-clear-bindings statement-handle)
                 :duckdb-success)
      (error 'duckdb-error
             :database (database (connection statement))
             :statement statement
             :error-message "Failed to clear statement bindings."))
    (loop
      :for i :from 1 :to parameter-count
      :for value :in values
      :for duckdb-type :in parameter-types
      :do (generate-parameter-binding-dispatch))))

(defun internal-query
    (connection sql-null-return-value query parameters)
  (with-all-float-traps-masked
      (with-statement (statement query :connection connection)
        (when parameters
          (bind-parameters statement parameters))
        (with-execute (result statement)
          (translate-result result sql-null-return-value)))))

(defun query (query parameters
              &key (connection *connection*)
                (sql-null-return-value *sql-null-return-value*)
                include-column-types)
  (multiple-value-bind (result-alist column-types row-count)
      (internal-query connection
                      sql-null-return-value
                      query
                      parameters)
    (declare (ignore row-count))
    (if include-column-types
        (loop :for (k . v) :in result-alist
              :for column-type :in column-types
              :collect (cons k (list v column-type)))
        result-alist)))

(defun q (query &rest parameters)
  "Query more conveniently from the REPL.

Relies on transient connection when no default connection is
available. See QUERY for more generic usage."
  (if *connection*
      (query query parameters)
      (with-transient-connection
        (query query parameters))))

(defun run (&rest queries)
  (with-all-float-traps-masked
      (loop :for q :in queries
            :if (stringp q) :do (with-statement (statement q)
                                  (perform statement))
              :else :do (with-statement (statement (car q))
                          (bind-parameters statement (cadr q))
                          (perform statement)))))

(defun get-result (results column &optional n)
  (labels ((compare (a b) (string= a (snake-case-to-param-case b))))
    (let* ((column-data (if (stringp column)
                            (alexandria:assoc-value results
                                                    column
                                                    :test #'string=)
                            (alexandria:assoc-value results
                                                    (string-downcase column)
                                                    :test #'compare)))
           (result-values (if (listp column-data)
                              (car column-data)
                              column-data)))
      (if n
          (aref result-values n)
          result-values))))

(defun format-query (query parameters
                     &key (connection *connection*)
                       (sql-null-return-value *sql-null-return-value*)
                       (stream *standard-output*))
  (multiple-value-bind (results types row-count)
      (internal-query connection
                      sql-null-return-value
                      query
                      parameters)
    (declare (ignore types))
    (let* ((columns (mapcar #'car results))
           (table (ascii-table:make-table
                   columns)))
      (loop :for i :below row-count
            :do (ascii-table:add-row
                 table
                 (loop :for column :in columns
                       :collect (get-result results column i))))
      (ascii-table:display table stream))))

(defun fq (query &rest parameters)
  "Format query more conveniently from the REPL.

Relies on transient connection when no default connection is
available. See FORMAT-QUERY for more generic usage."
  (if *connection*
      (format-query query parameters)
      (with-transient-connection
        (format-query query parameters))))

(defun spark-query (query parameters columns
                    &rest args
                    &key
                      (connection *connection*)
                      (stream *standard-output*)
                    &allow-other-keys)
  (loop :with results := (query query parameters :connection connection)
        :for column :in columns
        :for values := (loop :for v :across (get-result results column) :collect v)
        :do (format stream "~a ~a~%" column
                    (apply #'cl-spark:spark values
                           (alexandria:remove-from-plist args
                                                         :connection
                                                         :stream)))))

(defun vspark-query (query parameters label-column value-column
                     &rest args
                     &key
                       (connection *connection*)
                       (stream *standard-output*)
                     &allow-other-keys)
  (let* ((results (query query parameters :connection connection))
         (values (if label-column
                     (loop :for label :across (get-result results label-column)
                           :for value :across (get-result results value-column)
                           :collect label :into labels
                           :collect value :into values
                           :finally (return (list labels values)))
                     (loop :for value :across (get-result results value-column)
                           :collect value)))
         (vspark-args (alexandria:remove-from-plist args
                                                    :connection
                                                    :stream))
         (vspark (if label-column
                     (apply #'cl-spark:vspark (cadr values) :labels (car values)
                            vspark-args)
                     (apply #'cl-spark:vspark values vspark-args))))
    (cond
      (stream (princ vspark stream) nil)
      (t vspark))))

;;; Appenders

(defclass appender ()
  ((connection :initarg :connection :accessor connection)
   (schema :initarg :schema)
   (table :initarg :table)
   (column-count :initarg :column-count :accessor column-count)
   (types :initarg :types :accessor types)
   (handle :accessor handle :initarg :handle)))

(defun quote-identifier (s)
  (let* ((double-quote "\"")
         (escaped-double-quote (concat double-quote double-quote)))
    (concat double-quote
            (replace-substring double-quote escaped-double-quote s)
            double-quote)))

(defun get-column-types (connection table)
  "Try to automatically determine the column types for appending to a table"
  (labels ((wrap-parens (s) (concat " (" s ") ")))
    (let* ((table-id (quote-identifier table))
           (columns (map 'list #'identity
                         (get-result (query (concat "DESCRIBE " table-id)
                                            nil :connection connection)
                                     'column-name)))
           (column-ids (join ", " (mapcar #'quote-identifier columns)))
           (params (join ", " (make-list (length columns)
                                         :initial-element "?")))
           (query (concat "INSERT INTO " table-id (wrap-parens column-ids)
                          "VALUES " (wrap-parens params))))
      (with-statement (statement query :connection connection)
        (parameter-types statement)))))

(defun create-appender (table &key schema (connection *connection*) types)
  (with-foreign-object (p-appender 'duckdb-api:duckdb-appender)
    (let ((p-schema (if schema (foreign-string-alloc schema)
                        (null-pointer))))
      (unwind-protect
           (with-foreign-string (p-table table)
             (let* ((result (duckdb-api:duckdb-appender-create (handle connection)
                                                               p-schema
                                                               p-table
                                                               p-appender))
                    (appender (mem-ref p-appender 'duckdb-api:duckdb-appender)))
               (if (eql result :duckdb-success)
                   (let ((column-types (or types (get-column-types connection
                                                                   table))))
                     (make-instance 'appender
                                    :connection connection
                                    :schema schema
                                    :table table
                                    :column-count (length column-types)
                                    :types column-types
                                    :handle appender))
                   (let ((error-message (duckdb-api:duckdb-appender-error appender)))
                     (duckdb-api:duckdb-appender-destroy p-appender)
                     (error 'duckdb-error
                            :database (database connection)
                            :error-message error-message)))))
        (unless (null-pointer-p p-schema)
          (foreign-string-free p-schema))))))

(defun destroy-appender (appender)
  (with-foreign-object (p-appender 'duckdb-api:duckdb-appender)
    (setf (mem-ref p-appender 'duckdb-api:duckdb-appender)
          (handle appender))
    (duckdb-api:duckdb-appender-destroy p-appender)))

(defmacro with-appender
    ((appender-var table &key schema connection types)
     &body body)
  `(let ((,appender-var (create-appender ,table
                                         ,@(when connection
                                             `(:connection ,connection))
                                         :schema ,schema
                                         :types ,types)))
     (unwind-protect (progn ,@body)
       (destroy-appender ,appender-var))))

(defmacro generate-append-value-dispatch ()
  "Generates dispatch for appending values.
This macro captures variables from the surrounding scope of append-row
intentionally."
  (let ((appender-types
          '((:duckdb-boolean
             (case value
               (:false (duckdb-api:duckdb-append-bool handle nil))
               ;; :TRUE is treated as T in the clause below
               (t (duckdb-api:duckdb-append-bool handle value))))
            ((:duckdb-varchar :duckdb-enum)
             (duckdb-api:duckdb-append-varchar handle value))
            (:duckdb-blob
             (let ((length (length value)))
               (with-foreign-array (ptr value `(:array :uint8 ,length))
                 (duckdb-api:duckdb-append-blob handle ptr length))))
            (:duckdb-float (duckdb-api:duckdb-append-float handle value))
            (:duckdb-double (duckdb-api:duckdb-append-double handle value))
            (:duckdb-decimal
             (etypecase value
               (ratio (duckdb-api:duckdb-append-varchar
                       handle (rational-to-string value 38)))
               (single-float (duckdb-api:duckdb-append-float handle value))
               (double-float (duckdb-api:duckdb-append-double handle value))
               (integer (duckdb-api:duckdb-append-varchar
                         handle (format nil "~d" value)))))
            ;; 8-bit integers
            (:duckdb-tinyint (duckdb-api:duckdb-append-int8 handle value))
            (:duckdb-utinyint (duckdb-api:duckdb-append-uint8 handle value))
            ;; 16-bit integers
            (:duckdb-smallint (duckdb-api:duckdb-append-int16 handle value))
            (:duckdb-usmallint (duckdb-api:duckdb-append-uint16 handle value))
            ;; 32-bit integers
            (:duckdb-integer (duckdb-api:duckdb-append-int32 handle value))
            (:duckdb-uinteger (duckdb-api:duckdb-append-uint32 handle value))
            ;; 64-bit integers
            (:duckdb-bigint (duckdb-api:duckdb-append-int64 handle value))
            (:duckdb-ubigint (duckdb-api:duckdb-append-uint64 handle value))
            ;; hugeint
            (:duckdb-hugeint (duckdb-api:duckdb-append-hugeint handle value))
            (:duckdb-uhugeint (duckdb-api:duckdb-append-uhugeint handle value))
            (:duckdb-date (duckdb-api:duckdb-append-date handle value))
            (:duckdb-timestamp (duckdb-api:duckdb-append-timestamp handle value))
            (:duckdb-time (duckdb-api:duckdb-append-time handle value))
            (:duckdb-uuid (let ((s (fuuid:to-string value)))
                            (duckdb-api:duckdb-append-varchar handle s))))))
    `(ecase duckdb-type
       ,@(loop :for (type append-form) :in appender-types
               :collect `(,type
                          ;; Handle binding NULL with the exception
                          ;; of NIL being bound as FALSE for
                          ;; booleans:
                          (if (or ,(unless (eql type :duckdb-boolean)
                                     `(null value))
                                  (eql value :null))
                              (duckdb-api:duckdb-append-null handle)
                              ,append-form))))))

(defun append-row (appender values)
  (let ((error-message "Failed appending ~d value~:p to table with ~d column~:p."))
    (loop :with handle := (handle appender)
          :for i :from 0
          :for duckdb-type :in (types appender)
          :for value :in values
          :do (generate-append-value-dispatch)
          :finally (if (eql (column-count appender) i)
                       (duckdb-api:duckdb-appender-end-row handle)
                       (error 'duckdb-error
                              :database (database (connection appender))
                              :appender appender
                              :error-message
                              (format nil error-message
                                      i
                                      (column-count appender)))))))

(defmacro with-static-table ((table-name columns) &body body)
  (alexandria:with-gensyms (table-id)
    (alexandria:once-only (table-name)
      `(let ((,table-id (duckdb-api:add-table-reference
                         (duckdb-api:make-static-columns ,columns))))
         (unwind-protect
              (let ((duckdb-api:*static-table-bindings*
                      (acons (if (stringp ,table-name)
                                 ,table-name
                                 (param-case-to-snake-case ,table-name))
                             ,table-id
                             duckdb-api:*static-table-bindings*)))
                ,@body)
           (duckdb-api:clear-table-reference ,table-id))))))

(defmacro with-static-tables (((table-name columns) &rest more-clauses)
                              &body body)
  `(with-static-table (,table-name ,columns)
     ,@(if more-clauses
           `((with-static-tables ,more-clauses
               ,@body))
           body)))

(defun bind-static-table (table-name columns)
  (duckdb-api:add-global-table-reference
   (if (stringp table-name)
       table-name
       (param-case-to-snake-case table-name))
   (duckdb-api:make-static-columns columns))
  nil)

(defun unbind-static-table (table-name)
  (duckdb-api:clear-global-table-reference
   (if (stringp table-name)
       table-name
       (param-case-to-snake-case table-name)))
  nil)

(defun clear-static-tables ()
  (duckdb-api:clear-global-table-references)
  nil)

(defmacro with-static-table-type-map (type-alist &rest body)
  `(let ((duckdb-api:*static-table-type-map* ,type-alist))
     ,@body))

(defmacro with-transaction
    ((&key connection)
     &body body)
  (alexandria:with-gensyms (begin-statement
                            finish-statement
                            success
                            one-conn)
    `(let ((,success nil)
           ,@(when connection
               `((,one-conn ,connection))))
       (with-statement (,begin-statement "BEGIN TRANSACTION"
                        ,@(when connection
                            `(:connection ,one-conn)))
         (perform ,begin-statement))
       (unwind-protect
            (restart-case (multiple-value-prog1 (progn ,@body)
                            (setf ,success t))
              (commit-transaction ()
                :report "Continue with COMMIT."
                (setf ,success t))
              (rollback-transaction ()
                :report "Continue with ROLLBACK."
                nil))
         (with-statement (,finish-statement (if ,success "COMMIT" "ROLLBACK")
                          ,@(when connection
                              `(:connection ,one-conn)))
           (perform ,finish-statement))))))

;; Initialize the default number of threads based on DuckDB defaults
(eval-when (:load-toplevel :execute)
  (with-open-database (db :threads nil)
    (with-default-connection (db)
      (setf *default-thread-count*
            (get-result (query "SELECT current_setting('threads') AS thread_count"
                               nil)
                        'thread-count 0)))))
