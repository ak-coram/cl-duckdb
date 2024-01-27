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
  ((database :initarg :database)
   (statement :initarg :statement)
   (appender :initarg :appender)
   (arrow :initarg :arrow)
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
                    (list "threads" 1
                          "external_threads" #+bordeaux-threads threads
                                             #-bordeaux-threads 0)))
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
                                      (plusp threads))
                             (duckdb-api:start-worker-pool handle threads))))
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
                   (1- (etypecase threads
                         ((integer 1) threads)
                         (boolean *default-thread-count*))))))

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
  ((connection :initarg :connection)
   (statement :initarg :statement)
   (handle :accessor handle :initarg :handle)))

(defun make-result (connection statement p-result)
  (make-instance 'result
                 :connection connection
                 :statement statement
                 :handle p-result))

(defun execute (statement)
  "Runs STATEMENT and returns RESULT instance.
DESTROY-RESULT must be called on the returned value for resource
cleanup."
  (let ((connection (connection statement))
        (p-result (foreign-alloc '(:struct duckdb-api:duckdb-result))))
    (if (eql (duckdb-api:duckdb-execute-prepared (handle statement)
                                                 p-result)
             :duckdb-success)
        (make-result connection statement p-result)
        (let ((error-message (duckdb-api:duckdb-result-error p-result)))
          (duckdb-api:duckdb-destroy-result p-result)
          (foreign-free p-result)
          (error 'duckdb-error
                 :database (database connection)
                 :statement statement
                 :error-message error-message)))))

(defun perform (statement)
  "Same as EXECUTE, but doesn't return any results and needs no cleanup."
  (with-foreign-object (p-result '(:struct duckdb-api:duckdb-result))
    (if (eql (duckdb-api:duckdb-execute-prepared (handle statement)
                                                 p-result)
             :duckdb-success)
        (duckdb-api:duckdb-destroy-result p-result)
        (let ((error-message (duckdb-api:duckdb-result-error p-result)))
          (duckdb-api:duckdb-destroy-result p-result)
          (error 'duckdb-error
                 :database (database (connection statement))
                 :statement statement
                 :error-message error-message)))))

(defun destroy-result (result)
  (let ((p-result (handle result)))
    (duckdb-api:duckdb-destroy-result p-result)
    (foreign-free p-result)))

(defmacro with-execute ((result-var statement) &body body)
  `(let ((,result-var (execute ,statement)))
     (unwind-protect (progn ,@body)
       (destroy-result ,result-var))))

(defmacro translate-value-case (&rest cases)
  `(case vector-type
     (:duckdb-decimal (let ((decimal-scale aux))
                        (* v (expt 10 (- decimal-scale)))))
     (:duckdb-enum (let ((enum-alist aux))
                     (alexandria:assoc-value enum-alist v)))
     (:duckdb-bit
      (loop :with unused-bit-count := (aref v 0)
            :with octet-count := (1- (length v))
            :with bit-count := (- (* octet-count 8) unused-bit-count)
            :with bits := (make-array (list bit-count) :element-type 'bit)
            :for i :below bit-count
            :for j := (+ i unused-bit-count)
            :for octet-index := (floor j 8)
            :for octet := (aref v (1+ octet-index))
            :for octet-bit-index := (- 7 (mod j 8))
            :do (setf (aref bits i) (ldb (byte 1 octet-bit-index) octet))
            :finally (return bits)))
     ,@cases
     (t v)))

(defgeneric translate-composite
    (type aux vector v &optional sql-null-return-value))

(defmethod translate-composite ((type (eql :duckdb-list)) child-type vector v
                                &optional sql-null-return-value)
  (destructuring-bind (offset length) v
    (loop :with child-vector := (duckdb-api:duckdb-list-vector-get-child vector)
          :with child-validity := (duckdb-api:duckdb-vector-get-validity child-vector)
          :with child-data := (duckdb-api:duckdb-vector-get-data child-vector)
          :with (vector-type internal-type aux) := child-type
          :with vector-ffi-type := (duckdb-api:get-ffi-type (or internal-type
                                                                vector-type))
          :for i :from offset :below (+ offset length)
          :for is-valid := (duckdb-api:duckdb-validity-row-is-valid child-validity i)
          :for v := (unless (or (not is-valid) (eql vector-ffi-type :void))
                      (mem-aref child-data vector-ffi-type i))
          :collect (if is-valid
                       (translate-value-case
                        ((:duckdb-list :duckdb-map)
                         (translate-composite vector-type aux child-vector v
                                              sql-null-return-value))
                        ((:duckdb-struct :duckdb-union)
                         (translate-composite vector-type aux child-vector i
                                              sql-null-return-value)))
                       sql-null-return-value))))

(defmethod translate-composite ((type (eql :duckdb-struct)) child-types vector i
                                &optional sql-null-return-value)
  (loop :for child-index :from 0
        :for (name vector-type internal-type aux) :in child-types
        :for child-vector := (duckdb-api:duckdb-struct-vector-get-child vector
                                                                        child-index)
        :for child-validity := (duckdb-api:duckdb-vector-get-validity child-vector)
        :for child-data := (duckdb-api:duckdb-vector-get-data child-vector)
        :for vector-ffi-type := (duckdb-api:get-ffi-type (or internal-type
                                                             vector-type))
        :for is-valid := (duckdb-api:duckdb-validity-row-is-valid child-validity i)
        :for v := (unless (or (not is-valid) (eql vector-ffi-type :void))
                    (mem-aref child-data vector-ffi-type i))
        :collect (cons name
                       (if is-valid
                           (translate-value-case
                            ((:duckdb-list :duckdb-map)
                             (translate-composite vector-type aux child-vector v
                                                  sql-null-return-value))
                            ((:duckdb-struct :duckdb-union)
                             (translate-composite vector-type aux child-vector 0
                                                  sql-null-return-value)))
                           sql-null-return-value))))

(defmethod translate-composite ((type (eql :duckdb-union)) child-types vector i
                                &optional sql-null-return-value)
  (let* ((result (translate-composite :duckdb-struct
                                      ;; add value selector tag to struct members
                                      (cons '("" :duckdb-utinyint nil nil)
                                            child-types)
                                      vector i sql-null-return-value))
         (value-index (1+ (cdar result))))
    (cdr (nth value-index result))))

(defmethod translate-composite ((type (eql :duckdb-map)) child-types vector v
                                &optional sql-null-return-value)
  (loop :with entries
          := (translate-composite
              :duckdb-list `(:duckdb-struct nil ((k . ,(car child-types))
                                                 (v . ,(cadr child-types))))
              vector
              v
              sql-null-return-value)
        :for entry :in entries
        :for key := (alexandria:assoc-value entry 'k)
        :for value := (alexandria:assoc-value entry 'v)
        :collect (cons key value)))

(defun translate-vector
    (chunk-size vector results &optional sql-null-return-value)
  (destructuring-bind (vector-type internal-type aux)
      (duckdb-api:get-vector-type vector)
    (let* ((vector-ffi-type (duckdb-api:get-ffi-type (or internal-type 
                                                         vector-type)))
           (p-data (duckdb-api:duckdb-vector-get-data vector))
           (validity (duckdb-api:duckdb-vector-get-validity vector)))
      (loop :for i :below chunk-size
            :for value
              := (if (duckdb-api:duckdb-validity-row-is-valid validity i)
                     (let ((v (unless (eql vector-ffi-type :void)
                                (mem-aref p-data vector-ffi-type i))))
                       (translate-value-case
                        ((:duckdb-list :duckdb-map)
                         (translate-composite vector-type aux vector v
                                              sql-null-return-value))
                        ((:duckdb-struct :duckdb-union)
                         (translate-composite vector-type aux vector i
                                              sql-null-return-value))))
                     sql-null-return-value)
            :do (vector-push-extend value results chunk-size)
            :finally (return vector-type)))))

(defun translate-chunk (result-alist chunk &optional sql-null-return-value)
  (let ((column-count (duckdb-api:duckdb-data-chunk-get-column-count chunk))
        (chunk-size (duckdb-api:duckdb-data-chunk-get-size chunk)))
    (values
     (loop :for column-index :below column-count
           :for entry :in result-alist
           :for vector := (duckdb-api:duckdb-data-chunk-get-vector chunk column-index)
           :collect (translate-vector chunk-size vector (cdr entry)
                                      sql-null-return-value))
     chunk-size)))

(defun translate-result (result &optional sql-null-return-value)
  (let* ((p-result (handle result))
         (chunk-count (duckdb-api:result-chunk-count p-result))
         (column-count (duckdb-api:duckdb-column-count p-result))
         (result-alist
           (loop :for column-index :below column-count
                 :collect (cons (duckdb-api:duckdb-column-name p-result column-index)
                                (make-array '(0) :adjustable t :fill-pointer 0))))
         column-types
         (row-count
           (loop :for chunk-index :below chunk-count
                 :sum (duckdb-api:with-data-chunk (chunk p-result chunk-index)
                        (multiple-value-bind (types chunk-size)
                            (translate-chunk result-alist
                                             chunk
                                             sql-null-return-value)
                          (setf column-types types)
                          chunk-size)))))
    (values result-alist column-types row-count)))

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
