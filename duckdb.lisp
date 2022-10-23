;;;; duckdb.lisp

(in-package #:duckdb)

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
   (path :accessor path)))

(defmethod initialize-instance :after
    ((instance database) &key path)
  (with-foreign-object (p-database 'duckdb-api:duckdb-database)
    (with-foreign-object (p-error-message '(:pointer :string))
      (let* (;; prefer duckdb-open-ext over duckdb-open for error message
             (result (duckdb-api:duckdb-open-ext path
                                                 p-database
                                                 (null-pointer)
                                                 p-error-message)))
        (if (eq result :duckdb-success)
            (setf (handle instance)
                  (mem-ref p-database 'duckdb-api:duckdb-database)

                  (path instance) path)
            (error 'duckdb-error
                   :error-message (duckdb-api:get-message p-error-message)))))))

(defun open-database (&optional path)
  "Opens and returns database for PATH with \":memory:\" as default.
See CLOSE-DATABASE for cleanup."
  (make-instance 'database :path (or path ":memory:")))

(defun close-database (database)
  "Does resource cleanup for DATABASE, also see OPEN-DATABASE."
  (with-foreign-object (p-database 'duckdb-api:duckdb-database)
    (setf (mem-ref p-database 'duckdb-api:duckdb-database)
          (handle database))
    (duckdb-api:duckdb-close p-database)))

(defmacro with-open-database ((database-var &key path) &body body)
  "Opens database for PATH, binds it to DATABASE-VAR.
The database is closed after BODY is evaluated."
  `(let ((,database-var (open-database ,path)))
     (unwind-protect
          (progn ,@body)
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
      (if (eq result :duckdb-success)
          (setf (database instance) database

                (handle instance)
                (mem-ref p-connection 'duckdb-api:duckdb-connection))
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
     (unwind-protect
          (progn ,@body)
       (disconnect ,connection-var))))

(defvar *connection* nil
  "Used to refer to the current database connection.
It is intended to make interactive use more convenient and is used as
the default connection for functions where CONNECTION is an optional
or a keyword parameter.")

(defun initialize-default-connection (&optional path)
  "Connects to database for PATH and sets the value of *CONNECTION*.
Also see DISCONNECT-DEFAULT-CONNECTION for cleanup."
  (setf *connection* (connect (open-database path))))

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
     (unwind-protect
          (progn ,@body)
       (disconnect *connection*))))

(defmacro with-transient-connection (&body body)
  "Connects to new in-memory database and dynamically binds *CONNECTION*.
The connection and the database are cleaned up after BODY is evaluated."
  (alexandria:with-gensyms (database)
    `(let* ((,database (open-database)))
       (unwind-protect
            (with-default-connection (,database)
              (progn ,@body))
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
        (if (eq result :duckdb-success)
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
     (unwind-protect
          (progn ,@body)
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
    (if (eq (duckdb-api:duckdb-execute-prepared (handle statement)
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
    (if (eq (duckdb-api:duckdb-execute-prepared (handle statement)
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
     (unwind-protect
          (progn ,@body)
       (destroy-result ,result-var))))

(defun translate-vector (chunk-size vector results)
  (multiple-value-bind (vector-type internal-type decimal-scale)
      (duckdb-api:get-vector-type vector)
    (let* ((vector-ffi-type (duckdb-api:get-ffi-type (or internal-type vector-type)))
           (p-data (duckdb-api:duckdb-vector-get-data vector))
           (validity (duckdb-api:duckdb-vector-get-validity vector)))
      (loop :for i :below chunk-size
            :for value := (when (duckdb-api:duckdb-validity-row-is-valid validity i)
                            (let ((v (mem-aref p-data vector-ffi-type i)))
                              (if (eql vector-type :duckdb-decimal)
                                  (* v (expt 10 (- decimal-scale)))
                                  v)))
            :do (vector-push-extend value
                                    results
                                    chunk-size)))))

(defun translate-chunk (result-alist chunk)
  (let ((column-count (duckdb-api:duckdb-data-chunk-get-column-count chunk))
        (chunk-size (duckdb-api:duckdb-data-chunk-get-size chunk)))
    (loop :for column-index :below column-count
          :for entry :in result-alist
          :for vector := (duckdb-api:duckdb-data-chunk-get-vector chunk column-index)
          :do (translate-vector chunk-size vector (cdr entry)))))

(defun translate-result (result)
  (let* ((p-result (handle result))
         (chunk-count (duckdb-api:result-chunk-count p-result))
         (column-count (duckdb-api:duckdb-column-count p-result))
         (result-alist
           (loop :for column-index :below column-count
                 :collect (cons (duckdb-api:duckdb-column-name p-result column-index)
                                (make-array '(0) :adjustable t :fill-pointer 0)))))
    (loop :for chunk-index :below chunk-count
          :for chunk := (duckdb-api:result-get-chunk p-result chunk-index)
          :do (translate-chunk result-alist chunk))
    result-alist))

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
            (:duckdb-boolean boolean
             (duckdb-api:duckdb-bind-boolean statement-handle i value))
            (:duckdb-varchar string
             (duckdb-api:duckdb-bind-varchar statement-handle i value))
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
             (duckdb-api:duckdb-bind-hugeint statement-handle i value)))))
    `(case duckdb-type
       ,@(loop :for (type _ binding-form) :in parameter-binding-types
               :unless (eql type :_)    ; :_ is used to skip
                 :collect `(,type ,binding-form))
       ;; In some cases such as "SELECT ?" the type can not be
       ;; determined in advance by DuckDB, so we use the type of the
       ;; parameter value to bind it.
       (t (typecase value
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

(defun query (query parameters &key (connection *connection*))
  (with-statement (statement query :connection connection)
    (when parameters
      (bind-parameters statement parameters))
    (with-execute (result statement)
      (translate-result result))))

(defun run (&rest queries)
  (loop :for q :in queries
        :if (stringp q) :do (with-statement (statement q)
                              (perform statement))
          :else :do (with-statement (statement (car q))
                      (bind-parameters statement (cadr q))
                      (perform statement))))

(defun get-result (results column &optional n)
  (labels ((compare (a b) (string= a (str:param-case b))))
    (let ((result-values (if (stringp column)
                             (alexandria:assoc-value results
                                                     column
                                                     :test #'string=)
                             (alexandria:assoc-value results
                                                     (str:downcase column)
                                                     :test #'compare))))
      (if n
          (aref result-values n)
          result-values))))
