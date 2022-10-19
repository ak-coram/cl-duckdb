;;;; duckdb.lisp

(in-package #:duckdb)

(defun get-message (p-string)
  (foreign-string-to-lisp (mem-ref p-string '(:pointer :string))))

(define-condition duckdb-error (simple-error)
  ((database :initarg :database)
   (statement :initarg :statement)
   (appender :initarg :appender)
   (arrow :initarg :arrow)
   (error-message :initarg :error-message
                  :accessor error-message))
  (:report (lambda (condition stream)
             (format stream "~A."
                     (error-message condition)))))

;;; Databases

(defclass database ()
  ((handle :accessor handle)
   (path :accessor path)))

(defmethod initialize-instance :after
    ((instance database) &key path)
  (with-foreign-object (p-database 'duckdb-api:duckdb-database)
    (with-foreign-object (p-error-message '(:pointer :string))
      (let* ((path (or path ":memory:"))
             ;; prefer duckdb-open-ext over duckdb-open for error message
             (result (duckdb-api:duckdb-open-ext path
                                                 p-database
                                                 (null-pointer)
                                                 p-error-message)))
        (if (eq result :duckdb-success)
            (setf (handle instance)
                  (mem-ref p-database 'duckdb-api:duckdb-database)

                  (path instance) path)
            (error 'duckdb-error
                   :error-message (get-message p-error-message)))))))

(defun open-database (&optional path)
  (make-instance 'database :path path))

(defun close-database (database)
  (with-foreign-object (p-database 'duckdb-api:duckdb-database)
    (setf (mem-ref p-database 'duckdb-api:duckdb-database)
          (handle database))
    (duckdb-api:duckdb-close p-database)))

(defmacro with-open-database ((database-var &optional path) &body body)
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
  (make-instance 'connection :database database))

(defun disconnect (connection)
  (with-foreign-object (p-connection 'duckdb-api:duckdb-connection)
    (setf (mem-ref p-connection 'duckdb-api:duckdb-connection)
          (handle connection))
    (duckdb-api:duckdb-disconnect p-connection)))

(defmacro with-open-connection ((connection-var database) &body body)
  `(let ((,connection-var (connect ,database)))
     (unwind-protect
          (progn ,@body)
       (disconnect ,connection-var))))

;;; Statements

(defclass statement ()
  ((connection :initarg :connection :accessor connection)
   (query :initarg :query)
   (parameter-count :initarg :parameter-count :accessor parameter-count)
   (parameter-types :initarg :parameter-types :accessor parameter-types)
   (handle :accessor handle :initarg :handle)))

(defun prepare (connection query)
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

(defmacro with-statement ((statement-var connection query) &body body)
  `(let ((,statement-var (prepare ,connection ,query)))
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
  (let ((connection (connection statement))
        (p-result (foreign-alloc '(:struct duckdb-api:duckdb-result))))
    (if (eq (duckdb-api:duckdb-execute-prepared (handle statement)
                                                p-result)
            :duckdb-success)
        (make-result connection statement p-result)
        (error 'duckdb-error
               :database (database connection)
               :statement statement
               :error-message (duckdb-api:duckdb-result-error p-result)))))

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
                     (parameter-count statement)
                     (length values))))))

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
      :do (typecase value
            ;; Treat nil as false for duckdb-boolean parameters
            (null (if (eql duckdb-type :duckdb-boolean)
                      (duckdb-api:duckdb-bind-boolean statement-handle i value)
                      (duckdb-api:duckdb-bind-null statement-handle i)))
            (boolean (duckdb-api:duckdb-bind-boolean statement-handle
                                                     i
                                                     value))
            (string (duckdb-api:duckdb-bind-varchar statement-handle
                                                    i
                                                    value))
            (single-float (duckdb-api:duckdb-bind-float statement-handle
                                                        i
                                                        value))
            (double-float (duckdb-api:duckdb-bind-double statement-handle
                                                         i
                                                         value))
            ;; 8-bit integers
            ((integer -128 127)
             (duckdb-api:duckdb-bind-int8 statement-handle i value))
            ((integer 0 255)
             (duckdb-api:duckdb-bind-uint8 statement-handle i value))
            ;; 16-bit integers
            ((integer -32768 32767)
             (duckdb-api:duckdb-bind-int16 statement-handle i value))
            ((integer 0 65535)
             (duckdb-api:duckdb-bind-uint16 statement-handle i value))
            ;; 32-bit integers
            ((integer -2147483648 2147483647)
             (duckdb-api:duckdb-bind-int32 statement-handle i value))
            ((integer 0 4294967295)
             (duckdb-api:duckdb-bind-uint32 statement-handle i value))
            ;; 64-bit integers
            ((integer -9223372036854775808 9223372036854775807)
             (duckdb-api:duckdb-bind-int64 statement-handle i value))
            ((integer 0 18446744073709551615)
             (duckdb-api:duckdb-bind-uint64 statement-handle i value))
            ;; hugeint
            ((integer -170141183460469231731687303715884105727
                      170141183460469231731687303715884105727)
             (duckdb-api:duckdb-bind-hugeint statement-handle i value))))))

(defun query (connection query &rest parameters)
  (with-statement (statement connection query)
    (bind-parameters statement parameters)
    (with-execute (result statement)
      (translate-result result))))
