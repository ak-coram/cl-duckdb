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
  (with-foreign-object (p-database '(:pointer duckdb-api:duckdb-database))
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
  (with-foreign-object (p-database '(:pointer duckdb-api:duckdb-database))
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
  (with-foreign-object (p-connection '(:pointer duckdb-api:duckdb-connection))
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
  (with-foreign-object (p-connection '(:pointer duckdb-api:duckdb-connection))
    (setf (mem-ref p-connection 'duckdb-api:duckdb-connection)
          (handle connection))
    (duckdb-api:duckdb-disconnect p-connection)))

(defmacro with-open-connection ((connection-var database) &body body)
  `(let ((,connection-var (connect ,database)))
     (unwind-protect
          (progn ,@body)
       (disconnect ,connection-var))))

;;; Queries

(defclass result ()
  ((connection :initarg :connection)
   (handle :accessor handle :initarg :handle)))

(defun make-result (connection p-result)
  (make-instance 'result
                 :connection connection
                 :handle p-result))

(defun query (connection query)
  (with-foreign-object (p-result 'duckdb-api:p-duckdb-result)
    (with-foreign-string (p-query query)
      (if (eq (duckdb-api:duckdb-query (handle connection)
                                       p-query
                                       p-result)
              :duckdb-success)
          (make-result connection p-result)
          (error 'duckdb-error
                 :database (database connection)
                 :connection connection
                 :error-message (duckdb-api:duckdb-result-error p-result))))))

(defun destroy-result (result)
  (duckdb-api:duckdb-destroy-result (handle result)))

(defmacro with-query ((result-var connection query) &body body)
  `(let ((,result-var (query ,connection ,query)))
     (unwind-protect
          ,@body
       (destroy-result ,result-var))))

(defun translate-vector (chunk-size vector results)
  (let ((vector-type (duckdb-api:get-ffi-type (duckdb-api:get-vector-type vector)))
        (p-data (duckdb-api:duckdb-vector-get-data vector))
        (validity (duckdb-api:duckdb-vector-get-validity vector)))
    (loop :for i :below chunk-size
          :do (vector-push-extend
               (when (duckdb-api:duckdb-validity-row-is-valid validity i)
                 (mem-aref p-data vector-type i))
               results
               chunk-size))))

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

#+nil
(let* ((query (concatenate 'string
                           "SELECT True::boolean AS A"
                           ", -12::tinyint AS B"
                           ", -123::smallint AS C"
                           ", -1234::integer AS D"
                           ", -12345::bigint AS E"
                           ", 12::utinyint AS F"
                           ", 123::usmallint AS G"
                           ", 1234::uinteger AS H"
                           ", 12345::ubigint AS I"
                           ", -18446744073709551629::hugeint AS J"
                           ", 3.14::float AS K"
                           ", 2.71::double AS L"
                           ", VERSION() AS an_inline_string"
                           ", 'A pálpusztai egy finom sajt.'::text AS not_an_inline_string"
                           ", '\\xFF\\x00'::blob AS an_inline_blob"
                           ", ENCODE('바람 부는 대로, 물결 치는 대로')::blob AS not_an_inline_blob"
                           ", current_date AS current_date"
                           ", current_time AS current_time"
                           ", now()::timestamp AS current_typestamp"
                           ", 'epoch'::timestamptz AS epoch_tz"
                           ", gen_random_uuid() AS uuid"
                           ", INTERVAL 24 HOURS "
                           "+ INTERVAL 2 MINUTES "
                           "+ INTERVAL 55 SECONDS AS i1"
                           ", INTERVAL 6 MONTHS AS i2"
                           ", NULL AS null"))
       (multi-row-query (format nil "~{~A~^ UNION ALL ~}" (loop :repeat 10 :collect query))))
  (with-open-database (db)
    (with-open-connection (conn db)
      (with-query (result conn multi-row-query)
        (translate-result result)))))
