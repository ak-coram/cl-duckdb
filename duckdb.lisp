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
                  :accessor error-message)))

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
          ,@body
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
          ,@body
       (disconnect ,connection-var))))

;;; Queries

(defclass result ()
  ((connection :initarg :connection)
   (handle :accessor handle :initarg :handle)
   (column-names :initarg :column-names)
   (column-types :initarg :column-types)
   (column-count :initarg :column-count)
   (row-count :accessor row-count :initarg :row-count)
   (rows-changed :initarg :rows-changed)
   (error-message :initarg :error-message)))

(defun make-result (connection p-result)
  (with-foreign-slots ((duckdb-api:column-count
                        duckdb-api:row-count
                        duckdb-api:rows-changed
                        duckdb-api:error-message)
                       p-result
                       (:struct duckdb-api:duckdb-result))
    (make-instance 'result
                   :connection connection
                   :handle p-result
                   :column-count duckdb-api:column-count
                   :row-count duckdb-api:row-count
                   :rows-changed duckdb-api:rows-changed
                   :error-message
                   (foreign-string-to-lisp duckdb-api:error-message)
                   :column-names
                   (loop :for idx :below duckdb-api:column-count
                         :collect (duckdb-api:duckdb-column-name p-result
                                                                 idx)))))

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

(defun get-column-values (result column-index)
  (let* ((p-result (handle result))
         (p-data (duckdb-api:duckdb-column-data p-result
                                                column-index))
         (p-nullmask (duckdb-api:duckdb-nullmask-data p-result
                                                      column-index))
         (column-type (duckdb-api:duckdb-column-type p-result
                                                     column-index)))
    (loop :for i :below (row-count result)
          :collect (unless (mem-aref p-nullmask :bool i)
                     (mem-aref p-data
                               (duckdb-api:get-ffi-type column-type)
                               i)))))

;; (with-open-database (db)
;;   (with-open-connection (conn db)
;;     (with-query (result conn (concatenate 'string
;;                                           "SELECT 12345 AS A UNION "
;;                                           "SELECT NULL AS A UNION "
;;                                           "SELECT 54321 AS A"))
;;       (get-column-values result 0))))
