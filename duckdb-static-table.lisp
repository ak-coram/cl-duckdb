(in-package :duckdb-api)

(defparameter *static-tablespace* nil)

(defun get-table (name)
  (alexandria:assoc-value *static-tablespace* name :test #'string=))

(eval-when (:compile-toplevel)
  (defun static-table-column-types ()
    '(((simple-array (unsigned-byte 8)) :duckdb-utinyint)
      ((simple-array (signed-byte 8)) :duckdb-tinyint)
      ((simple-array (unsigned-byte 16)) :duckdb-usmallint)
      ((simple-array (signed-byte 16)) :duckdb-smallint)
      ((simple-array (unsigned-byte 32)) :duckdb-uinteger)
      ((simple-array (signed-byte 32)) :duckdb-integer)
      ((simple-array (unsigned-byte 64)) :duckdb-ubigint)
      ((simple-array (signed-byte 64)) :duckdb-bigint)
      ((simple-array single-float) :duckdb-float)
      ((simple-array double-float) :duckdb-double))))

(defmacro static-vector-type (vector)
  `(etypecase ,vector
     ,@(static-table-column-types)))

(defcstruct static-table-bind-data-struct
  (table-name :string))

(defcallback free-ptr :void ((ptr :pointer))
  (foreign-free ptr))

(defcallback static-table-bind :void ((info duckdb-bind-info))
  (handler-case
      (let* ((bind-data (foreign-alloc '(:struct static-table-bind-data-struct))))
        (with-duckdb-value (table-name-param (duckdb-bind-get-parameter info 0))
          (let* ((table-name (duckdb-get-varchar table-name-param))
                 (table (get-table table-name)))
            (unless table
              (error (format nil "table ~s is undefined" table-name)))
            (loop :for (column-name . values) :in table
                  :for duckdb-type := (static-vector-type values)
                  :do (with-logical-type (type duckdb-type)
                        (duckdb-bind-add-result-column info column-name type)))
            (setf (foreign-slot-value bind-data
                                      '(:struct static-table-bind-data-struct)
                                      'table-name)
                  table-name)))
        (duckdb-bind-set-bind-data info bind-data (callback free-ptr)))
    (error (c)
      (duckdb-bind-set-error info
                             (format nil "static_table bind - ~a" c)))))

(defcstruct static-table-init-data-struct
  (index :int64))

(defcallback static-table-init :void ((info duckdb-bind-info))
  (handler-case
      (let ((init-data (foreign-alloc '(:struct static-table-init-data-struct))))
        (setf (foreign-slot-value init-data
                                  '(:struct static-table-init-data-struct)
                                  'index)
              0)
        (duckdb-init-set-init-data info init-data (callback free-ptr)))
    (error (c)
      (duckdb-init-set-error info
                             (format nil "static_table init - ~a" c)))))

(defmacro copy-static-vector ()
  `(ecase duckdb-type
     ,@(loop :for (cl-type duckdb-type) :in (static-table-column-types)
             :collect
             `(,duckdb-type
               (let ((vector values))
                 (declare (,cl-type vector))
                 (loop :for i fixnum :below (duckdb-vector-size)
                       :for k fixnum :from index
                       :until (>= k data-length)
                       :do (setf (mem-aref data-ptr
                                           ,(get-ffi-type duckdb-type) i)
                                 (aref vector k))
                       :finally (return i)))))))

(defcallback static-table-function :void ((info duckdb-bind-info)
                                          (output duckdb-data-chunk))
  (handler-case
      (let* ((bind-data (duckdb-function-get-bind-data info))
             (init-data (duckdb-function-get-init-data info))
             (table-name (foreign-slot-value bind-data
                                             '(:struct static-table-bind-data-struct)
                                             'table-name)))
        (with-foreign-slots ((index) init-data (:struct static-table-init-data-struct))
          (let ((n (loop :for column :in (get-table table-name)
                         :for values :of-type vector := (cdr column)
                         :for column-index :from 0
                         :for duckdb-type := (static-vector-type values)
                         :for ffi-type := (get-ffi-type duckdb-type)
                         :for vector := (duckdb-data-chunk-get-vector output column-index)
                         :for data-length := (length values)
                         :for data-ptr := (duckdb-vector-get-data vector)
                         :maximize (copy-static-vector))))
            (setf index (+ n index))
            (duckdb-data-chunk-set-size output n))))
    (error (c)
      (duckdb-function-set-error info
                                 (format nil "static_table function - ~a" c)))))

(defun register-static-table-function (connection)
  (with-table-function (function)
    (with-logical-type (type :duckdb-varchar)
      (duckdb-table-function-set-name function "static_table")
      (duckdb-table-function-add-parameter function type)
      (duckdb-table-function-set-bind function (callback static-table-bind))
      (duckdb-table-function-set-init function (callback static-table-init))
      (duckdb-table-function-set-function function (callback static-table-function))
      (duckdb-register-table-function connection function))))
