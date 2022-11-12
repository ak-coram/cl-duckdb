(in-package :duckdb-api)

(defparameter *static-tablespace* nil)

(defun get-table (name)
  (alexandria:assoc-value *static-tablespace* name :test #'string=))

(defun get-vector-duckdb-type (vector)
  (etypecase vector
    ((vector (unsigned-byte 8)) :duckdb-utinyint)
    ((vector (signed-byte 8)) :duckdb-tinyint)
    ((vector (unsigned-byte 16)) :duckdb-usmallint)
    ((vector (signed-byte 16)) :duckdb-smallint)
    ((vector (unsigned-byte 32)) :duckdb-uinteger)
    ((vector (signed-byte 32)) :duckdb-integer)
    ((vector (unsigned-byte 64)) :duckdb-ubigint)
    ((vector (signed-byte 64)) :duckdb-bigint)
    ((vector single-float) :duckdb-float)
    ((vector double-float) :duckdb-double)))

(defcstruct static-table-bind-data-struct
  (table-name :string))

(defcallback free-ptr :void ((ptr :pointer))
  (foreign-free ptr))

(defcallback static-table-bind :void ((info duckdb-bind-info))
  (let* ((bind-data (foreign-alloc '(:struct static-table-bind-data-struct))))
    (with-duckdb-value (table-name-param (duckdb-bind-get-parameter info 0))
      (let* ((table-name (duckdb-get-varchar table-name-param)))
        (loop :for (column-name . values) :in (get-table table-name)
              :for duckdb-type := (get-vector-duckdb-type values)
              :do (with-logical-type (type duckdb-type)
                    (duckdb-bind-add-result-column info column-name type)))
        (setf (foreign-slot-value bind-data
                                  '(:struct static-table-bind-data-struct)
                                  'table-name)
              table-name)))
    (duckdb-bind-set-bind-data info bind-data (callback free-ptr))))

(defcstruct static-table-init-data-struct
  (index :int64))

(defcallback static-table-init :void ((info duckdb-bind-info))
  (let ((init-data (foreign-alloc '(:struct static-table-init-data-struct))))
    (setf (foreign-slot-value init-data
                              '(:struct static-table-init-data-struct)
                              'index)
          0)
    (duckdb-init-set-init-data info init-data (callback free-ptr))))

(defcallback static-table-function :void ((info duckdb-bind-info)
                                          (output duckdb-data-chunk))
  (let* ((bind-data (duckdb-function-get-bind-data info))
         (init-data (duckdb-function-get-init-data info))
         (table-name (foreign-slot-value bind-data
                                         '(:struct static-table-bind-data-struct)
                                         'table-name)))
    (with-foreign-slots ((index) init-data (:struct static-table-init-data-struct))
      (let ((n (loop :for (column-name . values) :in (get-table table-name)
                     :for column-index :from 0
                     :for ffi-type := (get-ffi-type (get-vector-duckdb-type values))
                     :for vector := (duckdb-data-chunk-get-vector output column-index)
                     :for data-length := (length values)
                     :for data-ptr := (duckdb-vector-get-data vector)
                     :maximize (loop :for i :below (duckdb-vector-size)
                                     :for k :from index
                                     :until (>= k data-length)
                                     :do (setf (mem-aref data-ptr ffi-type i)
                                               (aref values k))
                                     :finally (return i)))))
        (setf index (+ n index))
        (duckdb-data-chunk-set-size output n)))))

(defun register-static-table-function (connection)
  (with-table-function (function)
    (with-logical-type (type :duckdb-varchar)
      (duckdb-table-function-set-name function "static_table")
      (duckdb-table-function-add-parameter function type)
      (duckdb-table-function-set-bind function (callback static-table-bind))
      (duckdb-table-function-set-init function (callback static-table-init))
      (duckdb-table-function-set-function function (callback static-table-function))
      (duckdb-register-table-function connection function))))
