;;;; duckdb-static-table.lisp

(in-package :duckdb-api)

(defparameter *static-table-bindings* nil)
(defparameter *static-tablespace* (make-hash-table :test 'equal))

(defclass static-column ()
  ((name :initarg :name :accessor static-column-name)
   (values :initarg :values :accessor static-column-values)
   (value-count :initarg :value-count :accessor static-column-value-count)
   (length :initarg :length :accessor static-column-length)
   (type :initarg :type :accessor static-column-type)))

(defun add-table-reference (columns)
  (let ((table-id (format nil "~a" (uuid:make-v4-uuid))))
    (setf (gethash table-id *static-tablespace*)
          columns)
    table-id))

(defun clear-table-reference (table-id)
  (remhash table-id *static-tablespace*))

(defun get-table-columns (table-id)
  (gethash table-id *static-tablespace*))

(defun get-table-id (name)
  (alexandria:assoc-value *static-table-bindings* name :test #'string=))

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

(defun make-static-column (name values &key column-type length)
  (let* ((value-count (length values))
         (column-length (or length
                            value-count)))
    (make-instance 'static-column
                   :name name
                   :type (or column-type (static-vector-type values))
                   :values values
                   :value-count value-count
                   :length column-length)))

(defun make-static-columns (columns)
  (loop :for (column-name . column) :in columns
        :collect
        (cond
          ((vectorp column) (make-static-column column-name column))
          ((listp column) (apply #'make-static-column (cons column-name
                                                            column))))))

(defcstruct static-table-bind-data-struct
  (table-name :string)
  (table-uuid :string))

(defcallback free-ptr :void ((ptr :pointer))
  (foreign-free ptr))

(defcallback static-table-bind :void ((info duckdb-bind-info))
  (handler-case
      (let* ((bind-data (foreign-alloc '(:struct static-table-bind-data-struct))))
        (with-duckdb-value (table-name-param (duckdb-bind-get-parameter info 0))
          (let* ((name (duckdb-get-varchar table-name-param))
                 ;; Bind should run on the calling thread, so we
                 ;; should be able to retrieve the UUID for the table
                 ;; from the dynamic binding. This id is then used in
                 ;; init & the function stages to load table data from
                 ;; a global hash-table.
                 (table-id (get-table-id name)))
            (unless table-id
              (error (format nil "table ~s is undefined" name)))
            (loop :for column :in (get-table-columns table-id)
                  :for column-name := (static-column-name column)
                  :for duckdb-type := (static-column-type column)
                  :do (with-logical-type (type duckdb-type)
                        (duckdb-bind-add-result-column info column-name type)))
            (with-foreign-slots ((table-name table-uuid) bind-data
                                 (:struct static-table-bind-data-struct))
              (setf table-name name
                    table-uuid table-id))))
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
                 (loop :for i fixnum :below vector-size
                       :for k fixnum :from index
                       :until (>= k data-length)
                       :do (setf (mem-aref data-ptr
                                           ,(get-ffi-type duckdb-type) i)
                                 (aref vector k))
                       :finally (return i)))))))

(defmacro copy-static-list ()
  `(ecase duckdb-type
     ,@(loop
         :for (_ duckdb-type) :in (static-table-column-types)
         :collect
         `(,duckdb-type
           (loop :with validity-ptr
                   := (progn
                        (duckdb-vector-ensure-validity-writable vector)
                        (duckdb-vector-get-validity vector))
                 :for i fixnum :below vector-size
                 :for v :in (nthcdr index values)
                 :for is-null := (or (null v)
                                     (eql v :null))
                 :for validity-bit-index := (mod i 64)
                 :for validity-value :of-type (unsigned-byte 64)
                   := (if (zerop validity-bit-index)
                          #xffffffffffffffff
                          validity-value)
                 :if is-null
                   ;; set as invalid
                   :do (setf (ldb (byte 1 validity-bit-index)
                                  validity-value)
                             0)
                 :else :do (setf (mem-aref data-ptr
                                           ,(get-ffi-type duckdb-type) i)
                                 v)
                 :when (eql validity-bit-index 63)
                   :do (setf (mem-aref validity-ptr :uint64 (floor i 64))
                             validity-value)
                 :finally
                    (progn
                      (setf (mem-aref validity-ptr :uint64 (floor i 64))
                            validity-value)
                      (return i)))))))

(defcallback static-table-function :void ((info duckdb-bind-info)
                                          (output duckdb-data-chunk))
  (handler-case
      (let* ((vector-size (duckdb-vector-size))
             (bind-data (duckdb-function-get-bind-data info))
             (init-data (duckdb-function-get-init-data info))
             (table-id (foreign-slot-value bind-data
                                           '(:struct static-table-bind-data-struct)
                                           'table-uuid)))
        (with-foreign-slots ((index) init-data (:struct static-table-init-data-struct))
          (let ((n (loop :for column :in (get-table-columns table-id)
                         :for values := (static-column-values column)
                         :for column-index :from 0
                         :for duckdb-type := (static-column-type column)
                         :for ffi-type := (get-ffi-type duckdb-type)
                         :for vector := (duckdb-data-chunk-get-vector output column-index)
                         :for data-length := (static-column-value-count column)
                         :for data-ptr := (duckdb-vector-get-data vector)
                         :maximize (if (listp values)
                                       (copy-static-list)
                                       (copy-static-vector)))))
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
