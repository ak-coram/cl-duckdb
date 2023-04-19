;;;; duckdb-api.lisp

(in-package #:duckdb-api)

(define-foreign-library duckdb-lib
  (:darwin "libduckdb.dylib")
  (:unix "libduckdb.so")
  (:windows "duckdb.dll")
  (t (:default "libduckdb")))

(use-foreign-library duckdb-lib)

(defun get-message (p-string)
  (foreign-string-to-lisp (mem-ref p-string '(:pointer :string))))

(defctype duckdb-database (:pointer :void))
(defctype duckdb-connection (:pointer :void))
(defctype duckdb-prepared-statement (:pointer :void))
(defctype duckdb-appender (:pointer :void))
(defctype duckdb-config (:pointer :void))
(defctype duckdb-arrow (:pointer :void))
(defctype duckdb-arrow-schema (:pointer :void))
(defctype duckdb-arrow-array (:pointer :void))
(defctype duckdb-logical-type (:pointer :void))
(defctype duckdb-data-chunk (:pointer :void))
(defctype duckdb-vector (:pointer :void))
(defctype duckdb-value (:pointer :void))

(defctype duckdb-table-function (:pointer :void))
(defctype duckdb-bind-info (:pointer :void))
(defctype duckdb-init-info (:pointer :void))
(defctype duckdb-function-info (:pointer :void))

(defctype duckdb-replacement-scan-info (:pointer :void))

(defcenum duckdb-state
  (:duckdb-success 0)
  (:duckdb-error 1))

(defctype idx :uint64)
(defctype p-validity (:pointer :uint64))

(defvar *inline-length* 12)

(defcstruct (duckdb-string :class duckdb-string-type)
  (length :uint32)
  (data :uint8 :count 12))

(defun get-base-pointer (length data)
  (if (<= length *inline-length*)
      data
      (mem-ref data '(:pointer :uint8) 4)))

(defmethod translate-from-foreign (value (type duckdb-string-type))
  (with-foreign-slots ((length data) value (:struct duckdb-string))
    (foreign-string-to-lisp (get-base-pointer length data) :count length)))

(defcstruct (duckdb-hugeint :class duckdb-hugeint-type)
  (lower :uint64)
  (upper :int64))

(defmethod translate-from-foreign (value (type duckdb-hugeint-type))
  (with-foreign-slots ((lower upper) value (:struct duckdb-hugeint))
    (logior (ash upper 64) lower)))

(defmethod translate-into-foreign-memory
    (value (type duckdb-hugeint-type) ptr)
  (let ((multiplier (if (minusp value) -1 1)))
    (with-foreign-slots ((lower upper) ptr (:struct duckdb-hugeint))
      (setf lower (ldb (byte 64 0) value)
            upper (* multiplier (ldb (byte 64 64) value))))))

(defcstruct (duckdb-uuid :class duckdb-uuid-type)
  (lower :uint64)
  (upper :int64))

(defmethod translate-from-foreign (value (type duckdb-uuid-type))
  (with-foreign-slots ((lower upper) value (:struct duckdb-uuid))
    (let ((upper (logxor upper (ash 1 63))))
      ;; (format nil "~(~8,'0x-~4,'0x-~4,'0x-~4,'0x-~12,'0x~)"
      ;;             (ldb (byte 32 32) upper)
      ;;             (ldb (byte 16 16) upper)
      ;;             (ldb (byte 16 0) upper)
      ;;             (ldb (byte 16 48) lower)
      ;;             (ldb (byte 48 0) lower))
      (make-instance 'uuid:uuid
                     :time-low (ldb (byte 32 32) upper)
                     :time-mid (ldb (byte 16 16) upper)
                     :time-high (ldb (byte 16 0) upper)
                     :clock-seq-var (ldb (byte 8 56) lower)
                     :clock-seq-low (ldb (byte 8 48) lower)
                     :node (ldb (byte 48 0) lower)))))

(defcstruct (duckdb-blob :class duckdb-blob-type)
  (length :uint32)
  (data :uint8 :count 12))

(defmethod translate-from-foreign (value (type duckdb-blob-type))
  (with-foreign-slots ((length data) value (:struct duckdb-blob))
    (let ((p-base (get-base-pointer length data)))
      (loop :with result := (make-array length :element-type '(unsigned-byte 8))
            :for i :below length
            :do (setf (aref result i) (mem-ref p-base :uint8 i))
            :finally (return result)))))

(defcstruct (duckdb-date :class duckdb-date-type)
  (days :int32))

(defmethod translate-from-foreign (value (type duckdb-date-type))
  (with-foreign-slots ((days) value (:struct duckdb-date))
    (local-time:make-timestamp :day (- days 11017))))

(defmethod translate-into-foreign-memory
    (value (type duckdb-date-type) ptr)
  (with-foreign-slots ((days) ptr (:struct duckdb-date))
    (setf days (+ (local-time:day-of value) 11017))))

(defcstruct (duckdb-time :class duckdb-time-type)
  (micros :int64))

(defmethod translate-from-foreign (value (type duckdb-time-type))
  (with-foreign-slots ((micros) value (:struct duckdb-time))
    (local-time-duration:duration :nsec (* micros 1000))))

(defmethod translate-into-foreign-memory
    (value (type duckdb-time-type) ptr)
  (with-foreign-slots ((micros) ptr (:struct duckdb-time))
    (setf micros (round (local-time-duration:duration-as value :nsec)
                        1000))))

(defcstruct (duckdb-timestamp :class duckdb-timestamp-type)
  (micros :int64))

(defmethod translate-from-foreign (value (type duckdb-timestamp-type))
  (with-foreign-slots ((micros) value (:struct duckdb-timestamp))
    (multiple-value-bind (secs remainder) (floor micros 1000000)
      (local-time:unix-to-timestamp secs :nsec (* remainder 1000)))))

(defmethod translate-into-foreign-memory
    (value (type duckdb-timestamp-type) ptr)
  (with-foreign-slots ((micros) ptr (:struct duckdb-timestamp))
    (setf micros (+ (* 1000000 (+ (* (+ (local-time:day-of value) 11017)
                                     24 60 60)
                                  (local-time:sec-of value)))
                    (round (local-time:nsec-of value) 1000)))))

(defcstruct (duckdb-interval :class duckdb-interval-type)
  (months :int32)
  (days :int32)
  (micros :int64))

(defmethod translate-from-foreign (value (type duckdb-interval-type))
  (with-foreign-slots ((months days micros) value (:struct duckdb-interval))
    (serapeum:mvlet* ((years months (floor months 12))
                      (milliseconds microseconds (floor micros 1000))
                      (seconds milliseconds (floor milliseconds 1000))
                      (minutes seconds (floor seconds 60))
                      (hours minutes (floor minutes 60)))
      (periods:duration :years years
                        :months months
                        :days days
                        :hours hours
                        :minutes minutes
                        :seconds seconds
                        :milliseconds milliseconds
                        :microseconds microseconds))))

(defcstruct (duckdb-list :class duckdb-list-type)
  (offset idx)
  (length idx))

(defmethod translate-from-foreign (value (type duckdb-list-type))
  (with-foreign-slots ((offset length) value (:struct duckdb-list))
    (list offset length)))

(defcenum duckdb-type
  (:duckdb-invalid-type 0)
  (:duckdb-boolean)
  (:duckdb-tinyint)
  (:duckdb-smallint)
  (:duckdb-integer)
  (:duckdb-bigint)
  (:duckdb-utinyint)
  (:duckdb-usmallint)
  (:duckdb-uinteger)
  (:duckdb-ubigint)
  (:duckdb-float)
  (:duckdb-double)
  (:duckdb-timestamp)
  (:duckdb-date)
  (:duckdb-time)
  (:duckdb-interval)
  (:duckdb-hugeint)
  (:duckdb-varchar)
  (:duckdb-blob)
  (:duckdb-decimal)
  (:duckdb-timestamp-s)
  (:duckdb-timestamp-ms)
  (:duckdb-timestamp-ns)
  (:duckdb-enum)
  (:duckdb-list)
  (:duckdb-struct)
  (:duckdb-map)
  (:duckdb-uuid)
  (:duckdb-json))

(defun get-ffi-type (duckdb-type)
  (ecase duckdb-type
    (:duckdb-boolean :bool)
    (:duckdb-tinyint :int8)
    (:duckdb-smallint :int16)
    (:duckdb-integer :int32)
    (:duckdb-bigint :int64)
    (:duckdb-utinyint :uint8)
    (:duckdb-usmallint :uint16)
    (:duckdb-uinteger :uint32)
    (:duckdb-ubigint :uint64)
    (:duckdb-float :float)
    (:duckdb-double :double)
    (:duckdb-varchar '(:struct duckdb-string))
    (:duckdb-hugeint '(:struct duckdb-hugeint))
    (:duckdb-blob '(:struct duckdb-blob))
    (:duckdb-date '(:struct duckdb-date))
    (:duckdb-time '(:struct duckdb-time))
    (:duckdb-timestamp '(:struct duckdb-timestamp))
    (:duckdb-timestamp-s '(:struct duckdb-timestamp))
    (:duckdb-timestamp-ms '(:struct duckdb-timestamp))
    (:duckdb-timestamp-ns '(:struct duckdb-timestamp))
    (:duckdb-interval '(:struct duckdb-interval))
    (:duckdb-uuid '(:struct duckdb-uuid))
    (:duckdb-list '(:struct duckdb-list))
    (:duckdb-struct :void)
    (:duckdb-map '(:struct duckdb-list))))

(defcstruct duckdb-column)

(defctype p-duckdb-column
    (:pointer (:struct duckdb-column)))

(defcstruct (duckdb-result)
  (__deprecated-column-count idx)
  (__deprecated-row-count idx)
  (__deprecated-rows-changed idx)
  (__deprecated-deprecated-columns p-duckdb-column)
  (__deprecated-error-message (:pointer :string))
  (internal-data (:pointer :void)))

(defctype p-duckdb-result
    (:pointer (:struct duckdb-result)))

(defcfun duckdb-open duckdb-state
  (path :string)
  (out-database (:pointer duckdb-database)))

(defcfun duckdb-open-ext duckdb-state
  (path :string)
  (out-database (:pointer duckdb-database))
  (config duckdb-config)
  (out-error (:pointer :string)))

(defcfun duckdb-close :void
  (database (:pointer duckdb-database)))

(defcfun duckdb-connect duckdb-state
  (database duckdb-database)
  (out-connection (:pointer duckdb-connection)))

(defcfun duckdb-disconnect :void
  (connection (:pointer duckdb-connection)))

(defcfun duckdb-query duckdb-state
  (connection duckdb-connection)
  (query :string)
  (out-result (:pointer (:struct duckdb-result))))

(defcfun duckdb-destroy-result :void
  (result p-duckdb-result))

(defcfun duckdb-column-count idx
  (result p-duckdb-result))

(defcfun duckdb-row-count idx
  (result p-duckdb-result))

(defcfun duckdb-rows-changed idx
  (result p-duckdb-result))

(defcfun duckdb-result-error :string
  (result p-duckdb-result))

(defcfun duckdb-column-name :string
  (result p-duckdb-result)
  (col idx))

(defcfun duckdb-column-type duckdb-type
  (result p-duckdb-result)
  (col idx))

(defcfun duckdb-column-data (:pointer :void)
  (result p-duckdb-result)
  (col idx))

(defcfun duckdb-nullmask-data (:pointer :bool)
  (result p-duckdb-result)
  (col idx))

(defcfun duckdb-result-chunk-count idx
  (result (:struct duckdb-result)))

(defun result-chunk-count (p-result)
  (duckdb-result-chunk-count (mem-ref p-result '(:struct duckdb-result))))

(defcfun duckdb-result-get-chunk (duckdb-data-chunk)
  (result (:struct duckdb-result))
  (chunk-index idx))

(defcfun duckdb-destroy-data-chunk :void
  (chunk (:pointer duckdb-data-chunk)))

(defmacro with-data-chunk ((chunk-var result chunk-index) &body body)
  `(let ((,chunk-var (duckdb-result-get-chunk
                      (mem-ref ,result '(:struct duckdb-result))
                      ,chunk-index)))
     (unwind-protect
          (progn ,@body)
       (with-foreign-object (p-chunk '(:pointer duckdb-data-chunk))
         (setf (mem-ref p-chunk 'duckdb-data-chunk) ,chunk-var)
         (duckdb-destroy-data-chunk p-chunk)))))

(defun result-get-chunk (p-result i)
  (duckdb-result-get-chunk (mem-ref p-result '(:struct duckdb-result)) i))

(defcfun duckdb-data-chunk-get-column-count (idx)
  (chunk duckdb-data-chunk))

(defcfun duckdb-data-chunk-get-vector (duckdb-vector)
  (chunk duckdb-data-chunk)
  (col-idx idx))

(defcfun duckdb-data-chunk-get-size (idx)
  (chunk duckdb-data-chunk))

(defcfun duckdb-data-chunk-set-size :void
  (chunk duckdb-data-chunk)
  (size idx))

(defcfun duckdb-vector-size idx)

(defcfun duckdb-vector-get-column-type (duckdb-logical-type)
  (vector duckdb-vector))

(defcfun duckdb-get-type-id (duckdb-type)
  (type duckdb-logical-type))

(defcfun duckdb-decimal-internal-type duckdb-type
  (type duckdb-logical-type))

(defcfun duckdb-decimal-width :uint8
  (type duckdb-logical-type))

(defcfun duckdb-decimal-scale :uint8
  (type duckdb-logical-type))

(defcfun duckdb-enum-internal-type duckdb-type
  (type duckdb-logical-type))

(defcfun duckdb-enum-dictionary-size :uint32
  (type duckdb-logical-type))

(defcfun duckdb-enum-dictionary-value (:pointer :char)
  (type duckdb-logical-type)
  (index idx))

(defcfun duckdb-free :void
  (ptr (:pointer :void)))

(defmacro with-free ((var alloc-form) &body body)
  `(let ((,var ,alloc-form))
     (unwind-protect (progn ,@body)
       (duckdb-free ,var))))

(defun get-enum-alist (logical-type)
  (let ((enum-size (duckdb-enum-dictionary-size logical-type)))
    (loop :for index :below enum-size
          :for ptr := (duckdb-enum-dictionary-value logical-type index)
          :collect (unwind-protect `(,index . ,(foreign-string-to-lisp ptr))
                     (duckdb-free ptr)))))

(defmacro with-logical-type ((logical-type-var alloc-form) &body body)
  `(let ((,logical-type-var ,alloc-form))
     (unwind-protect (progn ,@body)
       (with-foreign-object (p-type '(:pointer duckdb-logical-type))
         (setf (mem-ref p-type 'duckdb-logical-type) ,logical-type-var)
         (duckdb-destroy-logical-type p-type)))))

(defun resolve-logical-type (logical-type)
  (let ((type (duckdb-get-type-id logical-type)))
    (case type
      (:duckdb-decimal (list type
                             (duckdb-decimal-internal-type logical-type)
                             (duckdb-decimal-scale logical-type)))
      (:duckdb-enum (list type
                          (duckdb-enum-internal-type logical-type)
                          (get-enum-alist logical-type)))
      (:duckdb-list
       (with-logical-type (child-type (duckdb-list-type-child-type logical-type))
         (list type
               nil
               (resolve-logical-type child-type))))
      (:duckdb-struct
       (loop :for i :below (duckdb-struct-type-child-count logical-type)
             :collect
             (with-logical-type (field-type (duckdb-struct-type-child-type logical-type i))
               (with-free (p-field-name (duckdb-struct-type-child-name logical-type i))
                 (cons (foreign-string-to-lisp p-field-name)
                       (resolve-logical-type field-type))))
               :into fields
             :finally (return (list type nil fields))))
      (:duckdb-map
       (list type
             nil
             (with-logical-type (key-type (duckdb-map-type-key-type logical-type))
               (with-logical-type (value-type (duckdb-map-type-value-type logical-type))
                 (list (resolve-logical-type key-type)
                       (resolve-logical-type value-type))))))
      (t (list type nil nil)))))

(defun get-vector-type (vector)
  (with-logical-type (logical-type (duckdb-vector-get-column-type vector))
    (resolve-logical-type logical-type)))

(defcfun duckdb-vector-get-data (:pointer :void)
  (vector duckdb-vector))

(defcfun duckdb-vector-get-validity p-validity
  (vector duckdb-vector))

(defcfun duckdb-vector-ensure-validity-writable :void
  (vector duckdb-vector))

(defcfun duckdb-vector-assign-string-element :void
  (vector duckdb-vector)
  (index idx)
  (str :string))

(defcfun duckdb-validity-row-is-valid :bool
  (validity p-validity)
  (row idx))

(defcfun duckdb-prepare-error :string
  (statement duckdb-prepared-statement))

(defcfun duckdb-prepare duckdb-state
  (connection duckdb-connection)
  (query :string)
  (out-prepared-statement duckdb-prepared-statement))

(defcfun duckdb-destroy-prepare :void
  (prepared-statement (:pointer duckdb-prepared-statement)))

(defcfun duckdb-execute-prepared duckdb-state
  (prepared-statement duckdb-prepared-statement)
  (out-result p-duckdb-result))

(defcfun duckdb-nparams idx
  (prepared-statement duckdb-prepared-statement))

(defcfun duckdb-param-type duckdb-type
  (prepared-statement duckdb-prepared-statement)
  (param-idx idx))

(defcfun duckdb-clear-bindings duckdb-state
  (prepared-statement duckdb-prepared-statement))

(defcfun duckdb-bind-null duckdb-state
  (prepared-statement duckdb-prepared-statement)
  (param-idx idx))

(defcfun duckdb-bind-boolean duckdb-state
  (prepared-statement duckdb-prepared-statement)
  (param-idx idx)
  (val :bool))

(defcfun duckdb-bind-varchar duckdb-state
  (prepared-statement duckdb-prepared-statement)
  (param-idx idx)
  (val :string))

(defcfun duckdb-bind-blob duckdb-state
  (prepared-statement duckdb-prepared-statement)
  (param-idx idx)
  (data (:pointer :void))
  (length idx))

(defcfun duckdb-bind-float duckdb-state
  (prepared-statement duckdb-prepared-statement)
  (param-idx idx)
  (val :float))

(defcfun duckdb-bind-double duckdb-state
  (prepared-statement duckdb-prepared-statement)
  (param-idx idx)
  (val :double))

(defcfun duckdb-bind-uint8 duckdb-state
  (prepared-statement duckdb-prepared-statement)
  (param-idx idx)
  (val :uint8))

(defcfun duckdb-bind-uint16 duckdb-state
  (prepared-statement duckdb-prepared-statement)
  (param-idx idx)
  (val :uint16))

(defcfun duckdb-bind-uint32 duckdb-state
  (prepared-statement duckdb-prepared-statement)
  (param-idx idx)
  (val :uint32))

(defcfun duckdb-bind-uint64 duckdb-state
  (prepared-statement duckdb-prepared-statement)
  (param-idx idx)
  (val :uint64))

(defcfun duckdb-bind-int8 duckdb-state
  (prepared-statement duckdb-prepared-statement)
  (param-idx idx)
  (val :int8))

(defcfun duckdb-bind-int16 duckdb-state
  (prepared-statement duckdb-prepared-statement)
  (param-idx idx)
  (val :int16))

(defcfun duckdb-bind-int32 duckdb-state
  (prepared-statement duckdb-prepared-statement)
  (param-idx idx)
  (val :int32))

(defcfun duckdb-bind-int64 duckdb-state
  (prepared-statement duckdb-prepared-statement)
  (param-idx idx)
  (val :int64))

(defcfun duckdb-bind-hugeint :void ; TODO: duckdb-state doesn't work
  (prepared-statement duckdb-prepared-statement)
  (param-idx idx)
  (val (:struct duckdb-hugeint)))

(defcfun duckdb-bind-date :void
  (prepared-statement duckdb-prepared-statement)
  (param-idx idx)
  (val (:struct duckdb-date)))

(defcfun duckdb-bind-timestamp :void
  (prepared-statement duckdb-prepared-statement)
  (param-idx idx)
  (val (:struct duckdb-timestamp)))

(defcfun duckdb-bind-time :void
  (prepared-statement duckdb-prepared-statement)
  (param-idx idx)
  (val (:struct duckdb-time)))

(defcfun duckdb-appender-create duckdb-state
  (connection duckdb-connection)
  (schema :string)
  (table :string)
  (out-appender duckdb-appender))

(defcfun duckdb-appender-error :string
  (appender duckdb-appender))

(defcfun duckdb-appender-flush duckdb-state
  (appender duckdb-appender))

(defcfun duckdb-appender-close duckdb-state
  (appender duckdb-appender))

(defcfun duckdb-appender-destroy duckdb-state
  (appender (:pointer duckdb-appender)))

(defcfun duckdb-appender-begin-row duckdb-state
  (appender duckdb-appender))

(defcfun duckdb-appender-end-row duckdb-state
  (appender duckdb-appender))

(defcfun duckdb-append-null duckdb-state
  (appender duckdb-appender))

(defcfun duckdb-append-bool duckdb-state
  (appender duckdb-appender)
  (val :bool))

(defcfun duckdb-append-varchar duckdb-state
  (appender duckdb-appender)
  (val :string))

(defcfun duckdb-append-blob duckdb-state
  (appender duckdb-appender)
  (data (:pointer :void))
  (length idx))

(defcfun duckdb-append-float duckdb-state
  (appender duckdb-appender)
  (val :float))

(defcfun duckdb-append-double duckdb-state
  (appender duckdb-appender)
  (val :double))

(defcfun duckdb-append-uint8 duckdb-state
  (appender duckdb-appender)
  (val :uint8))

(defcfun duckdb-append-uint16 duckdb-state
  (appender duckdb-appender)
  (val :uint16))

(defcfun duckdb-append-uint32 duckdb-state
  (appender duckdb-appender)
  (val :uint32))

(defcfun duckdb-append-uint64 duckdb-state
  (appender duckdb-appender)
  (val :uint64))

(defcfun duckdb-append-int8 duckdb-state
  (appender duckdb-appender)
  (val :int8))

(defcfun duckdb-append-int16 duckdb-state
  (appender duckdb-appender)
  (val :int16))

(defcfun duckdb-append-int32 duckdb-state
  (appender duckdb-appender)
  (val :int32))

(defcfun duckdb-append-int64 duckdb-state
  (appender duckdb-appender)
  (val :int64))

(defcfun duckdb-append-hugeint :void ; TODO: duckdb-state doesn't work
  (appender duckdb-appender)
  (val (:struct duckdb-hugeint)))

(defcfun duckdb-append-date :void
  (appender duckdb-appender)
  (val (:struct duckdb-date)))

(defcfun duckdb-append-timestamp :void
  (appender duckdb-appender)
  (val (:struct duckdb-timestamp)))

(defcfun duckdb-append-time :void
  (appender duckdb-appender)
  (val (:struct duckdb-time)))

(defcfun duckdb-create-logical-type duckdb-logical-type
  (type duckdb-type))

(defcfun duckdb-destroy-logical-type duckdb-logical-type
  (type (:pointer duckdb-type)))

(defcfun duckdb-list-type-child-type duckdb-logical-type
  (type duckdb-logical-type))

(defcfun duckdb-list-vector-get-child duckdb-vector
  (vector duckdb-vector))

(defcfun duckdb-map-type-key-type duckdb-logical-type
  (type duckdb-logical-type))

(defcfun duckdb-map-type-value-type duckdb-logical-type
  (type duckdb-logical-type))

(defcfun duckdb-struct-type-child-count idx
  (type duckdb-logical-type))

(defcfun duckdb-struct-type-child-name (:pointer :char)
  (type duckdb-logical-type)
  (index idx))

(defcfun duckdb-struct-type-child-type duckdb-logical-type
  (type duckdb-logical-type)
  (index idx))

(defcfun duckdb-struct-vector-get-child duckdb-vector
  (vector duckdb-vector)
  (index idx))

(defcfun duckdb-get-int64 :int64
  (value duckdb-value))

(defcfun duckdb-get-varchar :string
  (value duckdb-value))

(defcfun duckdb-create-varchar duckdb-value
  (value :string))

(defcfun duckdb-destroy-value :void
  (value (:pointer duckdb-value)))

(defcfun duckdb-create-table-function duckdb-table-function)

(defcfun duckdb-destroy-table-function :void
  (table-function (:pointer duckdb-table-function)))

(defcfun duckdb-table-function-set-name :void
  (table-function duckdb-table-function)
  (name :string))

(defcfun duckdb-table-function-add-parameter :void
  (table-function duckdb-table-function)
  (type duckdb-logical-type))

(defcfun duckdb-table-function-set-extra-info :void
  (table-function duckdb-table-function)
  (extra-info (:pointer :void))
  (destroy :pointer))

(defcfun duckdb-table-function-set-bind :void
  (table-function duckdb-table-function)
  (bind :pointer))

(defcfun duckdb-table-function-set-init :void
  (table-function duckdb-table-function)
  (init :pointer))

(defcfun duckdb-table-function-set-local-init :void
  (table-function duckdb-table-function)
  (init :pointer))

(defcfun duckdb-table-function-set-function :void
  (table-function duckdb-table-function)
  (function :pointer))

(defcfun duckdb-table-function-supports-projection-pushdown :void
  (table-function duckdb-table-function)
  (pushdown :bool))

(defcfun duckdb-register-table-function duckdb-state
  (connection duckdb-connection)
  (function duckdb-table-function))

(defcfun duckdb-bind-add-result-column :void
  (info duckdb-bind-info)
  (name :string)
  (type duckdb-logical-type))

(defcfun duckdb-bind-get-parameter-count idx
  (info duckdb-bind-info))

(defcfun duckdb-bind-get-parameter duckdb-value
  (info duckdb-bind-info)
  (index idx))

(defcfun duckdb-bind-set-bind-data :void
  (info duckdb-bind-info)
  (bind-data (:pointer :void))
  (destroy :pointer))

(defcfun duckdb-bind-set-cardinality :void
  (info duckdb-bind-info)
  (cardinality idx)
  (is-exact :bool))

(defcfun duckdb-bind-set-error :void
  (info duckdb-bind-info)
  (error :string))

(defcfun duckdb-init-get-extra-info (:pointer :void)
  (info duckdb-bind-info))

(defcfun duckdb-init-get-bind-data (:pointer :void)
  (info duckdb-bind-info))

(defcfun duckdb-init-set-init-data (:pointer :void)
  (info duckdb-bind-info)
  (init-data (:pointer :void))
  (destroy :pointer))

(defcfun duckdb-init-get-column-count idx
  (info duckdb-bind-info))

(defcfun duckdb-init-get-column-index idx
  (info duckdb-bind-info)
  (column-index idx))

(defcfun duckdb-init-set-max-threads :void
  (info duckdb-bind-info)
  (max-threads idx))

(defcfun duckdb-init-set-error :void
  (info duckdb-bind-info)
  (error :string))

(defcfun duckdb-function-get-extra-info (:pointer :void)
  (info duckdb-bind-info))

(defcfun duckdb-function-get-bind-data (:pointer :void)
  (info duckdb-bind-info))

(defcfun duckdb-function-get-init-data (:pointer :void)
  (info duckdb-bind-info))

(defcfun duckdb-function-get-local-init-data (:pointer :void)
  (info duckdb-bind-info))

(defcfun duckdb-function-set-error :void
  (info duckdb-bind-info)
  (error :string))

(defmacro with-duckdb-value ((value-var alloc-form) &body body)
  `(let ((,value-var ,alloc-form))
     (unwind-protect
          (progn ,@body)
       (with-foreign-object (p-value '(:pointer duckdb-value))
         (setf (mem-ref p-value 'duckdb-value) ,value-var)
         (duckdb-destroy-value p-value)))))

(defmacro with-table-function ((function-var) &body body)
  `(let ((,function-var (duckdb-create-table-function)))
     (unwind-protect
          (progn ,@body)
       (with-foreign-object (p-function '(:pointer duckdb-table-function))
         (setf (mem-ref p-function 'duckdb-table-function) ,function-var)
         (duckdb-destroy-table-function p-function)))))

(defcfun duckdb-add-replacement-scan :void
  (db duckdb-database)
  (replacement :pointer)
  (extra-data (:pointer :void))
  (delete-callback :pointer))

(defcfun duckdb-replacement-scan-set-function-name :void
  (info duckdb-replacement-scan-info)
  (function-name :string))

(defcfun duckdb-replacement-scan-add-parameter :void
  (info duckdb-replacement-scan-info)
  (parameter duckdb-value))

(defcfun duckdb-replacement-scan-set-error :void
  (info duckdb-replacement-scan-info)
  (error :string))

;;; Configuration

(defcfun duckdb-create-config duckdb-state
  (out-config (:pointer duckdb-config)))

(defcfun duckdb-set-config duckdb-state
  (config duckdb-config)
  (name :string)
  (option :string))

(defcfun duckdb-destroy-config :void
  (config (:pointer duckdb-config)))

(defun create-config ()
  (with-foreign-object (p-config 'duckdb-config)
    (duckdb-create-config p-config)
    (mem-ref p-config 'duckdb-config)))

(defun destroy-config (config)
  (with-foreign-object (p-config 'duckdb-config)
    (setf (mem-ref p-config 'duckdb-config) config)
    (duckdb-destroy-config p-config)))

(defmacro with-config ((config-var config-plist) &body body)
  (alexandria:with-gensyms (name option)
    `(let ((,config-var (create-config)))
       (unwind-protect
            (progn
              (alexandria:doplist (,name ,option ,config-plist)
                (duckdb-set-config ,config-var
                                   ,name
                                   (format nil "~a" ,option)))
              ,@body)
         (destroy-config ,config-var)))))

;;; Threads

(defctype duckdb-task-state (:pointer :void))

(defcfun duckdb-execute-tasks :void
  (database duckdb-database)
  (max-tasks idx))

(defcfun duckdb-create-task-state duckdb-task-state
  (database duckdb-database))

(defcfun duckdb-execute-tasks-state :void
  (state duckdb-task-state))

(defcfun duckdb-execute-n-tasks-state idx
  (state duckdb-task-state)
  (max-tasks idx))

(defcfun duckdb-finish-execution :void
  (state duckdb-task-state))

(defcfun duckdb-task-state-is-finished :bool
  (state duckdb-task-state))

(defcfun duckdb-destroy-task-state :void
  (state duckdb-task-state))

(defclass worker-pool ()
  ((task-state :initarg :task-state :accessor task-state)
   (worker-threads :initarg :worker-threads :accessor worker-threads)))

(defun start-worker-pool (database n)
  (let* ((task-state (duckdb-create-task-state database))
         (worker-threads
           (loop :for i :below n
                 :collect (bt:make-thread
                           (lambda ()
                             (duckdb-execute-tasks-state task-state))
                           :name (format nil "DuckDB worker thread #~a" i)))))
    (make-instance 'worker-pool :task-state task-state :worker-threads worker-threads)))

(defun stop-worker-pool (pool)
  (when pool
    (let ((task-state (task-state pool))
          (worker-threads (worker-threads pool)))
      (duckdb-finish-execution task-state)
      (loop :for worker-thread :in worker-threads
            :when (bt:thread-alive-p worker-thread)
              :do (bt:join-thread worker-thread)
            :finally (duckdb-destroy-task-state task-state)))))

