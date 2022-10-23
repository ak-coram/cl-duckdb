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

(defcstruct (duckdb-timestamp :class duckdb-timestamp-type)
  (micros :int64))

(defmethod translate-from-foreign (value (type duckdb-timestamp-type))
  (with-foreign-slots ((micros) value (:struct duckdb-timestamp))
    (multiple-value-bind (secs remainder) (floor micros 1000000)
      (local-time:unix-to-timestamp secs :nsec (* remainder 1000)))))

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
    (:duckdb-uuid '(:struct duckdb-uuid))))

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

(defun result-get-chunk (p-result i)
  (duckdb-result-get-chunk (mem-ref p-result '(:struct duckdb-result)) i))

(defcfun duckdb-data-chunk-get-column-count (idx)
  (chunk duckdb-data-chunk))

(defcfun duckdb-data-chunk-get-vector (duckdb-vector)
  (chunk duckdb-data-chunk)
  (col-idx idx))

(defcfun duckdb-data-chunk-get-size (idx)
  (chunk duckdb-data-chunk))

(defcfun duckdb-vector-size (idx))

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

(defun get-vector-type (vector)
  (let* ((logical-type (duckdb-vector-get-column-type vector))
         (type (duckdb-get-type-id logical-type))
         (is-decimal (eql type :duckdb-decimal)))
    (values type
            (when is-decimal
              (duckdb-decimal-internal-type logical-type))
            (when is-decimal
              (duckdb-decimal-scale logical-type)))))

(defcfun duckdb-vector-get-data (:pointer :void)
  (vector duckdb-vector))

(defcfun duckdb-vector-get-validity p-validity
  (vector duckdb-vector))

(defcfun duckdb-vector-get-size idx
  (vector duckdb-vector))

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
