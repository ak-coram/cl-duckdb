;;;; cl-duckdb.lisp

(in-package #:duckdb-api)

(define-foreign-library duckdb-lib
  (:unix "libduckdb.so")
  (t (:default "libduckdb")))

(use-foreign-library duckdb-lib)

(defctype duckdb-database (:pointer :void))
(defctype duckdb-connection (:pointer :void))
(defctype duckdb-prepared-statement (:pointer :void))
(defctype duckdb-appender (:pointer :void))
(defctype duckdb-config (:pointer :void))
(defctype duckdb-arrow (:pointer :void))
(defctype duckdb-arrow-schema (:pointer :void))
(defctype duckdb-arrow-array (:pointer :void))

(defcenum duckdb-state
  (:duckdb-success 0)
  (:duckdb-error 1))

(defctype idx :uint64)

(defcstruct (duckdb-hugeint :class duckdb-hugeint-type)
  (lower :uint64)
  (upper :int64))

(defmethod translate-from-foreign (value (type duckdb-hugeint-type))
  (with-foreign-slots ((lower upper) value (:struct duckdb-hugeint))
    (logior (ash upper 64) lower)))

(defcstruct (duckdb-blob :class duckdb-blob-type)
  (data (:pointer :void))
  (size idx))

(defmethod translate-from-foreign (value (type duckdb-blob-type))
  (with-foreign-slots ((data size) value (:struct duckdb-blob))
    (loop :with result := (make-array size :element-type '(unsigned-byte 8))
          :for i :below size
          :do (setf (aref result i) (mem-ref data :uint8 i))
          :finally (return result))))

(defcstruct (duckdb-date :class duckdb-date-type)
  (days :int32))

(defmethod translate-from-foreign (value (type duckdb-date-type))
  (with-foreign-slots ((days) value (:struct duckdb-date))
    (local-time:make-timestamp :day (- days 11017))))

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
    (periods:duration :months months :days days :microseconds micros)))

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
  (:duckdb-blob))

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
    (:duckdb-varchar :string)
    (:duckdb-hugeint '(:struct duckdb-hugeint))
    (:duckdb-blob '(:struct duckdb-blob))
    (:duckdb-date '(:struct duckdb-date))
    (:duckdb-time '(:struct duckdb-time))
    (:duckdb-timestamp '(:struct duckdb-timestamp))
    (:duckdb-interval '(:struct duckdb-interval))))

(defcstruct duckdb-column)

(defctype p-duckdb-column
  (:pointer (:struct duckdb-column)))

(defcstruct duckdb-result)

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
