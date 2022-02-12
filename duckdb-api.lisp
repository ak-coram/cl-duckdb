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
    (:duckdb-ubigint :uint64)))

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
