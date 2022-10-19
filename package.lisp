;;;; package.lisp

(defpackage #:duckdb-api
  (:use #:cl #:cffi)
  (:export #:get-ffi-type
           #:duckdb-result
           #:p-duckdb-result
           #:p-duckdb-column
           #:duckdb-database
           #:duckdb-connection
           #:duckdb-prepared-statement
           #:duckdb-appender
           #:duckdb-config
           #:duckdb-arrow
           #:duckdb-arrow-schema
           #:duckdb-arrow-array
           #:duckdb-open
           #:duckdb-open-ext
           #:duckdb-close
           #:duckdb-connect
           #:duckdb-disconnect
           #:duckdb-query
           #:duckdb-destroy-result
           #:duckdb-column-count
           #:duckdb-row-count
           #:duckdb-rows-changed
           #:duckdb-result-error
           #:duckdb-column-name
           #:duckdb-column-type
           #:duckdb-column-data
           #:duckdb-nullmask-data
           #:duckdb-p-result-chunk-count
           #:duckdb-vector-get-data
           #:duckdb-vector-get-validity
           #:duckdb-data-chunk-get-column-count
           #:duckdb-data-chunk-get-size
           #:duckdb-data-chunk-get-vector
           #:duckdb-prepared-statement
           #:duckdb-prepare-error
           #:duckdb-prepare
           #:duckdb-destroy-prepare
           #:duckdb-execute-prepared
           #:duckdb-nparams
           #:duckdb-param-type
           #:duckdb-clear-bindings
           #:duckdb-bind-null
           #:duckdb-bind-boolean
           #:duckdb-bind-varchar
           #:duckdb-bind-float
           #:duckdb-bind-double
           #:duckdb-bind-uint8
           #:duckdb-bind-uint16
           #:duckdb-bind-uint32
           #:duckdb-bind-uint64
           #:duckdb-bind-int8
           #:duckdb-bind-int16
           #:duckdb-bind-int32
           #:duckdb-bind-int64
           #:duckdb-bind-hugeint
           #:result-chunk-count
           #:result-get-chunk
           #:duckdb-validity-row-is-valid
           #:get-vector-type))

(defpackage #:duckdb
  (:nicknames #:ddb)
  (:use #:cl #:cffi)
  (:export #:open-database
           #:close-database
           #:connect
           #:disconnect
           #:prepare
           #:destroy-statement
           #:execute
           #:destroy-result
           #:with-open-database
           #:with-open-connection
           #:with-statement
           #:with-execute
           #:with-execute
           #:query))
