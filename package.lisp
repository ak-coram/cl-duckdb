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
           #:result-chunk-count
           #:result-get-chunk
           #:duckdb-validity-row-is-valid
           #:get-vector-type))

(defpackage #:duckdb
  (:nicknames #:ddb)
  (:use #:cl #:cffi)
  (:export #:open-database
           #:close-database
           #:with-open-database
           #:connect
           #:disconnect
           #:with-open-connection
           #:with-statement
           #:with-execute
           #:query
           #:translate-result))
