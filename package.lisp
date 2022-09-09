;;;; package.lisp

(defpackage #:duckdb-api
  (:use #:cl #:cffi)
  (:export #:get-ffi-type
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
           #:duckdb-p-result-chunk-count))

(defpackage #:duckdb
  (:nicknames #:ddb)
  (:use #:cl #:cffi)
  (:export #:open-database
           #:close-database
           #:with-open-database
           #:connect
           #:disconnect
           #:with-open-connection))
