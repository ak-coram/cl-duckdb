;;;; package.lisp

(defpackage #:duckdb-api
  (:use #:cl #:cffi)
  (:export #:duckdb-column
           #:p-duckdb-column
           #:duckdb-result
           #:column-count
           #:row-count
           #:rows-changed
           #:columns
           #:error-message
           #:get-ffi-type
           #:p-duckdb-result
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
           #:duckdb-result-error
           #:duckdb-column-name
           #:duckdb-column-type
           #:duckdb-column-data
           #:duckdb-nullmask-data))

(defpackage #:duckdb
  (:nicknames #:ddb)
  (:use #:cl #:cffi)
  (:export #:open-database
           #:close-database
           #:with-open-database
           #:connect
           #:disconnect
           #:with-open-connection))
