;;;; package.lisp

(defpackage #:duckdb-api
  (:use #:cl #:cffi)
  (:export #:get-message
           #:get-ffi-type
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
           #:duckdb-bind-blob
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
           #:duckdb-bind-date
           #:duckdb-bind-timestamp
           #:duckdb-bind-time
           #:result-chunk-count
           #:with-data-chunk
           #:duckdb-validity-row-is-valid
           #:get-vector-type
           #:duckdb-appender-create
           #:duckdb-appender-error
           #:duckdb-appender-flush
           #:duckdb-appender-close
           #:duckdb-appender-destroy
           #:duckdb-appender-begin-row
           #:duckdb-appender-end-row
           #:duckdb-append-null
           #:duckdb-append-bool
           #:duckdb-append-varchar
           #:duckdb-append-blob
           #:duckdb-append-float
           #:duckdb-append-double
           #:duckdb-append-uint8
           #:duckdb-append-uint16
           #:duckdb-append-uint32
           #:duckdb-append-uint64
           #:duckdb-append-int8
           #:duckdb-append-int16
           #:duckdb-append-int32
           #:duckdb-append-int64
           #:duckdb-append-hugeint
           #:duckdb-append-date
           #:duckdb-append-timestamp
           #:duckdb-append-time
           #:*static-table-bindings*
           #:make-static-columns
           #:add-global-table-reference
           #:clear-global-table-reference
           #:clear-global-table-references
           #:add-table-reference
           #:clear-table-reference
           #:clear-table-references
           #:register-static-table-function
           #:add-static-table-replacement-scan
           #:with-config
           #:start-worker-pool
           #:stop-worker-pool))

(defpackage #:duckdb
  (:nicknames #:ddb)
  (:use #:cl #:cffi)
  (:export #:*connection*
           #:open-database
           #:close-database
           #:connect
           #:disconnect
           #:prepare
           #:destroy-statement
           #:execute
           #:perform
           #:destroy-result
           #:with-open-database
           #:with-open-connection
           #:with-statement
           #:with-execute
           #:translate-result
           #:bind-parameters
           #:initialize-default-connection
           #:disconnect-default-connection
           #:with-default-connection
           #:with-transient-connection
           #:query
           #:format-query
           #:spark-query
           #:vspark-query
           #:run
           #:get-result
           #:quote-identifier
           #:with-appender
           #:append-row
           #:with-static-table
           #:with-static-tables
           #:bind-static-table
           #:unbind-static-table
           #:clear-static-tables))
