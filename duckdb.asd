;;;; duckdb.asd

(asdf:defsystem #:duckdb
  :description "CFFI wrapper around the DuckDB C API"
  :author "Ákos Kiss <ak@coram.pub>"
  :license  "MIT License"
  :serial t
  :depends-on (#:cffi
               #:cffi-libffi
               #:local-time
               #:local-time-duration
               #:periods
               #:uuid
               #:uiop)
  :components ((:file "package")
               (:file "duckdb-api")
               (:file "duckdb"))
  :in-order-to ((test-op (test-op "duckdb/test"))))

(asdf:defsystem #:duckdb/test
  :depends-on (#:duckdb
               #:fiveam
               #:str)
  :components ((:file "duckdb-test"))
  :perform (test-op (o c) (symbol-call :fiveam '#:run! :duckdb)))
