;;;; duckdb.asd

(asdf:defsystem #:duckdb
  :description "CFFI wrapper around the DuckDB C API"
  :author "Ákos Kiss <ak@coram.pub>"
  :license  "MIT License"
  :serial t
  :depends-on (#:bordeaux-threads
               #:cffi
               ;; Don't load libffi on ECL without DFFI
               (:feature (:or (:not :ecl) :dffi)
                         #:cffi-libffi)
               #:cl-ascii-table
               #:cl-spark
               #:cl-ppcre
               #:let-plus
               #:local-time
               #:local-time-duration
               #:periods
               #:frugal-uuid
               #:float-features
               #:uiop)
  :components ((:file "duckdb-ecl-sffi")
               (:file "package")
               (:file "duckdb-misc")
               (:file "duckdb-api")
               (:file "duckdb-static-table")
               (:file "duckdb"))
  :in-order-to ((test-op (test-op "duckdb/test"))))

(asdf:defsystem #:duckdb/test
  :depends-on (#:duckdb
               #:fiveam)
  :components ((:file "duckdb-test"))
  :perform (test-op (o c) (symbol-call :fiveam '#:run! :duckdb)))

(asdf:defsystem #:duckdb/benchmark
  :depends-on (#:duckdb
               #:trivial-benchmark)
  :components ((:file "duckdb-benchmark"))
  :perform (test-op
            (o c)
            (symbol-call :duckdb/benchmark '#:run-benchmarks)))

(asdf:defsystem #:duckdb/*
  :depends-on (#:duckdb
               #:duckdb/test
               #:duckdb/benchmark))
