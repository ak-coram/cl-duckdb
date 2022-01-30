;;;; duckdb.asd

(asdf:defsystem #:duckdb
  :description "CFFI wrapper around the DuckDB C API"
  :author "Ákos Kiss <ak@coram.pub>"
  :license  "MIT License"
  :version "0.0.1"
  :serial t
  :depends-on (:cffi)
  :components ((:file "package")
               (:file "duckdb-api")
               (:file "duckdb")))
