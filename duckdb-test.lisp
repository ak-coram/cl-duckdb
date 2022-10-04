;;;; duckdb-test.lisp

(defpackage #:duckdb-test
  (:use #:cl #:fiveam))
(in-package #:duckdb-test)

(def-suite :duckdb)
(in-suite :duckdb)

(defun run-test-query (query test-fn)
  (ddb:with-open-database (db)
    (ddb:with-open-connection (conn db)
      (ddb:with-query (result conn query)
        (funcall test-fn (ddb:translate-result result))))))

(defun get-first-value (result key)
  (aref (alexandria:assoc-value result key :test #'string=) 0))

(test uuid-test
  (is (run-test-query
       "SELECT uuid AS a, uuid::text AS b FROM (SELECT gen_random_uuid() AS uuid)"
       (lambda (result)
         (is (uuid:uuid= (get-first-value result "a")
                         (uuid:make-uuid-from-string (get-first-value result "b"))))))))
