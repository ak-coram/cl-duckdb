;;;; duckdb-test.lisp

(defpackage #:duckdb-test
  (:use #:cl #:fiveam))
(in-package #:duckdb-test)

(def-suite :duckdb)
(in-suite :duckdb)

(defun get-first-value (result key)
  (aref (alexandria:assoc-value result key :test #'string=) 0))

(defmacro test-query (query vals &body body)
  (alexandria:with-gensyms (db conn result cols)
    `(ddb:with-open-database (,db)
       (ddb:with-open-connection (,conn ,db)
         (ddb:with-query (,result ,conn ,query)
           (let* ((,cols (ddb:translate-result ,result))
                  ,@(loop :for val :in vals
                          :collect `(,val
                                     (get-first-value ,cols
                                                      (str:downcase (quote ,val))))))
             ,@body))))))

(test query-null
  (test-query "SELECT NULL as null" (null)
    (is-false null)))

(test query-non-ascii-string
  (let ((s "Árvíztűrő tükörfúrógép"))
    (test-query (str:concat "SELECT '" s "' AS a, LENGTH('" s "') AS b") (a b)
      (is (string= s a))
      (is (eql (length s) b)))))

(test query-boolean
  (test-query "SELECT True AS a, False AS b" (a b)
    (is-true a)
    (is-false b)))

(test query-integers
  (test-query
      (str:concat "SELECT "
                  " -18446744073709551629::hugeint AS hugeint"
                  ", -12::tinyint AS tinyint"
                  ", -123::smallint AS smallint"
                  ", -1234::integer AS integer"
                  ", -12345::bigint AS bigint"
                  ", 12::utinyint AS utinyint"
                  ", 123::usmallint AS usmallint"
                  ", 1234::uinteger AS uinteger"
                  ", 12345::ubigint AS ubigint")
      (hugeint
       tinyint smallint integer bigint
       utinyint usmallint uinteger ubigint)
    (is (eql -18446744073709551629 hugeint))
    (is (eql -12 tinyint))
    (is (eql -123 smallint))
    (is (eql -1234 integer))
    (is (eql -12345 bigint))
    (is (eql 12 utinyint))
    (is (eql 123 usmallint))
    (is (eql 1234 uinteger))
    (is (eql 12345 ubigint))))

(test query-floats
  (test-query
      (str:concat "SELECT 3.14::float AS float"
                  ", 2.71::double AS double")
      (float double)
    (is (eql (float 3.14) float))
    (is (eql 2.71d0 double))))

(test query-blob
  (let ((s "바람 부는 대로, 물결 치는 대로"))
    (test-query
        (str:concat "SELECT ENCODE('" s "') AS blob")
        (blob)
      (is (string= s (babel:octets-to-string blob))))))

(test query-uuid
  (test-query
      (str:concat "SELECT uuid AS a, uuid::text AS b "
                  "FROM (SELECT gen_random_uuid() AS uuid)")
      (a b)
    (is (uuid:uuid= a (uuid:make-uuid-from-string b)))))
