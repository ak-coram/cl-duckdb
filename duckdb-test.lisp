;;;; duckdb-test.lisp

(defpackage #:duckdb-test
  (:use #:cl #:fiveam))
(in-package #:duckdb-test)

(def-suite :duckdb)
(in-suite :duckdb)

(defun get-first-value (result key)
  (aref (alexandria:assoc-value result key :test #'string=) 0))

(defmacro test-query (query parameters result-syms &body body)
  (alexandria:with-gensyms (db conn result)
    `(ddb:with-open-database (,db)
       (ddb:with-open-connection (,conn ,db)
         (let* ((,result (ddb:query ,conn ,query ,@parameters))
                ,@(loop :for sym :in result-syms
                        :collect
                        `(,sym (get-first-value ,result
                                                (str:downcase (quote ,sym))))))
           ,@body)))))

(test query-null
  (test-query "SELECT NULL as null" nil
      (null)
    (is-false null)))

(test query-non-ascii-string
  (let ((s "Árvíztűrő tükörfúrógép"))
    (test-query (str:concat "SELECT '" s "' AS a, LENGTH('" s "') AS b") nil
        (a b)
      (is (string= s a))
      (is (eql (length s) b)))))

(test query-boolean
  (test-query "SELECT True AS a, False AS b" nil
      (a b)
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
      nil
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
  (test-query (str:concat "SELECT 3.14::float AS float"
                          ", 2.71::double AS double")
      nil
      (float double)
    (is (eql (float 3.14) float))
    (is (eql 2.71d0 double))))

(test query-blob
  (let ((s "바람 부는 대로, 물결 치는 대로"))
    (test-query
        (str:concat "SELECT ENCODE('" s "') AS blob") nil
        (blob)
      (is (string= s (babel:octets-to-string blob))))))

(test query-uuid
  (test-query (str:concat "SELECT uuid AS a, uuid::text AS b "
                          "FROM (SELECT gen_random_uuid() AS uuid)")
      nil
      (a b)
    (is (uuid:uuid= a (uuid:make-uuid-from-string b)))))

(test query-decimal
  (test-query (str:concat "SELECT 3.141::DECIMAL(4,3) AS a"
                          ", 3.14159265::DECIMAL(9,8) AS b"
                          ", 3.14159265358979323::DECIMAL(18,17) AS c"
                          ", 3.1415926535897932384626433832795028841::DECIMAL(38,37) AS d")
      nil
      (a b c d)
    (is (eql 3141/1000 a))
    (is (eql 314159265/100000000 b))
    (is (eql 314159265358979323/100000000000000000 c))
    (is (eql (/ 31415926535897932384626433832795028841
                10000000000000000000000000000000000000)
             d))))

(test query-date
  (test-query (str:concat "SELECT '1970-01-01'::date AS a"
                          ", '2243-10-17'::date AS b")
      nil
      (a b)
    (is (local-time:timestamp=
         (local-time:unix-to-timestamp 0)
         a))
    (is (local-time:timestamp=
         (local-time:unix-to-timestamp 8640000000)
         b))))

(test query-timestamp
  (test-query (str:concat "SELECT '1970-01-01 23:59:59'::timestamp AS a"
                          ", '2243-10-16 23:59:59'::timestamp AS b")
      nil
      (a b)
    (is (local-time:timestamp=
         (local-time:unix-to-timestamp 86399)
         a))
    (is (local-time:timestamp=
         (local-time:unix-to-timestamp 8639999999)
         b))))

(test query-interval
  (test-query (str:concat "SELECT t.i AS interval, epoch_ms(0) + t.i AS ts "
                          "FROM (SELECT INTERVAL 1001 YEAR "
                          "+ INTERVAL 1001 MONTH "
                          "+ INTERVAL 1001 DAY "
                          "+ INTERVAL 1001 HOUR "
                          "+ INTERVAL 1001 MINUTE "
                          "+ INTERVAL 1001 SECOND "
                          "+ INTERVAL 1001 MILLISECOND "
                          "+ INTERVAL 1001 MICROSECOND AS i) AS t")
      nil
      (interval ts)
    (is (local-time:timestamp=
         (periods:add-time (local-time:unix-to-timestamp 0)
                           interval)
         ts))))

(test query-time
  (test-query (str:concat "SELECT t.time AS d "
                          ", extract('hour' FROM t.time) AS hour "
                          ", extract('minute' FROM t.time) AS minute "
                          ", extract('microsecond' FROM t.time) AS microsecond "
                          "FROM (SELECT current_time AS time) AS t")
      nil
      (d hour minute microsecond)
    (local-time-duration:duration=
     (local-time-duration:duration :hour hour
                                   :minute minute
                                   :nsec (* microsecond 1000))
     d)))

(test bind-null
  (test-query "SELECT ? IS NULL AS a, ? AS b" (nil nil)
      (a b)
    (is-true a)
    (is (null b))))

(test bind-boolean
  (test-query "SELECT NOT(?) AS a, NOT(?) AS b" (t nil)
      (a b)
    (is-false a)
    (is-true b)))

(test bind-integers
  (test-query
      (str:concat "SELECT ?::hugeint AS minhugeint"
                  ", ?::hugeint AS maxhugeint"
                  ", ?::tinyint AS tinyint"
                  ", ?::utinyint AS utinyint"
                  ", ?::smallint AS smallint"
                  ", ?::usmallint AS usmallint"
                  ", ?::integer AS integer"
                  ", ?::uinteger AS uinteger"
                  ", ?::bigint AS bigint"
                  ", ?::ubigint AS ubigint")
      (-170141183460469231731687303715884105727
       170141183460469231731687303715884105727
       -128 255 -32768 65535 -2147483648 4294967295
       -9223372036854775808 18446744073709551615)
      (minhugeint
       maxhugeint
       tinyint smallint integer bigint
       utinyint usmallint uinteger ubigint)
    (is (eql -170141183460469231731687303715884105727 minhugeint))
    (is (eql 170141183460469231731687303715884105727 maxhugeint))
    (is (eql -128 tinyint))
    (is (eql 255 utinyint))
    (is (eql -32768 smallint))
    (is (eql 65535 usmallint))
    (is (eql -2147483648 integer))
    (is (eql 4294967295 uinteger))
    (is (eql -9223372036854775808 bigint))
    (is (eql 18446744073709551615 ubigint))))
