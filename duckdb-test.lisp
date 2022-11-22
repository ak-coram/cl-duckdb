;;;; duckdb-test.lisp

(defpackage #:duckdb-test
  (:use #:cl #:fiveam))
(in-package #:duckdb-test)

(def-suite :duckdb)
(in-suite :duckdb)

(test thread-setup
  (let ((query (str:concat "SELECT current_setting('threads') AS n"
                           " UNION ALL "
                           "SELECT current_setting('external_threads') AS n"))
        (cpu-count (cpus:get-number-of-processors)))
    (labels ((get-thread-counts ()
               (ddb:get-result (ddb:query query nil) 'n)))
      (ddb:with-threads nil
        (ddb:with-transient-connection
          (is (equalp (get-thread-counts) (vector cpu-count 0)))))
      (ddb:with-threads 1
        (ddb:with-transient-connection
          (is (equalp (get-thread-counts) (vector 1 0)))))
      (ddb:with-threads 2
        (ddb:with-transient-connection
          (is (equalp (get-thread-counts)
                      (if bt:*supports-threads-p*
                          (vector 1 1)
                          (vector 1 0))))))
      (ddb:with-threads t
        (ddb:with-transient-connection
          (is (equalp (get-thread-counts)
                      (if bt:*supports-threads-p*
                          (vector 1 (1- cpu-count))
                          (vector 1 0)))))))))

(defmacro test-query (query parameters result-syms &body body)
  (alexandria:with-gensyms (db conn results)
    `(ddb:with-open-database (,db)
       (ddb:with-open-connection (,conn ,db)
         (let* ((,results (ddb:query ,query (list ,@parameters) :connection ,conn))
                ,@(loop :for sym :in result-syms
                        :collect `(,sym (ddb:get-result ,results (quote ,sym) 0))))
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
    (is (eql 3.14s0 float))
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

(test query-enum
  (ddb:with-transient-connection
    (ddb:run "CREATE TYPE tuber AS ENUM ('carrot', 'potato', 'yam')")
    (let ((result (ddb:query "SELECT 'potato'::tuber AS value" nil)))
      (is (string= "potato" (ddb:get-result result 'value 0))))))

(test bind-null
  (test-query "SELECT ? IS NULL AS a, ? AS b" (nil nil)
      (a b)
    (is-true a)
    (is (null b))))

(test bind-null-keyword
  (test-query "SELECT ?::boolean IS NULL AS a, ? AS b" (:null :null)
      (a b)
    (is-true a)
    (is (null b))))

(test bind-boolean
  (test-query "SELECT NOT(?) AS a, NOT(?) AS b" (t nil)
      (a b)
    (is-false a)
    (is-true b)))

(test bind-boolean-keyword
  (test-query (str:concat "SELECT ?::boolean || '' AS a "
                          ", ?::boolean || '' AS b")
      (:false :true)
      (a b)
    (is (string= "false" a))
    (is (string= "true" b))))

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

(test bind-string
  (let ((s "Árvíztűrő tükörfúrógép"))
    (test-query (str:concat "SELECT t.s AS a, LENGTH(t.s) AS b "
                            "FROM (SELECT ?::text AS s) AS t")
        (s)
        (a b)
      (is (string= a s))
      (is (eql (length s) b)))))

(test bind-blob
  (let ((s "Árvíztűrő tükörfúrógép"))
    (test-query (str:concat "SELECT decode(?::blob) AS a")
        ((babel:string-to-octets s))
        (a)
      (is (string= a s)))))

(test bind-floats
  (let ((f 3.14s0)
        (d 2.71d0))
    (test-query (str:concat "SELECT ?::float AS float"
                            ", ?::double AS double")
        (f d)
        (float double)
      (is (eql f float))
      (is (eql d double)))))

(test bind-decimal
  (test-query (str:concat "SELECT ?::DECIMAL(38,38) AS a"
                          ", ?::DECIMAL(7,6) AS b")
      (1/3 355/113)
      (a b)
    (is (eql a (/ 33333333333333333333333333333333333333
                  100000000000000000000000000000000000000)))
    (is (eql b 3141592/1000000))))

(test bind-date
  (let ((today (local-time:today)))
    (test-query (str:concat "SELECT ?::date AS a")
        (today)
        (a)
      (is (local-time:timestamp= a today)))))

(test bind-timestamp
  (let ((now (local-time:now)))
    (test-query (str:concat "SELECT ?::timestamp AS a") (now) (a)
      (is (local-time:timestamp= a now)))))

(test bind-uuid
  (let ((uuid (uuid:make-v4-uuid)))
    (test-query "SELECT ?::uuid AS a" (uuid) (a)
      (is (uuid:uuid= a uuid)))))

(test bind-time
  (let ((d (local-time-duration:duration
            :week 1 :day 1 :hour 1 :minute 1 :sec 1 :nsec 1000)))
    (test-query "SELECT ?::time AS time" (d) (time)
      (local-time-duration:duration= time d))))

(test bind-enum
  (ddb:with-transient-connection
    (ddb:run "CREATE TYPE tuber AS ENUM ('carrot', 'potato', 'yam')")
    (let ((result (ddb:query "SELECT ?::tuber AS value" (list "yam"))))
      (is (string= "yam" (ddb:get-result result 'value 0))))))

(defmacro test-append (sql-type values &key convert test)
  (alexandria:once-only (values convert)
    (alexandria:with-gensyms (appender value results x y)
      `(ddb:with-transient-connection
         (ddb:run ,(str:concat "CREATE TABLE test (x " sql-type ")"))
         (ddb:with-appender (,appender "test")
           (loop :for ,value :in ,values
                 :do (ddb:append-row ,appender (list ,value))))
         (let* ((,results (ddb:query "SELECT x FROM test" nil)))
           (loop :for ,x :across (ddb:get-result ,results 'x)
                 :for ,y :in ,values
                 :do (let ((,x (if ,convert (funcall ,convert ,x) ,x))
                           (,y (if ,convert (funcall ,convert ,y) ,y)))
                       (is (,(or test 'equal) ,x ,y)))))))))

(test append-null
  (test-append "varchar" '(nil)))

(test append-null-keyword
  (test-append "varchar" '(:null)
    :convert (lambda (v) (if (eql v :null) nil v))))

(test append-boolean
  (test-append "boolean" '(nil t)))

(test append-boolean-keyword
  (ddb:with-transient-connection
    (ddb:run "CREATE TABLE booleans (x BOOLEAN, y BOOLEAN, z BOOLEAN)")
    (ddb:with-appender (appender "booleans")
      (loop :with booleans := '(:true :false)
            :for x :in booleans
            :do (loop :for y :in booleans
                      :do (loop :for z :in booleans
                                :do (ddb:append-row appender (list x y z))))))
    (let* ((query (str:concat "SELECT COUNT(*) AS count FROM "
                              "(SELECT DISTINCT x, y, z FROM booleans) AS b"))
           (results (ddb:query query nil)))
      (is (eql 8 (ddb:get-result results 'count 0))))))

(test append-string
  (test-append "varchar" '("Árvíztűrő tükörfúrógép")))

(test append-integers
  (test-append "tinyint" '(-128))
  (test-append "utinyint" '(255))
  (test-append "smallint" '(-32768))
  (test-append "usmallint" '(65535))
  (test-append "integer" '(-2147483648))
  (test-append "uinteger" '(4294967295))
  (test-append "bigint" '(-9223372036854775808))
  (test-append "ubigint"  '(18446744073709551615))
  (test-append "hugeint"
      '(-170141183460469231731687303715884105727
        170141183460469231731687303715884105727)))

(test append-blob
  (let ((s "Árvíztűrő tükörfúrógép"))
    (test-append "blob" (list (babel:string-to-octets s))
      :convert (lambda (v) (loop :for x :across v :collect x)))))

(test append-floats
  (test-append "float" '(3.14s0))
  (test-append "double" '(2.71d0)))

(test append-date
  (test-append "date" (list (local-time:today)) :test local-time:timestamp=))

(test append-timestamp
  (test-append "timestamp" (list (local-time:now))
    :test local-time:timestamp=))

(test append-uuid
  (test-append "uuid" (list (uuid:make-v4-uuid)) :test uuid:uuid=))

(test append-time
  (let ((d (local-time-duration:duration
            :week 1 :day 1 :hour 1 :minute 1 :sec 1 :nsec 1000)))
    (test-append "time" (list d) :test local-time-duration:duration=)))

(test append-enum
  (ddb:with-transient-connection
    (ddb:run "CREATE TYPE tuber AS ENUM ('carrot', 'potato', 'yam')"
             "CREATE TABLE garden (plant tuber)")
    (ddb:with-appender (appender "garden")
      (loop :with tubers := '("carrot" "potato" "yam")
            :for tuber :in tubers
            :do (ddb:append-row appender (list tuber))))))

(test static-table-booleans
  (ddb:with-transient-connection
    (ddb:with-static-table
        ("booleans" `(("a" . ,(make-array '(6) :element-type 'bit
                                               :initial-contents '(0 1 0 1 0 1)))
                      ("b" . ((t nil :null :false t t)
                              :duckdb-boolean))))
      (let ((r1 (ddb:query "SELECT COUNT(*) AS count FROM booleans GROUP BY a" nil))
            (r2 (ddb:query (str:concat "SELECT COUNT(*) AS count FROM booleans "
                                       "GROUP BY b ORDER BY COUNT(b) ASC")
                           nil)))
        (is (equalp (ddb:get-result r1 'count) #(3 3)))
        (is (equalp (ddb:get-result r2 'count) #(1 2 3)))))))

(test static-table-integers
  (labels ((get-table-name (type)
             (format nil "~a" type)))
    (ddb:with-transient-connection
      (let* ((types '((:duckdb-utinyint 255)
                      (:duckdb-tinyint 127)
                      (:duckdb-usmallint)
                      (:duckdb-smallint)
                      (:duckdb-uinteger)
                      (:duckdb-integer)
                      (:duckdb-ubigint)
                      (:duckdb-bigint)))
             (table-names
               (loop :for (duckdb-type limit) :in types
                     :for integer-list := (loop :for i :below (or limit
                                                                  2000)
                                                :collect i)
                     :for table-name := (get-table-name duckdb-type)
                     :for columns := `(("i" ,integer-list
                                            ,duckdb-type))
                     :do (ddb:bind-static-table table-name columns)
                     :collect table-name))
             (queries
               (loop :for table-name :in table-names
                     :collect
                     (format nil "SELECT sum(i)::integer AS sum FROM \"~a\""
                             table-name))))
        (loop :for (duckdb-type limit) :in types
              :for query :in queries
              :for sum := (loop :for i :below (or limit 2000)
                                :sum i)
              :do (is (eql sum
                           (ddb:get-result (ddb:query query nil) 'sum 0))))
        (dolist (table-name table-names)
          (ddb:unbind-static-table table-name))))))

(test static-table-floats
  (let* ((floats nil)
         (doubles nil)
         (sums (loop :for f := 0.5s0 :then (incf f)
                     :for d := 0.5d0 :then (incf d)
                     :until (> f 100)
                     :do (push f floats)
                     :do (push d  doubles)
                     :sum f :into float-sum
                     :sum d :into double-sum
                     :finally (return (cons float-sum double-sum))))
         (float-query "SELECT sum(f)::float AS sum FROM floats")
         (double-query "SELECT sum(d) AS sum FROM doubles"))
    (ddb:with-transient-connection
      (ddb:with-static-tables
          (("floats" `(("f" . (,floats :duckdb-float))))
           ("doubles" `(("d" . (,doubles :duckdb-double)))))
        (is (eql (car sums)
                 (ddb:get-result (ddb:query float-query nil) 'sum 0)))
        (is (eql (cdr sums)
                 (ddb:get-result (ddb:query double-query nil) 'sum 0)))))))

(test static-table-scopes
  (ddb:with-transient-connection
    (labels ((get-scope (table-name)
               (ddb:get-result (ddb:query (format nil "SELECT scope FROM ~a"
                                                  table-name)
                                          nil)
                               'scope 0))
             (get-columns (value)
               `(("scope" . ((,value) :duckdb-varchar)))))
      (let ((table-name (str:snake-case (format nil "test_~a" (uuid:make-v4-uuid)))))
        (ddb:bind-static-table table-name (get-columns "global1"))
        (is (string= "global1" (get-scope table-name)))
        (ddb:with-static-table (table-name (get-columns "inner"))
          (is (string= "inner" (get-scope table-name)))
          (ddb:with-static-table (table-name (get-columns "innermost"))
            (ddb:bind-static-table table-name (get-columns "global2"))
            (is (string= "innermost" (get-scope table-name)))))
        (is (string= "global2" (get-scope table-name)))
        (ddb:unbind-static-table table-name)))))
