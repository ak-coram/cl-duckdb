;;;; duckdb-benchmark.lisp

(trivial-benchmark:define-benchmark-package #:duckdb/benchmark
  (:use #:duckdb)
  (:export #:run-benchmarks))

(in-package #:duckdb/benchmark)

(defparameter *integer-operations* 10000)

(defun create-integers-table ()
  (ddb:run "CREATE TABLE integers (i INTEGER)"))

(define-benchmark measure-integer-query ()
  (declare (optimize speed))
  (ddb:with-transient-connection
    (ddb:with-statement (statement "SELECT ?::integer AS i")
      (loop :for i fixnum :below *integer-operations*
            :for params := (list i)
            :do (with-benchmark-sampling
                  (ddb:bind-parameters statement params)
                  (ddb:with-execute (result statement)
                    (ddb:translate-result result)))))))

(define-benchmark measure-integer-insert ()
  (declare (optimize speed))
  (ddb:with-transient-connection
    (create-integers-table)
    (ddb:with-statement (statement "INSERT INTO integers (i) VALUES (?)")
      (loop :for i fixnum :below *integer-operations*
            :for params := (list i)
            :do (with-benchmark-sampling
                  (ddb:bind-parameters statement params)
                  (ddb:perform statement))))))

(define-benchmark measure-integer-append ()
  (declare (optimize speed))
  (ddb:with-transient-connection
    (create-integers-table)
    (ddb:with-appender (appender "integers")
      (loop :for i fixnum :below *integer-operations*
            :for params := (list i)
            :do (with-benchmark-sampling
                  (ddb:append-row appender params))))))

(define-benchmark measure-large-integer-query ()
  (declare (optimize speed))
  (ddb:with-transient-connection
    (create-integers-table)
    (ddb:with-appender (appender "integers")
      (loop :for i fixnum :below 500
            :do (ddb:append-row appender (list i))))
    (loop :with test-query := (str:concat "SELECT i1.i FROM integers AS i1 "
                                          "JOIN integers AS i2 ON true")
          :for i fixnum :below 10
          :do (with-benchmark-sampling
                (ddb:query test-query nil)))))

(defun floatify-results (benchmark-results)
  (loop :for v :being :each :hash-values :of benchmark-results
          :using (hash-key k)
        :do (setf (gethash k benchmark-results)
                  (loop :for metric :in v
                        :collect (loop :for n :in metric
                                       :collect (typecase n
                                                  (ratio (float n))
                                                  (t n)))))
        :finally (return benchmark-results)))

(defun run-benchmarks ()
  (let ((results (run-package-benchmarks :package '#:duckdb/benchmark
                                         :verbose nil)))
    (report (floatify-results results))))
