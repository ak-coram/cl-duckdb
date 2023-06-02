;;;; duckdb-benchmark.lisp

(trivial-benchmark:define-benchmark-package #:duckdb/benchmark
  (:use #:duckdb)
  (:export #:run-benchmarks))

(in-package #:duckdb/benchmark)

(defparameter *integer-operations* 1000)

(defun create-integers-table ()
  (ddb:run "CREATE TABLE integers (i INTEGER)"))

(define-benchmark measure-integer-query ()
  (declare (optimize speed))
  (ddb:with-transient-connection
    (dotimes (_ 10)
      (with-benchmark-sampling
        (ddb:with-statement (statement "SELECT ?::integer AS i")
          (loop :for i fixnum :below *integer-operations*
                :for params := (list i)
                :do (ddb:bind-parameters statement params)
                :do (ddb:with-execute (result statement)
                      (ddb:translate-result result))))))))

(define-benchmark measure-integer-insert ()
  (declare (optimize speed))
  (dotimes (_ 10)
    (declare (type fixnum _))
    (with-benchmark-sampling
      (ddb:with-transient-connection
        (create-integers-table)
        (ddb:with-transaction ()
          (ddb:with-statement (statement "INSERT INTO integers (i) VALUES (?)")
            (loop :for i fixnum :below *integer-operations*
                  :for params := (list i)
                  :do (ddb:bind-parameters statement params)
                  :do (ddb:perform statement))))))))

(define-benchmark measure-integer-append ()
  (declare (optimize speed))
  (dotimes (_ 10)
    (with-benchmark-sampling
      (ddb:with-transient-connection
        (create-integers-table)
        (ddb:with-appender (appender "integers")
          (loop :for i fixnum :below *integer-operations*
                :for params := (list i)
                :do (ddb:append-row appender params)))))))

(define-benchmark measure-large-integer-query ()
  (declare (optimize speed))
  (ddb:with-transient-connection
    (create-integers-table)
    (ddb:with-appender (appender "integers")
      (loop :for i fixnum :below 500
            :do (ddb:append-row appender (list i))))
    (loop :with test-query := (ddb:concat "SELECT i1.i FROM integers AS i1 "
                                          "JOIN integers AS i2 ON true")
          :for i fixnum :below 10
          :do (with-benchmark-sampling
                (ddb:query test-query nil)))))

(defparameter *integer-sum-limit* 200000)

(define-benchmark measure-integer-append-sum ()
  (declare (optimize speed))
  (dotimes (_ 100)
    (ddb:with-transient-connection
      (with-benchmark-sampling
        (create-integers-table)
        (ddb:with-appender (appender "integers")
          (loop :for i fixnum :below *integer-sum-limit*
                :for params := (list i)
                :do (ddb:append-row appender params)))
        (ddb:query "SELECT sum(i) FROM integers" nil)))))

(define-benchmark measure-static-table-integer-vector-sum ()
  (declare (optimize speed))
  (let* ((integers (make-array (list *integer-sum-limit*)
                               :element-type '(signed-byte 32))))
    (loop :for i :of-type (signed-byte 32) :below *integer-sum-limit*
          :do (setf (aref integers i) i))
    (dotimes (_ 100)
      (ddb:with-transient-connection
        (with-benchmark-sampling
          (ddb:with-static-table ("integers" `(("i" . ,integers)))
            (ddb:query "SELECT sum(i) FROM integers" nil)))))))

(define-benchmark measure-static-table-integer-list-sum ()
  (declare (optimize speed))
  (let ((integers (loop :for i fixnum :below *integer-sum-limit* :collect i)))
    (dotimes (_ 100)
      (ddb:with-transient-connection
        (with-benchmark-sampling
          (ddb:with-static-table
              ("integers" `(("i" . (,integers :duckdb-integer))))
            (ddb:query "SELECT sum(i) FROM integers" nil)))))))

(define-benchmark measure-static-table-boolean-vector-count ()
  (declare (optimize speed))
  (let* ((booleans (make-array (list 100000) :element-type 'bit)))
    (loop :for i :of-type fixnum :below 100000
          :do (setf (aref booleans i) (if (evenp i) 1 0)))
    (dotimes (_ 100)
      (ddb:with-transient-connection
        (with-benchmark-sampling
          (ddb:with-static-table
              ("booleans" `(("v" . (,booleans :duckdb-boolean))))
            (ddb:query "SELECT v, COUNT(*) FROM booleans GROUP BY v" nil)))))))

(define-benchmark measure-static-table-boolean-list-count ()
  (declare (optimize speed))
  (let ((values (loop :for i :below 100000 :collect (evenp i))))
    (dotimes (_ 100)
      (ddb:with-transient-connection
        (with-benchmark-sampling
          (ddb:with-static-table
              ("booleans" `(("v" . (,values :duckdb-boolean))))
            (ddb:query "SELECT v, COUNT(*) FROM booleans GROUP BY v" nil)))))))

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
