;;;; duckdb-benchmark.lisp

(defpackage #:duckdb/benchmark
  (:use #:cl #:duckdb)
  (:import-from #:org.shirakumo.trivial-benchmark
                #:timer #:with-sampling #:compute #:metric-types #:samples
                #:*default-computations* #:*default-metrics*)
  (:export #:run-benchmarks))

(in-package #:duckdb/benchmark)

(defparameter *integer-operations* 1000)

(defun create-integers-table ()
  (ddb:run "CREATE TABLE integers (i INTEGER)"))

(defun measure-integer-query (timer)
  (declare (optimize speed))
  (ddb:with-transient-connection
    (dotimes (_ 10)
      (with-sampling (timer)
        (ddb:with-statement (statement "SELECT ?::integer AS i")
          (loop :for i fixnum :below *integer-operations*
                :for params := (list i)
                :do (ddb:bind-parameters statement params)
                :do (ddb:with-execute (result statement)
                      (ddb:translate-result result))))))))

(defun measure-integer-insert (timer)
  (declare (optimize speed))
  (dotimes (_ 10)
    (declare (type fixnum _))
    (with-sampling (timer)
      (ddb:with-transient-connection
        (create-integers-table)
        (ddb:with-transaction ()
          (ddb:with-statement (statement "INSERT INTO integers (i) VALUES (?)")
            (loop :for i fixnum :below *integer-operations*
                  :for params := (list i)
                  :do (ddb:bind-parameters statement params)
                  :do (ddb:perform statement))))))))

(defun measure-integer-append (timer)
  (declare (optimize speed))
  (dotimes (_ 10)
    (with-sampling (timer)
      (ddb:with-transient-connection
        (create-integers-table)
        (ddb:with-appender (appender "integers")
          (loop :for i fixnum :below *integer-operations*
                :for params := (list i)
                :do (ddb:append-row appender params)))))))

(defun measure-large-integer-query (timer)
  (declare (optimize speed))
  (ddb:with-transient-connection
    (create-integers-table)
    (ddb:with-appender (appender "integers")
      (loop :for i fixnum :below 500
            :do (ddb:append-row appender (list i))))
    (loop :with test-query := (ddb:concat "SELECT i1.i FROM integers AS i1 "
                                          "JOIN integers AS i2 ON true")
          :for i fixnum :below 10
          :do (with-sampling (timer)
                (ddb:query test-query nil)))))

(defparameter *integer-sum-limit* 200000)

(defun measure-integer-append-sum (timer)
  (declare (optimize speed))
  (dotimes (_ 100)
    (ddb:with-transient-connection
      (with-sampling (timer)
        (create-integers-table)
        (ddb:with-appender (appender "integers")
          (loop :for i fixnum :below *integer-sum-limit*
                :for params := (list i)
                :do (ddb:append-row appender params)))
        (ddb:query "SELECT sum(i) FROM integers" nil)))))

(defun measure-static-table-integer-vector-sum (timer)
  (declare (optimize speed))
  (let* ((integers (make-array (list *integer-sum-limit*)
                               :element-type '(signed-byte 32))))
    (loop :for i :of-type (signed-byte 32) :below *integer-sum-limit*
          :do (setf (aref integers i) i))
    (dotimes (_ 100)
      (ddb:with-transient-connection
        (with-sampling (timer)
          (ddb:with-static-table ("integers" `(("i" . ,integers)))
            (ddb:query "SELECT sum(i) FROM integers" nil)))))))

(defun measure-static-table-integer-list-sum (timer)
  (declare (optimize speed))
  (let ((integers (loop :for i fixnum :below *integer-sum-limit* :collect i)))
    (dotimes (_ 100)
      (ddb:with-transient-connection
        (with-sampling (timer)
          (ddb:with-static-table
              ("integers" `(("i" . (,integers :duckdb-integer))))
            (ddb:query "SELECT sum(i) FROM integers" nil)))))))

(defun measure-static-table-boolean-vector-count (timer)
  (declare (optimize speed))
  (let* ((booleans (make-array (list 100000) :element-type 'bit)))
    (loop :for i :of-type fixnum :below 100000
          :do (setf (aref booleans i) (if (evenp i) 1 0)))
    (dotimes (_ 100)
      (ddb:with-transient-connection
        (with-sampling (timer)
          (ddb:with-static-table
              ("booleans" `(("v" . (,booleans :duckdb-boolean))))
            (ddb:query "SELECT v, COUNT(*) FROM booleans GROUP BY v" nil)))))))

(defun measure-static-table-boolean-list-count (timer)
  (declare (optimize speed))
  (let ((values (loop :for i :below 100000 :collect (evenp i))))
    (dotimes (_ 100)
      (ddb:with-transient-connection
        (with-sampling (timer)
          (ddb:with-static-table
              ("booleans" `(("v" . (,values :duckdb-boolean))))
            (ddb:query "SELECT v, COUNT(*) FROM booleans GROUP BY v" nil)))))))

(defun report-benchmark (timer)
  ;; Use the query-output renderer; trivial-benchmark's formatter fails on CCL.
  (let ((table (ascii-table:make-table (cons :metric *default-computations*))))
    (dolist (metric (or *default-metrics* (metric-types timer)))
      (let ((samples (samples timer metric)))
        (when (plusp (length samples))
          (ascii-table:add-row
           table
           (cons metric (mapcar (lambda (computation)
                                  (compute computation samples))
                                *default-computations*))))))
    (ascii-table:display table)))

(defun run-benchmarks ()
  (let ((results (make-hash-table)))
    (dolist (benchmark '(measure-integer-query
                        measure-integer-insert
                        measure-integer-append
                        measure-large-integer-query
                        measure-integer-append-sum
                        measure-static-table-integer-vector-sum
                        measure-static-table-integer-list-sum
                        measure-static-table-boolean-vector-count
                        measure-static-table-boolean-list-count)
                       results)
      (let ((timer (make-instance 'timer)))
        (funcall benchmark timer)
        (setf (gethash benchmark results) timer)
        (format t "~&~A~%" benchmark)
        (report-benchmark timer)))))
