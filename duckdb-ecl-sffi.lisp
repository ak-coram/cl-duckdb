;;;; duckdb-ecl-sffi.lisp

#+ECL
(when (equalp (uiop:getenv "CL_DUCKDB_USE_SFFI") "T")
  (let* ((library-path (uiop:getenv "CL_DUCKDB_LIBRARY_PATH"))
         (quoted-search-arg (when library-path
                              (concatenate 'string
                                           " -L"
                                           (uiop:escape-sh-token library-path)))))
    (setf compiler:*user-ld-flags* (concatenate 'string compiler:*user-ld-flags*
                                                (when quoted-search-arg
                                                  quoted-search-arg)
                                                " -lduckdb"))
    (setf cffi-sys:*cffi-ecl-method* :c/c++)))
