;;;; duckdb-misc.lisp

(in-package #:duckdb)

(defun concat (&rest args)
  "Concatenate arguments into a string."
  (apply #'concatenate 'string args))

(defun join (separator xs)
  "Intersperse SEPARATOR between elements of XS and string-concatenate result."
  (apply #'concatenate 'string
         (butlast (mapcan (lambda (x) (list x separator)) xs))))

(defun replace-substring (match replacement s)
  "Replace all substrings matching MATCH with REPLACEMENT in S."
  (let ((cl-ppcre:*allow-quoting* t))
    (cl-ppcre:regex-replace-all (concat "\\Q" match)
                                s
                                (list replacement))))

(defun snake-case-to-param-case (s)
  "Downcase input and replace underscores with hyphens."
  (substitute #\- #\_ (string-downcase s)))

(defun param-case-to-snake-case (s)
  "Downcase input and replace hyphens with underscores."
  (substitute #\_ #\- (string-downcase s)))

(defun column= (a b)
  (if (and (stringp a) (stringp b))
      (string= a b)
      (string= (snake-case-to-param-case a)
               (snake-case-to-param-case b))))
