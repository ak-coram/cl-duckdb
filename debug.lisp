(let* ((p (periods:duration :YEARS 1084
                            :MONTHS 5
                            :DAYS 1001
                            :HOURS 1017
                            :MINUTES 57
                            :SECONDS 42
                            :MILLISECONDS 2
                            :MICROSECONDS 1
                            :NANOSECONDS 0))
       (epoch (local-time:unix-to-timestamp 0))
       (result (periods:add-time epoch p)))
  (setf local-time:*default-timezone* local-time:+utc-zone+)
  (format t "~%~%=====~% period: ~A~%epoch: ~A~%result: ~A~%=====~%"
          p epoch result))
