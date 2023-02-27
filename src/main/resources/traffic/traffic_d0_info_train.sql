t_6 AS (
    SELECT t.*
	FROM (
		SELECT t1.*, row_number() OVER(PARTITION BY FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF ORDER BY INSERT_DATE DESC) rn
		FROM (
		    SELECT *
		    FROM t_1 t
		    WHERE EX_DIF = 0
	    ) t1
    ) t
    WHERE t.rn = 1
),