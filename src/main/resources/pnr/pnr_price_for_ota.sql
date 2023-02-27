INSERT INTO %1$s (EX_DIF, FLIGHT_DATE, FLT_NO, DEP, ARR, BKG, MIN_PRICE, MAX_PRICE, ID)
SELECT t.*, %7$s.Nextval
FROM (
    WITH t_0 AS (
        SELECT sale_price as price1, flight_date, flt_no, to_Date(sale_time) as book_date, cabin, dep, arr, ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(sale_time))) AS ex_dif, 1 AS num
        FROM %2$s
        WHERE cabin not in (%6$s) AND TO_CHAR(FLIGHT_DATE, 'yyyy-MM-dd') =  '%3$s'
    ),
    t_1_0 AS (
        SELECT flight_date, flt_no, ex_dif, dep, arr, sum(num) AS bkg
        FROM t_0
        GROUP BY flight_date, flt_no, ex_dif, dep, arr
    ),
    t_1_1 AS (
        SELECT flight_date, flt_no, ex_dif, dep, arr, min(price1) AS min_price, max(price1) AS max_price, median(price1) AS median_price
        FROM t_0
        WHERE price1 > 0
        GROUP BY flight_date, flt_no, ex_dif, dep, arr
    ),
    t_1 AS (
        SELECT t.*, min_price, max_price, median_price
        FROM t_1_0 t
        LEFT JOIN t_1_1 t1
        ON t.flight_date = t1.flight_date AND t.flt_no = t1.flt_no and t.dep = t1.dep and t.arr = t1.arr and t.ex_dif = t1.ex_dif
    ),
    t_2 AS (
        SELECT flight_date, flt_no, dep, arr, sum(1), 1 AS tag
        FROM t_1
        GROUP BY flight_date, flt_no, dep, arr
    ),
    t_3 AS (
        SELECT ex_dif_a, 1 AS tag
        FROM (
            SELECT 0 AS ex_dif_a
            FROM dual UNION ALL
            SELECT LEVEL AS exdif
            FROM dual connect by level <= %4$s
        )
        WHERE ex_dif_a >= %5$s
    ),
    t_4 AS (
        SELECT ex_dif_a, flight_date, flt_no, dep, arr
        FROM t_3 t
        LEFT JOIN t_2 t1
        USING (tag)
        ORDER BY flight_date, flt_no, dep, arr, ex_dif_a
    ),
    t_5 AS (
        SELECT ex_dif_a, t.flight_date AS flight_date, t.flt_no AS flt_no, t.dep AS dep, t.arr AS arr, ex_dif, t1.flt_no AS flt_no1, min_price, median_price, max_price, bkg
        FROM t_4 t
        LEFT JOIN t_1 t1
        ON t.flight_date = t1.flight_date AND t.flt_no = t1.flt_no AND t.dep = t1.dep AND t.arr = t1.arr AND t.ex_dif_a = t1.ex_dif
    ),
    t_6 AS (
        SELECT t.*, (CASE WHEN ex_dif_a = ex_dif THEN 1 ELSE 0 end) AS cun
        FROM t_5 t
    ),
    t_7 AS (
        SELECT ex_dif_a, flight_date, flt_no, dep, arr, cun
        FROM t_6
        WHERE cun = 0
    ),
    t_8 AS (
        SELECT t.*, t1.ex_dif AS ex_dif1, t1.flt_no AS flt_no1, t1.min_price AS min_price1, t1.median_price AS median_price1, t1.max_price AS max_price1, t1.bkg AS bkg1
        FROM t_7 t
        LEFT JOIN t_1 t1
        ON t.ex_dif_a < t1.ex_dif AND t.flight_date = t1.flight_date AND t.flt_no = t1.flt_no AND t.dep = t1.dep AND t.arr = t1.arr
    ),
    t_9 AS (
        SELECT ex_dif_a, flight_date, flt_no, dep, arr, cun, ex_dif1, flt_no1, min_price1, median_price1, max_price1, bkg1
        FROM (
            SELECT t1.*, row_number() OVER(PARTITION BY FLIGHT_DATE, flt_no, DEP, ARR, ex_dif_a ORDER BY ex_dif1) rn
            FROM t_8 t1
        ) t1
        WHERE t1.rn = 1
    ),
    t_10 AS (
        SELECT t.*, t1.ex_dif AS ex_dif2, t1.flt_no AS flt_no2, t1.min_price AS min_price2, t1.median_price AS median_price2, t1.max_price AS max_price2, t1.bkg AS bkg2
        FROM t_9 t
        LEFT JOIN t_1 t1
        ON t.ex_dif_a > t1.ex_dif AND t.flight_date = t1.flight_date AND t.flt_no = t1.flt_no AND t.dep = t1.dep AND t.arr = t1.arr
    ),
    t_x AS (
        SELECT ex_dif_a, flight_date, flt_no, dep, arr, cun, ex_dif1, flt_no1, ex_dif2, flt_no2,
            (CASE WHEN min_price1 IS NULL THEN min_price2 ELSE min_price1 end) AS min_price,
            (CASE WHEN median_price1 IS NULL THEN median_price2 ELSE median_price1 end) AS median_price,
            (CASE WHEN max_price1 IS NULL THEN max_price2 ELSE max_price1 end) AS max_price
        FROM (
            SELECT t1.*, row_number() OVER(PARTITION BY FLIGHT_DATE, flt_no, DEP, ARR, ex_dif_a ORDER BY ex_dif2) rn
            FROM t_10 t1
        ) t3
        WHERE t3.rn = 1
    ),
    t_x1 AS (
        SELECT ex_dif_a, flight_date, flt_no, dep, arr,
            (CASE WHEN flt_no2 IS NOT NULL THEN 0 WHEN (flt_no2 IS NULL AND flt_no1 IS NOT NULL) THEN -1 ELSE -2 END) AS up_bkg,
            (CASE WHEN flt_no2 IS NOT NULL THEN min_price WHEN (flt_no2 IS NULL AND flt_no1 IS NOT NULL) THEN median_price ELSE -2 END) AS up_min_price,
            (CASE WHEN flt_no2 IS NOT NULL THEN max_price WHEN (flt_no2 IS NULL AND flt_no1 IS NOT NULL) THEN max_price ELSE -2 END) AS up_max_price
        FROM t_x
    )
    SELECT *
    FROM (
        SELECT *
        FROM (
            SELECT ex_dif_a as ex_dif, flight_date, flt_no, dep, arr, up_bkg AS bkg, up_min_price AS min_price, up_max_price AS max_price
            FROM t_x1
        ) UNION ALL (
            SELECT ex_dif_a as ex_dif, flight_date, flt_no, dep, arr, bkg, min_price, max_price
            FROM t_6
            WHERE cun = 1
        )
    )
    ORDER BY flight_date, flt_no, dep, arr, ex_dif
) t