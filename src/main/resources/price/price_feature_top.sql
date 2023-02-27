-- 计算与当前航班同航段除自己外的最低的6个价格
INSERT INTO %1$s (CREATE_TIME, FLIGHT_DATE, DEP, ARR, FLIGHT_NO, TOP_MEAN, TOP_MAX, TOP_MIN, TOP_STD, FEA_NEAREST_TIME_1, FEA_NEAREST_TIME_2, TOP_1, TOP_2, TOP_3, TOP_4, TOP_5, TOP_6, ID)
SELECT t.*, %7$s.Nextval
FROM (
    WITH t_0 AS (
        SELECT trim(BOTH ' ' FROM air_code) air_code,
            decode(ltrim(rtrim(dep)),'DAX', 'DZH', ltrim(rtrim(dep))) AS dep,
            decode(ltrim(rtrim(arr)),'DAX', 'DZH', ltrim(rtrim(arr))) AS arr,
            ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(create_time))) AS ex_dif,
            ltrim(rtrim(flight_no)) as flight_no, FLIGHT_DATE, DEP_TIME, PRICE, CREATE_TIME
        FROM %2$s
        WHERE length(ltrim(rtrim(flight_no))) <= 6 AND SHARE_IND != 1
        AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(create_time))) >= %3$s
        AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(create_time))) <= %4$s
        AND %5$s
    ),
    -- 去重
    t_1 AS (
        SELECT AIR_CODE, DEP, ARR, FLIGHT_DATE, FLIGHT_NO, PRICE, DEP_TIME, CREATE_TIME
        FROM (
            SELECT t1.*, row_number() OVER(PARTITION BY FLIGHT_DATE, DEP, ARR, flight_no, create_time ORDER BY create_time DESC) rn
            FROM t_0 t1
        ) t1
        WHERE t1.rn = 1
    ),
    t_2 AS (
        SELECT *
        FROM t_1
        WHERE AIR_CODE IN (%6$s)
    ),
    t_3 AS (
        SELECT t.*, t1.flight_no AS flight_no1, t1.DEP_TIME AS DEP_TIME1, t1.PRICE AS PRICE1, abs(t.DEP_TIME - t1.DEP_TIME) * 24 * 60 * 60 AS depTimeCha
        FROM t_2 t
        LEFT JOIN t_1 t1
        ON t.flight_date = t1.flight_date AND t.DEP=t1.DEP AND t.ARR=t1.ARR AND t.create_time =t1.create_time AND t.flight_no != t1.flight_no
        ORDER BY t.AIR_CODE, t.DEP, t.ARR, t.FLIGHT_DATE, t.CREATE_TIME
    ),
    t_4 AS (
        SELECT AIR_CODE, CREATE_TIME, DEP, ARR, FLIGHT_DATE, FLIGHT_NO, PRICE, DEP_TIME, DEP_TIME1, depTimeCha, PRICE1, flight_no1, rnum
        FROM (
            SELECT t.*, row_number() OVER (PARTITION BY create_time, FLIGHT_DATE, DEP, ARR ORDER BY depTimeCha, PRICE1, AIR_CODE) rnum
            FROM t_3 t
        )
        WHERE rnum <= 6
    ),
    t_5 AS (
        SELECT AIR_CODE, DEP, ARR, FLIGHT_DATE, CREATE_TIME, FLIGHT_NO, ROUND(avg(PRICE1), 2) as top_mean1, max(PRICE1) as top_max1, min(PRICE1) as top_min1, ROUND(STDDEV_POP(PRICE1), 2) as top_std1
        FROM t_4
        GROUP BY AIR_CODE, DEP, ARR, FLIGHT_DATE, CREATE_TIME, FLIGHT_NO
    ),
    t_6 AS (
        SELECT t.*, (CASE WHEN top_mean1 IS NULL THEN -1 ELSE top_mean1 END) AS top_mean,
            (CASE WHEN top_max1 IS NULL THEN -1 ELSE top_max1 END) AS top_max,
            (CASE WHEN top_min1 IS NULL THEN -1 ELSE top_min1 END) AS top_min,
            (CASE WHEN top_std1 IS NULL THEN -1 ELSE top_std1 END) AS top_std,
            (CASE WHEN t1.FLIGHT_NO IS NOT NULL AND t1.depTimeCha IS NOT NULL THEN t1.depTimeCha ELSE -1 END) AS FEA_NEAREST_TIME_1,
            (CASE WHEN t2.FLIGHT_NO IS NOT NULL AND t2.depTimeCha IS NOT NULL THEN t2.depTimeCha ELSE -1 END) AS FEA_NEAREST_TIME_2,
            (CASE WHEN t1.FLIGHT_NO IS NOT NULL AND t1.PRICE1 IS NOT NULL THEN t1.PRICE1 ELSE -1 END) AS TOP_1,
            (CASE WHEN t2.FLIGHT_NO IS NOT NULL AND t2.PRICE1 IS NOT NULL THEN t2.PRICE1 ELSE -1 END) AS TOP_2,
            (CASE WHEN t3.FLIGHT_NO IS NOT NULL THEN t3.PRICE1 ELSE -1 END) AS TOP_3, (CASE WHEN t4.FLIGHT_NO IS NOT NULL THEN t4.PRICE1 ELSE -1 END) AS TOP_4,
            (CASE WHEN t5.FLIGHT_NO IS NOT NULL THEN t5.PRICE1 ELSE -1 END) AS TOP_5, (CASE WHEN t6.FLIGHT_NO IS NOT NULL THEN t6.PRICE1 ELSE -1 END) AS TOP_6
        FROM t_5 t
        LEFT JOIN t_4 t1
        ON t.FLIGHT_DATE = t1.FLIGHT_DATE AND t.FLIGHT_NO = t1.FLIGHT_NO AND t.DEP = t1.DEP AND t.ARR = t1.ARR AND t.CREATE_TIME = t1.CREATE_TIME AND t1.rnum = 1
        LEFT JOIN t_4 t2
        ON t.FLIGHT_DATE = t2.FLIGHT_DATE AND t.FLIGHT_NO = t2.FLIGHT_NO AND t.DEP = t2.DEP AND t.ARR = t2.ARR AND t.CREATE_TIME = t2.CREATE_TIME AND t2.rnum = 2
        LEFT JOIN t_4 t3
        ON t.FLIGHT_DATE = t3.FLIGHT_DATE AND t.FLIGHT_NO = t3.FLIGHT_NO AND t.DEP = t3.DEP AND t.ARR = t3.ARR AND t.CREATE_TIME = t3.CREATE_TIME AND t3.rnum = 3
        LEFT JOIN t_4 t4
        ON t.FLIGHT_DATE = t4.FLIGHT_DATE AND t.FLIGHT_NO = t4.FLIGHT_NO AND t.DEP = t4.DEP AND t.ARR = t4.ARR AND t.CREATE_TIME = t4.CREATE_TIME AND t4.rnum = 4
        LEFT JOIN t_4 t5
        ON t.FLIGHT_DATE = t5.FLIGHT_DATE AND t.FLIGHT_NO = t5.FLIGHT_NO AND t.DEP = t5.DEP AND t.ARR = t5.ARR AND t.CREATE_TIME = t5.CREATE_TIME AND t5.rnum = 5
        LEFT JOIN t_4 t6
        ON t.FLIGHT_DATE = t6.FLIGHT_DATE AND t.FLIGHT_NO = t6.FLIGHT_NO AND t.DEP = t6.DEP AND t.ARR = t6.ARR AND t.CREATE_TIME = t6.CREATE_TIME AND t6.rnum = 6
    )
    SELECT CREATE_TIME, FLIGHT_DATE, DEP, ARR, FLIGHT_NO, TOP_MEAN, TOP_MAX, TOP_MIN, TOP_STD, FEA_NEAREST_TIME_1, FEA_NEAREST_TIME_2, TOP_1, TOP_2, TOP_3, TOP_4, TOP_5, TOP_6
    FROM t_6
) t