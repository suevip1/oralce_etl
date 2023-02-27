-- 计算同一天同航段的航班数量、各航司的航班数量、各航司所占比重
INSERT INTO %1$s (FLIGHT_DATE, DEP, ARR, CREATE_TIME, FLYNUM, FLYCOUNT, FLYRATE, LAST_1_FLY_NUM, LAST_1_FLY_COUNT, LAST_1_FLY_RATE, LAST_2_FLY_NUM, LAST_2_FLY_COUNT, LAST_2_FLY_RATE, LAST_3_FLY_NUM, LAST_3_FLY_COUNT, LAST_3_FLY_RATE, AFTER_1_FLY_NUM, AFTER_1_FLY_COUNT, AFTER_1_FLY_RATE, AFTER_2_FLY_NUM, AFTER_2_FLY_COUNT, AFTER_2_FLY_RATE, AFTER_3_FLY_NUM, AFTER_3_FLY_COUNT, AFTER_3_FLY_RATE, ID)
SELECT t.*, %7$s.Nextval
FROM (
    WITH t_0 AS (
     SELECT trim(BOTH ' ' FROM air_code) air_code,
            decode(ltrim(rtrim(dep)),'DAX','DZH',ltrim(rtrim(dep))) AS dep,
            decode(ltrim(rtrim(arr)),'DAX','DZH',ltrim(rtrim(arr))) AS arr,
            ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(create_time))) AS ex_dif,
            ltrim(rtrim(flight_no)) as flight_no, flight_date, create_time
     FROM %2$s
     WHERE length(ltrim(rtrim(flight_no))) <= 6 AND SHARE_IND != 1
       AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(create_time))) >= %3$s
       AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(create_time))) <= %4$s
       AND %5$s
    ),
    -- 按每天的数据去重
    t_1 AS (
        SELECT air_code, flight_date, dep, arr, flight_no, ex_dif, create_time
        FROM (
            SELECT t1.*, row_number()
                   OVER(PARTITION BY FLIGHT_DATE, DEP, ARR, flight_no, create_time ORDER BY create_time DESC) rn
            FROM t_0 t1
        )
        WHERE rn = 1
    ),
    -- 按航班去重
    t_2 AS (
        SELECT air_code, flight_date, dep, arr, flight_no, ex_dif, create_time
        FROM (
            SELECT t1.*, row_number()
                   OVER(PARTITION BY FLIGHT_DATE, DEP, ARR, flight_no ORDER BY EX_DIF) rn
            FROM t_1 t1
        )
        WHERE rn = 1
    ),
    -- 统计同一天各航段总共的航班
    t_3 AS (
        SELECT flight_date, dep, arr, count(flight_no) as flyCount
        FROM t_2
        GROUP BY flight_date, dep, arr
    ),
    -- 统计各天同一天各航段总共的航班
    t_4 AS (
        SELECT flight_date, dep, arr, create_time, count(flight_no) as flyNum
        FROM t_1
        GROUP BY flight_date, dep, arr, create_time
    ),
    t_5 AS (
        SELECT t.*, ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(create_time))) AS EX_DIF
        FROM (
            SELECT t.*, flyCount, (case when t1.flyCount > 0 then t.flyNum / t1.flyCount else 0 end) as flyRate
            FROM t_4 t
            LEFT JOIN t_3 t1
            ON t.FLIGHT_DATE = t1.FLIGHT_DATE AND t.DEP = t1.DEP AND t.ARR = t1.ARR
        ) t
    ),
    t_6 AS (
        SELECT *
        FROM t_5
        WHERE %6$s
    ),
    t_7 AS (
        SELECT flight_date, dep, arr, create_time, flyCount, flyNum, flyRate, ex_dif
        FROM (
            SELECT *
            FROM (
                SELECT t.*, row_number() OVER(PARTITION BY FLIGHT_DATE, DEP, ARR ORDER BY create_time) rn
                FROM t_5 t
                WHERE t.ex_dif = 0
            )
            WHERE rn = 1
        ) UNION ALL SELECT flight_date, dep, arr, create_time, flyCount, flyNum, flyRate, ex_dif
        FROM (
            SELECT *
            FROM t_5
            WHERE ex_dif > 0
        )
    ),
    t_8 AS (
        SELECT flight_date, DEP, ARR, CAST(COLLECT(COLLECT_DATE(CREATE_TIME)) AS COLLECT_DATES) AS CREATE_TIMES, min(ex_dif) AS min_ex_dif
        FROM t_7
        GROUP BY flight_date, DEP, ARR
    ),
    t_9 AS (
        SELECT t.*, FIND_FLY_TIME(t1.CREATE_TIMES, t.create_time, t.EX_DIF, 1, 0) AS nearCT1
        FROM t_6 t
        LEFT JOIN t_8 t1
        ON t.FLIGHT_DATE = t1.FLIGHT_DATE + 1 and t.DEP = t1.DEP AND t.arr = t1.arr
    ),
    t_10 AS (
        SELECT t.*, FIND_FLY_TIME(t1.CREATE_TIMES, t.create_time, t.EX_DIF, 2, 0) AS nearCT2
        FROM t_9 t
        LEFT JOIN t_8 t1
        ON t.FLIGHT_DATE = t1.FLIGHT_DATE + 2 and t.DEP = t1.DEP AND t.arr = t1.arr
    ),
    t_11 AS (
        SELECT t.*, FIND_FLY_TIME(t1.CREATE_TIMES, t.create_time, t.EX_DIF, 3, 0) AS nearCT3
        FROM t_10 t
        LEFT JOIN t_8 t1
        ON t.FLIGHT_DATE = t1.FLIGHT_DATE + 3 and t.DEP = t1.DEP AND t.arr = t1.arr
    ),
    t_12 AS (
        SELECT t.*, FIND_FLY_TIME(t1.CREATE_TIMES, t.create_time, t.EX_DIF, 1, 1) AS nearCT4
        FROM t_11 t
        LEFT JOIN t_8 t1
        ON t.FLIGHT_DATE = t1.FLIGHT_DATE - 1 and t.DEP = t1.DEP AND t.arr = t1.arr
    ),
    t_13 AS (
        SELECT t.*, FIND_FLY_TIME(t1.CREATE_TIMES, t.create_time, t.EX_DIF, 2, 1) AS nearCT5
        FROM t_12 t
        LEFT JOIN t_8 t1
        ON t.FLIGHT_DATE = t1.FLIGHT_DATE - 2 and t.DEP = t1.DEP AND t.arr = t1.arr
    ),
    t_14 AS (
        SELECT t.*, FIND_FLY_TIME(t1.CREATE_TIMES, t.create_time, t.EX_DIF, 3, 1) AS nearCT6
        FROM t_13 t
        LEFT JOIN t_8 t1
        ON t.FLIGHT_DATE = t1.FLIGHT_DATE - 3 and t.DEP = t1.DEP AND t.arr = t1.arr
    ),
    t_15 AS (
        SELECT t.*, t1.flyNum as last_1_fly_num, t1.flyCount as last_1_fly_count, t1.flyRate as last_1_fly_rate,
            t2.flyNum as last_2_fly_num, t2.flyCount as last_2_fly_count, t2.flyRate as last_2_fly_rate,
            t3.flyNum as last_3_fly_num, t3.flyCount as last_3_fly_count, t3.flyRate as last_3_fly_rate,
            t4.flyNum as AFTER_1_FLY_NUM, t4.flyCount as AFTER_1_FLY_COUNT, t4.flyRate as AFTER_1_FLY_RATE,
            t5.flyNum as AFTER_2_FLY_NUM, t5.flyCount as AFTER_2_FLY_COUNT, t5.flyRate as AFTER_2_FLY_RATE,
            t6.flyNum as AFTER_3_FLY_NUM, t6.flyCount as AFTER_3_FLY_COUNT, t6.flyRate as AFTER_3_FLY_RATE
        FROM t_14 t
        LEFT JOIN t_5 t1
        ON t.FLIGHT_DATE = t1.FLIGHT_DATE + 1 and t.DEP = t1.DEP AND t.arr = t1.arr AND t.nearCT1 = t1.create_time
        LEFT JOIN t_5 t2
        ON t.FLIGHT_DATE = t2.FLIGHT_DATE + 2 and t.DEP = t2.DEP AND t.arr = t2.arr AND t.nearCT2 = t2.create_time
        LEFT JOIN t_5 t3
        ON t.FLIGHT_DATE = t3.FLIGHT_DATE + 3 and t.DEP = t3.DEP AND t.arr = t3.arr AND t.nearCT3 = t3.create_time
        LEFT JOIN t_5 t4
        ON t.FLIGHT_DATE = t4.FLIGHT_DATE - 1 and t.DEP = t4.DEP AND t.arr = t4.arr AND t.nearCT4 = t4.create_time
        LEFT JOIN t_5 t5
        ON t.FLIGHT_DATE = t5.FLIGHT_DATE - 2 and t.DEP = t5.DEP AND t.arr = t5.arr AND t.nearCT5 = t5.create_time
        LEFT JOIN t_5 t6
        ON t.FLIGHT_DATE = t6.FLIGHT_DATE - 3 and t.DEP = t6.DEP AND t.arr = t6.arr AND t.nearCT6 = t6.create_time
    )
    SELECT FLIGHT_DATE, DEP, ARR, CREATE_TIME, FLYNUM, FLYCOUNT, FLYRATE, LAST_1_FLY_NUM, LAST_1_FLY_COUNT, LAST_1_FLY_RATE,
        LAST_2_FLY_NUM, LAST_2_FLY_COUNT, LAST_2_FLY_RATE, LAST_3_FLY_NUM, LAST_3_FLY_COUNT, LAST_3_FLY_RATE,
        AFTER_1_FLY_NUM, AFTER_1_FLY_COUNT, AFTER_1_FLY_RATE, AFTER_2_FLY_NUM, AFTER_2_FLY_COUNT, AFTER_2_FLY_RATE,
        AFTER_3_FLY_NUM, AFTER_3_FLY_COUNT, AFTER_3_FLY_RATE
    FROM t_15
) t