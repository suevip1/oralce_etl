-- 计算同航段同天的最低价航班
INSERT INTO %1$s (AIR_CODE, DEP, ARR, EX_DIF, FLIGHT_NO, FLIGHT_DATE, PRICE, CREATE_TIME, DEP_TIME, LOWEST_FLIGHT_NO, LOWEST_DEP_TIME, LOWEST_PRICE, ID)
SELECT t.*, %8$s.Nextval
FROM (
    WITH t_0 AS (
        SELECT trim(BOTH ' ' FROM air_code) air_code,
            decode(ltrim(rtrim(dep)),'DAX','DZH',ltrim(rtrim(dep))) AS dep,
            decode(ltrim(rtrim(arr)),'DAX','DZH',ltrim(rtrim(arr))) AS arr,
            ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(create_time))) AS ex_dif,
            ltrim(rtrim(flight_no)) as flight_no, flight_date, price, dep_time, create_time
        FROM %2$s
        WHERE length(ltrim(rtrim(flight_no))) <= 6 AND SHARE_IND != 1
        AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(create_time))) >= %3$s
        AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(create_time))) <= %4$s
        AND %5$s
    ),
    -- 去重
    t_1 AS (
        SELECT air_code, flight_date, dep, arr, ex_dif, flight_no, dep_time, price, create_time,
            (case when air_code = 'FM' then 'MU' ELSE air_code END) COMP
        FROM (
            SELECT t1.*, row_number()
                OVER(PARTITION BY FLIGHT_DATE, DEP, ARR, flight_no, create_time ORDER BY create_time DESC) rn
            FROM t_0 t1
        )
        WHERE rn = 1
    ),
    -- 集合
    t_2 AS (
        SELECT dep, arr, flight_date, create_time,
            CAST(COLLECT(LOW_FLIGHT_INFO(COMP, flight_no, price, dep_time)) AS LOW_FLIGHT_INFOS) AS flightTuple
        FROM t_1 t
        GROUP BY dep, arr, flight_date, create_time
    ),
    -- 取出预测航司数据
    t_3 AS (
        SELECT air_code, t.flight_date, t.dep, t.arr, ex_dif, flight_no, dep_time, price, t.create_time, COMP, flightTuple
        FROM (
            SELECT * FROM t_1 WHERE air_code in (%6$s)
        ) t
        LEFT JOIN t_2 t1
        ON t.dep = t1.dep AND t.arr=t1.arr AND t.flight_date = t1.flight_date AND t.create_time = t1.create_time
    ),
    t_4 AS (
        SELECT air_code, dep, arr, ex_dif, flight_no, dep_time, flight_date, price, create_time,
            %7$s
        FROM t_3 tb
    )
    SELECT air_code, dep, arr, ex_dif, flight_no, flight_date, price, create_time, dep_time,
        t.lowestFlight.FLIGHT_NO as LOWEST_FLIGHT_NO, t.lowestFlight.DEP_TIME as LOWEST_DEP_TIME,
        t.lowestFlight.PRICE as LOWEST_PRICE
    FROM t_4 t
) t