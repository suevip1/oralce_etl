-- 天级别的数据，运力计算
INSERT INTO %1$s (COMP, FLIGHT_DATE, DEP, ARR, EX_DIF, CAP_P, CAP_SUM, COM_MARKET_PER, CAPACITY_P, CAPACITY, OD_MARKET_PER, CAP_C, CAP_COUNT, AREA_MARKET_PER, ID)
SELECT t.*, %6$s.Nextval
FROM (
    WITH t_0 AS (
        SELECT FLIGHT_DATE, ltrim(rtrim(FLTNO)) as FLTNO,
            (CASE WHEN ltrim(rtrim(dep))='DAX' THEN 'DZH' ELSE ltrim(rtrim(dep)) END) AS dep,
            (CASE WHEN ltrim(rtrim(arr))='DAX' THEN 'DZH' ELSE ltrim(rtrim(arr)) END) AS arr,
            ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) AS EX_DIF, INSERT_DATE,
            ltrim(rtrim(COMP)) as comp, MAX
        FROM %2$s
        WHERE lengthb(ltrim(rtrim(FLTNO))) <= 6 AND CAP > 50
        AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) >= %3$s
        AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) <= %4$s
        AND %5$s
    ),
    -- 去重
    t_1 AS (
        SELECT *
        FROM (
            SELECT t1.*, row_number() OVER(PARTITION BY FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF ORDER BY INSERT_DATE DESC) rn
            FROM t_0 t1
        ) t1
        WHERE t1.rn = 1
    ),
    -- 将东航的三航航司为一个
    t_2 AS (
        SELECT FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, INSERT_DATE, MAX, (case when COMP = 'FM' then 'MU' ELSE COMP END) COMP
        FROM t_1 t
    ),
    t_3 AS (
        SELECT FLIGHT_DATE, COMP, EX_DIF, sum(MAX) AS cap_p
        FROM t_2
        GROUP BY (FLIGHT_DATE, COMP, EX_DIF)
    ),
    t_4 AS (
        SELECT FLIGHT_DATE, COMP, EX_DIF, cap_p, cap_sum,
            (case when cap_sum=0 then 0 else round(cap_p/cap_sum,2) end) AS com_market_per
        FROM t_3 t
        LEFT JOIN (
            SELECT FLIGHT_DATE, EX_DIF, sum(cap_p) AS cap_sum
            FROM t_3
            GROUP BY (FLIGHT_DATE, EX_DIF)
        ) t1
        USING (FLIGHT_DATE, EX_DIF)
    ),
    t_5 AS (
        SELECT FLIGHT_DATE, COMP, DEP, ARR, EX_DIF, sum(MAX) AS capacity_p
        FROM t_2
        GROUP BY (FLIGHT_DATE, COMP, DEP, ARR, EX_DIF)
    ),
    t_6 AS (
        SELECT FLIGHT_DATE, COMP, DEP, ARR, EX_DIF, capacity_p, capacity,
            (case when capacity=0 then 0 else round(capacity_p/capacity, 2) end) AS od_market_per
        FROM t_5 t
        LEFT JOIN (
            SELECT FLIGHT_DATE, DEP, ARR, EX_DIF, sum(capacity_p) AS capacity
            FROM t_5
            GROUP BY (FLIGHT_DATE, DEP, ARR, EX_DIF)
        ) t1
        USING (FLIGHT_DATE, DEP, ARR, EX_DIF)
    ),
    t_7 AS (
        SELECT FLIGHT_DATE, COMP, DEP, EX_DIF, sum(MAX) AS cap_c
        FROM t_2
        GROUP BY (FLIGHT_DATE, COMP, DEP, EX_DIF)
    ),
    t_8 AS (
        SELECT FLIGHT_DATE, COMP, DEP, EX_DIF, cap_c, cap_count,
            (case when cap_count= 0 then 0 else round(cap_c/cap_count,2) end) AS area_market_per
        FROM t_7 t
            LEFT JOIN (
            SELECT FLIGHT_DATE, DEP, EX_DIF, sum(cap_c) AS cap_count
            FROM t_7
            GROUP BY (FLIGHT_DATE, DEP, EX_DIF)
        ) t1
        USING (FLIGHT_DATE, DEP, EX_DIF)
    ),
    t_9 AS (
        SELECT *
        FROM t_6 t1
        LEFT JOIN t_4 t2
        USING (FLIGHT_DATE, COMP, EX_DIF)
        LEFT JOIN t_8 t3
        USING (FLIGHT_DATE, COMP, DEP, EX_DIF)
    )
    SELECT COMP, FLIGHT_DATE, DEP, ARR, EX_DIF, CAP_P, CAP_SUM, COM_MARKET_PER, CAPACITY_P, CAPACITY, OD_MARKET_PER, CAP_C,
        CAP_COUNT, AREA_MARKET_PER
    FROM t_9
) t