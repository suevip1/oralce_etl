-- 天级别的历史客座率
INSERT INTO %1$s (FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, KZL_HIS_1, KZL_HIS_2, KZL_HIS_3, KZL_HIS_4, KZL_HIS_5, KZL_HIS_6, KZL_HIS_7, ID)
SELECT t.*, %8$s.Nextval
FROM (
    WITH t_0 AS (
        SELECT FLIGHT_DATE, ltrim(rtrim(FLTNO)) as FLTNO,
            (CASE WHEN ltrim(rtrim(dep))='DAX' THEN 'DZH' ELSE ltrim(rtrim(dep)) END) AS dep,
            (CASE WHEN ltrim(rtrim(arr))='DAX' THEN 'DZH' ELSE ltrim(rtrim(arr)) END) AS arr,
            ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) AS EX_DIF,
            (CASE WHEN max = 0 THEN 0 WHEN max = -1 THEN -1 ELSE BKG/max END) AS kzl, INSERT_DATE
        FROM %2$s
        WHERE lengthb(ltrim(rtrim(FLTNO))) <= 6 AND CAP > 50 AND ltrim(rtrim(COMP)) in (%3$s)
        AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) >= %4$s
        AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) <= %5$s
        AND %6$s
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
    t_2 AS (
        SELECT *
        FROM t_1
        WHERE %7$s
    )
    -- 历史七天的kzl
    SELECT t.FLIGHT_DATE, t.FLTNO, t.DEP, t.ARR, t.EX_DIF,
        (CASE WHEN t1.KZL IS NULL THEN -1 ELSE t1.KZL END) AS kzl_his_1,
        (CASE WHEN t2.KZL IS NULL THEN -1 ELSE t2.KZL END) AS kzl_his_2,
        (CASE WHEN t3.KZL IS NULL THEN -1 ELSE t3.KZL END) AS kzl_his_3,
        (CASE WHEN t4.KZL IS NULL THEN -1 ELSE t4.KZL END) AS kzl_his_4,
        (CASE WHEN t5.KZL IS NULL THEN -1 ELSE t5.KZL END) AS kzl_his_5,
        (CASE WHEN t6.KZL IS NULL THEN -1 ELSE t6.KZL END) AS kzl_his_6,
        (CASE WHEN t7.KZL IS NULL THEN -1 ELSE t7.KZL END) AS kzl_his_7
    FROM t_2 t
    LEFT JOIN t_1 t1
    ON t.FLIGHT_DATE = t1.FLIGHT_DATE AND t.FLTNO = t1.FLTNO AND t.DEP = t1.DEP
    AND t.ARR = t1.ARR AND (t.EX_DIF+1)=t1.EX_DIF
    LEFT JOIN t_1 t2
    ON t.FLIGHT_DATE = t2.FLIGHT_DATE AND t.FLTNO = t2.FLTNO AND t.DEP = t2.DEP
    AND t.ARR = t2.ARR AND (t.EX_DIF+2)=t2.EX_DIF
    LEFT JOIN t_1 t3
    ON t.FLIGHT_DATE = t3.FLIGHT_DATE AND t.FLTNO = t3.FLTNO AND t.DEP = t3.DEP
    AND t.ARR = t3.ARR AND (t.EX_DIF+3)=t3.EX_DIF
    LEFT JOIN t_1 t4
    ON t.FLIGHT_DATE = t4.FLIGHT_DATE AND t.FLTNO = t4.FLTNO AND t.DEP = t4.DEP
    AND t.ARR = t4.ARR AND (t.EX_DIF+4)=t4.EX_DIF
    LEFT JOIN t_1 t5
    ON t.FLIGHT_DATE = t5.FLIGHT_DATE AND t.FLTNO = t5.FLTNO AND t.DEP = t5.DEP
    AND t.ARR = t5.ARR AND (t.EX_DIF+5)=t5.EX_DIF
    LEFT JOIN t_1 t6
    ON t.FLIGHT_DATE = t6.FLIGHT_DATE AND t.FLTNO = t6.FLTNO AND t.DEP = t6.DEP
    AND t.ARR = t6.ARR AND (t.EX_DIF+6)=t6.EX_DIF
    LEFT JOIN t_1 t7
    ON t.FLIGHT_DATE = t7.FLIGHT_DATE AND t.FLTNO = t7.FLTNO AND t.DEP = t7.DEP
    AND t.ARR = t7.ARR AND (t.EX_DIF+7)=t7.EX_DIF
) t