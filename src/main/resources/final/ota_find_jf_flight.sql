-- 查找竞飞航班
INSERT INTO %1$s (DEP, ARR, FLIGHT_DATE, FLIGHT_NO, CREATE_TIME, EX_DIF, LABEL, JF_FLIGHT_NO1, JF_FLIGHT_NO2, ID)
SELECT t.*, %7$s.Nextval
FROM (
    WITH t_0 AS (
        SELECT t.*, (CASE WHEN AIR_CODE = 'FM' THEN 'MU' ELSE AIR_CODE END) comp
        FROM %2$s t
    ),
    t_1 AS (
        SELECT DEP_CITY, ARR_CITY, FLIGHT_DATE, CREATE_TIME, CAST(COLLECT(OTA_JF_FLIGHT(comp, FLIGHT_NO, DEP, ARR, DEP_TIME, AIRCODE_CLASS, FLIGHT_TYPE_CODE, LABEL)) AS OTA_JF_FLIGHTS) jfFlights
        FROM t_0
        GROUP BY DEP_CITY, ARR_CITY, FLIGHT_DATE, CREATE_TIME
    ),
    t_2 AS (
        SELECT *
        FROM t_0
        WHERE AIR_CODE in (%3$s)
    ),
    t_3 AS (
        SELECT t.*, p.FLIGHT_NO as JF_FLT, SEQ
        FROM %5$s t, %6$s p
        WHERE t.ID = p.LINE_CFG_ID AND FLIGHT_DATE IN (
            SELECT DISTINCT FLIGHT_DATE
            FROM %2$s
        )
    ),
    t_4_0 AS (
        SELECT t.*
        FROM (
            SELECT t1.*, row_number() OVER(PARTITION BY FLIGHT_DATE, FLIGHT_NO, DEP, ARR ORDER BY SEQ) rn
            FROM t_3 t1
        ) t
        WHERE t.rn <= 2
    ),
    t_4_1 AS (
        select FLIGHT_DATE, FLIGHT_NO, DEP, ARR,
           listagg(JF_FLT,',') within group (order by rn) db_flts
        from t_4_0
        GROUP BY FLIGHT_DATE, FLIGHT_NO, DEP, ARR
    ),
    t_4 AS (
        SELECT FLIGHT_DATE, FLIGHT_NO, DEP, ARR, SUBSTR(db_flts, 0, 6) AS JF_FLT1, SUBSTR(db_flts, 8, 6) AS JF_FLT2
        FROM t_4_1
    ),
    t_5 AS (
        SELECT t.*, (case when t1.JF_FLT1 IS NOT NULL THEN t1.JF_FLT1 END) as JF_FLIGHT_NO_1,
               (case when t1.JF_FLT2 IS NOT NULL THEN t1.JF_FLT2 END) as JF_FLIGHT_NO_2
        FROM t_2 t
        LEFT JOIN t_4 t1
        ON t.FLIGHT_DATE = to_date(t1.FLIGHT_DATE, 'yyyy-MM-dd') and t.FLIGHT_NO = t1.FLIGHT_NO and t.DEP = t1.DEP and t.ARR = t1.ARR
    ),
    t_6 AS (
        SELECT t.*, jfFlights, %4$s(jfFlights, t.comp, t.DEP, t.ARR, t.DEP_TIME, t.AIRCODE_CLASS, 'a') AS JF_FLIGHT_NO_f
        FROM t_5 t
        LEFT JOIN t_1 t1
        ON t.FLIGHT_DATE = t1.FLIGHT_DATE AND t.DEP_CITY = t1.DEP_CITY AND t.ARR_CITY = t1.ARR_CITY AND t.CREATE_TIME = t1.CREATE_TIME
    ),
    t_7 AS (
        SELECT t.*, (CASE WHEN JF_FLIGHT_NO_1 IS NOT NULL THEN JF_FLIGHT_NO_1 ELSE JF_FLIGHT_NO_F END)  as JF_FLIGHT_NO1
        FROM t_6 t
    ),
    t_8 AS (
        SELECT DEP, ARR, FLIGHT_DATE, FLIGHT_NO, CREATE_TIME, EX_DIF, LABEL, JF_FLIGHT_NO1, JF_FLIGHT_NO_2,
            %4$s(jfFlights,t.comp, t.DEP, t.ARR, t.DEP_TIME, t.AIRCODE_CLASS, JF_FLIGHT_NO1) AS JF_FLIGHT_NO_F2
        FROM t_7 t
    )
    SELECT DEP, ARR, FLIGHT_DATE, FLIGHT_NO, CREATE_TIME, EX_DIF, LABEL, JF_FLIGHT_NO1,
           (CASE WHEN JF_FLIGHT_NO_2 IS NOT NULL THEN JF_FLIGHT_NO_2 ELSE JF_FLIGHT_NO_F2 END)  as JF_FLIGHT_NO2
    FROM t_8 t
) t