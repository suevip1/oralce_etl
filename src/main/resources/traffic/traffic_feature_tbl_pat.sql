-- 计算运价表各舱位的运价
INSERT INTO %1$s (FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, CABINS, PRICEARR, A_CABIN_PRICE, B_CABIN_PRICE, C_CABIN_PRICE, D_CABIN_PRICE, E_CABIN_PRICE, F_CABIN_PRICE, G_CABIN_PRICE, H_CABIN_PRICE, I_CABIN_PRICE, J_CABIN_PRICE, K_CABIN_PRICE, L_CABIN_PRICE, M_CABIN_PRICE, N_CABIN_PRICE, O_CABIN_PRICE, P_CABIN_PRICE, Q_CABIN_PRICE, R_CABIN_PRICE, S_CABIN_PRICE, T_CABIN_PRICE, U_CABIN_PRICE, V_CABIN_PRICE, W_CABIN_PRICE, X_CABIN_PRICE, Y_CABIN_PRICE, Z_CABIN_PRICE, ID)
SELECT t.*, %11$s.Nextval
FROM (
    WITH t_tp_0 AS (
        SELECT FLIGHT_DATE, DEPTIME, ARRTIME, ltrim(rtrim(FLTNO)) as FLTNO, COMP,
            (CASE WHEN ltrim(rtrim(dep))='DAX' THEN 'DZH' ELSE ltrim(rtrim(dep)) END) AS dep,
            (CASE WHEN ltrim(rtrim(arr))='DAX' THEN 'DZH' ELSE ltrim(rtrim(arr)) END) AS arr,
            ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) AS EX_DIF,
            INSERT_DATE, 1 AS tag, TO_DATE(TO_CHAR(INSERT_DATE, 'yyyy-MM-dd'), 'yyyy-MM-dd') AS ist_date
        FROM %2$s t
        WHERE lengthb(ltrim(rtrim(FLTNO))) <= 6 AND CAP > 50 AND ltrim(rtrim(COMP)) in (%3$s)
        AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) >= %4$s
        AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) <= %5$s
        AND %6$s
    ),
    t_tp_1 AS (
        SELECT DISTINCT *
        FROM (
            SELECT q.tag, regexp_substr(q.cabins, '[^,]+', 1, Level,'i') cabin
            FROM (
                SELECT '%9$s' AS cabins, 1 AS tag
                FROM dual
            ) q
            CONNECT BY LEVEL <= LENGTH(q.cabins) - LENGTH(REGEXP_REPLACE(q.cabins, ',', '')) + 1
            ORDER BY cabin
        )
    ),
    t_tp_2 AS (
        SELECT *
        FROM (
            SELECT FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, INSERT_DATE, IST_DATE, tag
            FROM (
                select t1.*, row_number() OVER(partition by FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF order by INSERT_DATE DESC) rn
                from t_tp_0 t1
            ) t1
            where t1.rn = 1
        ) t
        LEFT JOIN t_tp_1 t1
        USING (tag)
    ),
    t_tp_3 AS (
        SELECT ltrim(rtrim(FLTNO)) as FLTNO, (CASE WHEN ltrim(rtrim(DEPARTURE_3CODE))='DAX' THEN 'DZH' ELSE ltrim(rtrim(DEPARTURE_3CODE)) END) AS DEP,
            (CASE WHEN ltrim(rtrim(ARRIVAL_3CODE))='DAX' THEN 'DZH' ELSE ltrim(rtrim(ARRIVAL_3CODE)) END) AS ARR, AIRLINE_2CODE,
            CABIN, OW_PRICE, CABIN_DESC, to_date(START_DATE) as START_DATE, FLIGHT_DATE_START, FLIGHT_DATE_END, INSERT_DATE, "SOURCE"
        FROM %7$s tpi
        WHERE AIRLINE_2CODE in (%3$s)
        AND %8$s
    ),
    t_tp_4 AS (
        SELECT *
        FROM (
            select t1.*,
                row_number() OVER(partition by FLTNO, DEP, ARR, CABIN, START_DATE, FLIGHT_DATE_START, FLIGHT_DATE_END order by "SOURCE", INSERT_DATE DESC) rn
            from t_tp_3 t1
        ) t1
        where t1.rn = 1
    ),
    t_tp_5 AS (
        SELECT *
        FROM (
            SELECT t.*, FLIGHT_DATE_START, FLIGHT_DATE_END,
                (CASE WHEN t1.CABIN IS NULL THEN t.CABIN ELSE t1.CABIN END) AS cab,
                (CASE WHEN OW_PRICE IS NULL THEN 0 ELSE OW_PRICE END) AS OW_PRICE,
                (CASE WHEN FLIGHT_DATE_START IS NULL THEN 0 ELSE TO_NUMBER(to_date(FLIGHT_DATE) - to_date(FLIGHT_DATE_START)) END) AS diffSDay,
                (CASE WHEN FLIGHT_DATE_END IS NULL THEN 0 ELSE TO_NUMBER(to_date(FLIGHT_DATE_END) - to_date(FLIGHT_DATE)) END) AS diffEDay
            FROM t_tp_2 t
            LEFT JOIN (
                SELECT t.*, SUBSTR(cabin, 1, 1) AS bkg
                FROM t_tp_4 t
            ) t1
            ON t.FLTNO = t1.FLTNO AND t.DEP = t1.DEP AND t.ARR = t1.ARR AND t.ist_date = t1.START_DATE AND t.CABIN = t1.BKG
        )
        WHERE diffEDay >= 0 AND diffSDay >= 0
    ),
    t_tp_6 AS (
        SELECT *
        FROM (
            select t1.*, row_number() over(partition by FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, cabin, cab order by diffSDay) num
            from t_tp_5 t1
        ) t2
        where t2.num = 1
    ),
    t_tp_7 AS (
        SELECT t.*, t1.CABIN AS CABIN1, (CASE WHEN t1.CABIN IS NULL THEN 0 ELSE OW_PRICE END) as OW_PRICE, t.CABIN AS cab
        FROM t_tp_2 t
        LEFT JOIN t_tp_6 t1
        ON t.FLTNO = t1.FLTNO AND t.DEP = t1.DEP AND t.ARR = t1.ARR AND t.ist_date = t1.ist_date AND t.CABIN = t1.CABIN AND t.FLIGHT_DATE = t1.FLIGHT_DATE
    ),
    t_tp_8 AS (
        SELECT FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF,
               listagg(CABIN,',') within group (order by CABIN) CABINS,
               listagg(OW_PRICE,',') within group (order by CABIN) priceArr,
               CAST(COLLECT(PAT_INFO(CABIN, OW_PRICE)) AS PAT_INFOS) AS PATINFOS
        FROM t_tp_7 t
        GROUP BY FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF
    )
    SELECT FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, CABINS, pricearr, %10$s
    FROM t_tp_8 t
) t