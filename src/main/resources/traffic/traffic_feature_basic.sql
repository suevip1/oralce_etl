-- 流量模型的基础特征
INSERT INTO %1$s (FLIGHT_DATE, DEPTIME, ARRTIME, FLTNO, DEP, ARR, ARR2, EQT, CAP, MAX, COMP, A_BKG, B_BKG, C_BKG, D_BKG, E_BKG, F_BKG, G_BKG, H_BKG, I_BKG, J_BKG, K_BKG, L_BKG, M_BKG, N_BKG, O_BKG, P_BKG, Q_BKG, R_BKG, S_BKG, T_BKG, U_BKG, V_BKG, W_BKG, X_BKG, Y_BKG, Z_BKG, INSERT_DATE, HX, "YEAR", "MONTH", WEEK, HD, SUM_INCOME, HDLX, STD_PRICE, FLY_TIME, DISTANCE, SKEY, OD, BKD, EX_DIF, REST, KZL, TIME_PD, TRANSIT, HAO, "HOUR", DEP_MINUTE, ARR_TIME, ARR_MINUTE, IST_HOUR, CANCEL_BKG, AFFIRM_BKG, FLYHOURS, FLY_SEASON, NUMOFSOLD, REAL_BKG, KZL_P, REST_P, CLSN, CLSN_MAX, CLSN_CAP, CLSN_BKD, ID)
SELECT t.*, %10$s.Nextval
FROM (
    -- 取出预测或训练的数据
    WITH t_0 AS (
        SELECT t1.*, (CASE WHEN (DEPTIME IS NULL) OR lengthb(DEPTIME)=0 THEN -1 ELSE to_number(subStr(DEPTIME, 1, 2)) END) as HOUR,
            (CASE WHEN (DEPTIME IS NULL) OR lengthb(DEPTIME)=0 THEN -1 ELSE to_number(subStr(DEPTIME, 3, 2)) END) as minu,
            (CASE WHEN (ARRTIME IS NULL) OR lengthb(ARRTIME)=0 THEN -1 ELSE to_number(subStr(ARRTIME, 1, 2)) END) as aHOUR,
            (CASE WHEN (ARRTIME IS NULL) OR lengthb(ARRTIME)=0 THEN -1 ELSE to_number(subStr(ARRTIME, 3, 2)) END) as aminu,
            (FLTNO || '_' || DEP || '_' || ARR || '_' || DEPDATE) as SKEY,
            (CASE WHEN (DEPTIME IS NULL) OR lengthb(DEPTIME)=0 THEN NULL ELSE depDate||' '||DEPTIME END) AS flight_time,
            (CASE WHEN (ARRTIME IS NULL) OR lengthb(ARRTIME)=0 THEN NULL ELSE depDate||' '||ARRTIME END) AS daoda_time,
            extract(day from FLIGHT_DATE) AS HAO, (CASE WHEN max = 0 THEN 0 WHEN max = -1 THEN -1 ELSE bkd/max END) AS kzl,
            (CASE WHEN lengthb(trimhx) <= 6 THEN 0 WHEN lengthb(trimhx) > 9 THEN -1 WHEN substr(trimhx,1,6)=OD THEN 1 WHEN substr(trimhx,4,6)=OD THEN 2 ELSE 3 END) AS transit,
            (CASE WHEN (DEPTIME IS NULL) OR lengthb(DEPTIME)=0 THEN -1 WHEN to_number(DEPTIME) >= 0 AND to_number(DEPTIME) <= 730 THEN 1 WHEN to_number(DEPTIME) > 730 AND to_number(DEPTIME) <= 830 THEN 2 WHEN to_number(DEPTIME) > 830 AND to_number(DEPTIME) < 2000 THEN 3 WHEN to_number(DEPTIME) >= 2000 AND to_number(DEPTIME) <= 2159 THEN 4 ELSE 5 END) AS time_pd,
            (CASE WHEN (ARRTIME IS NULL) OR lengthb(ARRTIME)=0 THEN -1 WHEN to_number(ARRTIME) >= 0 AND to_number(ARRTIME) <= 730 THEN 1 WHEN to_number(ARRTIME) > 730 AND to_number(ARRTIME) <= 830 THEN 2 WHEN to_number(ARRTIME) > 830 AND to_number(ARRTIME) < 2000 THEN 3 WHEN to_number(ARRTIME) >= 2000 AND to_number(ARRTIME) <= 2159 THEN 4 ELSE 5 END) AS arr_time
        FROM (
            SELECT FLIGHT_DATE, ltrim(rtrim(COMP)) as comp, EQT, ltrim(rtrim(FLTNO)) as FLTNO,
                (CASE lengthb(DEPTIME) WHEN 0 THEN null WHEN 1 THEN '000'||DEPTIME WHEN 2 THEN '00'||DEPTIME WHEN 3 THEN '0'||DEPTIME ELSE DEPTIME END) AS DEPTIME,
                (CASE WHEN ltrim(rtrim(dep))='DAX' THEN 'DZH' ELSE ltrim(rtrim(dep)) END) AS dep,
                (CASE WHEN ltrim(rtrim(arr))='DAX' THEN 'DZH' ELSE ltrim(rtrim(arr)) END) AS arr,
                (CASE WHEN ltrim(rtrim(arr2))='DAX' THEN 'DZH' ELSE ltrim(rtrim(arr2)) END) AS arr2,
                (CASE lengthb(ARRTIME) WHEN 0 THEN null WHEN 1 THEN '000'||ARRTIME WHEN 2 THEN '00'||ARRTIME WHEN 3 THEN '0'||ARRTIME ELSE ARRTIME END) AS ARRTIME,
                CAP, MAX, BKG AS bkd, A_BKG, B_BKG, C_BKG, D_BKG, E_BKG, F_BKG, G_BKG, H_BKG, I_BKG, J_BKG, K_BKG,
                L_BKG, M_BKG, N_BKG, O_BKG, P_BKG, Q_BKG, R_BKG, S_BKG, T_BKG, U_BKG, V_BKG, W_BKG, X_BKG, Y_BKG, Z_BKG,
                INSERT_DATE, HX, "YEAR", "MONTH", WEEK, HD, DISTANCE, STD_PRICE, SUM_INCOME, HDLX, FLY_TIME, CLSN, CLSN_MAX, CLSN_CAP, CLSN_BKD,
                CONCAT(DEP, ARR) AS OD, TO_CHAR(FLIGHT_DATE, 'yyyy-MM-dd') AS depDate, REPLACE(HX,'-','') AS trimhx,
                max - BKG AS rest, to_number(to_char(INSERT_DATE, 'hh24')) as ist_Hour,
                ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) AS EX_DIF
            FROM %2$s t
            WHERE lengthb(ltrim(rtrim(FLTNO))) <= 6 AND CAP > 50 AND ltrim(rtrim(COMP)) in (%3$s)
            AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) >= %4$s
            AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) <= %5$s
            AND %6$s
        ) t1
    ),
    -- 数据去重
    t_1 AS (
        SELECT t.*, (CASE WHEN t.HOUR = -1 THEN -1 ELSE round((t.HOUR * 12 + minu)/5) END) AS dep_minute,
            (CASE WHEN t.aHOUR = -1 THEN -1 ELSE round((t.aHOUR * 12 + aminu)/5) END) AS arr_minute,
            (CASE WHEN flight_time IS null THEN null ELSE to_date(flight_time,'yyyy-mm-dd hh24:mi:ss') END) AS flightTime,
            (CASE WHEN daoda_time IS null THEN null ELSE to_date(daoda_time,'yyyy-mm-dd hh24:mi:ss') END) AS daodaTime,
            %8$s
        FROM (
            SELECT t1.*, row_number()
                 OVER(PARTITION BY FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, ist_Hour ORDER BY INSERT_DATE DESC) rn
            FROM t_0 t1
        ) t
        WHERE t.rn = 1
    ),
    -- 航季标识
    t_2 AS (
        SELECT FLIGHT_DATE, comp, EQT, FLTNO, DEPTIME, t.DEP, t.ARR, ARR2, ARRTIME, CAP, MAX, bkd, A_BKG, B_BKG, C_BKG,
            D_BKG, E_BKG, F_BKG, G_BKG, H_BKG, I_BKG, J_BKG, K_BKG, L_BKG, M_BKG, N_BKG, O_BKG, P_BKG, Q_BKG, R_BKG,
            S_BKG, T_BKG, U_BKG, V_BKG, W_BKG, X_BKG, Y_BKG, Z_BKG, INSERT_DATE, HX, "YEAR", "MONTH", WEEK, HD,
            (case when t.DISTANCE is NULL or t.DISTANCE = 0 THEN t1.DISTANCE ELSE t.DISTANCE END) as DISTANCE,
            STD_PRICE, SUM_INCOME, HDLX, OD, ex_dif, HOUR, cancel_bkg, affirm_bkg, numOfSold, SKEY, kzl, transit,
            time_pd, arr_time, dep_minute, arr_minute, HAO, rest, ist_Hour,
            TO_NUMBER(TO_CHAR(TO_DATE(t2.START_DATE, 'yyyy-MM-dd'), 'MM')) AS season_hour,
            round(TO_NUMBER(t.daodaTime - t.flightTime) * 24, 2) AS diffTime, FLY_TIME, CLSN, CLSN_MAX, CLSN_CAP, CLSN_BKD
        FROM t_1 t
        LEFT JOIN %7$s t1
        ON t.DEP = t1.DEP AND t.ARR = t1.ARR
        LEFT JOIN %9$s t2
        ON t.FLIGHT_DATE >= to_date(t2.START_DATE, 'yyyy-MM-dd') AND t.FLIGHT_DATE <= to_date(t2.END_DATE, 'yyyy-MM-dd')
    ),
    t_3 AS (
        SELECT FLIGHT_DATE, comp, EQT, FLTNO, DEPTIME, DEP, ARR, ARR2, ARRTIME, CAP, MAX, bkd, A_BKG, B_BKG, C_BKG,
            D_BKG, E_BKG, F_BKG, G_BKG, H_BKG, I_BKG, J_BKG, K_BKG, L_BKG, M_BKG, N_BKG, O_BKG, P_BKG, Q_BKG, R_BKG,
            S_BKG, T_BKG, U_BKG, V_BKG, W_BKG, X_BKG, Y_BKG, Z_BKG, INSERT_DATE, HX, "YEAR", "MONTH", WEEK, HD, DISTANCE,
            STD_PRICE, SUM_INCOME, HDLX, OD, ex_dif, HOUR, cancel_bkg, affirm_bkg, numOfSold, SKEY, kzl, transit,
            time_pd, arr_time, dep_minute, arr_minute, HAO, rest, ist_Hour, FLY_TIME, CLSN, CLSN_MAX, CLSN_CAP, CLSN_BKD,
            (CASE WHEN t.diffTime < 0 THEN t.diffTime + 24 ELSE t.diffTime END) AS diffTime,
            (CASE WHEN season_hour >= 3 and season_hour < 10 THEN 1 ELSE 0 end) as fly_season
        FROM t_2 t
    ),
    -- 处理经停航班的bkg
    t_4 AS (
        SELECT FLIGHT_DATE, FLTNO, HX, EX_DIF, INSERT_DATE, SUM(BKD) AS bkg, 1 AS transit
        FROM t_3 t
        WHERE transit = 1 OR transit = 3
        GROUP BY FLIGHT_DATE, FLTNO, HX, EX_DIF, INSERT_DATE
    ),
    t_5 AS (
        SELECT FLIGHT_DATE, FLTNO, HX, EX_DIF, INSERT_DATE, SUM(BKD) AS bkg, 2 AS transit
        FROM t_3 t
        WHERE transit = 2 OR transit = 3
        GROUP BY FLIGHT_DATE, FLTNO, HX, EX_DIF, INSERT_DATE
    ),
    t_6 AS (
        SELECT FLIGHT_DATE, FLTNO, HX, EX_DIF, INSERT_DATE,
            (CASE WHEN t.BKG >= t1.BKG THEN t.BKG ELSE t1.BKG END) AS bkg, 3 AS transit
        FROM t_4 t
        LEFT JOIN t_5 t1
        USING (FLIGHT_DATE, FLTNO, HX, EX_DIF, INSERT_DATE)
    ),
    t_7 AS (
        SELECT *
        FROM t_3 t
        LEFT JOIN (
            SELECT * FROM t_4 UNION ALL SELECT * FROM t_5 UNION ALL SELECT * FROM t_6
        ) t1
        USING (FLIGHT_DATE, FLTNO, HX, EX_DIF, INSERT_DATE, transit)
    ),
    t_8 AS (
        SELECT FLIGHT_DATE, COMP, EQT, FLTNO, DEPTIME, DEP, ARR, ARR2, ARRTIME, CAP, MAX, SUM_INCOME, BKD, INSERT_DATE,
            HX, "YEAR", "MONTH", HAO, WEEK, HD, DISTANCE, STD_PRICE, HDLX, OD, ex_dif, HOUR, A_BKG, B_BKG, C_BKG,
            D_BKG, E_BKG, F_BKG, G_BKG, H_BKG, I_BKG, J_BKG, K_BKG, L_BKG, M_BKG, N_BKG, O_BKG, P_BKG, Q_BKG, R_BKG,
            S_BKG, T_BKG, U_BKG, V_BKG, W_BKG, X_BKG, Y_BKG, Z_BKG, SKEY, kzl, transit, time_pd, arr_time, dep_minute,
            arr_minute, rest, ist_Hour, fly_season, CLSN, CLSN_MAX, CLSN_CAP, CLSN_BKD,
            (CASE WHEN diffTime IS NULL THEN FLY_TIME ELSE diffTime END) AS flyhours, FLY_TIME,
            (CASE WHEN bkg IS NULL THEN bkd ELSE bkg END) AS real_bkg, cancel_bkg, affirm_bkg, numOfSold
        FROM t_7 t
    ),
    t_9 AS (
        SELECT t.*
        FROM (
            SELECT t1.*, row_number() OVER(PARTITION BY FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, ist_Hour ORDER BY INSERT_DATE DESC) rn
            FROM t_8 t1
        ) t
        WHERE t.rn = 1
    )
    SELECT FLIGHT_DATE, DEPTIME, ARRTIME, FLTNO, DEP, ARR, ARR2, EQT, CAP, MAX, COMP, A_BKG, B_BKG, C_BKG,
        D_BKG, E_BKG, F_BKG, G_BKG, H_BKG, I_BKG, J_BKG, K_BKG, L_BKG, M_BKG, N_BKG, O_BKG, P_BKG, Q_BKG, R_BKG, S_BKG,
        T_BKG, U_BKG, V_BKG, W_BKG, X_BKG, Y_BKG, Z_BKG, INSERT_DATE, HX, "YEAR", "MONTH", WEEK, HD, SUM_INCOME, HDLX,
        STD_PRICE, FLY_TIME, DISTANCE, SKEY, OD, BKD, EX_DIF, REST, KZL, TIME_PD, TRANSIT, HAO, "HOUR", DEP_MINUTE,
        ARR_TIME, ARR_MINUTE, IST_HOUR, CANCEL_BKG, AFFIRM_BKG, FLYHOURS, FLY_SEASON, NUMOFSOLD, REAL_BKG,
        (CASE WHEN cap = 0 THEN 0 ELSE real_bkg/cap END) AS kzl_p, cap - real_bkg as REST_P,
        CLSN, CLSN_MAX, CLSN_CAP, CLSN_BKD
    FROM t_9
) t