-- 竞飞航班的各项特征
INSERT INTO %1$s (FLIGHT_DATE, AIR_CODE, FLIGHT_NO, DEP_TIME, DEP, ARR, CREATE_TIME, LABEL, TOP_1, TOP_2, TOP_3, TOP_4, TOP_5, TOP_6, FEA_AIR_CODE, FEA_FLIGHT_NO, FEA_DEP, FEA_ARR, EX_DIF, FEA_HR, FEA_HR_CLASS, AIRCODE_CLASS, FEA_NEAREST_TIME_1, FEA_NEAREST_TIME_2, FEA_DELT_DEPTIME, TOP_MEAN, TOP_MAX, TOP_MIN, TOP_STD, FLIGHTS_SELF, FLIGHTS_TOTAL, POWER, FEA_MONTH, FEA_DAY, FEA_WEEK, FLIGHT_TYPE_CODE, SKEY, H_EX_DIF, IST_HOUR, COMP, EQT, FLTNO, DEPTIME, CAP, MAX, BKD, REST, KZL, EQT_CODE, INSERT_DATE, DISTANCE, STD_PRICE, HDLX, OD, HX, ZGLSR, SUM_INCOME, FLY_TIME, CAP_P, CAP_SUM, CAPACITY, COM_MARKET_PER, CAP_C, CAP_COUNT, AREA_MARKET_PER, CAPACITY_P, OD_MARKET_PER, HOLIDAY, YOY_DATE, YOY_BKD, YOY_REST, SINGLE_LEG_TIME, AC_SINGLE_LEG_TIME, LAST_3_FLY_NUM, LAST_3_FLY_COUNT, LAST_3_FLY_RATE, LAST_2_FLY_NUM, LAST_2_FLY_COUNT, LAST_2_FLY_RATE, LAST_1_FLY_NUM, LAST_1_FLY_COUNT, LAST_1_FLY_RATE, FLY_NUM, FLY_COUNT, FLY_RATE, AFTER_1_FLY_NUM, AFTER_1_FLY_COUNT, AFTER_1_FLY_RATE, AFTER_2_FLY_NUM, AFTER_2_FLY_COUNT, AFTER_2_FLY_RATE, AFTER_3_FLY_NUM, AFTER_3_FLY_COUNT, AFTER_3_FLY_RATE, KZL_P, KZL_HIS_1, KZL_HIS_2, KZL_HIS_3, KZL_HIS_4, KZL_HIS_5, KZL_HIS_6, KZL_HIS_7, JF_TYPE, JF_FLIGHT_NO, JF_FEA_HR, JF_COM_MARKET_PER, JF_CAPACITY_P, JF_OD_MARKET_PER, JF_KZL, JF_FEA_FLIGHT_NO, JF_AIRCODE_CLASS, JF_PRICE, PRICE_GAP, JF_SINGLE_LEG_TIME, JF_AC_SINGLE_LEG_TIME, JF_DEP, JF_ARR, JF_FEA_HR_CLASS, JF2_TYPE, JF2_FLIGHT_NO, JF2_FEA_HR, JF2_CAPACITY_P, JF2_AIRCODE_CLASS, JF2_KZL, JF2_PRICE, JF2_SINGLE_LEG_TIME, JF2_AC_SINGLE_LEG_TIME, JF2_DEP, JF2_ARR, JF2_FEA_HR_CLASS, LOWEST_FLIGHT_NO, LOWEST_DEP_TIME, LOWEST_PRICE, FPRICE, CPRICE, JF_FPRICE, JF_CPRICE, JF2_FPRICE, JF2_CPRICE, JF_ID, JF2_ID, ID)
SELECT t.*, %10$s.Nextval
FROM (
    WITH t_0 AS (
        SELECT t.*, (CASE WHEN JF_FLIGHT_NO1 IS NOT NULL THEN t1.fea_hr ELSE -1 END) AS JF_FEA_HR,
            (CASE WHEN JF_FLIGHT_NO1 IS NOT NULL THEN t1.FLIGHT_NO ELSE '' END) AS JF_FLIGHT_NO,
            (CASE WHEN JF_FLIGHT_NO1 IS NOT NULL THEN t1.LABEL ELSE -1 END) AS JF_PRICE,
            (CASE WHEN JF_FLIGHT_NO1 IS NOT NULL THEN t.LABEL-t1.LABEL ELSE -1 END) AS PRICE_GAP,
            (CASE WHEN JF_FLIGHT_NO1 IS NOT NULL THEN t1.SINGLE_LEG_TIME ELSE -1 END) AS JF_SINGLE_LEG_TIME,
            (CASE WHEN JF_FLIGHT_NO1 IS NOT NULL THEN t1.AC_SINGLE_LEG_TIME ELSE -1 END) AS JF_AC_SINGLE_LEG_TIME,
            (CASE WHEN JF_FLIGHT_NO1 IS NOT NULL THEN t1.DEP ELSE '' END) AS JF_DEP,
            (CASE WHEN JF_FLIGHT_NO1 IS NOT NULL THEN t1.ARR ELSE '' END) AS JF_ARR,
            (CASE WHEN JF_FLIGHT_NO1 IS NOT NULL THEN t1.FEA_FLIGHT_NO ELSE -1 END) AS JF_FEA_FLIGHT_NO,
            (CASE WHEN JF_FLIGHT_NO1 IS NOT NULL THEN t1.FEA_HR_CLASS ELSE -1 END) AS JF_FEA_HR_CLASS,
            (CASE WHEN JF_FLIGHT_NO1 IS NOT NULL THEN t1.AIRCODE_CLASS ELSE -1 END) AS JF_AIRCODE_CLASS,
            (CASE WHEN JF_FLIGHT_NO1 IS NOT NULL THEN t1.FPRICE ELSE -1 END) AS JF_FPRICE,
            (CASE WHEN JF_FLIGHT_NO1 IS NOT NULL THEN t1.CPRICE ELSE -1 END) AS JF_CPRICE,
            (CASE WHEN JF_FLIGHT_NO1 IS NOT NULL THEN t1.DEP_TIME ELSE NULL END) AS JF_DEP_TIME,
            (CASE WHEN JF_FLIGHT_NO2 IS NOT NULL THEN t2.FEA_HR ELSE -1 END) AS JF2_FEA_HR,
            (CASE WHEN JF_FLIGHT_NO2 IS NOT NULL THEN t2.FLIGHT_NO ELSE '' END) AS JF2_FLIGHT_NO,
            (CASE WHEN JF_FLIGHT_NO2 IS NOT NULL THEN t2.LABEL ELSE -1 END) AS JF2_PRICE,
            (CASE WHEN JF_FLIGHT_NO2 IS NOT NULL THEN t2.SINGLE_LEG_TIME ELSE -1 END) AS JF2_SINGLE_LEG_TIME,
            (CASE WHEN JF_FLIGHT_NO2 IS NOT NULL THEN t2.AC_SINGLE_LEG_TIME ELSE -1 END) AS JF2_AC_SINGLE_LEG_TIME,
            (CASE WHEN JF_FLIGHT_NO2 IS NOT NULL THEN t2.DEP ELSE '' END) AS JF2_DEP,
            (CASE WHEN JF_FLIGHT_NO2 IS NOT NULL THEN t2.ARR ELSE '' END) AS JF2_ARR,
            (CASE WHEN JF_FLIGHT_NO2 IS NOT NULL THEN t2.FEA_HR_CLASS ELSE -1 END) AS JF2_FEA_HR_CLASS,
            (CASE WHEN JF_FLIGHT_NO2 IS NOT NULL THEN t2.AIRCODE_CLASS ELSE -1 END) AS JF2_AIRCODE_CLASS,
            (CASE WHEN JF_FLIGHT_NO2 IS NOT NULL THEN t2.FPRICE ELSE -1 END) AS JF2_FPRICE,
            (CASE WHEN JF_FLIGHT_NO2 IS NOT NULL THEN t2.CPRICE ELSE -1 END) AS JF2_CPRICE,
            (CASE WHEN JF_FLIGHT_NO2 IS NOT NULL THEN t2.DEP_TIME ELSE NULL END) AS JF2_DEP_TIME
        FROM %2$s t
        LEFT JOIN %3$s t1
        ON t.DEP = t1.DEP AND t.ARR = t1.ARR AND t.FLIGHT_DATE = t1.FLIGHT_DATE AND t.JF_FLIGHT_NO1 = t1.FLIGHT_NO AND t.CREATE_TIME = t1.CREATE_TIME
        LEFT JOIN %3$s t2
        ON t.DEP = t2.DEP AND t.ARR = t2.ARR AND t.FLIGHT_DATE = t2.FLIGHT_DATE AND t.JF_FLIGHT_NO2 = t2.FLIGHT_NO AND t.CREATE_TIME = t2.CREATE_TIME
    ),
    t_1_0 AS (
        SELECT *
        FROM (
            SELECT t1.*, row_number()
                OVER(PARTITION BY FLIGHT_DATE, DEP, ARR, flight_no, create_time ORDER BY create_time DESC) rn
            FROM t_0 t1
        )
        WHERE rn = 1
    ),
    t_1 AS (
        SELECT DEP, ARR, FLIGHT_DATE, FLIGHT_NO, CREATE_TIME, t.EX_DIF, t.LABEL, AIR_CODE, JF_FLIGHT_NO1, JF_FLIGHT_NO2,
            jf_fea_hr, JF_FLIGHT_NO, jf_price, price_gap, jf_single_leg_time, jf_ac_single_leg_time, JF_DEP, DEP_TIME,
            JF_ARR, JF_FEA_FLIGHT_NO, jf_fea_hr_class, jf2_fea_hr, JF2_FLIGHT_NO, jf2_price, jf2_single_leg_time, jf2_ac_single_leg_time,
            JF2_DEP, JF2_ARR, jf2_fea_hr_class, jf2_aircode_class, TOP_1, TOP_2, TOP_3, TOP_4, TOP_5, TOP_6, JF_AIRCODE_CLASS,
            FEA_AIR_CODE, FEA_FLIGHT_NO, FEA_DEP, FEA_ARR, FEA_HR, FEA_HR_CLASS, AIRCODE_CLASS, FEA_NEAREST_TIME_1,
            FEA_NEAREST_TIME_2, FEA_DELT_DEPTIME, TOP_MEAN, TOP_MAX, TOP_MIN, TOP_STD, STD_PRICE, FLIGHTS_SELF, FLIGHTS_TOTAL,
            POWER, FEA_MONTH, FEA_DAY, FEA_WEEK, FLIGHT_TYPE_CODE, SINGLE_LEG_TIME, AC_SINGLE_LEG_TIME, LAST_3_FLY_NUM,
            LAST_3_FLY_COUNT, LAST_3_FLY_RATE, LAST_2_FLY_NUM, LAST_2_FLY_COUNT, LAST_2_FLY_RATE, LAST_1_FLY_NUM,
            LAST_1_FLY_COUNT, LAST_1_FLY_RATE, FLY_NUM, FLY_COUNT, FLY_RATE, AFTER_1_FLY_NUM, AFTER_1_FLY_COUNT,
            AFTER_1_FLY_RATE, AFTER_2_FLY_NUM, AFTER_2_FLY_COUNT, AFTER_2_FLY_RATE, AFTER_3_FLY_NUM, AFTER_3_FLY_COUNT,
            AFTER_3_FLY_RATE, LOWEST_FLIGHT_NO, LOWEST_DEP_TIME, LOWEST_PRICE, DISTANCE, FD_PRICE, DEP_CITY, ARR_CITY,
            SKEY, H_FLIGHT_DATE, INSERT_DATE, COMP, EQT, FLTNO, DEPTIME, ARRTIME, H_DEP, H_ARR, CAP, MAX, H_EX_DIF,
            IST_HOUR, OD, BKD, LKZK, REST, KZL, DEPWEEK, TIME_PD, TRANSIT, HAO, "HOUR", DEP_MINUTE, ARR_TIME,
            ARR_MINUTE, FLYHOURS, HX, "YEAR", "MONTH", WEEK, SUM_INCOME, SUM_INCOME_NEW, HDLX, ZGLSR, HOLIDAY,
            AIR, EQT_CODE, FLIGHTNO, UP_CITY_CODE, DIS_CITY_CODE, HX_CODE, OD_CODE, FLY_TIME, Y_CABIN_P, NUMOFSOLD,
            PRICEARR, FINAL_DEMAND, D0_KZL, DEMAND, D0_PRICEARR, DIFF_IST_HOUR, DETAIL_DIFF_BKD, BKD_SUM, HIS_1, HIS_2,
            HIS_3, HIS_4, HIS_5, HIS_6, HIS_7, YOY_DATE, YOY_BKD, YOY_REST, YOY_BKD_SUM, YOY_DETAIL_DIFF_BKD, YOY_NUMOFSOLD,
            YOY_PRICEARR, YOY_FINAL_DEMAND, YOY_DEMAND, YOY_KZL, YOY_D0_KZL, YOY_BKD_1, YOY_REST_1, YOY_DIFF_BKD_1, YOY_D0_KZL_1,
            YOY_KZL_1, YOY_BKD_2, YOY_REST_2, YOY_DIFF_BKD_2, YOY_D0_KZL_2, YOY_KZL_2, YOY_BKD_3, YOY_REST_3, YOY_DIFF_BKD_3,
            YOY_D0_KZL_3, YOY_KZL_3, DEP_LONGITUDE, DEP_LATITUDE, ARR_LONGITUDE, ARR_LATITUDE, FLY_SEASON, CANCEL_BKG,
            AFFIRM_BKG, CAPACITY_P, CAPACITY, OD_MARKET_PER, CAP_P, CAP_SUM, COM_MARKET_PER, CAP_C, CAP_COUNT, AREA_MARKET_PER,
            KZL_P, KZL_HIS_1, KZL_HIS_2, KZL_HIS_3, KZL_HIS_4, KZL_HIS_5, KZL_HIS_6, KZL_HIS_7, FPRICE, CPRICE, JF_FPRICE, JF_CPRICE,
            JF2_FPRICE, JF2_CPRICE,
            (CASE WHEN JF_DEP_TIME IS NOT NULL THEN ROUND(abs(TO_NUMBER(JF_DEP_TIME - DEP_TIME)) * 24) ELSE -1 END) as jgcha,
            (CASE WHEN JF2_DEP_TIME IS NOT NULL THEN ROUND(abs(TO_NUMBER(JF2_DEP_TIME - DEP_TIME)) * 24) ELSE -1 END) as jgcha2
        FROM t_1_0 t
        LEFT JOIN %4$s t1
        USING (DEP, ARR, FLIGHT_DATE, FLIGHT_NO, CREATE_TIME)
    ),
    t_2 AS (
        SELECT t.*,
           (CASE WHEN JF_FLIGHT_NO1 IS NOT NULL THEN (case when t3.capacity_p is NULL then -1 ELSE t3.capacity_p END) ELSE -1 END) AS jf_capacity_p,
           (CASE WHEN JF_FLIGHT_NO1 IS NOT NULL THEN (case when t3.com_market_per is NULL then -1 ELSE t3.com_market_per END) ELSE -1 END) AS jf_com_market_per,
           (CASE WHEN JF_FLIGHT_NO1 IS NOT NULL THEN (case when t3.od_market_per is NULL then -1 ELSE t3.od_market_per END) ELSE -1 END) AS jf_od_market_per,
           (CASE WHEN JF_FLIGHT_NO2 IS NOT NULL THEN (case when t4.capacity_p is NULL then -1 ELSE t4.capacity_p END) ELSE -1 END) AS jf2_capacity_p
        FROM t_1 t
        LEFT JOIN %5$s t3
        ON t.JF_DEP = t3.DEP AND t.JF_ARR = t3.ARR AND t.FLIGHT_DATE = t3.FLIGHT_DATE AND t.EX_DIF = t3.EX_DIF AND t.AIR_CODE = t3.COMP
        LEFT JOIN %5$s t4
        ON t.JF2_DEP = t4.DEP AND t.JF2_ARR = t4.ARR AND t.FLIGHT_DATE = t4.FLIGHT_DATE AND t.EX_DIF = t4.EX_DIF AND t.AIR_CODE = t4.COMP
    ),
    t_3 AS (
        SELECT ID, FLIGHT_DATE, FLTNO, DEP, ARR, INSERT_DATE, BKG, MAX, (CASE WHEN MAX > 0 THEN BKG/MAX ELSE -1 END) as kzl,
            ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) AS EX_DIF
        FROM %6$s t
        WHERE lengthb(FLTNO) <= 6 AND CAP > 50
        AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) >= %7$s
        AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) <= %8$s
        AND %9$s
    ),
    t_4 AS (
        SELECT t.*
        FROM (
            SELECT t1.*, row_number()
                OVER(PARTITION BY FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, INSERT_DATE ORDER BY INSERT_DATE DESC) rn
            FROM t_3 t1
        ) t
        WHERE t.rn = 1
    ),
    t_5 AS (
        SELECT FLIGHT_DATE, FLTNO, DEP, ARR, CAST(COLLECT(COLLECT_DATE(INSERT_DATE)) AS COLLECT_DATES) AS INSERT_DATES
        FROM t_4
        GROUP BY FLIGHT_DATE, FLTNO, DEP, ARR
    ),
    t_6 AS (
        SELECT t.*, FIND_APPR_TIME(t1.INSERT_DATES, t.CREATE_TIME) as nearTime1,
               FIND_APPR_TIME(t2.INSERT_DATES, t.CREATE_TIME) as nearTime2
        FROM t_2 t
        LEFT JOIN t_5 t1
        ON t.FLIGHT_DATE = t1.FLIGHT_DATE AND t.JF_FLIGHT_NO = t1.FLTNO AND t.DEP = t1.DEP AND t.ARR = t1.ARR
        LEFT JOIN t_5 t2
        ON t.FLIGHT_DATE = t2.FLIGHT_DATE AND t.JF2_FLIGHT_NO = t2.FLTNO AND t.DEP = t2.DEP AND t.ARR = t2.ARR
    ),
    t_7 AS (
        SELECT t.*, t1.kzl as JF_KZL, t1.ID as JF_ID, t2.kzl as JF2_KZL, t2.ID as JF2_ID
        FROM t_6 t
        LEFT JOIN t_4 t1
        ON t.FLIGHT_DATE = t1.FLIGHT_DATE AND t.JF_FLIGHT_NO = t1.FLTNO AND t.DEP = t1.DEP AND t.ARR = t1.ARR AND t.nearTime1 = t1.INSERT_DATE
        LEFT JOIN t_4 t2
        ON t.FLIGHT_DATE = t2.FLIGHT_DATE AND t.JF2_FLIGHT_NO = t2.FLTNO AND t.DEP = t2.DEP AND t.ARR = t2.ARR AND t.nearTime2 = t2.INSERT_DATE
    ),
    t_8 AS (
        SELECT *
        FROM (
            SELECT t.*, row_number() OVER(PARTITION BY FLIGHT_DATE, DEP, ARR, flight_no, create_time ORDER BY create_time DESC) rn
            FROM t_7 t
        )
        WHERE rn = 1
    )
    SELECT FLIGHT_DATE, AIR_CODE, FLIGHT_NO, DEP_TIME, DEP, ARR, CREATE_TIME, LABEL, TOP_1, TOP_2, TOP_3, TOP_4, TOP_5,
        TOP_6, FEA_AIR_CODE, FEA_FLIGHT_NO, FEA_DEP, FEA_ARR, EX_DIF, FEA_HR, FEA_HR_CLASS, AIRCODE_CLASS, FEA_NEAREST_TIME_1,
        FEA_NEAREST_TIME_2, FEA_DELT_DEPTIME, TOP_MEAN, TOP_MAX, TOP_MIN, TOP_STD, FLIGHTS_SELF, FLIGHTS_TOTAL, POWER,
        FEA_MONTH, FEA_DAY, FEA_WEEK, FLIGHT_TYPE_CODE, SKEY, H_EX_DIF, IST_HOUR, COMP, EQT, FLTNO, DEPTIME, CAP, MAX,
        BKD, REST, KZL, EQT_CODE, INSERT_DATE, DISTANCE, STD_PRICE, HDLX, OD, HX, ZGLSR, SUM_INCOME, FLY_TIME, CAP_P,
        CAP_SUM, CAPACITY, COM_MARKET_PER, CAP_C, CAP_COUNT, AREA_MARKET_PER, CAPACITY_P, OD_MARKET_PER, HOLIDAY,
        YOY_DATE, YOY_BKD, YOY_REST, SINGLE_LEG_TIME, AC_SINGLE_LEG_TIME, LAST_3_FLY_NUM, LAST_3_FLY_COUNT, LAST_3_FLY_RATE,
        LAST_2_FLY_NUM, LAST_2_FLY_COUNT, LAST_2_FLY_RATE, LAST_1_FLY_NUM, LAST_1_FLY_COUNT, LAST_1_FLY_RATE, FLY_NUM,
        FLY_COUNT, FLY_RATE, AFTER_1_FLY_NUM, AFTER_1_FLY_COUNT, AFTER_1_FLY_RATE, AFTER_2_FLY_NUM, AFTER_2_FLY_COUNT,
        AFTER_2_FLY_RATE, AFTER_3_FLY_NUM, AFTER_3_FLY_COUNT, AFTER_3_FLY_RATE, KZL_P, KZL_HIS_1, KZL_HIS_2, KZL_HIS_3,
        KZL_HIS_4, KZL_HIS_5, KZL_HIS_6, KZL_HIS_7,
        (CASE WHEN jgcha > -1 and jgcha <= 3 THEN 1 WHEN jgcha > 3 THEN 2 ELSE 0 END) as JF_TYPE,
        JF_FLIGHT_NO, JF_FEA_HR, JF_COM_MARKET_PER, JF_CAPACITY_P, JF_OD_MARKET_PER, JF_KZL, JF_FEA_FLIGHT_NO,
        JF_AIRCODE_CLASS, JF_PRICE, PRICE_GAP, JF_SINGLE_LEG_TIME, JF_AC_SINGLE_LEG_TIME, JF_DEP, JF_ARR, JF_FEA_HR_CLASS,
        (CASE WHEN jgcha2 > -1 and jgcha2 <= 3 THEN 1 WHEN jgcha2 > 3 THEN 2 ELSE 0 END) as JF2_TYPE,
        JF2_FLIGHT_NO, JF2_FEA_HR, JF2_CAPACITY_P, JF2_AIRCODE_CLASS, JF2_KZL, JF2_PRICE, JF2_SINGLE_LEG_TIME,
        JF2_AC_SINGLE_LEG_TIME, JF2_DEP, JF2_ARR, JF2_FEA_HR_CLASS, LOWEST_FLIGHT_NO, LOWEST_DEP_TIME, LOWEST_PRICE, FPRICE,
        CPRICE, JF_FPRICE, JF_CPRICE, JF2_FPRICE, JF2_CPRICE, JF_ID, JF2_ID
    FROM t_8
) t