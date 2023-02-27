-- 计算基础特征
INSERT INTO %1$s (AIR_CODE, DEP, ARR, FLIGHT_DATE, FLIGHT_NO, CREATE_TIME, EX_DIF, DEP_CITY, ARR_CITY, DEP_TIME, ARR_TIME, LABEL, STD_PRICE, FLIGHT_TYPE_CODE, FPRICE, CPRICE, FEA_DELT_DEPTIME, AIRCODE_CLASS, FLIGHTS_SELF, FEA_HR, FEA_MONTH, FEA_DAY, FEA_WEEK, FEA_AIR_CODE, FEA_FLIGHT_NO, DISTANCE, SINGLE_LEG_TIME, AC_SINGLE_LEG_TIME, FEA_DEP, FEA_ARR, FEA_HR_CLASS, ID)
SELECT t.*, %12$s.Nextval
FROM (
    WITH t_0 AS (
        SELECT *
        FROM (
            SELECT trim(BOTH ' ' FROM air_code) air_code,
                decode(ltrim(rtrim(dep)), 'DAX', 'DZH', ltrim(rtrim(dep))) AS dep,
                decode(ltrim(rtrim(arr)), 'DAX', 'DZH', ltrim(rtrim(arr))) AS arr,
                ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(create_time))) AS ex_dif,
                ltrim(rtrim(flight_no)) as flight_no, flight_date, dep_time, arr_time, dep_city, arr_city, price, std_price,
                (CASE WHEN TRANS_TIME > 0 THEN 2 ELSE 1 END) AS flight_type_code,
                create_time, fprice, cprice, ROUND((dep_time - create_time) * 24) as hourCha, to_char(dep_time, 'HH24:MI') as hr,
                to_char(dep_time, 'HH24MI') as hm
            FROM %2$s
            WHERE length(ltrim(rtrim(flight_no))) <= 6 AND SHARE_IND != 1
            AND %3$s
            AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(create_time))) >= %4$s AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(create_time))) <= %5$s
        )
    ),
    -- 去重
    t_1 AS (
        SELECT air_code, flight_date, dep, arr, flight_no, ex_dif, create_time, dep_time, arr_time, fprice, cprice,
            dep_city, arr_city, price, std_price, (CASE WHEN hourCha > 24 THEN 24 ELSE hourCha END) as fea_delt_deptime,
            GET_AIR_CLASS(air_code) as aircode_class, flight_type_code,
            (to_number(substr(hr,1,2)) * 60 + to_number(substr(hr,4,5)) + 1) fea_hr,
            to_number(to_char(dep_time,'mm')) as fea_month,
            to_number(to_char(dep_time,'dd'))  fea_day,
            to_number(to_char(dep_time,'d')) - 1 fea_week,
            dep || arr || hm as single_leg_time,
            air_code || dep || arr || hm as ac_single_leg_time
        FROM (
            SELECT t1.*, row_number()
                OVER(PARTITION BY FLIGHT_DATE, DEP, ARR, flight_no, create_time ORDER BY create_time DESC) rn
            FROM t_0 t1
        )
        WHERE rn = 1
    ),
    -- 统计各航司每天的航班
    t_2 AS (
        SELECT air_code, flight_date, dep, arr, ex_dif, create_time, COUNT(flight_no) as flights_self
        FROM t_1 t
        GROUP BY air_code, flight_date, dep, arr, ex_dif, create_time
    ),
    -- 合并各辅助表的code
    t_4 AS (
        SELECT t.*, flights_self, t4.code as fea_air_code, t5.code as fea_flight_no,
            t6.DISTANCE as DISTANCE, t7.code as single_leg_time_code, t8.code as ac_single_leg_time_code, t9.code as fea_dep,
            tx.code as fea_arr
        FROM t_1 t
        LEFT JOIN t_2 t1
        ON t.create_time = t1.create_time AND t.FLIGHT_DATE = t1.FLIGHT_DATE AND t.DEP = t1.DEP AND t.ARR = t1.ARR AND t.ex_dif = t1.ex_dif AND t.air_code = t1.air_code
        LEFT JOIN %6$s t4
        on t.air_code = t4.comp
        LEFT JOIN %7$s t5
        on t.flight_no = t5.flt_no
        LEFT JOIN %8$s t6
        ON t.DEP = t6.DEP AND t.ARR = t6.ARR
        LEFT JOIN %9$s t7
        ON t.single_leg_time = t7.single_leg_time
        LEFT JOIN %10$s t8
        ON t.ac_single_leg_time = t8.ac_single_leg_time
        LEFT JOIN %11$s t9
        on t.dep = t9.city
        LEFT JOIN %11$s tx
        on t.arr = tx.city
    ),
    t_5 AS (
        SELECT *
        FROM (
            SELECT t1.*, row_number()
                 OVER(PARTITION BY FLIGHT_DATE, DEP, ARR, flight_no, create_time ORDER BY create_time DESC) rn
            FROM t_4 t1
        )
        WHERE rn = 1
    )
    SELECT air_code, dep, arr, flight_date, flight_no, create_time, ex_dif, dep_city, arr_city, dep_time, arr_time,
        (case when price > 0 THEN price ELSE (case when cprice > 0 then cprice else fprice end) end) as label, std_price,
        flight_type_code, fprice, cprice, fea_delt_deptime, aircode_class, flights_self, fea_hr, fea_month, fea_day,
        fea_week, fea_air_code, fea_flight_no, DISTANCE, single_leg_time_code as single_leg_time,
        ac_single_leg_time_code as ac_single_leg_time, fea_dep, fea_arr,
        (CASE WHEN fea_hr >= 630 and fea_hr < 1080 THEN 1
            WHEN (fea_hr >= 540 and fea_hr < 630) or (fea_hr >= 1080 and fea_hr < 1200) THEN 2
            WHEN (fea_hr >= 480 and fea_hr < 540) or (fea_hr >= 1200 and fea_hr < 1380) THEN 3 ELSE 4 END) fea_hr_class
    FROM t_5 t
) t