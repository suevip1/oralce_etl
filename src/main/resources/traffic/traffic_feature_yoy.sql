INSERT INTO %1$s (FLIGHT_DATE, FLTNO, DEP, ARR, INSERT_DATE, HOLIDAY, MEMO, YOY_DATE, YOY_BKD, YOY_REST, YOY_NUMOFSOLD, YOY_KZL, YOY_DETAIL_DIFF_BKD, YOY_BKD_SUM, YOY_FINAL_DEMAND, YOY_D0_KZL, YOY_DEMAND, YOY_BKD_1, YOY_REST_1, YOY_DIFF_BKD_1, YOY_KZL_1, YOY_BKD_2, YOY_REST_2, YOY_DIFF_BKD_2, YOY_KZL_2, YOY_BKD_3, YOY_REST_3, YOY_DIFF_BKD_3, YOY_KZL_3, YOY_D0_KZL_1, YOY_D0_KZL_2, YOY_D0_KZL_3, YOY_PRICEARR, YOY_CABINS, WOW_FLIGHT_NO, ID)
SELECT t.*, %17$s.Nextval
FROM (
    WITH t_0 AS (
        SELECT FLIGHT_DATE, ltrim(rtrim(COMP)) as comp, ltrim(rtrim(FLTNO)) as FLTNO,
            (CASE WHEN ltrim(rtrim(dep))='DAX' THEN 'DZH' ELSE ltrim(rtrim(dep)) END) AS dep,
            (CASE WHEN ltrim(rtrim(arr))='DAX' THEN 'DZH' ELSE ltrim(rtrim(arr)) END) AS arr,
            to_number(to_char(INSERT_DATE, 'hh24')) as ist_Hour,
            ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) AS EX_DIF, INSERT_DATE
        FROM %2$s t
        WHERE lengthb(ltrim(rtrim(FLTNO))) <= 6 AND CAP > 50 AND ltrim(rtrim(COMP)) in (%3$s)
        AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) >= %4$s
        AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) <= %5$s
        AND %6$s
    ),
    t_1 AS (
        SELECT *
        FROM (
            SELECT t1.*, row_number()
                OVER(PARTITION BY FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, ist_Hour ORDER BY INSERT_DATE DESC) rn
            FROM t_0 t1
        ) t
        WHERE t.rn = 1
    ),
    t_wow_1 AS (
        SELECT t.*, abs(to_date(FLIGHT_DATE_FROM, 'yyyy-MM-dd') - to_date(PEER_FLIGHT_DATE_FROM, 'yyyy-MM-dd')) as diffDays
        FROM %16$s t
        WHERE abs(to_date(FLIGHT_DATE_FROM, 'yyyy-MM-dd') - to_date(PEER_FLIGHT_DATE_FROM, 'yyyy-MM-dd')) < 300
    ),
    t_2_0 AS (
        SELECT FLIGHT_DATE, COMP, FLTNO, DEP, ARR, IST_HOUR, EX_DIF, INSERT_DATE,
            (CASE WHEN t1.DAYS IS NULL THEN 0 ELSE t1.DAYS END) AS HOLIDAY, t1.MEMO,
            (case when t2.diffDays is NULL or t2.diffDays <= 0 THEN 7 ELSE t2.diffDays END) as wowDiffDays,
            t2.PEER_FLIGHT_NO as WOW_FLIGHT_NO, substr(t2.ELINE, 0, 3) as WOW_DEP, substr(t2.ELINE, 4, 3) as WOW_ARR
        FROM t_1 t
        LEFT JOIN %7$s t1
        ON t.FLIGHT_DATE = t1.HOLIDAY
        LEFT JOIN t_wow_1 t2
        ON t.FLIGHT_DATE >= to_date(t2.FLIGHT_DATE_FROM, 'yyyy-MM-dd') and t.FLIGHT_DATE <= to_date(t2.FLIGHT_DATE_TO, 'yyyy-MM-dd')
            and t.FLTNO = t2.FLIGHT_NO and concat(t.DEP, t.ARR) = t2.ELINE
    ),
    t_2_1 AS (
        SELECT t.*,  FLTNO as WOW_FLIGHT_NO, DEP as WOW_DEP, ARR as WOW_ARR
        FROM (
            SELECT FLIGHT_DATE, COMP, FLTNO, DEP, ARR, IST_HOUR, EX_DIF, INSERT_DATE, HOLIDAY, MEMO, wowDiffDays
            FROM t_2_0
            WHERE WOW_FLIGHT_NO IS NULL
        ) t
    ),
    t_2 AS (
        SELECT t.*, t.FLIGHT_DATE - wowDiffDays as YOY_DATE,
            t.FLIGHT_DATE - 1 AS YOY_DATE_1, t.FLIGHT_DATE - 2 AS YOY_DATE_2, t.FLIGHT_DATE - 3 AS YOY_DATE_3
        FROM (
            SELECT * FROM (
                SELECT * FROM t_2_0 WHERE WOW_FLIGHT_NO IS NOT NULL
            ) UNION ALL SELECT * FROM t_2_1
        ) t
    ),
    t_3 AS (
        SELECT FLIGHT_DATE, ltrim(rtrim(COMP)) as COMP, ltrim(rtrim(FLTNO)) as FLTNO, BKG,
            (CASE WHEN ltrim(rtrim(dep))='DAX' THEN 'DZH' ELSE ltrim(rtrim(dep)) END) AS DEP,
            (CASE WHEN ltrim(rtrim(arr))='DAX' THEN 'DZH' ELSE ltrim(rtrim(arr)) END) AS ARR,
            to_number(to_char(INSERT_DATE, 'hh24')) as ist_Hour, max - BKG AS rest,
            ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) AS EX_DIF, INSERT_DATE,
            A_BKG, B_BKG, C_BKG, D_BKG, E_BKG, F_BKG, G_BKG, H_BKG, I_BKG, J_BKG, K_BKG, L_BKG, M_BKG, N_BKG, O_BKG,
            P_BKG, Q_BKG, R_BKG, S_BKG, T_BKG, U_BKG, V_BKG, W_BKG, X_BKG, Y_BKG, Z_BKG,
            (CASE WHEN max = 0 THEN 0 WHEN max = -1 THEN -1 ELSE bkg/max END) AS kzl,
            %9$s
        FROM %2$s
        WHERE FLIGHT_DATE IN (
            SELECT DISTINCT yoy_date
            FROM t_2
        ) AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) >= %4$s
        AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) <= (%5$s + 1)
        AND to_number(to_char(INSERT_DATE, 'hh24')) = 23 AND %8$s
    ),
    t_4_0 AS (
        SELECT *
        FROM (
            SELECT t1.*, row_number()
                OVER(PARTITION BY FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, IST_HOUR ORDER BY INSERT_DATE DESC) rn
            FROM t_3 t1
        ) t
        WHERE t.rn = 1
    ),
    t_4_1 AS (
        SELECT t.*, %10$s
        FROM t_4_0 t
        LEFT JOIN t_4_0 t1
        ON t.FLIGHT_DATE = t1.FLIGHT_DATE AND t.EX_DIF = (t1.EX_DIF + 1) AND t.FLTNO = t1.FLTNO AND t.DEP = t1.DEP AND t.ARR = t1.ARR
    ),
    t_4_2 AS (
        SELECT COMP, FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, BKG, IST_HOUR, REST, INSERT_DATE, A_BKG, B_BKG, C_BKG, D_BKG,
            E_BKG, F_BKG, G_BKG, H_BKG, I_BKG, J_BKG, K_BKG, L_BKG, M_BKG, N_BKG, O_BKG, P_BKG, Q_BKG, R_BKG, S_BKG,
            T_BKG, U_BKG, V_BKG, W_BKG, X_BKG, Y_BKG, Z_BKG, KZL, NUMOFSOLD,
            %11$s
        FROM t_4_1
    ),
    t_4_3 AS (
        SELECT t.*, PRICEARR, CABINS
        FROM t_4_2 t
        LEFT JOIN %15$s t1
        ON t.FLIGHT_DATE = t1.FLIGHT_DATE AND t.FLTNO = t1.FLTNO AND t.DEP = t1.DEP AND t.ARR = t1.ARR AND t.EX_DIF = t1.EX_DIF
    ),
    t_4 AS (
        SELECT COMP, FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, BKG, IST_HOUR, REST, INSERT_DATE, A_BKG, B_BKG, C_BKG, D_BKG,
            E_BKG, F_BKG, G_BKG, H_BKG, I_BKG, J_BKG, K_BKG, L_BKG, M_BKG, N_BKG, O_BKG, P_BKG, Q_BKG, R_BKG, S_BKG, T_BKG,
            U_BKG, V_BKG, W_BKG, X_BKG, Y_BKG, Z_BKG, KZL, NUMOFSOLD, DETAIL_DIFF_BKD, PRICEARR, CABINS,
            %12$s
        FROM t_4_3
    ),
    t_5_0 AS (
        SELECT FLIGHT_DATE, ltrim(rtrim(COMP)) as COMP, ltrim(rtrim(FLTNO)) as FLTNO, BKG,
            (CASE WHEN ltrim(rtrim(dep))='DAX' THEN 'DZH' ELSE ltrim(rtrim(dep)) END) AS DEP,
            (CASE WHEN ltrim(rtrim(arr))='DAX' THEN 'DZH' ELSE ltrim(rtrim(arr)) END) AS ARR,
            to_number(to_char(INSERT_DATE, 'hh24')) as ist_Hour, max - BKG AS rest,
            ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) AS EX_DIF, INSERT_DATE,
            A_BKG, B_BKG, C_BKG, D_BKG, E_BKG, F_BKG, G_BKG, H_BKG, I_BKG, J_BKG, K_BKG, L_BKG, M_BKG, N_BKG, O_BKG,
            P_BKG, Q_BKG, R_BKG, S_BKG, T_BKG, U_BKG, V_BKG, W_BKG, X_BKG, Y_BKG, Z_BKG,
            (CASE WHEN max = 0 THEN 0 WHEN max = -1 THEN -1 ELSE bkg/max END) AS KZL
        FROM %2$s
        WHERE FLIGHT_DATE >= (
            SELECT MIN(YOY_DATE_3) FROM t_2
        ) AND FLIGHT_DATE <= (
            SELECT MAX(FLIGHT_DATE) FROM t_2
        ) AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) >= %4$s
        AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) <= (%5$s + 1)
        AND to_number(to_char(INSERT_DATE, 'hh24')) = 23 AND %8$s
    ),
    t_5_1 AS (
        SELECT *
        FROM (
            SELECT t1.*, row_number()
                OVER(PARTITION BY FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, IST_HOUR ORDER BY INSERT_DATE DESC) rn
            FROM t_5_0 t1
        ) t
        WHERE t.rn = 1
    ),
    t_5_2 AS (
        SELECT t.*,%10$s
        FROM t_5_1 t
        LEFT JOIN t_5_1 t1
        ON t.FLIGHT_DATE = t1.FLIGHT_DATE AND t.EX_DIF = (t1.EX_DIF + 1) AND t.FLTNO = t1.FLTNO AND t.DEP = t1.DEP AND t.ARR = t1.ARR
    ),
    t_5_3 AS (
        SELECT COMP, FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, BKG, IST_HOUR, REST, INSERT_DATE, KZL,
            %11$s
        FROM t_5_2
    ),
    t_5 AS (
        SELECT FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, BKG, IST_HOUR, REST, INSERT_DATE, KZL, %12$s
        FROM t_5_3
    ),
    t_8 AS (
        SELECT t.*, t1.BKG AS YOY_BKD, t1.REST AS YOY_REST, t1.NUMOFSOLD as YOY_NUMOFSOLD, t1.KZL AS YOY_KZL,
            t1.DETAIL_DIFF_BKD AS YOY_DETAIL_DIFF_BKD, t1.BKD_SUM AS YOY_BKD_SUM, t1.A_BKG, t1.B_BKG, t1.C_BKG, t1.D_BKG,
            t1.E_BKG, t1.F_BKG, t1.G_BKG, t1.H_BKG, t1.I_BKG, t1.J_BKG, t1.K_BKG, t1.L_BKG, t1.M_BKG, t1.N_BKG, t1.O_BKG,
            t1.P_BKG, t1.Q_BKG, t1.R_BKG, t1.S_BKG, t1.T_BKG, t1.U_BKG, t1.V_BKG, t1.W_BKG, t1.X_BKG, t1.Y_BKG, t1.Z_BKG,
            t1.PRICEARR as YOY_PRICEARR, t1.CABINS as YOY_CABINS,
            t2.BKG as YOY_BKD_1, t2.REST as YOY_REST_1, t2.BKD_SUM as YOY_DIFF_BKD_1, t2.KZL as YOY_KZL_1,
            t3.BKG as YOY_BKD_2, t3.REST as YOY_REST_2, t3.BKD_SUM as YOY_DIFF_BKD_2, t3.KZL as YOY_KZL_2,
            t4.BKG as YOY_BKD_3, t4.REST as YOY_REST_3, t4.BKD_SUM as YOY_DIFF_BKD_3, t4.KZL as YOY_KZL_3
        FROM (
            SELECT *
            FROM t_2
            WHERE ist_Hour != 23
        ) t
        LEFT JOIN t_4 t1
        ON t.YOY_DATE = t1.FLIGHT_DATE AND t.EX_DIF = t1.EX_DIF - 1 AND t.WOW_FLIGHT_NO = t1.FLTNO AND t.WOW_DEP = t1.DEP AND t.WOW_ARR = t1.ARR
        LEFT JOIN t_5 t2
        ON t.YOY_DATE_1 = t2.FLIGHT_DATE AND t.EX_DIF = t2.EX_DIF - 1 AND t.FLTNO = t2.FLTNO AND t.DEP = t2.DEP AND t.ARR = t2.ARR
        LEFT JOIN t_5 t3
        ON t.YOY_DATE_2 = t3.FLIGHT_DATE AND t.EX_DIF = t3.EX_DIF - 1 AND t.FLTNO = t3.FLTNO AND t.DEP = t3.DEP AND t.ARR = t3.ARR
        LEFT JOIN t_5 t4
        ON t.YOY_DATE_3 = t4.FLIGHT_DATE AND t.EX_DIF = t4.EX_DIF - 1 AND t.FLTNO = t4.FLTNO AND t.DEP = t4.DEP AND t.ARR = t4.ARR
    ),
    t_9 AS (
        SELECT t.*, t1.BKG AS YOY_BKD, t1.REST AS YOY_REST, t1.NUMOFSOLD as YOY_NUMOFSOLD, t1.KZL AS YOY_KZL,
            t1.DETAIL_DIFF_BKD AS YOY_DETAIL_DIFF_BKD, t1.BKD_SUM AS YOY_BKD_SUM, t1.A_BKG, t1.B_BKG, t1.C_BKG, t1.D_BKG,
            t1.E_BKG, t1.F_BKG, t1.G_BKG, t1.H_BKG, t1.I_BKG, t1.J_BKG, t1.K_BKG, t1.L_BKG, t1.M_BKG, t1.N_BKG, t1.O_BKG,
            t1.P_BKG, t1.Q_BKG, t1.R_BKG, t1.S_BKG, t1.T_BKG, t1.U_BKG, t1.V_BKG, t1.W_BKG, t1.X_BKG, t1.Y_BKG, t1.Z_BKG,
            t1.PRICEARR as YOY_PRICEARR, t1.CABINS as YOY_CABINS,
            t2.BKG as YOY_BKD_1, t2.REST as YOY_REST_1, t2.BKD_SUM as YOY_DIFF_BKD_1, t2.KZL as YOY_KZL_1,
            t3.BKG as YOY_BKD_2, t3.REST as YOY_REST_2, t3.BKD_SUM as YOY_DIFF_BKD_2, t3.KZL as YOY_KZL_2,
            t4.BKG as YOY_BKD_3, t4.REST as YOY_REST_3, t4.BKD_SUM as YOY_DIFF_BKD_3, t4.KZL as YOY_KZL_3
        FROM (
            SELECT *
            FROM t_2
            WHERE ist_Hour = 23
        ) t
        LEFT JOIN t_4 t1
        ON t.YOY_DATE = t1.FLIGHT_DATE AND t.EX_DIF = t1.EX_DIF AND t.WOW_FLIGHT_NO = t1.FLTNO AND t.WOW_DEP = t1.DEP AND t.WOW_ARR = t1.ARR
        LEFT JOIN t_5 t2
        ON t.YOY_DATE_1 = t2.FLIGHT_DATE AND t.EX_DIF = t2.EX_DIF AND t.FLTNO = t2.FLTNO AND t.DEP = t2.DEP AND t.ARR = t2.ARR
        LEFT JOIN t_5 t3
        ON t.YOY_DATE_2 = t3.FLIGHT_DATE AND t.EX_DIF = t3.EX_DIF AND t.FLTNO = t3.FLTNO AND t.DEP = t3.DEP AND t.ARR = t3.ARR
        LEFT JOIN t_5 t4
        ON t.YOY_DATE_3 = t4.FLIGHT_DATE AND t.EX_DIF = t4.EX_DIF AND t.FLTNO = t4.FLTNO AND t.DEP = t4.DEP AND t.ARR = t4.ARR
    ),
    t_x0 AS (
        SELECT * FROM t_8 UNION ALL SELECT * FROM t_9
    ),
    t_x1 AS (
        SELECT *
        FROM (
            SELECT t.*, row_number() OVER(PARTITION BY FLIGHT_DATE, FLTNO, DEP, ARR ORDER BY INSERT_DATE DESC) rn
            FROM (
                SELECT FLIGHT_DATE, ltrim(rtrim(FLTNO)) as FLTNO,
                    (CASE WHEN ltrim(rtrim(dep))='DAX' THEN 'DZH' ELSE ltrim(rtrim(dep)) END) AS DEP,
                    (CASE WHEN ltrim(rtrim(arr))='DAX' THEN 'DZH' ELSE ltrim(rtrim(arr)) END) AS ARR, INSERT_DATE,
                    (CASE WHEN max = 0 THEN 0 WHEN max = -1 THEN -1 ELSE bkg/max END) AS kzl, A_BKG, B_BKG, C_BKG, D_BKG,
                    E_BKG, F_BKG, G_BKG, H_BKG, I_BKG, J_BKG, K_BKG, L_BKG, M_BKG, N_BKG, O_BKG,
                    P_BKG, Q_BKG, R_BKG, S_BKG, T_BKG, U_BKG, V_BKG, W_BKG, X_BKG, Y_BKG, Z_BKG,
                    %9$s
                FROM %2$s
                WHERE FLIGHT_DATE IN (
                    SELECT DISTINCT yoy_date
                    FROM t_2
                ) AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) = 0 AND %8$s
            ) t
        )
        WHERE rn = 1
    ),
    t_x2 AS (
        SELECT *
        FROM (
            SELECT t.*, row_number() OVER(PARTITION BY FLIGHT_DATE, FLTNO, DEP, ARR ORDER BY INSERT_DATE DESC) rn
            FROM (
                SELECT FLIGHT_DATE, ltrim(rtrim(FLTNO)) as FLTNO,
                     (CASE WHEN ltrim(rtrim(dep))='DAX' THEN 'DZH' ELSE ltrim(rtrim(dep)) END) AS DEP,
                     (CASE WHEN ltrim(rtrim(arr))='DAX' THEN 'DZH' ELSE ltrim(rtrim(arr)) END) AS ARR,  INSERT_DATE,
                     (CASE WHEN max = 0 THEN 0 WHEN max = -1 THEN -1 ELSE bkg/max END) AS kzl
                FROM %2$s
                WHERE FLIGHT_DATE >= (
                    SELECT MIN(YOY_DATE_3) FROM t_2
                ) AND FLIGHT_DATE <= (
                    SELECT MAX(YOY_DATE_1) FROM t_2
                ) AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) = 0 AND %8$s
            ) t
        )
        WHERE rn = 1
    ),
    t_x3 AS (
        SELECT t.FLIGHT_DATE, t.FLTNO, t.DEP, t.ARR, t.INSERT_DATE, HOLIDAY, MEMO, YOY_DATE, YOY_BKD, YOY_REST, YOY_NUMOFSOLD, YOY_KZL,
            YOY_DETAIL_DIFF_BKD, YOY_BKD_SUM, YOY_BKD_1, YOY_REST_1, YOY_DIFF_BKD_1, YOY_KZL_1, YOY_BKD_2, YOY_REST_2,
            YOY_DIFF_BKD_2, YOY_KZL_2, YOY_BKD_3, YOY_REST_3, YOY_DIFF_BKD_3, YOY_KZL_3, YOY_PRICEARR, YOY_CABINS,
            (CASE WHEN t1.numOfSold IS NULL THEN null ELSE t1.numOfSold END) as YOY_FINAL_DEMAND, WOW_FLIGHT_NO,
            (CASE WHEN t1.numOfSold IS NULL THEN -1 ELSE t1.KZL END) as YOY_D0_KZL, %13$s
            (CASE WHEN t2.kzl IS NULL THEN -1 ELSE t2.kzl END) as YOY_D0_KZL_1,
            (CASE WHEN t3.kzl IS NULL THEN -1 ELSE t3.kzl END) as YOY_D0_KZL_2,
            (CASE WHEN t4.kzl IS NULL THEN -1 ELSE t4.kzl END) as YOY_D0_KZL_3
        FROM t_x0 t
        LEFT JOIN t_x1 t1
        ON t.YOY_DATE = t1.FLIGHT_DATE AND t.WOW_FLIGHT_NO = t1.FLTNO AND t.WOW_DEP = t1.DEP AND t.WOW_ARR = t1.ARR
        LEFT JOIN t_x2 t2
        ON t.YOY_DATE_1 = t2.FLIGHT_DATE AND t.FLTNO = t2.FLTNO AND t.DEP = t2.DEP AND t.ARR = t2.ARR
        LEFT JOIN t_x2 t3
        ON t.YOY_DATE_2 = t3.FLIGHT_DATE AND t.FLTNO = t3.FLTNO AND t.DEP = t3.DEP AND t.ARR = t3.ARR
        LEFT JOIN t_x2 t4
        ON t.YOY_DATE_2 = t4.FLIGHT_DATE AND t.FLTNO = t4.FLTNO AND t.DEP = t4.DEP AND t.ARR = t4.ARR
    )
    SELECT FLIGHT_DATE, FLTNO, DEP, ARR, INSERT_DATE, HOLIDAY, MEMO, YOY_DATE, YOY_BKD, YOY_REST, YOY_NUMOFSOLD, YOY_KZL,
        YOY_DETAIL_DIFF_BKD, YOY_BKD_SUM, YOY_FINAL_DEMAND, YOY_D0_KZL,
        %14$s,
        YOY_BKD_1, YOY_REST_1, YOY_DIFF_BKD_1, YOY_KZL_1, YOY_BKD_2, YOY_REST_2, YOY_DIFF_BKD_2, YOY_KZL_2, YOY_BKD_3,
        YOY_REST_3, YOY_DIFF_BKD_3, YOY_KZL_3, YOY_D0_KZL_1, YOY_D0_KZL_2, YOY_D0_KZL_3, YOY_PRICEARR, YOY_CABINS, WOW_FLIGHT_NO
    FROM t_x3
) t