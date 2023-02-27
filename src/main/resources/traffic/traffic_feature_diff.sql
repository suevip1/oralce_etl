-- 计算diff值
INSERT INTO %1$s (FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, IST_HOUR, INSERT_DATE, A_BKG, B_BKG, C_BKG, D_BKG, E_BKG, F_BKG, G_BKG, H_BKG, I_BKG, J_BKG, K_BKG, L_BKG, M_BKG, N_BKG, O_BKG, P_BKG, Q_BKG, R_BKG, S_BKG, T_BKG, U_BKG, V_BKG, W_BKG, X_BKG, Y_BKG, Z_BKG, DIFF_IST_HOUR, DETAIL_DIFF_BKD, BKD_SUM, HIS_1, HIS_2, HIS_3, HIS_4, HIS_5, HIS_6, HIS_7, DETABKG, DETBBKG, DETCBKG, DETDBKG, DETEBKG, DETFBKG, DETGBKG, DETHBKG, DETIBKG, DETJBKG, DETKBKG, DETLBKG, DETMBKG, DETNBKG, DETOBKG, DETPBKG, DETQBKG, DETRBKG, DETSBKG, DETTBKG, DETUBKG, DETVBKG, DETWBKG, DETXBKG, DETYBKG, DETZBKG, ID)
SELECT t.*, %11$s.Nextval
FROM (
    -- 提取计算diff的值
    WITH t_0 AS (
        SELECT FLIGHT_DATE, ltrim(rtrim(FLTNO)) as FLTNO,
            (CASE WHEN ltrim(rtrim(dep))='DAX' THEN 'DZH' ELSE ltrim(rtrim(dep)) END) AS dep,
            (CASE WHEN ltrim(rtrim(arr))='DAX' THEN 'DZH' ELSE ltrim(rtrim(arr)) END) AS arr,
            ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) AS EX_DIF,
            A_BKG, B_BKG, C_BKG, D_BKG, E_BKG, F_BKG, G_BKG, H_BKG, I_BKG, J_BKG, K_BKG, L_BKG, M_BKG, N_BKG, O_BKG,
            P_BKG, Q_BKG, R_BKG, S_BKG, T_BKG, U_BKG, V_BKG, W_BKG, X_BKG, Y_BKG, Z_BKG,
            to_number(to_char(INSERT_DATE, 'hh24')) as IST_HOUR, INSERT_DATE
        FROM %2$s
        WHERE lengthb(ltrim(rtrim(FLTNO))) <= 6 AND CAP > 50 AND ltrim(rtrim(COMP)) in (%3$s)
        AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) >= %4$s
        AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) <= %5$s
        AND %6$s
    ),
    t_1 AS (
        SELECT *
        FROM (
            SELECT t1.*, row_number() OVER(PARTITION BY FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, IST_HOUR ORDER BY INSERT_DATE DESC) rn
            FROM t_0 t1
        ) t1
        WHERE t1.rn = 1
    ),
    t_2 AS (
        SELECT FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, t.IST_HOUR, t.INSERT_DATE, t.A_BKG, t.B_BKG, t.C_BKG,
            t.D_BKG, t.E_BKG, t.F_BKG, t.G_BKG, t.H_BKG, t.I_BKG, t.J_BKG, t.K_BKG, t.L_BKG, t.M_BKG, t.N_BKG, t.O_BKG,
            t.P_BKG, t.Q_BKG, t.R_BKG, t.S_BKG, t.T_BKG, t.U_BKG, t.V_BKG, t.W_BKG, t.X_BKG, t.Y_BKG, t.Z_BKG,
            %8$s(CASE WHEN t1.INSERT_DATE IS NULL THEN -1 ELSE round(TO_NUMBER((t1.INSERT_DATE - t.INSERT_DATE)*24)) END) AS diff_ist_hour
        FROM (
            SELECT * FROM t_1 WHERE IST_HOUR != 23
        ) t
        LEFT JOIN (
            SELECT * FROM t_1 WHERE IST_HOUR = 23
        ) t1
        USING (FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF)
    ),
    t_3 AS (
        SELECT t.FLIGHT_DATE, t.FLTNO, t.DEP, t.ARR, t.EX_DIF, t.IST_HOUR, t.INSERT_DATE, t.A_BKG, t.B_BKG, t.C_BKG,
            t.D_BKG, t.E_BKG, t.F_BKG, t.G_BKG, t.H_BKG, t.I_BKG, t.J_BKG, t.K_BKG, t.L_BKG, t.M_BKG, t.N_BKG, t.O_BKG,
            t.P_BKG, t.Q_BKG, t.R_BKG, t.S_BKG, t.T_BKG, t.U_BKG, t.V_BKG, t.W_BKG, t.X_BKG, t.Y_BKG, t.Z_BKG,
            %8$s(CASE WHEN t1.INSERT_DATE IS NULL THEN -1 ELSE round(TO_NUMBER((t1.INSERT_DATE - t.INSERT_DATE)*24)) END) AS diff_ist_hour
        FROM (
            SELECT * FROM t_1 WHERE IST_HOUR = 23
        ) t
        LEFT JOIN (
            SELECT * FROM t_1 WHERE IST_HOUR = 23
        ) t1
        ON t.FLIGHT_DATE = t1.FLIGHT_DATE AND t.FLTNO = t1.FLTNO AND t.DEP = t1.DEP AND t.ARR = t1.ARR AND t.EX_DIF = (t1.EX_DIF+1)
    ),
    t_4 AS (
        SELECT t.*, %9$s
        FROM (
            SELECT * FROM t_2 UNION ALL SELECT * FROM t_3
        ) t
    ),
    t_5 AS (
        SELECT t.*, %10$s
        FROM t_4 t
    ),
    t_6 AS (
        SELECT *
        FROM t_5
        WHERE IST_HOUR = 23
    ),
    t_7 AS (
        SELECT *
        FROM t_5
        WHERE %7$s
    ),
    t_8 AS (
        SELECT t.*, (CASE WHEN t1.bkd_sum IS NULL THEN -1 ELSE t1.bkd_sum END) AS his_1,
            (CASE WHEN t2.bkd_sum IS NULL THEN -1 ELSE t2.bkd_sum END) AS his_2,
            (CASE WHEN t3.bkd_sum IS NULL THEN -1 ELSE t3.bkd_sum END) AS his_3,
            (CASE WHEN t4.bkd_sum IS NULL THEN -1 ELSE t4.bkd_sum END) AS his_4,
            (CASE WHEN t5.bkd_sum IS NULL THEN -1 ELSE t5.bkd_sum END) AS his_5,
            (CASE WHEN t6.bkd_sum IS NULL THEN -1 ELSE t6.bkd_sum END) AS his_6,
            (CASE WHEN t7.bkd_sum IS NULL THEN -1 ELSE t7.bkd_sum END) AS his_7
        FROM (
            SELECT *
            FROM t_7
            WHERE IST_HOUR != 23
        ) t
        LEFT JOIN t_6 t1
        ON t.FLIGHT_DATE = t1.FLIGHT_DATE AND t.FLTNO = t1.FLTNO AND t.DEP = t1.DEP AND t.ARR = t1.ARR AND t.EX_DIF = (t1.EX_DIF-2)
        LEFT JOIN t_6 t2
        ON t.FLIGHT_DATE = t2.FLIGHT_DATE AND t.FLTNO = t2.FLTNO AND t.DEP = t2.DEP AND t.ARR = t2.ARR AND t.EX_DIF = (t2.EX_DIF-3)
        LEFT JOIN t_6 t3
        ON t.FLIGHT_DATE = t3.FLIGHT_DATE AND t.FLTNO = t3.FLTNO AND t.DEP = t3.DEP AND t.ARR = t3.ARR AND t.EX_DIF = (t3.EX_DIF-4)
        LEFT JOIN t_6 t4
        ON t.FLIGHT_DATE = t4.FLIGHT_DATE AND t.FLTNO = t4.FLTNO AND t.DEP = t4.DEP AND t.ARR = t4.ARR AND t.EX_DIF = (t4.EX_DIF-5)
        LEFT JOIN t_6 t5
        ON t.FLIGHT_DATE = t5.FLIGHT_DATE AND t.FLTNO = t5.FLTNO AND t.DEP = t5.DEP AND t.ARR = t5.ARR AND t.EX_DIF = (t5.EX_DIF-6)
        LEFT JOIN t_6 t6
        ON t.FLIGHT_DATE = t6.FLIGHT_DATE AND t.FLTNO = t6.FLTNO AND t.DEP = t6.DEP AND t.ARR = t6.ARR AND t.EX_DIF = (t6.EX_DIF-7)
        LEFT JOIN t_6 t7
        ON t.FLIGHT_DATE = t7.FLIGHT_DATE AND t.FLTNO = t7.FLTNO AND t.DEP = t7.DEP AND t.ARR = t7.ARR AND t.EX_DIF = (t7.EX_DIF-8)
    ),
    t_9 AS (
        SELECT t.*, (CASE WHEN t1.bkd_sum IS NULL THEN -1 ELSE t1.bkd_sum END) AS his_1,
            (CASE WHEN t2.bkd_sum IS NULL THEN -1 ELSE t2.bkd_sum END) AS his_2,
            (CASE WHEN t3.bkd_sum IS NULL THEN -1 ELSE t3.bkd_sum END) AS his_3,
            (CASE WHEN t4.bkd_sum IS NULL THEN -1 ELSE t4.bkd_sum END) AS his_4,
            (CASE WHEN t5.bkd_sum IS NULL THEN -1 ELSE t5.bkd_sum END) AS his_5,
            (CASE WHEN t6.bkd_sum IS NULL THEN -1 ELSE t6.bkd_sum END) AS his_6,
            (CASE WHEN t7.bkd_sum IS NULL THEN -1 ELSE t7.bkd_sum END) AS his_7
        FROM (
            SELECT *
            FROM t_7
            WHERE IST_HOUR = 23
        ) t
        LEFT JOIN t_6 t1
        ON t.FLIGHT_DATE = t1.FLIGHT_DATE AND t.FLTNO = t1.FLTNO AND t.DEP = t1.DEP AND t.ARR = t1.ARR AND t.EX_DIF = (t1.EX_DIF-1)
        LEFT JOIN t_6 t2
        ON t.FLIGHT_DATE = t2.FLIGHT_DATE AND t.FLTNO = t2.FLTNO AND t.DEP = t2.DEP AND t.ARR = t2.ARR AND t.EX_DIF = (t2.EX_DIF-2)
        LEFT JOIN t_6 t3
        ON t.FLIGHT_DATE = t3.FLIGHT_DATE AND t.FLTNO = t3.FLTNO AND t.DEP = t3.DEP AND t.ARR = t3.ARR AND t.EX_DIF = (t3.EX_DIF-3)
        LEFT JOIN t_6 t4
        ON t.FLIGHT_DATE = t4.FLIGHT_DATE AND t.FLTNO = t4.FLTNO AND t.DEP = t4.DEP AND t.ARR = t4.ARR AND t.EX_DIF = (t4.EX_DIF-4)
        LEFT JOIN t_6 t5
        ON t.FLIGHT_DATE = t5.FLIGHT_DATE AND t.FLTNO = t5.FLTNO AND t.DEP = t5.DEP AND t.ARR = t5.ARR AND t.EX_DIF = (t5.EX_DIF-5)
        LEFT JOIN t_6 t6
        ON t.FLIGHT_DATE = t6.FLIGHT_DATE AND t.FLTNO = t6.FLTNO AND t.DEP = t6.DEP AND t.ARR = t6.ARR AND t.EX_DIF = (t6.EX_DIF-6)
        LEFT JOIN t_6 t7
        ON t.FLIGHT_DATE = t7.FLIGHT_DATE AND t.FLTNO = t7.FLTNO AND t.DEP = t7.DEP AND t.ARR = t7.ARR AND t.EX_DIF = (t7.EX_DIF-7)
    )
    SELECT FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, IST_HOUR, INSERT_DATE, A_BKG, B_BKG, C_BKG, D_BKG, E_BKG, F_BKG, G_BKG,
        H_BKG, I_BKG, J_BKG, K_BKG, L_BKG, M_BKG, N_BKG, O_BKG, P_BKG, Q_BKG, R_BKG, S_BKG, T_BKG, U_BKG, V_BKG, W_BKG,
        X_BKG, Y_BKG, Z_BKG, diff_ist_hour, detail_diff_bkd, bkd_sum, his_1, his_2, his_3, his_4, his_5, his_6, his_7,
        detAbkg, detBbkg, detCbkg, detDbkg, detEbkg, detFbkg, detGbkg, detHbkg, detIbkg, detJbkg, detKbkg, detLbkg, detMbkg,
        detNbkg, detObkg, detPbkg, detQbkg, detRbkg, detSbkg, detTbkg, detUbkg, detVbkg, detWbkg, detXbkg, detYbkg, detZbkg
    FROM (
        SELECT * FROM t_8 UNION ALL SELECT * FROM t_9
    ) t2
) t