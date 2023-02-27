t_5 AS (
    SELECT FLIGHT_DATE, ltrim(rtrim(COMP)) as comp, ltrim(rtrim(FLTNO)) as FLTNO, ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) as EX_DIF,
        (CASE WHEN DEP='DAX' THEN 'DZH' ELSE DEP END) AS dep,
        (CASE WHEN ARR='DAX' THEN 'DZH' ELSE ARR END) AS arr,
        (CASE WHEN MAX = 0 THEN 0 ELSE BKG/max END) as KZL,
        A_BKG, B_BKG, C_BKG, D_BKG, E_BKG, F_BKG, G_BKG, H_BKG, I_BKG, J_BKG, K_BKG, L_BKG, M_BKG, N_BKG, O_BKG,
        P_BKG, Q_BKG, R_BKG, S_BKG, T_BKG, U_BKG, V_BKG, W_BKG, X_BKG, Y_BKG, Z_BKG, INSERT_DATE,
        %3$s
    FROM %1$s
    WHERE lengthb(FLTNO) <= 6 AND CAP > 50 AND COMP in (%2$s)
    AND ROUND(TO_NUMBER(TO_DATE(flight_date) - TO_DATE(INSERT_DATE))) = 0
    AND FLIGHT_DATE IN (
        SELECT DISTINCT FLIGHT_DATE FROM %4$s
    )
),
-- 去重
t_6 AS (
    SELECT t.*
    FROM (
        SELECT t1.*, row_number()
             OVER(PARTITION BY FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF ORDER BY INSERT_DATE DESC) rn
        FROM t_5 t1
    ) t
    WHERE t.rn = 1
),