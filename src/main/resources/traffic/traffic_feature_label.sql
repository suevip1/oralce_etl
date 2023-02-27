-- label级别的特征
INSERT INTO %1$s (FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, IST_HOUR, INSERT_DATE, DIFF_IST_HOUR, DETAIL_DIFF_BKD, BKD_SUM, HIS_1, HIS_2, HIS_3, HIS_4, HIS_5, HIS_6, HIS_7, Y_CABIN_P, SUM_INCOME_NEW, CABINS, PRICEARR, DETAIL_DIFF_BKG_1, DETAIL_PRICE, DETAIL_BKD, ID)
SELECT t.*, %5$s.Nextval
FROM (
    WITH t_0 AS (
        SELECT *
        FROM %2$s
    ),
    t_1 AS (
        SELECT *
        FROM t_0 t
        LEFT JOIN %3$s t1
        USING (FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF)
    )
    SELECT FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, IST_HOUR, INSERT_DATE, diff_ist_hour, detail_diff_bkd, bkd_sum, his_1,
        his_2, his_3, HIS_4, HIS_5, HIS_6, HIS_7, y_cabin_p, sum_income_new, cabins, priceArr,
        t.ddff.DIFFBKGS AS detail_diff_bkg_1, t.ddff.PRICES AS detail_price, t.ddff.DETAILBKGS AS detail_bkd
    FROM (
        SELECT FLIGHT_DATE, FLTNO, DEP, ARR, EX_DIF, IST_HOUR, INSERT_DATE, diff_ist_hour, detail_diff_bkd, bkd_sum, his_1,
            his_2, his_3, HIS_4, HIS_5, HIS_6, HIS_7, y_cabin_price AS y_cabin_p, cabins, priceArr,
            %4$s
        FROM t_1
    ) t
) t