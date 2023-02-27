package com.aograph.characteristics.control;

import com.aograph.characteristics.dao.DataBaseDao;
import com.aograph.characteristics.properties.SourceDataBaseProperties;
import com.aograph.characteristics.utils.ConstantUtil;
import com.aograph.characteristics.utils.LogHelper;
import com.aograph.characteristics.utils.Util;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Map;

/**
 * @Package: com.aograph.characteristics.control
 * @Author: tangqipeng
 * @CreateTime: 2022/12/23 13:42
 * @Description:
 */
@Component
public class SupplementHisFareService {

    // 数据库操作
    @Autowired
    private DataBaseDao mDataBaseDao;


    //数据源配置
    @Resource
    private SourceDataBaseProperties mSourceDataBaseProperties;

    public void supplementHisFare(int source, int add_hx) throws Exception {
        String addHxCondition = " AND CONCAT(DEPARTURE_3CODE, ARRIVAL_3CODE) NOT IN (\n" +
                "\tSELECT DISTINCT CONCAT(DEP, ARR) OD\n" +
                "\tFROM " + mSourceDataBaseProperties.getFdl() + "\n" +
                ")";
        if (add_hx == ConstantUtil.DEFAULT)
            addHxCondition = "";
        String minDate = getPatMinDate(addHxCondition, source);
        String maxDate = getPatMaxDate(addHxCondition, source);
        if (minDate == null || "".equals(minDate) || maxDate == null || "".equals(maxDate))
            throw new Exception("运价表没有数据");

        String beginDate = "2019-12-31";
        if (beginDate.compareTo(minDate) <= 0) {
            String conditionStr = "to_date('" + minDate + "', 'yyyy-MM-dd') + 1";
            conditionStr += addHxCondition;
            String mathSymbol = "-";
            int diffDays = Util.daysBetween(beginDate, minDate);
            LogHelper.log("minDate is " + minDate + "; diffDays is " + diffDays);
            supplementPat(mathSymbol, conditionStr, diffDays);
        }

        String endDate = Util.stampToDateString(System.currentTimeMillis());
        if (maxDate.compareTo(endDate) <= 0) {
            String conditionStr = "to_date('" + maxDate + "', 'yyyy-MM-dd') - 1";
            conditionStr += addHxCondition;
            String mathSymbol = "+";
            int diffDays = Util.daysBetween(maxDate, endDate);
            LogHelper.log("maxDate is " + maxDate + "; diffDays is " + diffDays);
            supplementPat(mathSymbol, conditionStr, diffDays);
        }

        if (minDate.compareTo(maxDate) < 0) {
            int cunDiff = Util.daysBetween(minDate, maxDate);
            int addDays = cunDiff / 2;
            String conditionStr = "to_date('" + minDate + "', 'yyyy-MM-dd') + 1";
            conditionStr += addHxCondition;
            String mathSymbol = "+";
            LogHelper.log("minDate is " + minDate + "; addDays is " + addDays);
            supplementPat(mathSymbol, conditionStr, addDays);
        }

        if (minDate.compareTo(maxDate) < 0) {
            int cunDiff = Util.daysBetween(minDate, maxDate);
            int addDays = cunDiff / 2;
            int redDays = cunDiff - addDays;
            String conditionStr = "to_date('" + maxDate + "', 'yyyy-MM-dd') - 1";
            conditionStr += addHxCondition;
            String mathSymbol = "-";
            LogHelper.log("maxDate is " + maxDate + "; redDays is " + redDays);
            supplementPat(mathSymbol, conditionStr, redDays);
        }

    }


    public String getPatMinDate(String addHxCondition, int source) {
        String minDateSql = "SELECT to_char(min(START_DATE) - 1, 'yyyy-MM-dd') as START_DATE FROM " + mSourceDataBaseProperties.getAograph_tbl_pat() + " WHERE SOURCE = " + source + addHxCondition;
        LogHelper.log("minDateSql sql is " + minDateSql);
        Map<String, Object> map = mDataBaseDao.selectMapBySql(minDateSql);
        if (map != null && map.get("START_DATE") != null) {
            return map.get("START_DATE").toString();
        }
        return null;
    }

    public String getPatMaxDate(String addHxCondition, int source) {
        String minDateSql = "SELECT to_char(max(START_DATE) + 1, 'yyyy-MM-dd') as END_DATE FROM " + mSourceDataBaseProperties.getAograph_tbl_pat() + " WHERE SOURCE = " + source + addHxCondition;
        LogHelper.log("minDateSql sql is " + minDateSql);
        Map<String, Object> map = mDataBaseDao.selectMapBySql(minDateSql);
        if (map != null && map.get("END_DATE") != null) {
            return map.get("END_DATE").toString();
        }
        return null;
    }

    public void supplementPat(String mathSymbol, String conditionStr, int diffDays) {
        String supplementSql = "SELECT FLTNO, DEPARTURE_3CODE, ARRIVAL_3CODE, AIRLINE_2CODE, CABIN, OW_PRICE, CABIN_DESC, START_DATE " + mathSymbol + " %1$s AS START_DATE, FLIGHT_DATE_START " + mathSymbol + " %1$s AS FLIGHT_DATE_START, ADD_MONTHS((FLIGHT_DATE_START" + mathSymbol + " %1$s), 12) as FLIGHT_DATE_END, (INSERT_DATE - 1/24) AS INSERT_DATE, FLIGHT_DATE_END2, SOURCE\n" +
                "FROM " + mSourceDataBaseProperties.getAograph_tbl_pat() + "\n" +
                "WHERE to_date(START_DATE) = " + conditionStr;
        LogHelper.log("diffDays is " + diffDays);
        for (int i = 1; i <= diffDays; i++) {
            String supplementHisSql = String.format(supplementSql, i + "");
            String insertSql = "Insert into " + mSourceDataBaseProperties.getAograph_tbl_pat() + "(FLTNO, DEPARTURE_3CODE, ARRIVAL_3CODE, AIRLINE_2CODE, CABIN, OW_PRICE, CABIN_DESC, START_DATE, FLIGHT_DATE_START, FLIGHT_DATE_END, INSERT_DATE, FLIGHT_DATE_END2, SOURCE, ID) \n" +
                    "SELECT t.*,ASSIST_TBL_PAT_INFO_SEQ.Nextval  FROM (" + supplementHisSql + ") t";
            LogHelper.log("insertSql is " + insertSql);
            mDataBaseDao.executeSql(insertSql);
        }
    }

}
