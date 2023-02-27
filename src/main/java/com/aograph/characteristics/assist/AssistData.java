package com.aograph.characteristics.assist;

import com.aograph.characteristics.dao.DataBaseDao;
import com.aograph.characteristics.properties.AssistDataBaseProperties;
import com.aograph.characteristics.utils.FreeMarkHelper;
import com.aograph.characteristics.utils.LogHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.Map;

@Component
public class AssistData {

    @Autowired
    private DataBaseDao mBaseDao;

    @Resource
    private AssistDataBaseProperties assistDataBaseProperties;

    /**
     * 辅助表自增
     * @param assistTable 增加数据的辅助表名
     * @param sourceTable 数据源表名
     * @param assistField 辅助表对应的字段名
     * @param sourceField 数据源表对应的字段名
     * @param field1 条件字段名
     * @param add 是否是累增，0：会搜索整个数据源表，1：从昨天开始搜索
     */
    private void checkAssist(String assistTable, String sourceTable, String assistField, String sourceField, String field1, int add) {
        String sql = "SELECT DISTINCT s.${sourceField}\n" +
                "FROM ${sourceTable} s\n" +
                "WHERE s.${sourceField} not in (select ${assistField} from ${assistTable}) \n" +
                "AND s.${sourceField} is not null";
        if (add == 1)
            sql += " AND TRUNC(${field1}) >= TRUNC(CURRENT_DATE-1)";

        if (sourceTable.contains("ANALY_OTA_PRICE")){
            sql += " AND SHARE_IND != 1";
        }

        sql += "\nORDER BY ${sourceField}";

        String assistSql = "INSERT INTO ${assistTable} (CODE, ${assistField})\n" +
                "SELECT ${assistTable}_CODE.Nextval, ${sourceField} \n" +
                "FROM (" + sql + ")";

        Map<String, String> values = new HashMap<>();
        values.put("assistTable", assistTable);
        values.put("sourceTable", sourceTable);
        values.put("assistField", assistField);
        values.put("sourceField", sourceField);
        values.put("field1", field1);

        String assisSql = convert(assistSql, values);
        LogHelper.log("assisSql is " + assisSql);
        mBaseDao.executeSql(assisSql);
    }

    /**
     * 航段
     * @param sourceTable 数据源
     * @param field1 条件字段
     * @param add 是否是累增，0：会搜索整个数据源表，1：从昨天开始搜索
     */
    public void checkOd(String sourceTable, String field1, int add) {
        String sql = "SELECT DISTINCT concat(dep, arr) as od\n" +
                "FROM ${sourceTable} s\n" +
                "WHERE concat(dep, arr) not in (SELECT od FROM ${assistTable}) \n" +
                "AND concat(dep, arr) is not null";
        if (add == 1)
            sql += " AND TRUNC(${field1}) >= TRUNC(CURRENT_DATE-1)";

        if (sourceTable.contains("ANALY_OTA_PRICE")){
            sql += " AND SHARE_IND != 1";
        }

        sql += "\nORDER BY od";

        String assistOdSql = "INSERT INTO ${assistTable} (code, od)\n" +
                "SELECT ${assistTable}_CODE.Nextval, od \n" +
                "FROM (" + sql + ")";

        Map<String, String> values = new HashMap<>();
        values.put("assistTable", assistDataBaseProperties.getOd());
        values.put("sourceTable", sourceTable);
        values.put("field1", field1);

        String citySql = convert(assistOdSql, values);
        LogHelper.log("citySql is " + citySql);
        mBaseDao.executeSql(citySql);
    }

    /**
     * 航司
     * @param sourceTable 数据源
     * @param sourceField 数据源对应的字段
     * @param field1 条件字段
     * @param add 是否是累增，0：会搜索整个数据源表，1：从昨天开始搜索
     */
    public void checkComp(String sourceTable, String sourceField, String field1, int add) {
        checkAssist(assistDataBaseProperties.getComp(), sourceTable, "comp", sourceField, field1, add);
    }

    // eqt辅助表增量更新
    public void checkEqt(String sourceTable, String field1, int add) {
        checkAssist(assistDataBaseProperties.getEqt(), sourceTable, "eqt", "eqt", field1, add);
    }

    // city辅助表增量更新
    public void checkFltno(String sourceTable, String sourceField, String field1, int add) {
        checkAssist(assistDataBaseProperties.getFltno(), sourceTable, "flt_no", sourceField, field1, add);
    }

    // hx辅助表增量更新
    public void checkHx(String sourceTable, String field1, int add) {
        checkAssist(assistDataBaseProperties.getHx(), sourceTable, "hx", "hx", field1, add);
    }

    /**
     * SingleLegTime表
     * @param sourceTable 数据源
     * @param field1 条件字段
     * @param add 是否是累增，0：会搜索整个数据源表，1：从昨天开始搜索
     */
    public void checkSingleLegTime(String sourceTable, String field1, int add) {
        String sql = "SELECT DISTINCT " +
                (sourceTable.contains("aograph_air_for_model".toUpperCase()) ? "dep || arr || deptime" : "dep || arr || TO_CHAR(DEP_TIME, 'HH24MI')") +
                " as ${assistField}\n" +
                "FROM ${sourceTable} s\n" +
                "WHERE " + (sourceTable.contains("aograph_air_for_model".toUpperCase()) ? "(dep || arr || deptime)" : "(dep || arr || TO_CHAR(DEP_TIME, 'HH24MI'))") +" not in (SELECT ${assistField} FROM ${assistTable})";
        if (add == 1)
            sql += " AND TRUNC(${field1}) >= TRUNC(CURRENT_DATE-1)";

        if (sourceTable.contains("ANALY_OTA_PRICE")){
            sql += " AND SHARE_IND != 1";
        }

        sql += "\nORDER BY ${assistField}";

        String singleSql = "INSERT INTO ${assistTable} (CODE, ${assistField})\n" +
                "SELECT ${assistTable}_CODE.Nextval, ${assistField} \n" +
                "FROM (" + sql + ")";

        Map<String, String> values = new HashMap<>();
        values.put("assistTable", assistDataBaseProperties.getSingle_leg_time());
        values.put("assistField", "SINGLE_LEG_TIME");
        values.put("sourceTable", sourceTable);
        values.put("sourceField", "SINGLE_LEG_TIME");
        values.put("field1", field1);

        String sSql = convert(singleSql, values);
        LogHelper.log("checkSingleLegTime sSql is " + sSql);
        mBaseDao.executeSql(sSql);
    }

    /**
     * AcSingleLegTime表
     * @param sourceTable 数据源
     * @param field1 条件字段
     * @param add 是否是累增，0：会搜索整个数据源表，1：从昨天开始搜索
     */
    public void checkAcSingleLegTime(String sourceTable, String field1, int add) {
        String sql = "SELECT DISTINCT " +
                (sourceTable.contains("aograph_air_for_model".toUpperCase()) ? "comp || dep || arr || deptime" : "air_code || dep || arr || TO_CHAR(DEP_TIME, 'HH24MI')") +
                " as ${assistField}\n" +
                "FROM ${sourceTable} s\n" +
                "WHERE " + (sourceTable.contains("aograph_air_for_model".toUpperCase()) ? "(comp || dep || arr || deptime)" : "(air_code || dep || arr || TO_CHAR(DEP_TIME, 'HH24MI'))") + " not in (SELECT ${assistField} FROM ${assistTable})";
        if (add == 1)
            sql += " AND TRUNC(${field1}) >= TRUNC(CURRENT_DATE-1)";

        if (sourceTable.contains("ANALY_OTA_PRICE")){
            sql += " AND SHARE_IND != 1";
        }

        sql += "\nORDER BY ${assistField}";

        String acSingleSql = "INSERT INTO ${assistTable} (CODE, ${assistField})\n" +
                "SELECT ${assistTable}_CODE.Nextval, ${assistField} \n" +
                "FROM (" + sql + ")";

        Map<String, String> values = new HashMap<>();
        values.put("assistTable", assistDataBaseProperties.getAc_single_leg_time());
        values.put("assistField", "ac_single_leg_time");
        values.put("sourceTable", sourceTable);
        values.put("sourceField", "ac_single_leg_time");
        values.put("field1", field1);

        String acsSql = convert(acSingleSql, values);
        LogHelper.log("checkAcSingleLegTime acsSql is " + acsSql);
        mBaseDao.executeSql(acsSql);
    }

    // city辅助表增量更新，这张表是从airport中产生的
    public void checkCity(String sourceTable, String field1, int add) {
        checkAssist(assistDataBaseProperties.getCity(), sourceTable, "city", "dep", field1, add);
        checkAssist(assistDataBaseProperties.getCity(), sourceTable, "city", "arr", field1, add);
    }

    /**
     * sql字段匹配
     * @param content 字符串
     * @param values 值
     * @return String
     */
    private static String convert(String content, Map<String, ?> values) {
        return FreeMarkHelper.convert(content, values);
    }
}
