package com.aograph.characteristics.control;

import com.aograph.characteristics.dao.DataBaseDao;
import com.aograph.characteristics.properties.AssistDataBaseProperties;
import com.aograph.characteristics.utils.LogHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Map;

/**
 * @Package: com.aograph.characteristics.control
 * @Author: tangqipeng
 * @CreateTime: 2023/1/3 17:58
 * @Description:
 */
@Component
public class HolidayTransitService {
    public static final int RESET = 0;
    public static final int APPEND = 1;

    @Autowired
    private DataBaseDao mDataBaseDao;

    //辅助表配置
    @Resource
    private AssistDataBaseProperties mAssistDataBaseProperties;

    /**
     * 假期表添加数据
     * @param reset 是否重制，0：否；1：是
     */
    public void addHolidayDate(int reset) {
        String beginDate = "2018-01-01";
        if (reset == 0) {
            String bDate = getStartDate();
            if (bDate == null || "".equals(bDate))
                bDate = "2018-01-01";
            beginDate = bDate;
        } else {
            String deleteSql = "TRUNCATE TABLE " + mAssistDataBaseProperties.getHoliday();
            mDataBaseDao.executeSql(deleteSql);
        }
        String addHolidaySql = "INSERT INTO " + mAssistDataBaseProperties.getHoliday() + " (HOLIDAY, DAYS, MEMO, ID)\n" +
                "SELECT HOLIDAY, JIAQI AS DAYS, MEMO, ASSIST_HOLIDAY_SEQ.Nextval\n" +
                "FROM (\n" +
                "\tSELECT TO_DATE(HOLIDAY, 'yyyy-MM-dd') AS HOLIDAY, DAYS, MEMO, OVERFLOW, SEQ, \n" +
                "\t\t(CASE WHEN (OVERFLOW > 0 AND SEQ < 0) THEN DAYS + 1 WHEN (OVERFLOW > 0 AND SEQ > DAYS) THEN DAYS + 1 \n" +
                "\t\t\tWHEN (OVERFLOW = 0 AND SEQ = 1) THEN DAYS WHEN (OVERFLOW = 0 AND SEQ > 1 AND SEQ < DAYS) THEN  DAYS - 1 ELSE DAYS END) AS JIAQI\n" +
                "\tFROM " + mAssistDataBaseProperties.getHoliday_cfg() + " \n" +
                "\tWHERE TO_DATE(HOLIDAY, 'yyyy-MM-dd') > trunc(TO_DATE('" + beginDate + "', 'yyyy-MM-dd')) AND ELINE = '*' \n" +
                ")";
        LogHelper.log("addHolidaySql is " + addHolidaySql);
        mDataBaseDao.executeSql(addHolidaySql);
    }

    private String getStartDate() {
        String maxDateSql = "SELECT to_char(max(HOLIDAY), 'yyyy-MM-dd') as START_DATE FROM " + mAssistDataBaseProperties.getHoliday();
//        LogHelper.log("maxDateSql sql is " + maxDateSql);
        Map<String, Object> map = mDataBaseDao.selectMapBySql(maxDateSql);
        if (map != null && map.get("START_DATE") != null) {
            return map.get("START_DATE").toString();
        }
        return null;
    }

}
