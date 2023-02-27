package com.aograph.characteristics.jobHandler;

import com.alibaba.fastjson.JSONObject;
import com.aograph.characteristics.utils.LogHelper;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.io.*;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
public class ReadFlightInfoJob extends AbstractFileJob {
    @Resource(name = "oracleDataSource")
    private DataSource oracleDataSource;

    @XxlJob(value ="readFlightInfoJob")
    public void run(String myparam) throws Exception {
        String param = XxlJobHelper.getJobParam();
        if (StringUtils.isNotBlank(myparam)) {
            param = myparam;
        }
        JSONObject map = new JSONObject();
        if (StringUtils.isNotBlank(param)) {
            map = JSONObject.parseObject(param);
        }
        boolean history = "true".equals(map.getString("history"));

        processFiles(new ContainFilter("info"), history);
    }

    public void processFile(File file) throws IOException {
        LogHelper.log("process file: " + file.getName());

        parseFile(file);
    }

    // 将数据插入表中
    private void insertData(List<Map<String, Object>> list) {
        JdbcTemplate template = getJdbcTemplate();

        createSequence(template, "ORG_FLIGHT_INFO", "ORG_FLIGHT_INFO_SEQ");

        Timestamp createTime = new Timestamp(System.currentTimeMillis());
        for (int i = 0; i < list.size(); i++) {
            Map<String, Object> m = list.get(i);
            m.put("CREATE_TIME", createTime);
        }

        String sql = "INSERT INTO ORG_FLIGHT_INFO\n" +
                "(ID, FLIGHT_NO, AIR_CODE, LEG_IND, EQT, F_CAP, C_CAP, Y_CAP, CAP, DISTANCE, LANE_CD, LANE_TYPE, " +
                "LANE_AREA, SEG_NBR, CANCELLED, DEP, ARR, DEP_DATE, DEP_TIME, ARR_DATE, ARR_TIME, ORG_FLIGHT_NO, ELINE, " +
                "F_OPN, C_OPN, Y_OPN, CLSN, NATION_FLAG, AREA_FLAG, FLT_SALE_TYPE, LANE_NAME, EQT_COST, FLT_MONTH, " +
                "ERROR_TYPE_CD, ROUTE_SHARED_FLAG, Y_PRICE, F_PAX_QTY, C_PAX_QTY, Y_PAX_QTY, PAX_QTY_SEG, PAX_QTY_LEG, " +
                "GOSHOW_PAX_QTY, NOSHOW_PAX_QTY, GROUP_PAX_QTY, FLT_INCOME, FLT_INCOME_SEG, FLT_INCOME_LEG, FLT_CNT, " +
                "TOTAL_CAP_QTY_LEG, CAP_QTY_SEG, SEAT_KM_SEG, SEAT_KM_LEG, PAX_KM_SEG, PAX_KM_LEG, TOTAL_COST, " +
                "PAX_QTY_KPI, PAX_QTY_INCOME_KPI, CAP_QTY_KPI, PREDICT_PAX_QTY, PREDICT_INCOME, PREDICT_CAP_QTY, " +
                "PAX_QTY_FFP, FFP_INCOME, PAX_QTY_CORP, CORP_INCOME, F_PAX_QTY_LEG, C_PAX_QTY_LEG, Y_PAX_QTY_LEG, " +
                "F_FLT_INCOME, C_FLT_INCOME, Y_FLT_INCOME, F_FLT_INCOME_LEG, C_FLT_INCOME_LEG, Y_FLT_INCOME_LEG, " +
                "PERF_FLT_INCOME, PERF_PROFIT, MARGINAL_CONTRIBUTION, VARIABLE_COST, LANE_DISTANCE, INTER_PAX_QTY, " +
                "INTER_FARE_AMT, INTER_AVG_PRICE, P2P_PAX_QTY, P2P_FARE_AMT, P2P_AVG_PRICE, INTER_ALL_FARE_AMT, " +
                "TAX_YQ_AMT, BAG_OVER_WEIGHT, ACTUAL_BAG_OVER_WEIGHT, BAG_OVER_FEE, ACTUAL_BAG_OVER_FEE, UPGRADE_FEE, " +
                "PLATINUM_INCOME, GOLD_INCOME, SILVER_INCOME, STANDARD_INCOME, POINT_INCOME, ACR_POINT_VAL, " +
                "RDM_POINT_VAL, ACR_POINT_AMT, RDM_POINT_AMT, GROUP_PENALTY, RASK_CLUSTER, LF_CLUSTER, SEASONALITY, " +
                "GRP_TICKETED_QTY, GRP_CONFIRMED_QTY, CREATE_TIME)\n" +
                "VALUES(ORG_FLIGHT_INFO_SEQ.nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                "?, ?, ?, ?, ?, ?, ?)";

        String[] names = sql.substring(sql.indexOf(",") + 1, sql.indexOf(")")).split(",");
        for (int i = 0; i < names.length; i++) {
            names[i] = names[i].trim();
        }
        batchInsert(sql, names, list);
    }

    // 解析文件
    private List<Map<String, Object>> parseFile(File file) throws IOException {
        List<Map<String, Object>> list = new ArrayList<>();

        Date now = new Date(System.currentTimeMillis());

        int total = 0;

        SimpleDateFormat format1 = new SimpleDateFormat("yyyy/MM/dd");
        SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd");

        InputStream inputStream = new FileInputStream(file);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
        CSVReader csvReader = new CSVReaderBuilder(bufferedReader).build();
        Iterator<String[]> iterator = csvReader.iterator();
        iterator.next();    // title line
        while (iterator.hasNext()) {
            String[] cols = iterator.next();
            // 最后一行为无用数据
            if (cols.length < 3) {
                break;
            }

            // 去掉多余的空格
            for (int i = 0; i < cols.length; i++) {
                cols[i] = cols[i].trim();
            }

            Map<String, Object> record = new HashMap<>();
            list.add(record);

            record.put("FLIGHT_NO", cols[1].trim() + cols[2].trim() + cols[22]);
            record.put("AIR_CODE", cols[1]);
            record.put("DEP", cols[3]);
            record.put("ARR", cols[4]);
            record.put("LEG_IND", cols[5]);
            record.put("EQT", cols[6]);
            record.put("F_CAP", cols[7]);
            record.put("C_CAP", cols[8]);
            record.put("Y_CAP", cols[9]);
            record.put("CAP", cols[10]);
            record.put("DISTANCE", cols[11]);
            record.put("LANE_CD", cols[12]);
            record.put("LANE_TYPE", cols[13]);
            record.put("LANE_AREA", cols[14]);
            record.put("SEG_NBR", cols[17]);
            record.put("CANCELLED", cols[18]);
            Calendar cal = Calendar.getInstance();
            java.util.Date date = null;
            try {
                date = format1.parse(cols[0].substring(0, cols[0].indexOf(" ")));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            cal.setTime(date);
            cal.add(Calendar.DAY_OF_YEAR, Integer.parseInt(cols[27]));
            record.put("DEP_DATE", format2.format(cal.getTime()));
            record.put("DEP_TIME", cols[19].substring(cols[19].indexOf(" ") + 1, cols[19].lastIndexOf(":")));   // HH:mm
            cal.setTime(date);
            cal.add(Calendar.DAY_OF_YEAR, Integer.parseInt(cols[28]));
            record.put("ARR_DATE", format2.format(cal.getTime()));
            record.put("ARR_TIME", cols[20].substring(cols[20].indexOf(" ") + 1, cols[20].lastIndexOf(":")));   // HH:mm
            record.put("ORG_FLIGHT_NO", cols[35].trim() + cols[21]);
            record.put("ELINE", cols[23]);
            record.put("F_OPN", cols[24]);
            record.put("C_OPN" , cols[25]);
            record.put("Y_OPN", cols[26]);
            record.put("CLSN", cols[29].endsWith("/") ? cols[29].substring(0, cols[29].length() - 1) : cols[29]);   // 去掉最后的/
            record.put("NATION_FLAG", cols[30]);
            record.put("AREA_FLAG", cols[31]);
            record.put("FLT_SALE_TYPE", cols[32]);
            record.put("LANE_NAME", cols[33]);
            record.put("EQT_COST", cols[36]);
            record.put("FLT_MONTH", cols[37]);
            record.put("ERROR_TYPE_CD", cols[38]);
            record.put("ROUTE_SHARED_FLAG", cols[39]);
            record.put("Y_PRICE", cols[40]);
            record.put("F_PAX_QTY", cols[41]);
            record.put("C_PAX_QTY", cols[42]);
            record.put("Y_PAX_QTY", cols[43]);
            record.put("PAX_QTY_SEG", cols[44]);
            record.put("PAX_QTY_LEG", cols[45]);
            record.put("GOSHOW_PAX_QTY", cols[46]);
            record.put("NOSHOW_PAX_QTY", cols[47]);
            record.put("GROUP_PAX_QTY", cols[48]);
            record.put("FLT_INCOME", cols[49]);
            record.put("FLT_INCOME_SEG", cols[50]);
            record.put("FLT_INCOME_LEG", cols[51]);
            record.put("FLT_CNT", cols[52]);
            record.put("TOTAL_CAP_QTY_LEG", cols[53]);
            record.put("CAP_QTY_SEG", cols[54]);
            record.put("SEAT_KM_SEG", cols[55]);
            record.put("SEAT_KM_LEG", cols[56]);
            record.put("PAX_KM_SEG", cols[57]);
            record.put("PAX_KM_LEG", cols[58]);
            record.put("TOTAL_COST", cols[59]);
            record.put("PAX_QTY_KPI", cols[60]);
            record.put("PAX_QTY_INCOME_KPI", cols[61]);
            record.put("CAP_QTY_KPI", cols[62]);
            record.put("PREDICT_PAX_QTY", cols[63]);
            record.put("PREDICT_INCOME", cols[64]);
            record.put("PREDICT_CAP_QTY", cols[65]);
            record.put("PAX_QTY_FFP", cols[66]);
            record.put("FFP_INCOME", cols[67]);
            record.put("PAX_QTY_CORP", cols[68]);
            record.put("CORP_INCOME", cols[69]);
            record.put("F_PAX_QTY_LEG", cols[70]);
            record.put("C_PAX_QTY_LEG", cols[71]);
            record.put("Y_PAX_QTY_LEG", cols[72]);
            record.put("F_FLT_INCOME", cols[73]);
            record.put("C_FLT_INCOME", cols[74]);
            record.put("Y_FLT_INCOME", cols[75]);
            record.put("F_FLT_INCOME_LEG", cols[76]);
            record.put("C_FLT_INCOME_LEG", cols[77]);
            record.put("Y_FLT_INCOME_LEG", cols[78]);
            record.put("PERF_FLT_INCOME", cols[79]);
            record.put("PERF_PROFIT", cols[80]);
            record.put("MARGINAL_CONTRIBUTION", cols[81]);
            record.put("VARIABLE_COST", cols[82]);
            record.put("LANE_DISTANCE", cols[83]);
            record.put("INTER_PAX_QTY", cols[84]);
            record.put("INTER_FARE_AMT", cols[85]);
            record.put("INTER_AVG_PRICE", cols[86]);
            record.put("P2P_PAX_QTY", cols[87]);
            record.put("P2P_FARE_AMT", cols[88]);
            record.put("P2P_AVG_PRICE", cols[89]);
            record.put("INTER_ALL_FARE_AMT", cols[90]);
            record.put("TAX_YQ_AMT", cols[91]);
            record.put("BAG_OVER_WEIGHT", cols[92]);
            record.put("ACTUAL_BAG_OVER_WEIGHT", cols[93]);
            record.put("BAG_OVER_FEE", cols[94]);
            record.put("ACTUAL_BAG_OVER_FEE", cols[95]);
            record.put("UPGRADE_FEE", cols[96]);
            record.put("PLATINUM_INCOME", cols[97]);
            record.put("GOLD_INCOME", cols[98]);
            record.put("SILVER_INCOME", cols[99]);
            record.put("STANDARD_INCOME", cols[100]);
            record.put("POINT_INCOME", cols[101]);
            record.put("ACR_POINT_VAL", cols[102]);
            record.put("RDM_POINT_VAL", cols[103]);
            record.put("ACR_POINT_AMT", cols[104]);
            record.put("RDM_POINT_AMT", cols[105]);
            record.put("GROUP_PENALTY", cols[106]);
            record.put("RASK_CLUSTER", cols[107]);
            record.put("LF_CLUSTER", cols[108]);
            record.put("SEASONALITY", cols[109]);
            record.put("GRP_TICKETED_QTY", cols[110]);
            record.put("GRP_CONFIRMED_QTY", cols[111]);
            record.put("CREATE_TIME", now);

            total++;

            if (list.size() == 10000) {
                LogHelper.log("insert data: " + list.size());

                insertData(list);
                list.clear();
            }
        }

        if (list.size() > 0) {
            insertData(list);
        }

        LogHelper.log("total: " + total);
        return list;
    }
}
