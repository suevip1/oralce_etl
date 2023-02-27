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
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
public class ReadFlightScheduleJob extends AbstractFileJob {
    @Resource(name = "oracleDataSource")
    private DataSource oracleDataSource;

    @XxlJob(value ="readFlightScheduleJob")
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

        processFiles(new ContainFilter("schedule"), history);
    }

    public void processFile(File file) throws IOException {
        LogHelper.log("process file: " + file.getName());

        parseFile(file);
    }

    // 将数据插入表中
    private void insertData(List<Map<String, Object>> list) {
        JdbcTemplate template = getJdbcTemplate();

        createSequence(template, "ORG_MKT_FLIGHT_SCHEDULE", "ORG_MKT_FLIGHT_SCHEDULE_SEQ");

        Timestamp createTime = new Timestamp(System.currentTimeMillis());
        for (int i = 0; i < list.size(); i++) {
            Map<String, Object> m = list.get(i);
            m.put("CREATE_TIME", createTime);
        }

        String sql = "INSERT INTO ORG_MKT_FLIGHT_SCHEDULE " +
                "(ID, FLIGHT_NO, ARR_TIME, AIR_CODE, LEG_IND, LEG_FLAG, ORG_FLIGHT_NO, SERVICE_LEVEL, WEEKS, " +
                "DEP_DATE, DEP_TIME, DEP_TERM, ARR_TERM, DEP, ARR, EQT, CABINS, CABINS_ADJ, MEAL, D_OR_I, EQT_AIR_CODE, " +
                "MEMO, LAYOUT, F_CAP, C_CAP, Y_CAP, CAP, VALID, ELINE, CREATE_TIME) " +
                "VALUES(ORG_MKT_FLIGHT_SCHEDULE_SEQ.nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String[] names = sql.substring(sql.indexOf(",") + 1, sql.indexOf(")")).split(",");
        for (int i = 0; i < names.length; i++) {
            names[i] = names[i].trim();
        }
        batchInsert(sql, names, list);
    }

    // 解析文件
    private List<Map<String, Object>> parseFile(File file) throws IOException {
        List<Map<String, Object>> list = new ArrayList<>();

        SimpleDateFormat format1 = new SimpleDateFormat("yyyy/MM/dd");
        SimpleDateFormat format2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

        int total = 0;

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

            Date date = null;
            try {
                date = format1.parse(cols[0].substring(0, cols[0].indexOf(" ")));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            record.put("FLIGHT_NO", cols[1].trim() + cols[2].trim() + cols[9]);
            record.put("ORG_FLIGHT_NO", cols[1].trim() + cols[8].trim());
            record.put("ARR_TIME", cols[13].substring(cols[13].indexOf(" ")+1,  cols[13].lastIndexOf(":")));    // HH:mm
            record.put("AIR_CODE", cols[1].trim());
            record.put("LEG_IND", cols[6].trim());
            record.put("LEG_FLAG", cols[7]);
            record.put("DEP_DATE", cols[5].substring(0, cols[5].indexOf(" ")).replaceAll("/", "-"));
            record.put("DEP_TIME", cols[12].substring(cols[12].indexOf(" ")+1,  cols[12].lastIndexOf(":")));    // HH:mm
            record.put("DEP", cols[3]);
            record.put("ARR", cols[4]);
            record.put("SERVICE_LEVEL", cols[10]);
            record.put("WEEKS", cols[11]);
            record.put("DEP_TERM", cols[20]);
            record.put("ARR_TERM", cols[21]);
            record.put("EQT", cols[22]);
            record.put("CABINS", cols[23]);
            record.put("CABINS_ADJ", cols[24]);
            record.put("MEAL", cols[25]);
            record.put("D_OR_I", cols[27]);
            record.put("EQT_AIR_CODE", cols[28]);
            record.put("MEMO", cols[37]);
            record.put("LAYOUT", cols[41]);
            record.put("F_CAP", cols[42]);
            record.put("C_CAP", cols[43]);
            record.put("Y_CAP", cols[44]);
            record.put("CAP", cols[45]);
            record.put("VALID", cols[46]);
            record.put("ELINE", cols[47]);
            try {
                record.put("CREATE_TIME", format2.parse(cols[50].substring(0, cols[50].lastIndexOf("."))));
            } catch (ParseException e) {
                e.printStackTrace();
            }

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

    private String normalize(String t) {
        if (t.length() == 5) {
            return t;
        }

        int i = t.indexOf(":");
        if (i == 1) {
            t = "0" + t;
        }
        if (t.length() == 5) {
            return t;
        }
        t = t.substring(0, 3) + "0" + t.substring(t.length() - 1);
        return t;
    }
}
