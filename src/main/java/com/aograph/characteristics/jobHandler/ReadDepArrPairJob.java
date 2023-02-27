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
import java.text.SimpleDateFormat;
import java.util.*;

@Component
public class ReadDepArrPairJob extends AbstractFileJob {
    @Resource(name = "oracleDataSource")
    private DataSource oracleDataSource;

    @XxlJob(value ="readDepArrPairJob")
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

        processFiles(new ContainFilter("od_pair"), history);
    }

    public void processFile(File file) throws IOException {
        LogHelper.log("process file: " + file.getName());

        List<Map<String, Object>> list = parseFile(file);
        if (list != null) {
            JdbcTemplate template = getJdbcTemplate();

            createSequence(template, "ASSIST_AIRPORT_OD_PAIR", "ASSIST_AIRPORT_OD_PAIR_SEQ");

            // 批量插入
            for (int i = 0; i < list.size(); i += 10000) {
                insertData(list.subList(i, Math.min(i + 10000, list.size())));
            }

            LogHelper.log("insert data: " + list.size());
        }
    }

    // 将数据插入表中
    private void insertData(List<Map<String, Object>> list) {
        // 重复数据无意义
        String sql = "MERGE INTO ASSIST_AIRPORT_OD_PAIR a\n" +
                "USING (select ? as DEP, ? as ARR, ? as DEP_CITY, ? as ARR_CITY, ? as DISTANCE from dual) b " +
                "ON ( a.DEP=b.DEP and a.ARR=b.ARR)\n" +
                "WHEN NOT MATCHED THEN\n" +
                "  INSERT (ID, DEP, ARR, DEP_CITY, ARR_CITY, DISTANCE, CREATE_TIME) VALUES(ASSIST_AIRPORT_OD_PAIR_SEQ.nextval, b.DEP, b.ARR, b.DEP_CITY, b.ARR_CITY, b.DISTANCE, sysdate)";

        batchInsert(sql, new String[]{"DEP", "ARR", "DEP_CITY", "ARR_CITY", "DISTANCE"}, list);
    }

    // 解析文件
    private List<Map<String, Object>> parseFile(File file) throws IOException {
        List<Map<String, Object>> list = new ArrayList<>();

        SimpleDateFormat format1 = new SimpleDateFormat("yyyy/M/d");
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

            Map<String, Object> pair = new HashMap<>();
            list.add(pair);

            pair.put("DEP", cols[0]);
            pair.put("ARR", cols[1]);
            pair.put("DEP_CITY", cols[2]);
            pair.put("ARR_CITY", cols[3]);
            pair.put("DISTANCE", cols[4]);
        }

        return list;
    }
}
