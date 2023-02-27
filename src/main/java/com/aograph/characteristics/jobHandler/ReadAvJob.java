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
public class ReadAvJob extends AbstractFileJob {
    @Resource(name = "oracleDataSource")
    private DataSource oracleDataSource;

    @XxlJob(value ="readAvJob")
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

        processFiles(new ContainFilter("av"), history);
    }

    // 解析文件
    public void processFile(File file) throws IOException {
        parseFile(file);
    }

    // 将数据插入表中
    private void insertData(List<Map<String, Object>> list) {
        JdbcTemplate template = getJdbcTemplate();

        createSequence(template, "ORG_FACT_AV", "ORG_FACT_AV_SEQ");

        Timestamp createTime = new Timestamp(System.currentTimeMillis());

        for (int i = 0; i < list.size(); i++) {
            Map<String, Object> m = list.get(i);
            m.put("CREATE_TIME", createTime);
        }

        String sql = "INSERT INTO ORG_FACT_AV\n" +
                "(ID, FLIGHT_NO, ARR_DATE, ARR_TIME, AIR_CODE, CLZ_CODE, CLZ_STATUS, DEP_DATE, DEP_TIME, DEP, ARR, CREATE_TIME)\n" +
                "values(ORG_FACT_AV_SEQ.nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        String[] names = sql.substring(sql.indexOf(",") + 1, sql.indexOf(")")).split(",");
        for (int i = 0; i < names.length; i++) {
            names[i] = names[i].trim();
        }
        batchInsert(sql, names, list);
    }

    private List<Map<String, Object>> parseFile(File file) throws IOException {
        List<Map<String, Object>> list = new ArrayList<>();

        SimpleDateFormat format1 = new SimpleDateFormat("yyyy/M/d");
        SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd");

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

            Map<String, Object> av = new HashMap<>();
            list.add(av);

            Calendar cal = Calendar.getInstance();
            Date date = null;
            try {
                date = format1.parse(cols[0]);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            av.put("FLIGHT_NO", cols[1].trim() + cols[2].trim());
            cal.setTime(date);
            cal.add(Calendar.DAY_OF_YEAR, Integer.parseInt(cols[10]));
            av.put("ARR_DATE", format2.format(cal.getTime()));
            av.put("ARR_TIME", cols[8]);
            av.put("AIR_CODE", cols[1].trim());
            av.put("CLZ_CODE", cols[5].trim());
            av.put("CLZ_STATUS", cols[6]);
            cal.setTime(date);
            cal.add(Calendar.DAY_OF_YEAR, Integer.parseInt(cols[9]));
            av.put("DEP_DATE", format2.format(cal.getTime()));
            av.put("DEP_TIME", cols[7]);
            av.put("DEP", cols[3]);
            av.put("ARR", cols[4]);

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
