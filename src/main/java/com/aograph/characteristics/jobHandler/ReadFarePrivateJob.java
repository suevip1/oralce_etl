package com.aograph.characteristics.jobHandler;

import com.alibaba.fastjson.JSONObject;
import com.aograph.characteristics.control.SupplementHisFareService;
import com.aograph.characteristics.utils.AirlinkConst;
import com.aograph.characteristics.utils.ConstantUtil;
import com.aograph.characteristics.utils.DateTimeHelper;
import com.aograph.characteristics.utils.LogHelper;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
public class ReadFarePrivateJob extends AbstractFileJob {

    @Resource
    private SupplementHisFareService supplementHisFareService;

    @XxlJob(value ="readFarePrivateJob")
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

        processFiles(new ContainFilter("fulture"), history);

        if (history) {
            supplementHisFareService.supplementHisFare(ConstantUtil.IS_PRIVATE, map.getIntValue("add_hx"));
        }
    }

    public void processFile(File file) throws IOException {
        LogHelper.log("process file: " + file.getName());

        parseFile(file);
    }

    // 将数据插入表中
    private void insertData(List<Map<String, Object>> list, File file) {
        JdbcTemplate template = getJdbcTemplate();

        createSequence(template, "ORG_FARE_PRIVATE", "ORG_FARE_PRIVATE_SEQ");

        String minFlightDate = null;
        for (Map<String, Object> m : list) {
            String flightDate = (String) m.get("FLIGHT_DATE");
            if (minFlightDate == null || minFlightDate.compareTo(flightDate) > 0) {
                minFlightDate = flightDate;
            }
        }

        String[] parts = file.getName().substring(0, file.getName().lastIndexOf(".")).split("_");
        Timestamp createTime = null;
        try {
            createTime = new Timestamp(new SimpleDateFormat("yyyyMMddHHmmss").parse(parts[2]+parts[3]).getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Date startDate = new Date(DateTimeHelper.getDate(minFlightDate, AirlinkConst.TIME_DATE_FORMAT).getTime());
        for (Map<String, Object> m : list) {
            m.put("CREATE_TIME", createTime);
            m.put("START_DATE", startDate);
        }

        String sql = "INSERT INTO ORG_FARE_PRIVATE\n" +
                "(ID, DEP, ARR, AIR_CODE, FLIGHT_NO, FLIGHT_DATE, SUB_CLASS_CD, SUB_CLASS_PRICE, CLASS_CD, CURRENCY, CREATE_TIME)\n" +
                "VALUES(ORG_FARE_PRIVATE_SEQ.nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        String[] names = sql.substring(sql.indexOf(",") + 1, sql.indexOf(")")).split(",");
        for (int i = 0; i < names.length; i++) {
            names[i] = names[i].trim();
        }
        batchInsert(sql, names, list);

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");

        // ASSIST_TBL_PAT_INFO使用有效时间段
        sql = "INSERT INTO ASSIST_TBL_PAT_INFO\n" +
                "(FLTNO, DEPARTURE_3CODE, ARRIVAL_3CODE, AIRLINE_2CODE, CABIN, OW_PRICE, CABIN_DESC, START_DATE, " +
                "FLIGHT_DATE_START, FLIGHT_DATE_END, INSERT_DATE, FLIGHT_DATE_END2, ID, SOURCE)\n" +
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, null, ASSIST_TBL_PAT_INFO_SEQ.Nextval, 1)";

        names = new String[]{"FLTNO", "DEPARTURE_3CODE", "ARRIVAL_3CODE", "AIRLINE_2CODE", "CABIN", "OW_PRICE",
                "CABIN_DESC", "START_DATE", "FLIGHT_DATE_START", "FLIGHT_DATE_END", "INSERT_DATE"};
        List<Map<String, Object>> pats = new ArrayList<>();
        for (Map<String, Object> m : list) {
            Map<String, Object> pat = new HashMap<>();
            pats.add(pat);

            pat.put("FLTNO", m.get("FLIGHT_NO"));
            pat.put("DEPARTURE_3CODE", m.get("DEP"));
            pat.put("ARRIVAL_3CODE", m.get("ARR"));
            pat.put("AIRLINE_2CODE", m.get("AIR_CODE"));
            pat.put("CABIN", m.get("SUB_CLASS_CD"));
            pat.put("OW_PRICE", m.get("SUB_CLASS_PRICE"));
            pat.put("CABIN_DESC", m.get("CLASS_CD"));
//            pat.put("START_DATE", m.get("START_DATE"));

            java.util.Date flightDate = DateTimeHelper.getDate((String) m.get("FLIGHT_DATE"), AirlinkConst.TIME_DATE_FORMAT);

            pat.put("START_DATE", new Date(flightDate.getTime()));
            pat.put("FLIGHT_DATE_START", new Date(flightDate.getTime()));

            pat.put("FLIGHT_DATE_END", new Date(DateTimeHelper.getDateAfter(flightDate, 365).getTime()));
            pat.put("INSERT_DATE", new Timestamp(((java.util.Date) m.get("CREATE_TIME")).getTime()));

        }
        batchInsert(sql, names, pats);

        // 复制数据到明天
        sql = "INSERT INTO ASSIST_TBL_PAT_INFO\n" +
                "(FLTNO, DEPARTURE_3CODE, ARRIVAL_3CODE, AIRLINE_2CODE, CABIN, OW_PRICE, CABIN_DESC, START_DATE, " +
                "FLIGHT_DATE_START, FLIGHT_DATE_END, INSERT_DATE, FLIGHT_DATE_END2, ID, SOURCE)\n" +
                "VALUES(?, ?, ?, ?, ?, ?, ?, ? + 1, ? + 1, ? + 1, trunc(?) + 1 + 2/24, null, ASSIST_TBL_PAT_INFO_SEQ.Nextval, 1)";
        batchInsert(sql, names, pats);

        // FLIGHT_DATE_END2只是为计算时间段而存在
//        JdbcTemplate template = getJdbcTemplate();
//        template.execute("UPDATE ASSIST_TBL_PAT_INFO SET FLIGHT_DATE_END=FLIGHT_DATE_END2, FLIGHT_DATE_END2=null WHERE FLIGHT_DATE_END2 IS NOT null");
    }

    // 解析文件
    private List<Map<String, Object>> parseFile(File file) throws IOException {
        List<Map<String, Object>> list = new ArrayList<>();

        Date now = new Date(System.currentTimeMillis());

        int total = 0;

        SimpleDateFormat format1 = new SimpleDateFormat("yyyy/M/d");
        SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd");

        InputStream inputStream = new FileInputStream(file);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
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

            record.put("DEP", cols[0]);
            record.put("ARR", cols[1]);
            record.put("AIR_CODE", cols[2]);
            record.put("FLIGHT_NO", cols[2] + cols[3]);
            try {
                record.put("FLIGHT_DATE", format2.format(new Date(format1.parse(cols[4]).getTime())));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            record.put("SUB_CLASS_CD", cols[5]);
            record.put("SUB_CLASS_PRICE", cols[6]);
            record.put("CLASS_CD", cols[7]);
            record.put("CURRENCY", cols[8]);
            record.put("CREATE_TIME", now);

            total++;

            if (list.size() == 10000) {
                LogHelper.log("insert data: " + list.size());

                insertData(list, file);
                list.clear();
            }
        }

        if (list.size() > 0) {
            insertData(list, file);
        }

        LogHelper.log("total: " + total);

        return list;
    }
}
