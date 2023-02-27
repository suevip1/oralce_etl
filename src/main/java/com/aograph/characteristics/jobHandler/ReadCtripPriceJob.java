package com.aograph.characteristics.jobHandler;

import com.alibaba.fastjson.JSONObject;
import com.aograph.characteristics.utils.DateTimeHelper;
import com.aograph.characteristics.utils.FreeMarkHelper;
import com.aograph.characteristics.utils.LogHelper;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.io.*;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class ReadCtripPriceJob extends AbstractFileJob {
    @Resource(name = "oracleDataSource")
    private DataSource oracleDataSource;

    @XxlJob(value ="readCtripPriceJob")
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

        processFiles(new ContainFilter("ctrip"), history);
    }

    public void processFile(File file) throws IOException {
        LogHelper.log("process file: " + file.getName());

        // 读取城市名
        Map<String, String> cites = new HashMap<>();
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(oracleDataSource);
        List<Map<String, Object>> pairs = template.queryForList("select DEP, DEP_CITY, ARR, ARR_CITY from assist_airport_od_pair");
        if (pairs != null) {
            for (Map<String, Object> pair : pairs) {
                cites.put((String) pair.get("DEP"), (String) pair.get("DEP_CITY"));
                cites.put((String) pair.get("ARR"), (String) pair.get("ARR_CITY"));
            }
        }

        List<Map<String, Object>> priceList = template.queryForList("select distinct air_code, dep, arr, SUB_CLASS_PRICE, CREATE_TIME from (" +
                "select distinct air_code, dep, arr, SUB_CLASS_PRICE, CREATE_TIME from org_fare_fd " +
                "where CLASS_CD='Y' and SUB_CLASS_CD='Y' " +
                "union " +
                "select distinct air_code, dep, arr, SUB_CLASS_PRICE, CREATE_TIME from org_fare_private " +
                "where CLASS_CD='Y' and SUB_CLASS_CD='Y') t order by CREATE_TIME");
        Map<String, List<Map<String, Object>>> priceMap = priceList.stream().collect(Collectors.groupingBy(x -> (String) x.get("air_code") + x.get("dep") + x.get("arr")));

        parseFile(file, cites, priceMap);
    }

    // 将数据插入表中
    private void insertData(List<Map<String, Object>> list, Map<String, String> cites, Map<String, List<Map<String, Object>>> priceMap) {
        JdbcTemplate template = getJdbcTemplate();

        createSequence(template, "ORG_PRICE_DOM_CTRIP", "ORG_PRICE_DOM_CTRIP_SEQ");

        String sql = "INSERT INTO ORG_PRICE_DOM_CTRIP\n" +
                "(ID, FLIGHT_NO, AIR_CODE, AIR_NAME, DEP, ARR, DEP_DATE, ARR_DATE, DEP_TIME, ARR_TIME, SHARE_IND, SHARE_AIR," +
                " SHARE_FLIGHT_NO, FLY_TIME, STOPS, F_CABIN, F_AMT, C_CABIN, C_AMT, Y_CABIN, Y_AMT," +
                " EX_TIME, FLY_TYPE, TRANSFER_IND, CREATE_TIME)\n" +
                "VALUES(ORG_PRICE_DOM_CTRIP_SEQ.nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd");

        template.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement stat, int i) throws SQLException {
                Map<String, Object> m = list.get(i);
                int k = 1;
                stat.setString(k++, (String)m.get("FLIGHT_NO"));
                stat.setString(k++, (String)m.get("AIR_CODE"));
                stat.setString(k++, (String)m.get("AIR_NAME"));
                stat.setString(k++, (String)m.get("DEP"));
                stat.setString(k++, (String)m.get("ARR"));
                stat.setString(k++, (String)m.get("DEP_DATE"));
                stat.setString(k++, (String)m.get("ARR_DATE"));
                stat.setString(k++, (String)m.get("DEP_TIME"));
                stat.setString(k++, (String)m.get("ARR_TIME"));
                stat.setString(k++, (String)m.get("SHARE_IND"));
                stat.setString(k++, (String)m.get("SHARE_AIR"));
                stat.setString(k++, (String)m.get("SHARE_FLIGHT_NO"));
                stat.setString(k++, (String)m.get("FLY_TIME"));
                stat.setString(k++, (String)m.get("STOPS"));
                stat.setString(k++, (String)m.get("F_CABIN"));
                stat.setString(k++, (String)m.get("F_AMT"));
                stat.setString(k++, (String)m.get("C_CABIN"));
                stat.setString(k++, (String)m.get("C_AMT"));
                stat.setString(k++, (String)m.get("Y_CABIN"));
                stat.setString(k++, (String)m.get("Y_AMT"));
                try {
                    stat.setTimestamp(k++, new Timestamp(format1.parse((String) m.get("EX_TIME")).getTime()));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                stat.setString(k++, (String)m.get("FLY_TYPE"));
                stat.setString(k++, (String)m.get("TRANSFER_IND"));
                stat.setTimestamp(k++, (Timestamp)m.get("CREATE_TIME"));
            }

            @Override
            public int getBatchSize() {
                return list.size();
            }
        });

        // 下面生成ANALY_OTA_PRICE
        List<Map<String, Object>> prices = new ArrayList<>();
        for (Map<String, Object> item : list) {
            Map<String, Object> price = new HashMap<>();
            prices.add(price);

            price.put("AIR_CODE", item.get("AIR_CODE"));
            price.put("COMP", item.get("AIR_NAME"));
            price.put("FLIGHT_NO", item.get("FLIGHT_NO"));
            price.put("SHARE_IND", item.get("SHARE_IND"));
            price.put("FLIGHT_DATE", item.get("DEP_DATE"));
            price.put("FLIGHT_TYPE", Integer.parseInt((String)item.get("STOPS")) == 0 ? "直飞" : "经停");
            price.put("DEP", item.get("DEP"));
            price.put("ARR", item.get("ARR"));
            price.put("DEP_CITY", cites.get(item.get("DEP")));
            price.put("ARR_CITY", cites.get(item.get("ARR")));
            price.put("DEP_TIME", item.get("DEP_DATE") + " " + item.get("DEP_TIME") + ":00");
            price.put("ARR_TIME", item.get("ARR_DATE") + " " + item.get("ARR_TIME") + ":00");
            try {
                price.put("FLY_TIME", getMinuteBetween(format1.parse((String) price.get("DEP_TIME")), format1.parse((String) price.get("ARR_TIME"))));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            price.put("PLATFORM", "CTRIP");
            price.put("TRANS_TIME", item.get("STOPS"));
            price.put("STD_PRICE", "0");
            price.put("DISCOUNT", "0");
            price.put("PRICE", (String) item.get("Y_AMT"));
            price.put("FPRICE", (String) item.get("F_AMT"));
            price.put("CPRICE", (String) item.get("C_AMT"));

            try {
                price.put("CREATE_TIME", new Timestamp(format1.parse((String) item.get("EX_TIME")).getTime()));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            if ("Y".equals((String) item.get("Y_CABIN"))) {
                price.put("STD_PRICE", price.get("PRICE").toString());
                price.put("DISCOUNT", "100");
            }

            if ("0".equals(item.get("STD_PRICE"))) {
                String airCode = (String) item.get("AIR_CODE");
                String dep = (String) item.get("DEP");
                String arr = (String) item.get("ARR");

                Date exTime = (Date)price.get("CREATE_TIME");
                List<Map<String, Object>> p = priceMap.get(airCode + dep + arr);
                Map<String, Object> stdPrice = p.stream().filter(x -> !exTime.before((java.util.Date) x.get("CREATE_TIME"))).findFirst().orElse(null);
                price.put("STD_PRICE", stdPrice == null ? "0" : stdPrice.get("SUB_CLASS_PRICE").toString());
                price.put("DISCOUNT", Integer.toString(Integer.parseInt((String)price.get("PRICE")) * 100 / Integer.parseInt((String)price.get("STD_PRICE"))));
            }
        }

        createSequence(template, "ANALY_OTA_PRICE", "ANALY_OTA_PRICE_SEQ");

        sql = "INSERT INTO ANALY_OTA_PRICE\n" +
                "(ID, AIR_CODE, COMP, FLIGHT_NO, SHARE_IND, FLIGHT_DATE, DEP, ARR, DEP_CITY, ARR_CITY, DEP_TIME, ARR_TIME, FLY_TIME, " +
                "DISCOUNT, PRICE, FPRICE, CPRICE, STD_PRICE, MODEL, FLIGHT_TYPE, TRANS_TIME, TRANS_CITY, PLATFORM, CREATE_TIME, VERSION, " +
                "INSERT_TIME, OTA_INSERT_TIME) " +
                "values (ANALY_OTA_PRICE_SEQ.nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        Timestamp now = new Timestamp(System.currentTimeMillis());

        template.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement stat, int i) throws SQLException {
                Map<String, Object> m = prices.get(i);
                int k = 1;
                stat.setString(k++, (String) m.get("AIR_CODE"));
                stat.setString(k++, (String) m.get("COMP"));
                stat.setString(k++, (String) m.get("FLIGHT_NO"));
                stat.setString(k++, (String) m.get("SHARE_IND"));
                try {
                    stat.setDate(k++, new Date(format2.parse((String) m.get("FLIGHT_DATE")).getTime()));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                stat.setString(k++, (String) m.get("DEP"));
                stat.setString(k++, (String) m.get("ARR"));
                stat.setString(k++, (String) m.get("DEP_CITY"));
                stat.setString(k++, (String) m.get("ARR_CITY"));
                try {
                    stat.setTimestamp(k++, new Timestamp(format1.parse((String) m.get("DEP_TIME")).getTime()));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                try {
                    stat.setTimestamp(k++, new Timestamp(format1.parse((String) m.get("ARR_TIME")).getTime()));
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                stat.setString(k++, (String) m.get("FLY_TIME"));
                stat.setString(k++, m.get("DISCOUNT").toString());
                stat.setString(k++, (String) m.get("PRICE"));
                stat.setString(k++, (String) m.get("FPRICE"));
                stat.setString(k++, (String) m.get("CPRICE"));
                stat.setString(k++, (String) m.get("STD_PRICE"));
                stat.setString(k++, (String) m.get("MODEL"));
                stat.setString(k++, (String) m.get("FLIGHT_TYPE"));
                stat.setString(k++, (String) m.get("TRANS_TIME"));
                stat.setString(k++, (String) m.get("TRANS_CITY"));
                stat.setString(k++, (String) m.get("PLATFORM"));
                stat.setTimestamp(k++, (Timestamp)m.get("CREATE_TIME"));
                stat.setString(k++, (String) m.get("VERSION"));
                stat.setTimestamp(k++, now);
                stat.setTimestamp(k++, null);
            }

            @Override
            public int getBatchSize() {
                return prices.size();
            }
        });

//        sql = getSql("to_ota_price.txt");
//        Map<String, String> values = new HashMap<>();
//        template.execute(convert(sql, values));
    }

    private String getMinuteBetween(java.util.Date d1, java.util.Date d2) {
        int diff = (int)((d2.getTime() - d1.getTime()) / 1000 / 60);
        return Integer.toString(diff);
    }

    // 解析文件
    private List<Map<String, Object>> parseFile(File file, Map<String, String> cites, Map<String, List<Map<String, Object>>> priceMap) throws IOException {
        List<Map<String, Object>> list = new ArrayList<>();

        String tm = file.getName().split("_")[3]+ DateTimeHelper.date2String(new java.util.Date(), "HHmmss");
        Timestamp now = new Timestamp(DateTimeHelper.getDate(tm, "yyyyMMddHHmmss").getTime());

        int total = 0;

        InputStream inputStream = new FileInputStream(file);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
        CSVReader csvReader = new CSVReaderBuilder(bufferedReader).build();
        Iterator<String[]> iterator = csvReader.iterator();
        iterator.next();    // title line
        while (iterator.hasNext()) {
            String[] cols = iterator.next();
            // 最后一行为无用数据
            if (cols.length < 23) {
                break;
            }

            // 去掉多余的空格
            for (int i = 0; i < cols.length; i++) {
                cols[i] = cols[i].trim();
            }

            Map<String, Object> record = new HashMap<>();
            list.add(record);

            record.put("FLIGHT_NO", cols[3] + cols[7].trim());
            record.put("AIR_CODE", cols[3]);
            record.put("AIR_NAME", cols[8]);
            record.put("DEP", cols[0]);
            record.put("ARR", cols[1]);
            record.put("DEP_DATE", cols[2].substring(0, cols[2].indexOf(" ")).replaceAll("/", "-"));
            record.put("ARR_DATE", cols[5].substring(0, cols[5].indexOf(" ")).replaceAll("/", "-"));
            record.put("DEP_TIME", cols[4].substring(cols[4].indexOf(" ")+1, cols[4].lastIndexOf(":")));
            record.put("ARR_TIME", cols[6].substring(cols[6].indexOf(" ")+1, cols[6].lastIndexOf(":")));
            record.put("SHARE_IND", cols[9]);
            record.put("SHARE_AIR", cols[10]);
            record.put("SHARE_FLIGHT_NO", cols[11]);
            record.put("FLY_TIME", cols[12]);
            record.put("STOPS", cols[13]);
            record.put("F_CABIN", cols[15]);
            record.put("F_AMT", normalizeInt(cols[16]));
            record.put("C_CABIN", cols[17]);
            record.put("C_AMT", normalizeInt(cols[18]));
            record.put("Y_CABIN", cols[19]);
            record.put("Y_AMT", normalizeInt(cols[20]));
            record.put("EX_TIME", cols[21].substring(0, cols[21].lastIndexOf(".")).replaceAll("/", "-"));
            record.put("FLY_TYPE", cols[22]);
            record.put("TRANSFER_IND", cols[23]);
            record.put("CREATE_TIME", now);

            total++;

            if (list.size() == 10000) {
                LogHelper.log("insert data: " + list.size());

                insertData(list, cites, priceMap);
                list.clear();
            }
        }

        if (list.size() > 0) {
            insertData(list, cites, priceMap);
        }

        LogHelper.log("total: " + total);

        return list;
    }

    private String normalizeInt(String s) {
        s = s.replaceAll(",", "");
        if (s.contains(".")) {
            s = s.substring(0, s.indexOf("."));
        }
        return s;
    }

    private String getSql(String file) {
        try {
            return IOUtils.toString(getClass().getResourceAsStream(file), "UTF-8");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String convert(String content, Map<String, ?> values) {
        return FreeMarkHelper.convert(content, values);
    }
}
