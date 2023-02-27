package com.aograph.characteristics.jobHandler;

import com.alibaba.fastjson.JSONObject;
import com.aograph.characteristics.utils.AirlinkConst;
import com.aograph.characteristics.utils.DateTimeHelper;
import com.aograph.characteristics.utils.LogHelper;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.io.*;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Component
public class ReadFlpJob extends AbstractFileJob {
    @Resource(name = "oracleDataSource")
    private DataSource oracleDataSource;

    @XxlJob(value ="readFlpJob")
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

        processFiles(new ContainFilter("flp"), history);
    }

    public void processFile(File file) throws IOException {
        LogHelper.log("process file: " + file.getName());

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(oracleDataSource);
        String sql = "WITH\n" +
                "t as (SELECT FLIGHT_NO, ARR_TIME, WEEKS, DEP_DATE, DEP_TIME, DEP, ARR, EQT, CABINS, D_OR_I, F_CAP, C_CAP, Y_CAP, CAP, ELINE, create_time FROM ORG_MKT_FLIGHT_SCHEDULE)\n" +
                "SELECT * FROM (SELECT t.*, row_number() OVER (PARTITION BY DEP_DATE, FLIGHT_NO, DEP, ARR ORDER BY create_time DESC) limit_order FROM t) WHERE limit_order <= 1";
        List<Map<String, Object>> flts = template.queryForList(sql);
        Map<String, List<Map<String, Object>>> fltsByNo = flts.stream().collect(
                Collectors.groupingBy(x -> (String) x.get("FLIGHT_NO") + x.get("DEP") + x.get("ARR")));
        for (String key : fltsByNo.keySet()) {
            List<Map<String, Object>> _flts = fltsByNo.get(key);
            _flts = _flts.stream().sorted((x1, x2) -> ((String)x2.get("DEP_DATE")).compareTo((String) x1.get("DEP_DATE"))).collect(Collectors.toList());
            fltsByNo.put(key, _flts);
        }

        parseFile(file, fltsByNo);
    }

    // 数据插入ANALY_AOGRAPH_AIR_FOR_MODEL表中
    private void insertFdl(JdbcTemplate template, List<Map<String, Object>> fdls) {
        // 从ORG_OTA_PRICE中读取起降时间和标准价格
        Date flightDateStart = fdls.stream().map(x -> (Date)x.get("flight_date")).collect(Collectors.minBy((x1, x2) -> x1.compareTo(x2))).orElse(null);
        Date flightDateEnd = fdls.stream().map(x -> (Date)x.get("flight_date")).collect(Collectors.maxBy((x1, x2) -> x1.compareTo(x2))).orElse(null);

        String sql = "select distinct flight_no, flight_date, dep, arr, fly_time, std_price from ANALY_OTA_PRICE where flight_date between ? and ?";
        List<Map<String, Object>> priceList = template.queryForList(sql, flightDateStart, flightDateEnd);
        Map<String, Map<String, Object>> priceMap = priceList.stream().collect(Collectors.toMap(x ->
                (String) x.get("flight_no") + DateTimeHelper.date2String((Date)x.get("flight_date"), AirlinkConst.TIME_DATE_FORMAT) + x.get("dep") + x.get("arr"), x -> x, (x1, x2) -> x1));
        for (Map<String, Object> fdl : fdls) {
            String key = (String) fdl.get("fltno") + fdl.get("flight_date") + fdl.get("dep") + fdl.get("arr");
            Map<String, Object> price = priceMap.get(key);
            if (price != null) {
                fdl.put("std_price", price.get("std_price"));
                fdl.put("fly_time", ((Number)price.get("fly_time")).intValue() / 60.);     // 单位为小时
            } else {
                // 缺省值
                fdl.put("std_price", 0);
                fdl.put("fly_time", 0D);
            }
        }

        StringBuffer sb = new StringBuffer("INSERT INTO ANALY_AOGRAPH_AIR_FOR_MODEL\n" +
                "(ID, FLIGHT_DATE, COMP, EQT, FLTNO, HX, DEP, ARR, ARR2, ARR3, DEPTIME, ARRTIME, A_BKG, B_BKG, C_BKG, D_BKG, E_BKG, F_BKG, G_BKG, H_BKG, I_BKG, J_BKG, K_BKG,\n" +
                " L_BKG, M_BKG, N_BKG, O_BKG, P_BKG, Q_BKG, R_BKG, S_BKG, T_BKG, U_BKG, V_BKG, W_BKG, X_BKG, Y_BKG, Z_BKG, A_CAP, B_CAP, C_CAP, D_CAP, E_CAP, F_CAP, G_CAP, H_CAP,\n" +
                " I_CAP, J_CAP, K_CAP, L_CAP, M_CAP, N_CAP, O_CAP, P_CAP, Q_CAP, R_CAP, S_CAP, T_CAP, U_CAP, V_CAP, W_CAP, X_CAP, Y_CAP, Z_CAP, BKG, CAP, CLSN, CLSN_MAX, CLSN_CAP, CLSN_BKD, " +
                "MAX, INSERT_DATE, EX_DIF, \"YEAR\", \"MONTH\", WEEK, HD, STD_PRICE, HDLX, FLY_TIME, ORG_INSERT_DATE, JOB_ID)");
        sb.append(" values (ANALYAOGRAPHAIRFORMODEL_SEQ.nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \n" +
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \n" +
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \n" +
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \n" +
                "?)");

        template.batchUpdate(sb.toString(), new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement stat, int i) throws SQLException {
                Map<String, Object> m = fdls.get(i);
                int k = 1;
                stat.setDate(k++, (java.sql.Date) m.get("flight_date"));
                stat.setString(k++, (String)m.get("comp"));
                stat.setString(k++, (String)m.get("eqt"));
                stat.setString(k++, (String)m.get("fltno"));
                stat.setString(k++, (String)m.get("hx"));
                stat.setString(k++, (String)m.get("dep"));
                stat.setString(k++, (String)m.get("arr"));
                stat.setString(k++, (String)m.get("arr2"));
                stat.setString(k++, (String)m.get("arr3"));
                stat.setString(k++, (String)m.get("deptime"));
                stat.setString(k++, (String)m.get("arrtime"));
                // 并非每个舱都有效
                for (char c = 'a' ; c <= 'z' ; c++) {
                    String cabin = Character.toString(c);
                    Integer d = m.get(cabin+"_bkg") == null ? 0 : ((Number) m.get(cabin+"_bkg")).intValue();
                    stat.setInt(k++, d == null ? 0 : d);
                }
                for (char c = 'a' ; c <= 'z' ; c++) {
                    String cabin = Character.toString(c);
                    Integer d = 0;
                    try {
                        d = m.get(cabin+"_cap") == null? 0 : ((Number) m.get(cabin+"_cap")).intValue();
                    } catch (NumberFormatException e) {
                        // e.printStackTrace();
                    }
                    stat.setInt(k++, d == null ? 0 : d);
                }
                stat.setInt(k++, ((Number) m.get("bkg")).intValue());
                stat.setInt(k++, ((Number) m.get("cap")).intValue());
                stat.setString(k++, (String)m.get("clsn"));
                stat.setString(k++, (String)m.get("clsn_max"));
                stat.setString(k++, (String)m.get("clsn_cap"));
                stat.setString(k++, (String)m.get("clsn_bkd"));
                stat.setInt(k++, ((Number) m.get("max")).intValue());
                stat.setTimestamp(k++, (java.sql.Timestamp) m.get("insert_date"));
                stat.setInt(k++, DateTimeHelper.daysBetween((Date) m.get("insert_date"), (Date) m.get("flight_date")));
                stat.setInt(k++, (Integer) m.get("year"));
                stat.setInt(k++, (Integer) m.get("month"));
                stat.setInt(k++, (Integer) m.get("week"));
                stat.setString(k++, (String)m.get("hd"));
                stat.setInt(k++, m.get("std_price") == null ? 0 : ((Number) m.get("std_price")).intValue());
                stat.setInt(k++, (Integer)m.get("hdlx"));
                stat.setDouble(k++, (Double) m.get("fly_time"));
                stat.setString(k++, (String) m.get("org_insert_date"));
                stat.setString(k++, (String)m.get("job_id"));
            }

            @Override
            public int getBatchSize() {
                return fdls.size();
            }
        });
    }

    private void insertData(List<Map<String, Object>> list, Map<String, List<Map<String, Object>>> fltsByNo) {
        JdbcTemplate template = getJdbcTemplate();

        createSequence(template, "ORG_FACT_FLP", "ORG_FACT_FLP_SEQ");

        Map<String, Map<String, Object>> fdls = new HashMap<>();

        Timestamp createTime = new Timestamp(System.currentTimeMillis());
        for (int i = 0; i < list.size(); i++) {
            Map<String, Object> m = list.get(i);
            m.put("CREATE_TIME", createTime);

            String flightDate = (String) m.get("FLIGHT_DATE");

            String key = (String)m.get("FLIGHT_NO") + m.get("FLIGHT_DATE") + m.get("DEP") + m.get("ARR");
            Map<String, Object> fdl = fdls.get(key);
            if (fdl == null) {
                String _key = (String)m.get("FLIGHT_NO") + m.get("DEP") + m.get("ARR");
                if (fltsByNo.get(_key) != null) {
                    List<Map<String, Object>> _flts = fltsByNo.get(_key);
                    Map<String, Object> flt = _flts.stream().filter(x -> ((String)x.get("DEP_DATE")).compareTo(flightDate) <= 0).findFirst().orElse(null);
                    if (flt == null) {
                        flt = _flts.get(0);
                    }
                    fdl = getFdl(m, flt);
                    fdls.put(key, fdl);
                }
            }

            if (fdl != null) {
                String cabin = (String) m.get("SUB_CLZ_CODE");
                fdl.put(cabin.toLowerCase()+"_bkg", m.get("BKD"));
            }
        }

        for (Map<String, Object> fdl : fdls.values()) {
            int bkd = 0;
            for (char c = 'a' ; c <= 'z' ; c++) {
                String cabin = Character.toString(c);
                if (fdl.get(cabin+"_bkg") != null) {
                    bkd += (Integer) fdl.get(cabin+"_bkg");
                }
            }
            fdl.put("bkg", bkd);

            String clsn = (String)fdl.get("clsn");
            String[] cabins = clsn.split("/");
            List<Integer> clsn_bkd = new ArrayList<>();
            for (String cabin : cabins) {
                bkd = 0;
                for (int i = 0 ; i < cabin.length() ; i++) {
                    String c = cabin.substring(i, i+1).toLowerCase();
                    if (fdl.get(c + "_bkg") != null) {
                        bkd += (Integer) fdl.get(c+"_bkg");
                    }
                }
                clsn_bkd.add(bkd);
            }
            // 应该为JSON的数组格式
//                fdl.put("clsn_max", clsn_max.toString());
//                fdl.put("clsn_cap", clsn_cap.toString());
            fdl.put("clsn_bkd", clsn_bkd.toString());
        }

        // 批量插入
        String sql = "INSERT INTO ORG_FACT_FLP\n" +
                "(ID, FLIGHT_NO, FLIGHT_DATE, DEP, ARR, AIR_CODE, CLZ_CODE, SUB_CLZ_CODE, BKD, LAYOUT, EQT, LEG_IND, LANE_SEG_NO, CREATE_TIME)\n" +
                "VALUES(ORG_FACT_FLP_SEQ.nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        String[] names = sql.substring(sql.indexOf(",") + 1, sql.indexOf(")")).split(",");
        for (int i = 0; i < names.length; i++) {
            names[i] = names[i].trim();
        }
        batchInsert(sql, names, list);

        LogHelper.log("insert data: " + list.size());

        ArrayList<Map<String, Object>> fdlList = new ArrayList<>(fdls.values());

        createSequence(template, "ANALY_AOGRAPH_AIR_FOR_MODEL", "ANALYAOGRAPHAIRFORMODEL_SEQ");

        for (int i = 0; i < fdlList.size(); i += 10000) {
            insertFdl(template, fdlList.subList(i, Math.min(i + 10000, fdlList.size())));
        }

        LogHelper.log("insert fdl data: " + fdlList.size());
    }

    private Map<String, Object> getFdl(Map<String, Object> flp, Map<String, Object> flt) {
        Map<String, Object> fdl = new HashMap<>();
        fdl.put("flight_date", new java.sql.Date(DateTimeHelper.getDate((String)flp.get("FLIGHT_DATE"), AirlinkConst.TIME_DATE_FORMAT).getTime()));
        fdl.put("fltno", flp.get("FLIGHT_NO"));
        fdl.put("comp", flp.get("AIR_CODE"));
        fdl.put("eqt", flp.get("EQT"));

        String dep = (String) flp.get("DEP");
        String arr = (String) flp.get("ARR");

        String hx = (String) flt.get("ELINE");

        fdl.put("hx", hx);      // 航线
        fdl.put("dep", dep);
        fdl.put("arr", arr);
        String arr2;
        if (hx.length() == 7) {
            arr2 = "";                                     // 直飞航线
        } else if (hx.startsWith(dep) && hx.endsWith(arr)) {
            arr2 = "#" + hx.substring(4, 7);        // 经停中间站
        } else if (hx.startsWith(dep)) {
            arr2 = hx.substring(8);                        // 经停最后站
        } else {
            arr2 = "&" + hx.substring(0, 3);        // 经停第一站
        }
        fdl.put("arr2", arr2);
        fdl.put("arr3", "");
        fdl.put("max", flt.get("CAP"));
        fdl.put("insert_date", flp.get("CREATE_TIME"));
        fdl.put("org_insert_date", DateTimeHelper.date2String((Date)flp.get("CREATE_TIME"), AirlinkConst.TIME_FULL_FORMAT));

        Date flightDate = (Date) fdl.get("flight_date");
        Calendar cal = Calendar.getInstance();
        cal.setTime(flightDate);

        fdl.put("year", cal.get(Calendar.YEAR));
        fdl.put("month", cal.get(Calendar.MONTH));
        int week = cal.get(Calendar.DAY_OF_WEEK);
        fdl.put("week", week == 0 ? 7 : week);
        fdl.put("hd", fdl.get("dep") + "-" + fdl.get("arr"));   // 航段
        fdl.put("hdlx", "DD".equals(flt.get("D_OR_I")) ? 1 : 4);         // 国内航线
        fdl.put("job_id", "CSV");

        fdl.put("cap", flt.get("CAP"));

        String layout = (String)flp.get("LAYOUT");
        StringBuffer cabins = new StringBuffer((String)flt.get("CABINS"));
        if (cabins.indexOf("XX") == 0) {
            cabins.setLength(0);
        }
        for (int i = 0 ; i < layout.length() ; i++) {
            String c = layout.substring(i, i+1);
            if (c.compareTo("A") >= 0 && c.compareTo("Z") <= 0) {
                int p = cabins.indexOf(c);
                if (p > 0) {
                    cabins.insert(p, "/");
                }
            }
        }
        fdl.put("clsn", cabins.toString());

        String[] parts = Pattern.compile("[A-Z]").split(layout);
        List<Integer> clsn_cap = new ArrayList<>();
        for (int i = 1; i < parts.length; i++) {
            clsn_cap.add(Integer.parseInt(parts[i]));
        }
        // 应该为JSON的数组格式
        fdl.put("clsn_cap", clsn_cap.toString());
        fdl.put("clsn_max", clsn_cap.toString());

        fdl.put("deptime", ((String)flt.get("DEP_TIME")).replaceAll(":", ""));
        fdl.put("arrtime", ((String)flt.get("ARR_TIME")).replaceAll(":", ""));

        return fdl;
    }

    private List<Map<String, Object>> parseFile(File file, Map<String, List<Map<String, Object>>> fltsByNo) throws IOException {
        List<Map<String, Object>> list = new ArrayList<>();

        int total = 0;

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

            for (int i = 0; i < cols.length; i++) {
                cols[i] = cols[i].trim();
            }

            Map<String, Object> flp = new HashMap<>();
            list.add(flp);

            // FLIGHT_NO, FLIGHT_DATE, AIR_CODE, CLZ_CODE, SUB_CLZ_CODE, BKD, LAYOUT, EQT, LEG_IND, LANE_SEG_NO
            flp.put("FLIGHT_NO", cols[1].trim() + cols[2].trim());
            try {
                flp.put("FLIGHT_DATE", format2.format(format1.parse(cols[0])));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            flp.put("AIR_CODE", cols[1].trim());
            flp.put("DEP", cols[3]);
            flp.put("ARR", cols[4]);
            flp.put("CLZ_CODE", cols[6]);
            flp.put("SUB_CLZ_CODE", cols[5]);
            flp.put("BKD", Integer.parseInt(cols[7]));
            flp.put("LAYOUT", cols[10]);
            flp.put("EQT", cols[11]);
            flp.put("LEG_IND", Integer.parseInt(cols[12]));
            flp.put("LANE_SEG_NO", Integer.parseInt(cols[13]));

            total++;

            if (list.size() == 10000) {
                LogHelper.log("insert data: " + list.size());

                insertData(list, fltsByNo);
                list.clear();
            }
        }

        if (list.size() > 0) {
            insertData(list, fltsByNo);
        }

        LogHelper.log("total: " + total);

        return list;
    }
}
