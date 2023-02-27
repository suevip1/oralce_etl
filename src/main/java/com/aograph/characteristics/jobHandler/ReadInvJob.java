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
import java.util.stream.Collectors;

@Component
public class ReadInvJob extends AbstractFileJob {
    @Resource(name = "oracleDataSource")
    private DataSource oracleDataSource;

    @Resource
    private TravelSkyEtl travelSkyEtl;

    @XxlJob(value ="readInvJob")
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

        processFiles(new InvFilter(), history);
    }

    public void processFile(List<File> files) throws IOException {
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(oracleDataSource);
        List<Map<String, Object>> flightInfos = template.queryForList("SELECT distinct FLIGHT_NO, EQT, DISTANCE, DEP, ARR FROM ORG_FLIGHT_INFO");

        // leg, seg必须成对处理
        List<Map<String, Object>> legs = new ArrayList<>();
        List<Map<String, Object>> segs = new ArrayList<>();
        for (File file : files) {
            if (file.getName().contains("_leg_")) {
                try {
                    List<Map<String, Object>> _legs = parseLegFile(file, flightInfos);
                    legs.addAll(_legs);
                } catch (Exception e) {
                    e.printStackTrace();
                    LogHelper.error(file.getName(), e);
                }
            }
        }

        for (File file : files) {
            if (file.getName().contains("_seg_")) {
                try {
                    List<Map<String, Object>> _segs = parseSegFile(file);
                    segs.addAll(_segs);
                } catch (Exception e) {
                    e.printStackTrace();
                    LogHelper.error(file.getName(), e);
                }
            }
        }

        List<Map<String, Object>> fdls = new ArrayList<>();

        Map<String, List<Map<String, Object>>> legsByNo = legs.stream().collect(Collectors.groupingBy(x -> (String) x.get("leg-FlightNumber") + x.get("leg-DepartureDate") + x.get("hdr-DTTM")));

        for (Map<String, Object> flight : segs) {
            // Avail给一个缺省值
            for (char cabin = 'A'; cabin <= 'Z'; cabin++) {
                if (flight.get("segment-cabin" + cabin + "-Avail") == null) {
                    flight.put("segment-cabin" + cabin + "-Avail", "0");
                }
            }

            String dep = (String) flight.get("segment-OriginAirport");
            String arr = (String) flight.get("segment-DestinationAirport");

            String key = (String) flight.get("FlightNumber") + flight.get("segment-DepartureDate") + flight.get("hdr-DTTM");
            String eline = null;
            List<Map<String, Object>> myNo = legsByNo.get(key);
            if (myNo != null) {
                if (myNo.size() > 1) {
                    // 按AB, BC排序以便计算航线值
                    if (!((String)myNo.get(0).get("leg-ScheduledArrival-AirportCode")).equalsIgnoreCase((String) myNo.get(1).get("leg-ScheduledDeparture-AirportCode"))) {
                        Map<String, Object> v = myNo.get(0);
                        myNo.set(0, myNo.get(1));
                        myNo.set(1, v);
                    }
                }
                for (Map<String, Object> leg : myNo) {
                    if (eline == null) {
                        eline = (String) leg.get("leg-ScheduledDeparture-AirportCode");
                    }
                    eline += "-" + leg.get("leg-ScheduledArrival-AirportCode");
                }
            } else {
                eline = dep + "-" + arr;
            }

            //            flight.put("segment-DepartureTime", leg.get("leg-ScheduledDeparture-Time"));
            int flyTime = 0;
            int max = -1;

            while (myNo != null) {
                String finalDep = dep;
                Map<String, Object> leg = myNo.stream().filter(x -> finalDep.equals(x.get("leg-ScheduledDeparture-AirportCode"))).findFirst().orElse(null);
                if (leg == null) {
                    break;
                }

                // 按起降时间计算飞行时间
                String depTime = (String) leg.get("leg-ScheduledDeparture-Time");
                String arrTime = (String) leg.get("leg-ScheduledArrival-Time");
                if (StringUtils.isNotBlank(depTime) && StringUtils.isNotBlank(arrTime)) {
                    int used = Integer.parseInt(arrTime.substring(0, 2)) * 60 + Integer.parseInt(arrTime.substring(2)) -
                            (Integer.parseInt(depTime.substring(0, 2)) * 60 + Integer.parseInt(depTime.substring(2)));
                    if (used < 0) {
                        used += 24 * 60;
                    }
                    flyTime += used;
                }

                int _max = 0;
                // segment的MAX为各舱等MAX之和
                for (int k = 0; k < 6; k++) {
                    String s = (String) leg.get("leg-cabin" + k + "-MAX");
                    if (s == null) {
                        break;
                    }
                    _max += Integer.parseInt(s);
                }

                if (max == -1 || _max < max) {
                    max = _max;
                }

                String _arr = (String) leg.get("leg-ScheduledArrival-AirportCode");
                if (arr.equals(_arr)) {
                    break;
                }
                dep = _arr;
            }


            flight.put("segment-FlyTime", ""+flyTime);
            flight.put("segment-eline", eline);
            flight.put("segment-Max", ""+max);

            // myNo == null说明缺leg, 这种情况下不应该生成fdl
            if (myNo != null) {
                StringBuffer cos = new StringBuffer();
                // cos格式应为JCDIO/WS/YPBMHKUALQEVZTNRGX
                String finalDep = dep;
                Map<String, Object> leg = myNo.stream().filter(x -> finalDep.equals(x.get("leg-OriginAirport"))).findFirst().orElse(null);
                if (leg != null) {
                    for (int i = 0; ; i++) {
                        String clz = (String) leg.get("leg-cabin" + i + "-class");
                        if (clz == null) {
                            break;
                        }
                        cos.append(leg.get("leg-cabin" + i + "-ClassOfService")).append("/");
                    }
                    cos.setLength(cos.length() - 1);
                }

                Map<String, Object> fdl = getFdl(flight, cos.toString());
                fdl.put("cap", 0);
                fdl.put("clsn_max", "[]");
                fdl.put("clsn_cap", "[]");
                fdl.put("clsn_bkd", "[]");

                // 找到和本segment相同起飞机场的leg
                leg = myNo.stream().filter(x -> ((String) flight.get("segment-OriginAirport")).equals(x.get("leg-OriginAirport"))).findFirst().orElse(null);
                if (leg != null) {
                    fdl.put("eqt", leg.get("leg-Equipment"));
                    fdl.put("deptime", leg.get("leg-ScheduledDeparture-Time"));
                    fdl.put("arrtime", leg.get("leg-ScheduledArrival-Time"));

                    // 各舱等的cap, bkd, max
                    List<Integer> clsn_cap = new ArrayList<>();
                    List<Integer> clsn_bkd = new ArrayList<>();
                    List<Integer> clsn_max = new ArrayList<>();
                    int cap = 0;
                    for (int clzIndex = 0; clzIndex < 6; clzIndex++) {
                        String maxStr = (String) leg.get("leg-cabin" + clzIndex + "-MAX");
                        if (StringUtils.isNotBlank(maxStr)) {
                            clsn_max.add(Integer.parseInt(maxStr));
                        }
                        String capStr = (String) leg.get("leg-cabin" + clzIndex + "-CAP");
                        if (StringUtils.isNotBlank(capStr)) {
                            cap += Integer.parseInt(capStr);
                            clsn_cap.add(Integer.parseInt(capStr));
                        }
                        String tbStr = (String) leg.get("leg-cabin" + clzIndex + "-TB");
                        if (StringUtils.isNotBlank(tbStr)) {
                            clsn_bkd.add(Integer.parseInt(tbStr));
                        }
                    }
                    // 应该为JSON的数组格式
                    fdl.put("clsn_max", clsn_max.toString());
                    fdl.put("clsn_cap", clsn_cap.toString());
                    fdl.put("clsn_bkd", clsn_bkd.toString());
                    fdl.put("cap", cap);
                }

                fdls.add(fdl);
            }
        }

        // 数据插入leg, seg表中
        LogHelper.log("insert legs: " + legs.size());
        LogHelper.log("insert segs: " + legs.size());

        createSequence(template, "ORG_INV_LEG", "ORG_INV_LEG_SEQ");
        createSequence(template, "ORG_INV_SEGMENT", "ORG_INV_SEGMENT_SEQ");

        travelSkyEtl.insertInvData(template, legs, segs);

        createSequence(template, "ANALY_AOGRAPH_AIR_FOR_MODEL", "ANALYAOGRAPHAIRFORMODEL_SEQ");

        // 批量插入
        for (int i = 0 ; i < fdls.size() ; i += 10000) {
            insertFdl(template, fdls.subList(i, Math.min(i + 10000, fdls.size())));
        }
        LogHelper.log("insert fdl: " + fdls.size());
    }

    // 数据插入ANALY_AOGRAPH_AIR_FOR_MODEL表中
    private void insertFdl(JdbcTemplate template, List<Map<String, Object>> fdls) {
        // 从ORG_OTA_PRICE中读取起降时间和标准价格
        Date flightDateStart = fdls.stream().map(x -> (Date)x.get("flight_date")).collect(Collectors.minBy((x1, x2) -> x1.compareTo(x2))).orElse(null);
        Date flightDateEnd = fdls.stream().map(x -> (Date)x.get("flight_date")).collect(Collectors.maxBy((x1, x2) -> x1.compareTo(x2))).orElse(null);
        String sql = "select distinct flight_no, flight_date, dep, arr, TO_CHAR(dep_time, 'HH24MI') as dep_time," +
                " TO_CHAR(arr_time, 'HH24MI') as arr_time, fly_time, std_price from ANALY_OTA_PRICE where flight_date between ? and ?";
        List<Map<String, Object>> priceList = template.queryForList(sql, flightDateStart, flightDateEnd);
        Map<String, List<Map<String, Object>>> priceMap = priceList.stream().collect(Collectors.groupingBy(x ->
                (String) x.get("flight_no") + DateTimeHelper.date2String((Date) x.get("flight_date"), AirlinkConst.TIME_DATE_FORMAT) + x.get("dep") + x.get("arr")));
        for (Map<String, Object> fdl : fdls) {
            String key = (String) fdl.get("fltno") + fdl.get("flight_date") + fdl.get("dep") + fdl.get("arr");
            List<Map<String, Object>> prices = priceMap.get(key);
            if (prices != null && prices.size() > 0) {
                Map<String, List<String>> timeGroup = prices.stream().map(x -> (String) x.get("dep_time")).collect(Collectors.groupingBy(x -> x));
                String deptime = null;
                int max = 0;
                for (String t : timeGroup.keySet()) {
                    if (timeGroup.get(t).size() > max) {
                        deptime = t;
                        max = timeGroup.get(t).size();
                    }
                }

                fdl.put("deptime", deptime.replaceAll(":", ""));

                timeGroup = prices.stream().map(x -> (String) x.get("arr_time")).collect(Collectors.groupingBy(x -> x));
                String arrtime = null;
                max = 0;
                for (String t : timeGroup.keySet()) {
                    if (timeGroup.get(t).size() > max) {
                        arrtime = t;
                        max = timeGroup.get(t).size();
                    }
                }
                fdl.put("arrtime", arrtime.replaceAll(":", ""));
                fdl.put("std_price", prices.get(0).get("std_price"));
                fdl.put("fly_time", ((Number)prices.get(0).get("fly_time")).intValue() / 60.);     // 单位为小时
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
                    Integer d = m.get(cabin+"_bkg") == null ? 0 : Integer.parseInt((String) m.get(cabin+"_bkg"));
                    stat.setInt(k++, d == null ? 0 : d);
                }
                for (char c = 'a' ; c <= 'z' ; c++) {
                    String cabin = Character.toString(c);
                    Integer d = 0;
                    try {
                        d = m.get(cabin + "_cap") == null ? 0 : Integer.parseInt((String) m.get(cabin + "_cap"));
                    } catch (NumberFormatException e) {
                        // e.printStackTrace();
                    }
                    stat.setInt(k++, d == null ? 0 : d);
                }
                stat.setInt(k++, (Integer) m.get("bkg"));
                stat.setInt(k++, (Integer) m.get("cap"));
                stat.setString(k++, (String)m.get("clsn"));
                stat.setString(k++, (String)m.get("clsn_max"));
                stat.setString(k++, (String)m.get("clsn_cap"));
                stat.setString(k++, (String)m.get("clsn_bkd"));
                stat.setInt(k++, Integer.parseInt((String) m.get("max")));
                stat.setTimestamp(k++, (Timestamp) m.get("insert_date"));
                stat.setInt(k++, DateTimeHelper.daysBetween((Date) m.get("insert_date"), (Date) m.get("flight_date")));
                stat.setInt(k++, (Integer) m.get("year"));
                stat.setInt(k++, (Integer) m.get("month"));
                stat.setInt(k++, (Integer) m.get("week"));
                stat.setString(k++, (String)m.get("hd"));
                stat.setInt(k++, m.get("std_price") == null ? 0 : ((Number) m.get("std_price")).intValue());
                stat.setInt(k++, (Integer)m.get("hdlx"));
                stat.setDouble(k++, (Double) m.get("fly_time"));
                stat.setTimestamp(k++, (Timestamp) m.get("org_insert_date"));
                stat.setString(k++, (String)m.get("job_id"));
            }

            @Override
            public int getBatchSize() {
                return fdls.size();
            }
        });
    }

    private Map<String, Object> getFdl(Map<String, Object> flight, String cabins) {
        Map<String, Object> fdl = new HashMap<>();
        fdl.put("flight_date", flight.get("segment-DepartureDate"));
        fdl.put("fltno", (String)flight.get("AirlineCode") + flight.get("FlightNumber") + flight.get("Suffix"));
        fdl.put("comp", flight.get("AirlineCode"));

        String dep = (String) flight.get("segment-OriginAirport");
        String arr = (String) flight.get("segment-DestinationAirport");
        String hx = (String) flight.get("segment-eline");

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
        fdl.put("max", flight.get("segment-Max"));
        fdl.put("insert_date", flight.get("hdr-DTTM"));
        fdl.put("org_insert_date", flight.get("hdr-DTTM"));

        Date flightDate = (Date) fdl.get("flight_date");
        Calendar cal = Calendar.getInstance();
        cal.setTime(flightDate);

        fdl.put("year", cal.get(Calendar.YEAR));
        fdl.put("month", cal.get(Calendar.MONTH));
        int week = cal.get(Calendar.DAY_OF_WEEK);
        fdl.put("week", week == 0 ? 7 : week);
        fdl.put("hd", fdl.get("dep") + "-" + fdl.get("arr"));   // 航段
        fdl.put("hdlx", 1);     // 国内航线
        fdl.put("job_id", "CSV");

        int bkd = 0;
        // 并非每个舱都有效
        for (char c = 'a' ; c <= 'z' ; c++) {
            String cabin = Character.toString(c);
            fdl.put(cabin+"_bkg", flight.get("segment-cabin" + cabin.toUpperCase() + "-BKD"));
            if (fdl.get(cabin+"_bkg") != null) {
                bkd += Integer.parseInt((String) fdl.get(cabin+"_bkg"));
            }
            try {
                fdl.put(cabin+"_cap", ""+Integer.parseInt((String)flight.get("segment-cabin" + cabin.toUpperCase() + "-LSV")));
            } catch (NumberFormatException e) {
                fdl.put(cabin+"_cap", "0");
            }
        }
        fdl.put("bkg", bkd);
        fdl.put("clsn", cabins);

        return fdl;
    }

    private List<Map<String, Object>> parseSegFile(File file) throws IOException {
        LogHelper.log("process file: " + file.getName());

        List<Map<String, Object>> segs = new ArrayList<>();

        java.sql.Date today = new java.sql.Date(System.currentTimeMillis());

        SimpleDateFormat format1 = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat format4 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Map<String, Object> prev = null;
        Map<String, Object> seg = null;

        Timestamp dttm = null;

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

            if  (dttm == null) {
                try {
                    dttm = new Timestamp(format4.parse(cols[0]).getTime());
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }

            String flightNumber = cols[2];
            java.sql.Date flightDate = null;
            try {
                flightDate = new java.sql.Date(format1.parse(cols[3]).getTime());
            } catch (ParseException e) {
                e.printStackTrace();
            }
            String dep = cols[4];
            String arr = cols[5];
            if (seg == null || prev != null && (!prev.get("FlightNumber").equals(flightNumber) ||
                    !prev.get("segment-DepartureDate").equals(flightDate) ||
                    !prev.get("segment-OriginAirport").equals(dep) ||
                    !prev.get("segment-DestinationAirport").equals(arr))) {
                seg = new HashMap<>();
                segs.add(seg);
                prev = seg;

                seg.put("file", file.getName());
                seg.put("hdr-DTTM", dttm);
                seg.put("hdr-FLAG", "CSV");
                seg.put("Type", "DOMESTIC");
                seg.put("Date", today);

                seg.put("AirlineCode", cols[1].trim());
                seg.put("FlightNumber", flightNumber);
                seg.put("Suffix", "");
                seg.put("segment-DepartureDate", flightDate);
                seg.put("segment-OriginAirport", dep);
                seg.put("segment-DestinationAirport", arr);
            }

            String type = cols[6];
            seg.put("segment-cabin" + type + "-BKD", cols[8]);
            seg.put("segment-cabin" + type + "-GRS", cols[9]);
            seg.put("segment-cabin" + type + "-BLK", cols[10]);
            seg.put("segment-cabin" + type + "-WL", cols[11]);
            seg.put("segment-cabin" + type + "-LT", cols[14]);
            seg.put("segment-cabin" + type + "-LSS", cols[13].trim());
            // 有的数为xx.0
            seg.put("segment-cabin" + type + "-LSV", cols[12].contains(".") ? cols[12].substring(0, cols[12].indexOf(".")) : cols[12]);
//                record.put("segment-cabin" + type + "-SMT", classOfServiceInfo.getString("classOfServiceType"));
            seg.put("segment-cabin" + type + "-IND", cols[18].trim());
            seg.put("segment-cabin" + type + "-NoShow", cols[15]);
            seg.put("segment-cabin" + type + "-GoShow", cols[16]);
            String avail = (String) seg.get("segment-cabin" + type + "-LSS");
            if (!StringUtils.isNumeric(avail)) {
                avail = "0";
            }
            seg.put("segment-cabin" + type + "-Avail", avail);
            seg.put("segment-cabin" + type + "-AvailStatus", cols[17]);

        }
        csvReader.close();

        return segs;
    }

    private List<Map<String, Object>> parseLegFile(File file, List<Map<String, Object>> flightInfos) throws IOException {
        LogHelper.log("process file: " + file.getName());

        List<Map<String, Object>> legs = new ArrayList<>();

        java.sql.Date today = new java.sql.Date(System.currentTimeMillis());

        SimpleDateFormat format1 = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat format4 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Map<String, Object> prev = null;
        int clzIndex = 0;
        Map<String, Object> leg = null;

        Timestamp dttm = null;

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

            if  (dttm == null) {
                try {
                    dttm = new Timestamp(format4.parse(cols[0]).getTime());
                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }

            String flightNumber = cols[2];
            String comp = cols[1];
            java.sql.Date flightDate = null;
            try {
                flightDate = new java.sql.Date(format1.parse(cols[3]).getTime());
            } catch (ParseException e) {
                e.printStackTrace();
            }
            String dep = cols[4];
            String arr = cols[5];
            if (leg == null || prev != null && (!prev.get("leg-FlightNumber").equals(flightNumber) ||
                    !prev.get("leg-DepartureDate").equals(flightDate) ||
                    !prev.get("leg-OriginAirport").equals(dep) ||
                    !prev.get("leg-DestinationAirport").equals(arr))) {
                leg = new HashMap<>();
                legs.add(leg);
                prev = leg;
                clzIndex = 0;

                leg.put("file", file.getName());
                leg.put("hdr-DTTM", dttm);
                leg.put("hdr-FLAG", "CSV");
                leg.put("Type", cols[6].equals("D") ? "DOMESTIC" : "INTL");
                leg.put("Date", today);
                leg.put("AirlineCode", comp);
                leg.put("FlightNumber", flightNumber);
                leg.put("Suffix", "");

                leg.put("leg-CarrierCode", comp);
                leg.put("leg-FlightNumber", flightNumber);
                leg.put("leg-Suffix", "");
                leg.put("leg-DepartureDate", flightDate);
                leg.put("leg-OriginAirport", dep);
                leg.put("leg-DestinationAirport", arr);

//                leg.put("leg-LegMiles", cols[8]);
//                leg.put("leg-Seq", cols[6]);

//                leg.put("leg-Equipment", cols[10]);
                leg.put("leg-ScheduledDeparture-AirportCode", dep);
                leg.put("leg-ScheduledDeparture-DepartureDate", flightDate);
                leg.put("leg-ScheduledDeparture-Time", cols[7]);
                leg.put("leg-ScheduledArrival-AirportCode", arr);
                leg.put("leg-ScheduledArrival-ArrivalDate", DateTimeHelper.date2String(DateTimeHelper.getDateAfter(flightDate, Integer.parseInt(cols[9])), AirlinkConst.TIME_DATE_FORMAT));
                leg.put("leg-ScheduledArrival-Time", cols[8]);

                String flightDateStr = DateTimeHelper.date2String(flightDate, AirlinkConst.TIME_DATE_FORMAT);
                Map<String, Object> flightInfo = flightInfos.stream().filter(x -> (comp + flightNumber).equals(x.get("FLIGHT_NO")) &&
                        flightDateStr.equals(x.get("DEP_DATE")) && dep.equals(x.get("DEP")) && arr.equals(x.get("ARR"))).findFirst().orElse(null);
                if (flightInfo != null) {
                    leg.put("leg-LegMiles", flightInfo.get("DISTANCE"));
                    leg.put("leg-Equipment", flightInfo.get("EQT"));
                }
            }

            leg.put("leg-cabin" + clzIndex + "-class", cols[10]);
            leg.put("leg-cabin" + clzIndex + "-ClassOfService", cols[11]);

            leg.put("leg-cabin" + clzIndex + "-AV", cols[21]);
            leg.put("leg-cabin" + clzIndex + "-OPN", cols[13]);
            leg.put("leg-cabin" + clzIndex + "-MAX", cols[14]);
            leg.put("leg-cabin" + clzIndex + "-CAP", cols[12]);
            leg.put("leg-cabin" + clzIndex + "-TB", cols[15]);
//            leg.put("leg-cabin" + clzIndex + "-GT", cabinInfo.getString("groupNumber"));  //团队表单
            leg.put("leg-cabin" + clzIndex + "-GRO", cols[16]);  // 团队开放数
            leg.put("leg-cabin" + clzIndex + "-GRS", cols[17]);  // 团队订座数
            leg.put("leg-cabin" + clzIndex + "-BLK", cols[18]);
            leg.put("leg-cabin" + clzIndex + "-LT", cols[19]);
            leg.put("leg-cabin" + clzIndex + "-LSS", cols[20]);
            leg.put("leg-cabin" + clzIndex + "-NoShow", cols[22]);
            leg.put("leg-cabin" + clzIndex + "-GoShow", cols[23]);
            leg.put("leg-cabin" + clzIndex + "-IND", cols[24]);
            leg.put("leg-cabin" + clzIndex + "-AVS", cols[25]);
            leg.put("leg-cabin" + clzIndex + "-PCF", cols[26]);
            leg.put("leg-cabin" + clzIndex + "-CND", cols[27]);
            leg.put("leg-cabin" + clzIndex + "-SMT", cols[28]);

            clzIndex++;
        }
        csvReader.close();

        return legs;
    }

    static class InvFilter implements Filter {
        @Override
        public boolean match(String name) {
            // 只处理inv_leg/seg文件，排除inv_his_leg/seg文件
            return name.startsWith("inv_") && !name.contains("_his_");
        }

        @Override
        public String getType() {
            return "inv";
        }
    }
}
