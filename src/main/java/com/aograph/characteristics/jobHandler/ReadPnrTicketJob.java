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
import java.util.stream.Collectors;

@Component
public class ReadPnrTicketJob extends AbstractFileJob {
    @Resource(name = "oracleDataSource")
    private DataSource oracleDataSource;

    @XxlJob(value ="readPnrTicketJob")
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

        processFiles(new ContainFilter("pnr"), history);
    }

    // 解析文件
    public void processFile(File file) throws IOException {
        LogHelper.log("process file: " + file.getName());

        JdbcTemplate template = getJdbcTemplate();
        List<Map<String, Object>> names = template.queryForList("select 1 as flag, ROWNUM from ORG_PNR_TICKET_DETL where FILE_NAME=? and ROWNUM <= 1", file.getName());
        if (names != null && names.size() > 0) {
            // 同一个文件拒绝重复导入
            return;
        }

        parseFile(file);
    }

    // 将数据插入表中
    private void insertData(List<Map<String, Object>> list) {
        JdbcTemplate template = getJdbcTemplate();

        createSequence(template, "ORG_PNR_TICKET_DETL", "ORG_PNR_TICKET_DETL_SEQ");

        String sql = "INSERT INTO ORG_PNR_TICKET_DETL\n" +
                "(ID, CREATE_TIME, PNR_CREATE_DATE, GDS_CD, PNR_GRP_FLAG, PNR_CNCL_FLAG, HEAD_CONTENT, PAX_ID, SEG_ID, CARRIER_CD, OPR_CARRIER_CD, OPR_FLT_NBR, " +
                "FLT_NBR, FLT_NBR_ORIG, DEP, ARR, FLIGHT_DATE, DEP_DATE, DEP_TIME, ARR_DATE, ARR_TIME, NIGHT_FLT_IND, CLASS_CD, " +
                "SUB_CLASS_CD, CDSHR_TYPE_CD, OVER_PAX_QTY, MARR_SGMT_IND, OPR_STAT_CD, BOOKING_CRT_TXN_ID, BOOKING_CRT_TM, " +
                "TICKET_RLTV_POSN, PAX_DATA_ID, TICKET_TXN_ID, TICKET_TM, TICKET_AGENT_OFFICE_NBR, CORP_ID, FARE_AMT_CNY, " +
                "FARE_AMT_TOTAL, CURRENCY_CD, OD_ID, OD_TYPE_CD, OD_TYPE_NAME, OD_SEG_NBR, TRVL_AIRPORT_CD_LIST, TRVL_CITY_CD_LIST, " +
                "TRVL_CARRIER_CD_LIST, TRVL_FLT_NBR_LIST, TRVL_FLT_DPT_DT_LIST, TRVL_SUB_CLASS_CD_LIST, TRVL_DEP, TRVL_ARR, " +
                "TRVL_DEP_CITY, TRVL_DEP_CITY_CNAME, TRVL_ARR_CITY_CD, TRVL_ARR_CITY_CNAME, TRVL_DEP_COUNTRY, " +
                "TRVL_DEP_COUNTRY_CNAME, TRVL_ARR_COUNTRY, TRVL_ARR_COUNTRY_CNAME, TRVL_DEP_REGION_CD, TRVL_ARR_REGION_CD, " +
                "PAX_AGE_CTG_CD, FFP_CARRIER_CD, FLT_ROUTE_CD, FLT_ROUTE_NAME, NATION_FLAG, TOTAL_CAP_QTY, F_CAP_QTY, " +
                "C_CAP_QTY, Y_CAP_QTY, FILE_NAME)\n" +
                "VALUES(ORG_PNR_TICKET_DETL_SEQ.Nextval, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, " +
                "?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        String[] names = sql.substring(sql.indexOf(",") + 1, sql.indexOf(")")).split(",");
        for (int i = 0; i < names.length; i++) {
            names[i] = names[i].trim();
        }
        batchInsert(sql, names, list);

        createSequence(template, "ANALY_PNR_PRICE_DERIVATION_ALL", "ANALYPNRPRICEDERIVATIONALL_SEQ");
        sql = "INSERT INTO ANALY_PNR_PRICE_DERIVATION_ALL (ID, FLT_NO, DEP, ARR, FLIGHT_DATE, SALE_PRICE, SALE_TIME, TKT_NO, CABIN) " +
                "  VALUES(ANALYPNRPRICEDERIVATIONALL_SEQ.Nextval, ?, ?, ?, ?, ?, ?, ?, ?)\n";

        list = list.stream().sorted((x1, x2) -> ((String)x1.get("PNR_CREATE_DATE")).compareTo((String)x2.get("PNR_CREATE_DATE"))).collect(Collectors.toList());

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        for (int i = 0; i < list.size(); i++) {
            Map<String, Object> m = list.get(i);

            m.put("FLT_NO", m.get("FLT_NBR"));
            m.put("SALE_PRICE", m.get("FARE_AMT_CNY"));
            m.put("SALE_TIME", m.get("BOOKING_CRT_TM"));
            m.put("TKT_NO", "T"+m.get("ID"));
            m.put("CABIN", m.get("SUB_CLASS_CD"));
//            m.put("TAG", m.get("SUB_CLASS_PRICE"));
            try {
                m.put("FLIGHT_DATE", new Date(format.parse((String)m.get("FLIGHT_DATE")).getTime()));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        batchInsert(sql, new String[]{"FLT_NO", "DEP", "ARR", "FLIGHT_DATE", "SALE_PRICE", "SALE_TIME", "TKT_NO", "CABIN"}, list);
    }

    // 解析文件
    private List<Map<String, Object>> parseFile(File file) throws IOException {
        List<Map<String, Object>> list = new ArrayList<>();

        SimpleDateFormat format1 = new SimpleDateFormat("yyyy/MM/dd");
        SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat format3 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Timestamp now = new Timestamp(System.currentTimeMillis());

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

            Map<String, Object> pnr = new HashMap<>();
            list.add(pnr);

            pnr.put("GDS_CD", cols[2]);
            pnr.put("PNR_GRP_FLAG", cols[3]);
            pnr.put("PNR_CNCL_FLAG", cols[4]);
            pnr.put("HEAD_CONTENT", cols[5]);
            pnr.put("PAX_ID", cols[6]);
            pnr.put("SEG_ID", cols[7]);
            pnr.put("CARRIER_CD", cols[8]);
            pnr.put("OPR_CARRIER_CD", cols[9]);
            pnr.put("OPR_FLT_NBR", cols[9] + cols[10]);
            pnr.put("FLT_NBR", cols[8] + cols[11]+ cols[13]);
            pnr.put("FLT_NBR_ORIG", cols[8] + cols[12]+ cols[13]);
            pnr.put("DEP", cols[14]);
            pnr.put("ARR", cols[15]);
            pnr.put("FLIGHT_DATE", cols[16].substring(0, cols[16].indexOf(" ")).replaceAll("/", "-"));
            pnr.put("DEP_DATE", cols[17].substring(0, cols[17].indexOf(" ")).replaceAll("/", "-"));
            pnr.put("DEP_TIME", cols[18].substring(cols[18].indexOf(" ")+1, cols[18].lastIndexOf(":")));
            pnr.put("ARR_DATE", cols[19].substring(0, cols[19].indexOf(" ")).replaceAll("/", "-"));
            pnr.put("ARR_TIME", cols[20].substring(cols[20].indexOf(" ")+1, cols[20].lastIndexOf(":")));
            pnr.put("NIGHT_FLT_IND", cols[21]);
            pnr.put("CLASS_CD", cols[22]);
            pnr.put("SUB_CLASS_CD", cols[23]);
            pnr.put("CDSHR_TYPE_CD", cols[24]);
            pnr.put("OVER_PAX_QTY", cols[25]);
            pnr.put("MARR_SGMT_IND", cols[26]);
            pnr.put("OPR_STAT_CD", cols[27]);
            pnr.put("BOOKING_CRT_TXN_ID", cols[28]);
            try {
                pnr.put("BOOKING_CRT_TM", new Timestamp(format3.parse(
                        cols[29].substring(0, cols[29].indexOf(" ")).replaceAll("/", "-") +
                                cols[30].substring(cols[30].indexOf(" "), cols[30].lastIndexOf("."))).getTime()));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            pnr.put("TICKET_RLTV_POSN", cols[31]);
            pnr.put("PAX_DATA_ID", cols[32]);
            pnr.put("TICKET_TXN_ID", cols[33]);
            pnr.put("TICKET_TM", cols[34].length() == 0 || cols[35].length() == 0 ? "" :
                    cols[34].substring(0, cols[34].indexOf(" ")).replaceAll("/", "-") +
                            cols[35].substring(cols[35].indexOf(" "), cols[35].lastIndexOf(".")));
            pnr.put("TICKET_AGENT_OFFICE_NBR", cols[36]);
            pnr.put("CORP_ID", cols[37]);
            pnr.put("FARE_AMT_CNY", cols[38]);
            pnr.put("FARE_AMT_TOTAL", cols[39]);
            pnr.put("CURRENCY_CD", cols[40]);
            pnr.put("OD_ID", cols[41]);
            pnr.put("OD_TYPE_CD", cols[42]);
            pnr.put("OD_TYPE_NAME", cols[43]);
            pnr.put("OD_SEG_NBR", cols[44]);
            pnr.put("TRVL_AIRPORT_CD_LIST", cols[45]);
            pnr.put("TRVL_CITY_CD_LIST", cols[46]);
            pnr.put("TRVL_CARRIER_CD_LIST", cols[47]);
            pnr.put("TRVL_FLT_NBR_LIST", cols[48]);
            pnr.put("TRVL_FLT_DPT_DT_LIST", cols[49]);
            pnr.put("TRVL_SUB_CLASS_CD_LIST", cols[50]);
            pnr.put("TRVL_DEP", cols[51]);
            pnr.put("TRVL_ARR", cols[52]);
            pnr.put("TRVL_DEP_CITY", cols[53]);
            pnr.put("TRVL_DEP_CITY_CNAME", cols[54]);
            pnr.put("TRVL_ARR_CITY_CD", cols[55]);
            pnr.put("TRVL_ARR_CITY_CNAME", cols[56]);
            pnr.put("TRVL_DEP_COUNTRY", cols[57]);
            pnr.put("TRVL_DEP_COUNTRY_CNAME", cols[58]);
            pnr.put("TRVL_ARR_COUNTRY", cols[59]);
            pnr.put("TRVL_ARR_COUNTRY_CNAME", cols[60]);
            pnr.put("TRVL_DEP_REGION_CD", cols[61]);
            pnr.put("TRVL_ARR_REGION_CD", cols[62]);
            pnr.put("PAX_AGE_CTG_CD", cols[63]);
            pnr.put("FFP_CARRIER_CD", cols[64]);
            pnr.put("FLT_ROUTE_CD", cols[65]);
            pnr.put("FLT_ROUTE_NAME", cols[66]);
            pnr.put("NATION_FLAG", cols[67]);
            pnr.put("TOTAL_CAP_QTY", cols[68]);
            pnr.put("F_CAP_QTY", cols[69]);
            pnr.put("C_CAP_QTY", cols[70]);
            pnr.put("Y_CAP_QTY", cols[71]);
            try {
                pnr.put("PNR_CREATE_DATE", format2.format(format1.parse(cols[0])));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            pnr.put("CREATE_TIME", now);
            pnr.put("FILE_NAME", file.getName());

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
