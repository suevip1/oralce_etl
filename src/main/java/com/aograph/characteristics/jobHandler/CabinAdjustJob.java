package com.aograph.characteristics.jobHandler;

import cn.hutool.core.date.ChineseDate;
import com.alibaba.fastjson.JSONObject;
import com.aograph.characteristics.utils.*;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 调舱任务。可接受参数格式为
 *   {"from":"D0", "to":"D2", "holiday":true}。
 * 其中Dn为起飞前天数，holiday表示是否为节假日。forecastDate为预测时间，没有时取最新预测时间。
 */
@Component
public class CabinAdjustJob {
    @Value("${ftp.host}")           // FTP服务器地址
    private String host;
    @Value("${ftp.port}")           // FTP端口号
    private int port;
    @Value("${ftp.security_name}")  // FTP用户名
    private String user;
    @Value("${ftp.security_code}")  // FTP密码
    private String password;

    @Value("${spring.datasource.model.aircode}")
    private String airCodes;

    @Resource(name = "oracleDataSource")
    private DataSource dataSource;

    @Resource
    private CabinAdjustService cabinAdjustService;

    /**
     * 运行调舱任务
     *
     * @param myparam 格式为 {"from":"D0", "to":"D2", "holiday":true, "forecastDate":"2022-11-01 10:23:45"}。其中Dn为起飞前天数，holiday表示是否为节假日。forecastDate为预测时间，没有时取最新预测时间
     * @throws Exception
     */
    @XxlJob(value ="cabinAdjustJob")
    public void run(String myparam) throws Exception {
        String param = XxlJobHelper.getJobParam();
        if (StringUtils.isNotBlank(myparam)) {
            param = myparam;
        }
        JSONObject map = new JSONObject();
        if (StringUtils.isNotBlank(param)) {
            map = JSONObject.parseObject(param);
        }
        // 格式为 {"from":"D0", "to":"D2", "holiday":true, "optimize":true}

        boolean holiday = "true".equals(map.getString("holiday"));
        boolean todayIsHoliday = isHoliday();
        if (holiday && !todayIsHoliday || !holiday && todayIsHoliday) {
//            return;
        }

        boolean optimize = map.getString("optimize") == null || "true".equals(map.getString("optimize"));

        String fromStr = map.getString("from");
        String toStr = map.getString("to");
        int from = Integer.parseInt(fromStr.substring(1));
        int to = Integer.parseInt(toStr.substring(1));

        String forcastDate = (String) map.get("forecastDate");
        if(StringUtils.isBlank(forcastDate)){
            // 读取最新的预测时间
            forcastDate = cabinAdjustService.queryMaxForecastDate(from, to);
        }

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        // 读取预测的所有航班
        List<Map<String, Object>> flights = cabinAdjustService.queryFlights(forcastDate, from, to);
        if (flights == null || flights.size() == 0) {
            return;
        }

        List<String> dataList = new ArrayList<>();
        // title
        dataList.add("Carrier_Cd,Flt_Nbr,Orig_Airport_Cd,Dest_Airport_Cd,Flt_Dpt_Dt,Dcp_Dt,Dcp_Tm,Dcp_Point,Cap_Qty,Pax_Qty,ODIF,LSV,Open_Ind");

        Map<String, Map<String, String>> clsnMap = new HashMap<>();     // key: flight_date
        Map<String, List<Map<String, Object>>> roMap = new HashMap<>(); // key: flight_date

        for (Map<String, Object> flight : flights) {
            // 生成调舱对象
            CabinAdjustService.Adjust adjust = cabinAdjustService.createForecastAdjust((String) flight.get("fltno"),
                    DateTimeHelper.date2String((Timestamp) flight.get("flight_date"), AirlinkConst.TIME_DATE_FORMAT),
                    (String) flight.get("dep"), (String) flight.get("arr"), getAllocateDetail(flight), forcastDate);

            Map<String, String> myCLsnMap = clsnMap.get(adjust.getFlightDate());
            if (myCLsnMap == null) {
                // 取最新的clsn. clsn格式为JCDIO/WS/YPBMHKUALQEVZTNRGX
                String sql = "WITH " +
                        "t1 as (select clsn, fltno, insert_date from ANALY_AOGRAPH_AIR_FOR_MODEL where flight_date=DATE '%s' " +
                        "and length(clsn) > 0 and comp in ('"+airCodes.replaceAll(",", "','")+"')) " +
                        "SELECT * FROM (" +
                                "  SELECT t1.*, row_number() OVER (PARTITION BY fltno ORDER BY insert_date DESC) limit_order FROM t1) WHERE limit_order <= 1";

                List<Map<String, Object>> data = template.queryForList(String.format(sql, adjust.getFlightDate()));
                myCLsnMap = data.stream().collect(Collectors.toMap(x -> (String)x.get("fltno"), x -> (String)x.get("clsn")));
                clsnMap.put(adjust.getFlightDate(), myCLsnMap);
            }

            List<Map<String, Object>> ro = roMap.get(adjust.getFlightDate());
            if (ro == null) {
                // 读当天的最新的RO
                StringBuffer sql = new StringBuffer();
                sql.append("WITH\n" +
                        "t1 as (SELECT dttm, seg_dep_date as flight_date, (airline_code || flight_number || suffix) as fltno," +
                        "  seg_dep, seg_arr, " +
                        " seg_ca_ind, seg_cb_ind, seg_cc_ind, seg_cd_ind, seg_ce_ind, seg_cf_ind, seg_cg_ind, seg_ch_ind, seg_ci_ind, seg_cj_ind, seg_ck_ind, seg_cl_ind, seg_cm_ind, seg_cn_ind, seg_co_ind, seg_cp_ind, " +
                        " seg_cq_ind, seg_cr_ind, seg_cs_ind, seg_ct_ind, seg_cu_ind, seg_cv_ind, seg_cw_ind, seg_cx_ind, seg_cy_ind, seg_cz_ind, " +
                        " seg_ca_lsv, seg_cb_lsv, seg_cc_lsv, seg_cd_lsv, seg_ce_lsv, seg_cf_lsv, seg_cg_lsv, seg_ch_lsv, seg_ci_lsv, seg_cj_lsv, seg_ck_lsv, seg_cl_lsv, seg_cm_lsv, seg_cn_lsv, seg_co_lsv, seg_cp_lsv, " +
                        " seg_cq_lsv, seg_cr_lsv, seg_cs_lsv, seg_ct_lsv, seg_cu_lsv, seg_cv_lsv, seg_cw_lsv, seg_cx_lsv, seg_cy_lsv, seg_cz_lsv, " +
                        " seg_ca_bkd, seg_cb_bkd, seg_cc_bkd, seg_cd_bkd, seg_ce_bkd, seg_cf_bkd, seg_cg_bkd, seg_ch_bkd, seg_ci_bkd, seg_cj_bkd, seg_ck_bkd, seg_cl_bkd, seg_cm_bkd, seg_cn_bkd, seg_co_bkd, seg_cp_bkd, " +
                        " seg_cq_bkd, seg_cr_bkd, seg_cs_bkd, seg_ct_bkd, seg_cu_bkd, seg_cv_bkd, seg_cw_bkd, seg_cx_bkd, seg_cy_bkd, seg_cz_bkd " +
                        "  from ORG_INV_SEGMENT \n" +
//                        "  where to_date(dttm)=to_date(sysdate) and seg_dep_date=DATE '").append(adjust.getFlightDate()).append("'),\n" +
                        "  where seg_dep_date=DATE '").append(adjust.getFlightDate()).append("'),\n" +
                        "t2 as (SELECT * FROM (" +
                        "  SELECT t1.*, row_number() OVER (PARTITION BY flight_date, fltno, seg_dep, seg_arr ORDER BY dttm DESC) limit_order FROM t1) WHERE limit_order <= 1) " +
                        "select t2.* from t2");
                ro = template.queryForList(sql.toString());

                roMap.put(adjust.getFlightDate(), ro);
            }

            Map<String, Object> myRo = ro.stream().filter(x -> adjust.getFlightNo().equals(x.get("fltno")) &&
                    adjust.getDep().equals(x.get("seg_dep")) && adjust.getArr().equals(x.get("seg_arr"))).findFirst().orElse(null);
            // 如果当天没有RO数据， 放弃调舱
            if (myRo != null) {
                dataList.addAll(cabinAdjustService.createAdjustCabinCommands(adjust, myCLsnMap.get(adjust.getFlightNo()), myRo, optimize));
            }
        }

        LogHelper.log("create data: " + (dataList.size() - 1));

        // 指令文件备份到这个目录下
        String home = System.getProperty("user.dir");
        File processedDir = new File(home, "data/cmds/"+ DateTimeHelper.date2String(new Date(), AirlinkConst.TIME_DATE_FORMAT));
        if (!processedDir.exists()) {
            processedDir.mkdirs();
        }

        // 生成指定名字的文件
        File cmdFile = new File(processedDir, "renren_RM_LSV_"+DateTimeHelper.date2String(new Date(), "yyyyMMdd_HHmmss")+".txt");
        FileUtils.writeLines(cmdFile, "UTF-8", dataList);

        SftpUtil ftp = new SftpUtil();

        ftp.setServer(host);
        ftp.setPort(port);
        ftp.setUser(user);
        ftp.setPassword(password);
        LogHelper.log("connect ftp "+host);
        ftp.connect();
        LogHelper.log("ftp connected");


        // 上传到ftp
        String dir = SpringContextUtil.getString("ftp.upload_dir", "/upload");
        if (!ftp.isFileExist(dir)) {
            ftp.createDirectory(dir);
        }
        ftp.uploadFile(cmdFile.getAbsolutePath(), dir + "/"+cmdFile.getName());

        LogHelper.log("put on ftp: " + cmdFile.getName());
    }

    // 判断是否节假日
    private boolean isHoliday() {
        Calendar cal = Calendar.getInstance();
        ChineseDate cd = new ChineseDate(cal.getTime());
        int month = cd.getMonth();
        int day = cd.getDay();
        if (month == 12 && day >= 16 || month == 1 && day <= 25) {  // 春运
            return true;
        }

        month = cal.get(Calendar.MONTH);
        if (month == 7 || month == 8) {     // 暑运
            return true;
        }

        day = cal.get(Calendar.DAY_OF_MONTH);
        if (month == 9 && day >= 28 || month == 10 && day <= 8) {   // 国庆
            return true;
        }

        String today = DateTimeHelper.date2String(cal.getTime(), AirlinkConst.TIME_DATE_FORMAT);
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);
        List<String> list = template.queryForList("select MEMO from ASSIST_HOLIDAY_CFG where HOLIDAY='" + today + "'", String.class);
        if (list != null && list.size() > 0) {     // 法定节假日
            return true;
        }

        return false;
    }

    private String getAllocateDetail(Map<String, Object> flight) {
        // 将经济舱，商务舱，头等舱的预测结果合到一起

        // format like [{"price": 3160, "code": "Y", "cap": 9}, {"price": 2840, "code": "T", "cap": 10}]
        StringBuffer s = new StringBuffer((String)flight.get("ALLOCATE_DETAIL"));
        s.setLength(s.length() - 1);
        String c = (String)flight.get("C_ALLOCATE_DETAIL");
        if (c != null && c.startsWith("[")) {
            s.append(",").append(c.substring(1, c.length() - 1));
        }
        String f = (String)flight.get("F_ALLOCATE_DETAIL");
        if (f != null && f.startsWith("[")) {
            s.append(",").append(f.substring(1, f.length() - 1));
        }
        s.append("]");
        return s.toString();
    }
}
