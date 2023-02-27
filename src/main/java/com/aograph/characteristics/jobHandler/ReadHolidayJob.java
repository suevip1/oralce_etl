package com.aograph.characteristics.jobHandler;

import cn.hutool.core.date.ChineseDate;
import com.alibaba.fastjson.JSONObject;
import com.aograph.characteristics.control.ApiController;
import com.aograph.characteristics.control.HolidayTransitService;
import com.aograph.characteristics.utils.DateTimeHelper;
import com.aograph.characteristics.utils.LogHelper;
import com.aograph.characteristics.utils.SftpUtil;
import com.aograph.characteristics.utils.SpringContextUtil;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.annotation.Resource;
import javax.sql.DataSource;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 读取假日任务
 */
@Component
public class ReadHolidayJob {
    @Resource(name = "oracleDataSource")
    private DataSource dataSource;

    @Resource
    private HolidayTransitService holidayTransitService;

    @XxlJob(value ="readHolidayJob")
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

        initLunar();

        SftpUtil ftp = new SftpUtil();

        ftp.setServer(SpringContextUtil.getString("ftp.host", null));
        ftp.setPort(SpringContextUtil.getInt("ftp.port", 21));
        ftp.setUser(SpringContextUtil.getString("ftp.security_name", null));
        ftp.setPassword(SpringContextUtil.getString("ftp.security_code", null));
        LogHelper.log("connect ftp");
        ftp.connect();
        LogHelper.log("ftp connected");

        Exception ex = null;
        String dir = history ? SpringContextUtil.getString("ftp.history_dir", "/history") : SpringContextUtil.getString("ftp.download_dir", "/MU_FILES");
        List<String> files = ftp.listFiles(dir);
        for (String file : files) {
            if (file.startsWith("holiday") && (file.endsWith(".htm") || file.endsWith(".html"))) {
                LogHelper.log("process file: " + file);

                // 备份到这个目录下， 目录按类型，日期存放
                String home = System.getProperty("user.dir");
                File processedDir = new File(home, "data/holiday/");
                if (!processedDir.exists()) {
                    processedDir.mkdirs();
                }

                File dataFile = new File(processedDir, file);
                ftp.download(dir +"/" + file, dataFile.getAbsolutePath());
                ftp.deleteFile(dir + "/" + file);

                try {
                    readFile(dataFile);
                } catch (Exception e) {
                    e.printStackTrace();
                    ex = e;
                    LogHelper.error(file, e);
                }
            }
        }

        holidayTransitService.addHolidayDate(HolidayTransitService.APPEND);

        if (ex != null) {
            throw ex;
        }
    }

    private void readFile(File dataFile) throws IOException, ParserConfigurationException, SAXException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        Pattern pattern = Pattern.compile("(\\d+)月(\\d+)日");

        String content = FileUtils.readFileToString(dataFile, "UTF-8");
        int i = content.indexOf("年节日放假");
        int year = Integer.parseInt(content.substring(i - 4, i));
        cal.set(Calendar.YEAR, year);

        i = content.indexOf("<table ");
        int j = content.indexOf("</table>", i);
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        Document doc = db.parse(new ByteArrayInputStream((content.substring(i, j) + "</table>").getBytes(StandardCharsets.UTF_8)));
        NodeList trs = doc.getElementsByTagName("tr");

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        // 计算可用的ID值和节假日序号。
        // 节假日序号用来表示一个节假日。
        Map<String, Object> max = template.queryForMap("select max(ID) as ID, max(HOLIDAY_SEQ) as HOLIDAY_SEQ from ASSIST_HOLIDAY_CFG");
        int id = max.get("ID") == null ? 1 : ((Number)max.get("ID")).intValue() + 1;
        int holidaySeq = max.get("HOLIDAY_SEQ") == null ? 1 : ((Number)max.get("HOLIDAY_SEQ")).intValue() + 1;

        // 重复插入也不出错
        String sql = "MERGE INTO ASSIST_HOLIDAY_CFG a\n" +
                "USING (SELECT ? as ID, ? as HOLIDAY, ? as MEMO, ? as DAYS, ? as SEQ, ? as HOLIDAY_SEQ, ? as OVERFLOW FROM dual) b\n" +
                "ON ( a.HOLIDAY=b.HOLIDAY)\n" +
                "WHEN NOT MATCHED THEN\n" +
                "  INSERT (ID, HOLIDAY, MEMO, ELINE, EXCLUDE_ELINE, DAYS, OVERFLOW, SEQ, HOLIDAY_SEQ) VALUES(b.ID, b.HOLIDAY, b.MEMO, '*', '', b.DAYS, b.OVERFLOW, b.SEQ, b.HOLIDAY_SEQ)";

        //遍历该集合，显示结合中的元素及其子元素的名字
        for(int k = 1; k < trs.getLength() ; k++) {
            Element node = (Element) trs.item(k);
            NodeList tds = node.getElementsByTagName("td");
            String desc = ((Element) tds.item(0)).getElementsByTagName("a").item(0).getFirstChild().getNodeValue();
            String holiday = tds.item(1).getFirstChild().getNodeValue();

            Matcher mt = pattern.matcher(holiday);
            mt.find();
            cal.set(Calendar.MONTH, Integer.parseInt(mt.group(1)) - 1);
            cal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(mt.group(2)));
            Date startDate = cal.getTime();
            mt.find();
            try {
                cal.set(Calendar.MONTH, Integer.parseInt(mt.group(1)) - 1);
                cal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(mt.group(2)));
            } catch (RuntimeException e) {
                // 只放一天
            }
            Date endDate = cal.getTime();
            cal.setTime(startDate);
            // 有可能假日是从12-31到01-02，出现跨年
            if (startDate.after(endDate)) {
                cal.add(Calendar.YEAR, -1);
                startDate = cal.getTime();
            }

            int seq = 1;
            while(!cal.getTime().after(endDate)) {
                template.update(sql, id++, sdf.format(cal.getTime()), year+"年"+desc, DateTimeHelper.daysBetween(startDate, endDate) + 1, seq++, holidaySeq, 0);

                cal.add(Calendar.DAY_OF_YEAR, 1);
            }

            // 缺省溢出1天
            cal.setTime(startDate);
            cal.add(Calendar.DAY_OF_YEAR, -1);
            template.update(sql, id++, sdf.format(cal.getTime()), year+"年"+desc, DateTimeHelper.daysBetween(startDate, endDate) + 1, -1, holidaySeq, 1);
            cal.setTime(endDate);
            cal.add(Calendar.DAY_OF_YEAR, 1);
            template.update(sql, id++, sdf.format(cal.getTime()), year+"年"+desc, DateTimeHelper.daysBetween(startDate, endDate) + 1, seq++, holidaySeq, 1);

            holidaySeq++;
        }
    }

    /**
     * 自动生成农历阳历对照表
     */
    private void initLunar() {
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        String lday = template.queryForObject("select max(lday) from ASSIST_LUNAR", String.class);
        if (lday != null) {
            return;
        }

        String sql = "INSERT INTO ASSIST_LUNAR (ID, GDAY, LDAY) VALUES(?, ?, ?)";

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

        int id = 1;
        for (int y = 2018 ; y <= 2028 ; ) {
            int month = 12;
            int day = 16;
            for ( ; ; ) {
                ChineseDate ch = new ChineseDate(y, month, day);
                if (ch.getGregorianYear() > 2000) { // means there is no 12-30
                    template.update(sql, id++, sdf.format(ch.getGregorianDate()), String.format("%d-%02d-%02d", ch.getChineseYear(), ch.getMonth(), ch.getDay()));
                }

                if (day == 30) {
                    month = 1;
                    day = 1;
                    y++;
                } else {
                    day++;
                }
                if (month == 1 && day == 26) {
                    break;
                }
            }
        }
    }
}
