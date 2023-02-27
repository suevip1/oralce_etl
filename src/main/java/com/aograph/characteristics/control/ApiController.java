package com.aograph.characteristics.control;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aograph.characteristics.utils.ConstantUtil;
import com.aograph.characteristics.utils.FreeMarkHelper;
import com.aograph.characteristics.jobHandler.CabinAdjustService;
import com.aograph.characteristics.utils.AirlinkConst;
import com.aograph.characteristics.utils.DateTimeHelper;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * API 实现类
 */
@RestController
@RequestMapping("api")
@Slf4j
public class ApiController {
    @Value("${spring.datasource.model.aircode}")
    private String airCodes;

    @Value("${remote.fchcabin.url}")
    private String emsrbUrl;

    @Resource
    private CabinAdjustService cabinAdjustService;

    @Resource
    private HolidayTransitService holidayTransitService;

//    @Resource
//    private RestTemplate restTemplate;

    @Resource(name = "oracleDataSource")
    private DataSource dataSource;

    /**
     * 获取有关预测的一些关键指标数据
     *
     * @param flightNo 航班号
     * @param flightDate 航班日期
     * @param dep 起飞机场
     * @param arr 到达机场
     * @param forecaseTime 预测时间
     * @return
     */
    @GetMapping("/keyData")
    public ResponseEntity getKeyData(@RequestParam("flight_no") String flightNo,
                                     @RequestParam("flight_date") String flightDate,
                                     @RequestParam("dep") String dep, @RequestParam("arr") String arr, @RequestParam(value = "forecast_time", required = false) String forecaseTime) {
        try {
            JdbcTemplate template = new JdbcTemplate();
            template.setDataSource(dataSource);

            String sql = getSql("key_data.txt");
            Map<String, Object> values = new HashMap<>();
            values.put("flight_no", flightNo);
            values.put("flight_date", flightDate);
            values.put("dep", dep);
            values.put("arr", arr);

            Map<String, Object> data = template.queryForMap(convert(sql, values));

            Map<String, Object> result = new HashMap<>();

            if (data == null) {
                return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), result));
            }

            int exDif = ((Number)data.get("EX_DIF")).intValue();

            Map<String, Object> forecast = null;
            if (StringUtils.isNotBlank(forecaseTime)) {
                sql = "select * from PREDICT_FLIGHT_HE_FORECAST" +
                        "where FLTNO='${flight_no}' and DEP='${dep}' and ARR='${arr}' and FLIGHT_DATE=DATE '${flight_date}' and FORECAST_TIME=TIMESTAMP '${forecast_time}'";
                values.put("forecast_time", forecaseTime);
                forecast = template.queryForMap(convert(sql, values));
            }

            if (forecast != null) {
                result.put("predict_ylowest_price", forecast.get("CUR_LOW_PRICE"));
                result.put("predict_clowest_price", forecast.get("C_CUR_LOW_PRICE"));

                String cabin = (String) forecast.get("CUR_LOW_CABIN");
                JSONObject json = JSONObject.parseObject((String) forecast.get("ORG_ALLOCATE"));
                JSONArray cabins = json.getJSONArray("z_cabin");
                JSONArray prices = json.getJSONArray("z_ow_price");
                for (int i = 0; i < cabins.size(); i++) {
                    if (cabin.equals(cabins.getString(i))) {
                        result.put("ylowest_price", prices.getIntValue(i));
                    }
                }

                cabin = (String) forecast.get("C_CUR_LOW_CABIN");
                json = JSONObject.parseObject((String) forecast.get("C_ORG_ALLOCATE"));
                cabins = json.getJSONArray("z_cabin");
                prices = json.getJSONArray("z_ow_price");
                for (int i = 0; i < cabins.size(); i++) {
                    if (cabin.equals(cabins.getString(i))) {
                        result.put("clowest_price", prices.getIntValue(i));
                    }
                }

                result.put("group_prob", forecast.get("CTGL"));                                             // 成团概率
                result.put("pnr_avg_price", forecast.get("AVG_PNR_PRICE"));                                 // 过去一个月PNR均价
                result.put("seat_ratio_predict", forecast.get("PREDICT_KZL"));                              // 预测客座率
                result.put("ota_dcp", forecast.get("OTA_DCP_TIME"));                                        // ota采集时间
                result.put("inv_dcp", forecast.get("DCP_TIME"));                                            // 采集时间
                result.put("holiday_efffect", forecast.get("HOLIDAY"));                                     // 是否采用了假期,1/采用,0/不采用
                result.put("holiday", forecast.get("HOLIDAY_INFO"));                                        // 采用了哪个假期
                result.put("seat_ratio_no_group", forecast.get("OTA_KZL"));                                 // 当前去团客座率
                int isCrave = ((Number)forecast.get("IS_CARVE")).intValue();                                   // 最低价预测结果
                result.put("seat_ratio_type", isCrave == 1 ? "highest" : (isCrave == 2 ? "lowest" : ""));   // 客座率峰值低谷 highest/lowest
                result.put("lowest_predict_price", forecast.get("ORG_PREDICT_PRICE"));                      // 最低价预测结果
                result.put("advice_price", forecast.get("ADVISE_PRICE"));                                   // 控制博弈后最低价预测结果
            }

            sql = "select WEEKS, ROWNUM from (select WEEKS from ORG_MKT_FLIGHT_SCHEDULE where FLIGHT_NO=? and DEP_DATE=? and DEP=? and ARR=? order by CREATE_TIME desc) where ROWNUM <= 1";
            Map<String, Object> schedule = template.queryForMap(convert(sql, values));
            if  (schedule != null) {
                result.put("DOW", schedule.get("WEEKS"));
            }
            
            result.put("std_price", data.get("STD_PRICE"));                                     // Y舱全价

            result.put("saled", data.get("BKD"));                                               // 当前上客数
//            result.put("saled", data.get("BKD"));                                                 // 预测当天上客数, 暂不支持
//            result.put("seat_ratio_today", null);                                            // 预测今日最终客座率, 暂不支持
            result.put("seat_ratio", data.get("KZL"));                                          // 客座率
            result.put("seat_ratio_YOY", data.get("YOY_KZL"));                                  // 同比客座率
            result.put("seat_ratio_D0", data.get("D0_KZL"));                                    // 预测D0客座率
            int cap = ((Number)data.get("CAP")).intValue();
            int weekBefore = ((Number)data.get("HIS_6")).intValue();
            result.put("seat_ratio_wow", weekBefore == -1 ? 0 : ((double)weekBefore) / cap);    // 环比客座率

            int bkd5 = 0;
            for (int i = 1 ; i <= 5 ; i++) {
                int bkd = ((Number)data.get("HIS_"+i)).intValue();
                bkd5 += bkd == -1 ? 0 : bkd;
            }
            result.put("seat_ratio_h5", ((double)bkd5) / 5 / cap);      // 历史5天平均客座率

            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), result));
        } catch (Exception e) {
            log.error("", e);
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.RUNTIME_FAIL.getCode(), CommonCodeEnum.RUNTIME_FAIL.getDesc() + ": " + e.getMessage()));
        }
    }

    /**
     *
     * 返回上客节奏数据
     *
     * @param flightNo 航班号
     * @param flightDate 航班日期
     * @param dep 起飞机场
     * @param arr 到达机场
     * @return
     */
    @GetMapping("/salesData")
    public ResponseEntity cabinSalesChartData(@RequestParam("flight_no") String flightNo,
                                              @RequestParam("flight_date") String flightDate,
                                              @RequestParam("dep") String dep, @RequestParam("arr") String arr) {
        try {
            JdbcTemplate template = new JdbcTemplate();
            template.setDataSource(dataSource);

            String sql = getSql("sales.txt");
            Map<String, Object> values = new HashMap<>();
            values.put("flight_no", flightNo);
            values.put("flight_date", flightDate);
            values.put("dep", dep);
            values.put("arr", arr);

            List<Map<String, Object>> list = template.queryForList(convert(sql, values));

            List<Map<String, Object>> todayList = list.stream().filter(x -> ((Number)x.get("EX_DIF")).intValue() <= 14).collect(Collectors.toList());

            List<Map<String, Object>> result = new ArrayList<>(list.size());
            for (Map<String, Object> map : todayList) {
                int cap = ((Number) map.get("CAP")).intValue();
                String s = (String) map.get("PREDICT_BKD");

                int pbkd = 0;
                for (String i : s.substring(1, s.length() - 1).split(",")) {
                    pbkd += Integer.parseInt(i);
                }

                int todayExDif = ((Number)map.get("EX_DIF")).intValue();
                Map<String, Object> yestoday = list.stream().filter(x -> ((Number)x.get("EX_DIF")).intValue() >= todayExDif + 1).findFirst().orElse(null);
                Map<String, Object> weekBefore = list.stream().filter(x -> ((Number)x.get("EX_DIF")).intValue() >= todayExDif + 7).findFirst().orElse(null);

                Map<String, Object> m = new HashMap<>();
                m.put("seat_ratio", map.get("KZL"));
                m.put("seat_ratio_wow", weekBefore == null ? 0 : weekBefore.get("KZL"));
                m.put("seat_ratio_yoy", map.get("YOY_KZL"));
                m.put("seat_ratio_predict", ((double)pbkd) / cap);
                m.put("cap", cap);
                m.put("bkd", map.get("BKD"));
                m.put("dbkd", yestoday == null ? ((Number)map.get("BKD")).intValue() : ((Number)map.get("BKD")).intValue() - ((Number)yestoday.get("BKD")).intValue());    // 今日新增
                m.put("bkd_wow", weekBefore == null ? 0 : ((Number)weekBefore.get("BKD")).intValue());   // 环期上客数
                m.put("dbkd_wow", weekBefore == null ? ((Number)map.get("BKD")).intValue() : ((Number)map.get("BKD")).intValue() - ((Number)weekBefore.get("BKD")).intValue()); // 环期增量
                m.put("ex_dif", map.get("EX_DIF"));     // 起飞前天数
                result.add(m);
            }

            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), result));
        } catch (Exception e) {
            log.error("获取历史舱位销售记录错误：", e);
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.RUNTIME_FAIL.getCode(), CommonCodeEnum.RUNTIME_FAIL.getDesc() + ": " + e.getMessage()));
        }
    }

    /**
     *
     * 返回历史价格
     *
     * @param flightNo 航班号
     * @param flightDate 航班日期
     * @param dep 起飞机场
     * @param arr 到达机场
     * @param startDate 价格开始时间
     * @param endDate 价格结束时间
     * @return
     */
    @GetMapping("/priceHistory")
    public ResponseEntity getPriceHistory(@RequestParam("flight_no") String flightNo,
                                          @RequestParam("flight_date") String flightDate,
                                          @RequestParam("dep") String dep,
                                          @RequestParam("arr") String arr,
                                          @RequestParam("startDate") String startDate,
                                          @RequestParam("endDate") String endDate) {
        try {
            String fltno = flightNo;

            JdbcTemplate template = new JdbcTemplate();
            template.setDataSource(dataSource);

            SimpleDateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd");

            String sql = "select fltno, insert_date, bkg/cap as seat_ratio\n" +
                    "  from ANALY_aograph_air_for_model\n" +
                    "  where trunc(insert_date) between DATE '"+startDate+"' and DATE '"+endDate+"' and flight_date = DATE '"+flightDate+"'\n" +
                    "    and (fltno=? or comp not in ('"+airCodes.replaceAll(",", "','")+"')) and dep=? and arr=?";
            List<Map<String, Object>> seatRatioList = template.queryForList(sql, flightNo, dep, arr);
            for (Map<String, Object> map : seatRatioList) {
                map.put("insert_date", dateFormat1.format((Date)map.get("insert_date")));
            }

            sql = "select flight_no, create_time, price\n" +
                    "  from ANALY_OTA_PRICE\n" +
                    "  where trunc(create_time) between DATE '"+startDate+"' and DATE '"+endDate+"' and flight_date = DATE '"+flightDate+"'\n" +
                    "    and (flight_no=? or comp not in ('"+airCodes.replaceAll(",", "','")+"')) and dep=? and arr=?";
            List<Map<String, Object>> priceList = template.queryForList(sql, flightNo, dep, arr);
            for (Map<String, Object> map : priceList) {
                map.put("create_time", dateFormat1.format((Date)map.get("create_time")));
            }

            Map<String, Object> result = makePriceHistoryResult(priceList, fltno, seatRatioList);
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), result));
        } catch (Exception e) {
            log.error("queryCompeteList错误：", e);
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.RUNTIME_FAIL.getCode(), CommonCodeEnum.RUNTIME_FAIL.getDesc() + ": " + e.getMessage()));
        }
    }

    /**
     * 形成chart数据
     *
     * @param priceList 价格列表
     * @param fltno 航班号
     * @param seatRatioList 客座率列表
     * @return
     */
    private Map<String, Object> makePriceHistoryResult(List<Map<String, Object>> priceList, String fltno, List<Map<String, Object>> seatRatioList){
//        Map<String, Object> seatRatioMap = Maps.newHashMap();
        List<String> fltnoList = Lists.newArrayList();
//        Map<String, String> flightTimeMap = Maps.newHashMap();
        seatRatioList.stream().forEach(j->{
//            seatRatioMap.put(j.get("fltno")+"_"+j.get("insert_date"), j.get("seat_ratio"));
            if(!fltnoList.contains(j.get("fltno"))){
                fltnoList.add((String) j.get("fltno"));
//                flightTimeMap.put((String) j.get("fltno"),(String) j.get("deptime"));
            }
        });
        Map<String, Object> resultObject = new HashMap<>();
        //用于曲线横轴
        List<String> xtimeList = priceList.stream().map(p -> (String)p.get("create_time")).distinct().sorted().collect(Collectors.toList());

        Map<String, List<Map<String, Object>>> flightNoMap = priceList.stream().collect(Collectors.groupingBy(p -> (String)p.get("flight_no")));
        for(String flightNo : fltnoList){

            List<Map<String, Object>> valList = (List<Map<String, Object>>)flightNoMap.get(flightNo);
            Map<String, List<Map<String, Object>>> crtMap = valList.stream().collect(Collectors.groupingBy(v -> (String) v.get("create_time")));
            Map<String, Object> jsonObject = valList.get(0);
            for(String str:xtimeList){
                if(!crtMap.containsKey(str)){
                    JSONObject j = new JSONObject();
                    j.put("create_time",str);
//                    j.put("flight_no",jsonObject.get("flight_no"));
                    j.put("dep",jsonObject.get("dep"));
                    j.put("arr",jsonObject.get("arr"));
//                    j.put("flightDate",jsonObject.get("flightDate"));
                    j.put("price",((Number)jsonObject.get("price")).intValue());
                    valList.add(j);
                }
                List<Map<String, Object>> timeJsonList = (List<Map<String, Object>>)crtMap.get(str);
                if(timeJsonList!=null && timeJsonList.size()>0){
                    jsonObject = timeJsonList.get(0);
                }
            }
            List<Map<String, Object>> resultList = valList.stream().sorted(Comparator.comparing(fs -> (String)fs.get("create_time"))).collect(Collectors.toList());
            resultObject.put(flightNo,resultList);
        }

        Map<String, List<Map<String, Object>>> ownMap = new HashMap<>();
        List<Map<String, Object>> ownList = (List<Map<String, Object>>)resultObject.get(fltno);
        if(ownList!=null && ownList.size()>0){
            ownMap = ownList.stream().collect(Collectors.groupingBy(o -> (String) o.get("create_time")));
        }

        for(Map.Entry entry : resultObject.entrySet()){
            String flight_no = (String) entry.getKey();
            List<Map<String, Object>> list = (List<Map<String, Object>>)entry.getValue();

            List<Map<String, Object>> ratios = seatRatioList.stream().filter(x -> x.get("fltno").equals(flight_no)).
                    sorted(Comparator.comparing((Map<String, Object> x) -> (String)x.get("insert_date")).reversed()).
                    collect(Collectors.toList());

            for(Map<String, Object> js : list){
                js.remove("FLIGHT_NO");
                String dep = (String) js.get("dep");
                String arr = (String) js.get("arr");
//                String flight_date = (String) js.get("flightDate");
                js.put("seat_ratio",ratios.stream().
                        filter(x -> ((String)js.get("create_time")).compareTo((String)x.get("insert_date")) >= 0).
                        findFirst().map(x -> ((Number)x.get("seat_ratio")).floatValue()).orElse(null));
//                js.put("flightTime",flightTimeMap.get(flight_no));
                List<Map<String, Object>> jsonObjectList = ownMap.get(js.get("create_time"));
                if(jsonObjectList!=null && jsonObjectList.size()>0){
                    int myPrice = ((Number)jsonObjectList.get(0).get("price")).intValue();
                    js.put("price",((Number)js.get("price")).intValue());
//                    js.put("ownTime",ownObject.get("create_time"));
                    js.put("price_diff",(Integer)js.get("price") - myPrice);
                }
            }
        }

        resultObject.put("xtime",xtimeList);//横轴时刻
        //获取本航班数据
        return resultObject;
    }

    /**
     *
     * 读取白名单列表。可以分页读取。
     *
     * @param pageNo 第几页，从1开始。
     * @param pageSize 每天条数，缺省值为20.
     * @return
     */
    @GetMapping("/whitelist/list")
    public ResponseEntity<?> getWhiteList(@RequestParam(value = "dep", required = false) String dep,
                                          @RequestParam(value = "arr", required = false) String arr,
                                          @RequestParam(value = "fromDate", required = false) String flightDateFrom,
                                          @RequestParam(value = "toDate", required = false) String flightDateTo,
                                          @RequestParam(value = "pageNo", defaultValue = "1") Integer pageNo,
                                          @RequestParam(value = "pageSize", defaultValue = "20") Integer pageSize) {
        int startRow = (pageNo - 1) * pageSize;
        int endRow = startRow + pageSize;
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);
        StringBuffer sql = new StringBuffer("SELECT A.*, ROWNUM RN FROM ASSIST_WHITELIST A where 1=1 ");
        if (StringUtils.isNotBlank(dep)) {
            sql.append("and dep='"+dep+"' ");
        }
        if (StringUtils.isNotBlank(arr)) {
            sql.append("and arr='"+arr+"' ");
        }
        if (StringUtils.isNotBlank(flightDateFrom) && StringUtils.isNotBlank(flightDateTo)) {
            sql.append("and flight_date_from <='"+flightDateFrom+"' and flight_date_to >='"+flightDateTo+"' ");
        } else if (StringUtils.isNotBlank(flightDateFrom)) {
            sql.append("and flight_date_to >='"+flightDateFrom+"' ");
        } else if (StringUtils.isNotBlank(flightDateTo)) {
            sql.append("and flight_date_from <='"+flightDateTo+"' ");
        }
        Integer total = template.queryForObject("select count(*) from (" + sql + ")", Integer.class);
        if (total == null) {
            total = 0;
        }

        sql.append(" order by DEP, ARR, FLIGHT_DATE_FROM, FLIGHT_DATE_TO) WHERE RN >= " + startRow+" and RN < "+endRow);

        List<Map<String, Object>> list = template.queryForList("SELECT * FROM (" + sql);
        list = list.stream().map(x -> {
            x.put("CREATE_TIME", DateTimeHelper.date2String((Date)x.get("CREATE_TIME"), AirlinkConst.TIME_FULL_FORMAT));
            x.put("UPDATE_TIME", DateTimeHelper.date2String((Date)x.get("UPDATE_TIME"), AirlinkConst.TIME_FULL_FORMAT));
            x.remove("RN");
            return x;
        }).collect(Collectors.toList());
        Map<String, Object> result = new HashMap<>();
        result.put("list", list);
        result.put("total", total);
        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), result));
    }

    /**
     * 删除白名单。可以删除一个，也可以删除多个
     *
     * @param id ID值，多个值可以用","分隔
     * @return
     */
    @PostMapping("/whitelist/delete")
    public ResponseEntity<?> deleteWhiteList(@RequestParam("id") String id) {
        if (StringUtils.isBlank(id)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "id needed"));
        }

        id = id.replaceAll(" ", "");
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);
        template.update("DELETE FROM ASSIST_WHITELIST_DETAIL WHERE WHITELIST_ID in ("+id+")");
        template.update("DELETE FROM ASSIST_WHITELIST WHERE ID in ("+id+")");
        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    /**
     * 更新白名单
     *
     * @return
     */
    @PostMapping("/whitelist/update")
    public ResponseEntity<?> updateWhiteList(@RequestBody() String params) {
        JSONObject json = JSON.parseObject(params);
        if (json == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "invalid JSON"));
        }

        Integer id = json.getInteger("id");
        String dep = json.getString("dep");
        String arr = json.getString("arr");
        String fromDate = json.getString("fromDate");
        String toDate = json.getString("toDate");
        Integer exDifFrom = json.getInteger("exDifFrom");
        Integer exDifTo = json.getInteger("exDifTo");
        String memo = json.getString("memo");

        // 参数检查
        if (id == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "id needed"));
        }

        if (fromDate != null && toDate != null && toDate.compareTo(fromDate) < 0) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "toDate should bigger than fromDate"));
        }

        if (exDifFrom != null && exDifTo != null && exDifFrom > exDifTo) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "exDifTo should bigger than exDifFrom"));
        }

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        String currFromDate = null;
        String currToDate = null;
        if (StringUtils.isNotBlank(fromDate) || StringUtils.isNotBlank(toDate)) {
            List<Map<String, Object>> list = template.queryForList("select * from ASSIST_WHITELIST where id=?", id);
            if (list == null || list.size() == 0) {
                return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "data not found for id: "+id));
            }

            Map<String, Object> curr = list.get(0);
            currFromDate = (String)curr.get("FLIGHT_DATE_FROM");
            currToDate = (String)curr.get("FLIGHT_DATE_TO");
            if (StringUtils.isNotBlank(fromDate) && fromDate.equals(currFromDate)) {
                fromDate = null;
            }
            if (StringUtils.isNotBlank(toDate) && toDate.equals(currToDate)) {
                toDate = null;
            }
        }

        if (StringUtils.isNotBlank(fromDate) && StringUtils.isNotBlank(currFromDate)) {
            if (currFromDate.compareTo(fromDate) > 0) {
                if (StringUtils.isBlank(dep) || StringUtils.isBlank(arr)) {
                    List<Map<String, Object>> list = template.queryForList("select * from ASSIST_WHITELIST where id=?", id);
                    if (list == null || list.size() == 0) {
                        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "data not found for id: "+id));
                    }

                    Map<String, Object> curr = list.get(0);
                    createWhiteListDetail(template, id, (String)curr.get("DEP"), (String)curr.get("ARR"), fromDate, DateTimeHelper.getDateStrAfter(currFromDate, -1, AirlinkConst.TIME_DATE_FORMAT));
                } else {
                    createWhiteListDetail(template, id, dep, arr, fromDate, DateTimeHelper.getDateStrAfter(currFromDate, -1, AirlinkConst.TIME_DATE_FORMAT));
                }
            } else if (currFromDate.compareTo(fromDate) < 0) {
                deleteWhiteListDetail(template, id, currFromDate, DateTimeHelper.getDateStrAfter(fromDate, -1, AirlinkConst.TIME_DATE_FORMAT));
            }
        }

        if (StringUtils.isNotBlank(toDate) && StringUtils.isNotBlank(currToDate)) {
            if (currToDate.compareTo(toDate) < 0) {
                if (StringUtils.isBlank(dep) || StringUtils.isBlank(arr)) {
                    List<Map<String, Object>> list = template.queryForList("select * from ASSIST_WHITELIST where id=?", id);
                    if (list == null || list.size() == 0) {
                        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "data not found for id: "+id));
                    }

                    Map<String, Object> curr = list.get(0);
                    createWhiteListDetail(template, id, (String)curr.get("DEP"), (String)curr.get("ARR"), DateTimeHelper.getDateStrAfter(currToDate, 1, AirlinkConst.TIME_DATE_FORMAT), toDate);
                } else {
                    createWhiteListDetail(template, id, dep, arr, fromDate, DateTimeHelper.getDateStrAfter(currFromDate, -1, AirlinkConst.TIME_DATE_FORMAT));
                }
            } else if (currToDate.compareTo(toDate) > 0) {
                deleteWhiteListDetail(template, id, DateTimeHelper.getDateStrAfter(toDate, 1, AirlinkConst.TIME_DATE_FORMAT), currToDate);
            }
        }

        StringBuffer sql = new StringBuffer("UPDATE ASSIST_WHITELIST SET ");
        if (StringUtils.isNotBlank(dep)) {
            sql.append("DEP='").append(dep).append("',");
        }
        if (StringUtils.isNotBlank(arr)) {
            sql.append("ARR='").append(arr).append("',");
        }
        if (StringUtils.isNotBlank(fromDate)) {
            sql.append("FLIGHT_DATE_FROM='").append(fromDate).append("',");
        }
        if (StringUtils.isNotBlank(toDate)) {
            sql.append("FLIGHT_DATE_TO='").append(toDate).append("',");
        }
        if (StringUtils.isNotBlank(memo)) {
            sql.append("MEMO='").append(memo).append("',");
        }
        if (exDifFrom != null) {
            sql.append("EX_DIF_FROM='").append(exDifFrom).append("',");
        }
        if (exDifTo != null) {
            sql.append("EX_DIF_TO='").append(exDifTo).append("',");
        }
        sql.append("UPDATE_TIME=SYSDATE WHERE ID=?");

        template.update(sql.toString(), id);
        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    /**
     * 生成新的白名单。参数格式为
     *  {"flight_no":"MU3102", "dep": "CTU", "to":"PEK", "from_date":"2022-11-17", "to_date":"2022-11-20", "enabled":1, "y_ratio":0.6, "c_ratio":1.0}
     *
     * @param params
     * @return
     */
    @PostMapping("/whitelist/create")
    public ResponseEntity<?> createWhiteList(@RequestBody() String params) {
        JSONObject json = JSON.parseObject(params);
        if (json == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "invalid JSON"));
        }

        String dep = json.getString("dep");
        String arr = json.getString("arr");
        String fromDate = json.getString("fromDate");
        String toDate = json.getString("toDate");
        Integer exDifFrom = json.getInteger("exDifFrom");
        Integer exDifTo = json.getInteger("exDifTo");
        String memo = json.getString("memo");

        // 参数检查
        if (StringUtils.isBlank(dep)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "dep needed"));
        }

        if (StringUtils.isBlank(arr)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "arr needed"));
        }

        if (StringUtils.isBlank(fromDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "fromDate needed"));
        }

        if (StringUtils.isBlank(toDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "toDate needed"));
        }

        if (exDifFrom == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "exDifFrom needed"));
        }

        if (exDifTo == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "exDifTo needed"));
        }

        if (toDate.compareTo(fromDate) < 0) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "toDate should bigger than fromDate"));
        }

        if (exDifFrom > exDifTo) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "exDifTo should bigger than exDifFrom"));
        }

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        // 检测时间范围是否有冲突
        if (checkWhiteListRangeConflict(template, dep, arr, fromDate, toDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "时间范围有冲突"));
        }

        // 计算可以使用的ID值
        Integer max = template.queryForObject("select max(ID) from ASSIST_WHITELIST", Integer.class);
        Integer id = max == null ? 1 : max + 1;

        template.update("INSERT INTO ASSIST_WHITELIST\n" +
                "(ID, DEP, ARR, FLIGHT_DATE_FROM, FLIGHT_DATE_TO, EX_DIF_FROM, EX_DIF_TO, MEMO, CREATE_TIME, UPDATE_TIME)\n" +
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, sysdate, sysdate)", id, dep, arr, fromDate, toDate, exDifFrom, exDifTo, memo);

        createWhiteListDetail(template, id, dep, arr, fromDate, toDate);

        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    @PostMapping("/whitelist/create/auto")
    public ResponseEntity<?> createWhiteListAuto(@RequestBody() String params) {
        String fromDate = null;
        String toDate = null;
        Integer exDifFrom = null;
        Integer exDifTo = null;

        JSONObject json = JSON.parseObject(params);
        if (json != null) {
            fromDate = json.getString("fromDate");
            toDate = json.getString("toDate");
            exDifFrom = json.getInteger("exDifFrom");
            exDifTo = json.getInteger("exDifTo");

            if (toDate.compareTo(fromDate) < 0) {
                return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "toDate should bigger than fromDate"));
            }

            if (exDifFrom > exDifTo) {
                return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "exDifTo should bigger than exDifFrom"));
            }
        }

        if (StringUtils.isBlank(fromDate)) {
            fromDate = DateTimeHelper.date2String(new Date(), AirlinkConst.TIME_DATE_FORMAT);
        }

        if (StringUtils.isBlank(toDate)) {
            toDate = DateTimeHelper.getDateStrAfter(fromDate, 365, AirlinkConst.TIME_DATE_FORMAT);
        }

        if (exDifFrom == null) {
            exDifFrom = 1;
        }

        if (exDifTo == null) {
            exDifTo = 180;
        }

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);
        String sql = "select distinct SEG_DEP as dep, SEG_ARR as arr from ORG_INV_SEGMENT " +
                "where SEG_DEP_DATE between to_date(?, 'yyyy-MM-dd') and to_date(?, 'yyyy-MM-dd') and (SEG_DEP, SEG_ARR) not in (select DEP, ARR from ASSIST_WHITELIST)";
        List<Map<String, Object>> list = template.queryForList(sql, fromDate, toDate);

        if (list != null && list.size() > 0) {
            // 计算可以使用的ID值
            Integer max = template.queryForObject("select max(ID) from ASSIST_WHITELIST", Integer.class);
            Integer id = max == null ? 1 : max + 1;

            String memo = "";
            for (Map<String, Object> item : list) {
                String dep = (String)item.get("dep");
                String arr = (String)item.get("arr");

                template.update("INSERT INTO ASSIST_WHITELIST\n" +
                        "(ID, DEP, ARR, FLIGHT_DATE_FROM, FLIGHT_DATE_TO, EX_DIF_FROM, EX_DIF_TO, MEMO, CREATE_TIME, UPDATE_TIME)\n" +
                        "VALUES(?, ?, ?, ?, ?, ?, ?, ?, sysdate, sysdate)", id, dep, arr, fromDate, toDate, exDifFrom, exDifTo, memo);

                createWhiteListDetail(template, id, dep, arr, fromDate, toDate);

                id = id + 1;
            }
        }

        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    private boolean checkWhiteListRangeConflict(JdbcTemplate template, String dep, String arr, String fromDate, String toDate) {
        List<Map<String, Object>> list = template.queryForList("select FLIGHT_DATE_FROM, FLIGHT_DATE_TO, EX_DIF_FROM, EX_DIF_TO " +
                "from ASSIST_WHITELIST where DEP=? and ARR=?", dep, arr);
        if (list == null || list.size() == 0) {
            return false;
        }

        for (Map<String, Object> item : list) {
            String _fromDate = (String) item.get("FLIGHT_DATE_FROM");
            String _toDate = (String) item.get("FLIGHT_DATE_TO");

            if (_fromDate.compareTo(fromDate) <= 0 && _toDate.compareTo(fromDate) >= 0 || _fromDate.compareTo(toDate) <= 0 && _toDate.compareTo(toDate) >= 0) {
                return true;
            }
        }

        return false;
    }

    /**
     * 创建白名单明细
     *
     * @param template
     * @param id 白名单表ID
     * @param fromDate
     * @param toDate
     */
    private void createWhiteListDetail(JdbcTemplate template, int id, String dep, String arr, String fromDate, String toDate) {
        // 计算可以使用的ID值
        Integer max = template.queryForObject("select max(ID) from ASSIST_WHITELIST_DETAIL", Integer.class);
        int did = max == null ? 1 : max + 1;

        Calendar cal = Calendar.getInstance();
        cal.setTime(DateTimeHelper.getDate(fromDate, AirlinkConst.TIME_DATE_FORMAT));

        String sql = "select distinct (AIRLINE_CODE || FLIGHT_NUMBER) as flight_no, SEG_DEP_DATE as flight_date from ORG_INV_SEGMENT " +
                "where SEG_DEP_DATE between to_date(?, 'yyyy-MM-dd') and to_date(?, 'yyyy-MM-dd') and SEG_DEP=? and SEG_ARR=?";
        List<Map<String, Object>> list = template.queryForList(sql, fromDate, toDate, dep, arr);
        Map<String, List<Map<String, Object>>> listByDate = list.stream().
                map(x -> {x.put("flight_date", DateTimeHelper.date2String((Date)x.get("flight_date"), AirlinkConst.TIME_DATE_FORMAT)) ; return x;}).
                collect(Collectors.groupingBy(x -> (String)x.get("flight_date")));

        // 每天一条数据
        while (true) {
            String flightDate = DateTimeHelper.date2String(cal.getTime(), AirlinkConst.TIME_DATE_FORMAT);
            int dow = cal.get(Calendar.DAY_OF_WEEK);
            if (dow == 0) {
                dow = 7;
            }

            List<Map<String, Object>> flights = listByDate.get(flightDate);
            if (flights != null) {
                for (Map<String, Object> flight : flights) {
                    template.update("INSERT INTO ASSIST_WHITELIST_DETAIL\n" +
                            "(ID, WHITELIST_ID, FLIGHT_NO, FLIGHT_DATE, DOW, Y_RATIO, C_RATIO, F_RATIO, MIN_EX_DIF, ENABLED, CREATE_TIME, UPDATE_TIME)\n" +
                            "values(?, ?, ?, ?, ?, 1, 1, 1, -1, 1, sysdate, sysdate)", did++, id, (String)flight.get("flight_no"), flightDate, dow);
                }
            }
            if (flightDate.equals(toDate)) {
                break;
            }

            cal.add(Calendar.DAY_OF_YEAR, 1);
        }
    }

    /**
     * 删除白名单明细
     *
     * @param template
     * @param id 白名单明细ID
     * @param fromDate
     * @param toDate
     */
    private void deleteWhiteListDetail(JdbcTemplate template, int id, String fromDate, String toDate) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(DateTimeHelper.getDate(fromDate, AirlinkConst.TIME_DATE_FORMAT));

        // 逐条删除
        while (true) {
            String flightDate = DateTimeHelper.date2String(cal.getTime(), AirlinkConst.TIME_DATE_FORMAT);
            template.update("DELETE FROM ASSIST_WHITELIST_DETAIL WHERE WHITELIST_ID=? and FLIGHT_DATE=?", id, flightDate);
            if (flightDate.equals(toDate)) {
                break;
            }

            cal.add(Calendar.DAY_OF_YEAR, 1);
        }
    }

    /**
     *
     * 读取白名单明细列表。可以分页读取。
     *
     * @param pageNo 第几页，从1开始。
     * @param pageSize 每天条数，缺省值为20.
     * @return
     */
    @GetMapping("/whitelist/detail/list")
    public ResponseEntity<?> getWhiteListDetail(@RequestParam(value = "dep", required = false) String dep,
                                          @RequestParam(value = "arr", required = false) String arr,
                                          @RequestParam(value = "flightNo", required = false) String flightNo,
                                          @RequestParam(value = "fromDate", required = false) String flightDateFrom,
                                          @RequestParam(value = "toDate", required = false) String flightDateTo,
                                          @RequestParam(value = "pageNo", defaultValue = "1") Integer pageNo,
                                          @RequestParam(value = "pageSize", defaultValue = "20") Integer pageSize) {
        // 参数检查
        if (flightDateFrom != null && flightDateTo != null && flightDateTo.compareTo(flightDateFrom) < 0) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "toDate should bigger than fromDate"));
        }

        int startRow = (pageNo - 1) * pageSize;
        int endRow = startRow + pageSize;
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        StringBuffer sql = new StringBuffer("SELECT d.FLIGHT_NO, w.DEP, w.ARR, d.FLIGHT_DATE, d.DOW, " +
                "d.Y_RATIO, d.C_RATIO, d.MIN_EX_DIF, d.ENABLED, d.ID, ROWNUM RN " +
                "FROM ASSIST_WHITELIST w, ASSIST_WHITELIST_DETAIL d WHERE w.ID =d.WHITELIST_ID ");
        if (StringUtils.isNotBlank(dep)) {
            sql.append("and w.dep='"+dep+"' ");
        }
        if (StringUtils.isNotBlank(arr)) {
            sql.append("and w.arr='"+arr+"' ");
        }
        if (StringUtils.isNotBlank(flightNo)) {
            sql.append("and FLIGHT_NO='"+flightNo+"' ");
        }
        if (StringUtils.isNotBlank(flightDateFrom)) {
            sql.append("and flight_date >='"+flightDateFrom+"' ");
        }
        if (StringUtils.isNotBlank(flightDateTo)) {
            sql.append("and flight_date <='"+flightDateTo+"' ");
        }
        Integer total = template.queryForObject("select count(*) from (" + sql + ")", Integer.class);
        if (total == null) {
            total = 0;
        }

        sql.append(" order by FLIGHT_DATE, FLIGHT_NO, DEP, ARR) WHERE RN >= " + startRow+" and RN < "+endRow);
        List<Map<String, Object>> list = template.queryForList("SELECT * FROM (" + sql);
        list = list.stream().map(x -> {
            String fltno = (String)x.get("FLIGHT_NO");
            x.put("FLIGHT_NO", fltno);
            x.put("ELINE", (String)x.get("DEP") + "-" + x.get("ARR"));
            x.remove("RN");
            return x;
        }).collect(Collectors.toList());
        Map<String, Object> result = new HashMap<>();
        result.put("list", list);
        result.put("total", total);
        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), result));
    }

    /**
     * 为编辑页面提供白名单信息
     *
     * @param id 白名单明细ID
     * @return
     */
    @GetMapping("/whitelist/detail/info")
    public ResponseEntity<?> getWhiteListDetailInfo(@RequestParam(value = "id") int id) {
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        // FIXME 暂不支持

        List<Map<String, Object>> list = template.queryForList("select * from ASSIST_WHITELIST where id=(select WHITELIST_ID from ASSIST_WHITELIST_DETAIL where id=?)", id);
        if (list == null || list.size() == 0) {
            // 数据不存在，可能是被删除了
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.RUNTIME_FAIL, "No data for id: "+id));
        }

        Map<String, Object> map = list.get(0);
        list = template.queryForList("select max( from ASSIST_WHITELIST where seq=(select seq from ASSIST_WHITELIST where id=?)", id);
        map = list.get(0);

        int min_seq = ((Number)map.get("min_seq")).intValue();
        int max_seq = ((Number)map.get("max_seq")).intValue();
        int days = ((Number)map.get("days")).intValue();
        int count = ((Number)map.get("count")).intValue();
        String min_day = (String)map.get("min_day");
        String max_day = (String)map.get("max_day");
        String memo = (String)map.get("memo");
        String eline = (String)map.get("eline");
        String exclude_eline = (String)map.get("exclude_eline");

        int overflowHead = min_seq < 0 ? -min_seq : 0;
        int overflowTail = count - days - overflowHead;
        String fromDate = DateTimeHelper.getDateStrAfter(min_day, overflowHead, AirlinkConst.TIME_DATE_FORMAT);
        String toDate = DateTimeHelper.getDateStrAfter(max_day, -overflowTail, AirlinkConst.TIME_DATE_FORMAT);

        Map<String, Object> result = new HashMap<>();
        result.put("memo", memo);
        result.put("eline", eline);
        result.put("excludeEline", exclude_eline);
        result.put("fromDate", fromDate);
        result.put("toDate", toDate);
        result.put("overflowHead", overflowHead);
        result.put("overflowTail", overflowTail);

        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), result));
    }

    /**
     * 更新白名单明细
     *
     * @param params
     * @return
     */
    @PostMapping("/whitelist/detail/update")
    public ResponseEntity<?> updateWhiteListDetail(@RequestBody() String params) {
        JSONObject json = JSON.parseObject(params);
        if (json == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "invalid JSON"));
        }

        Integer id = json.getInteger("whitelistId");
        String flightNo = json.getString("flightNo");
        Integer enabled = json.getInteger("enabled");
        Float yRatio = json.getFloat("y_ratio");
        Float cRatio = json.getFloat("c_ratio");
        String fromDate = json.getString("fromDate");
        String toDate = json.getString("toDate");
        Integer minExDif = json.getInteger("minExDif");

        // 参数检查
        if (id == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "whitelistId needed"));
        }

        if (enabled == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "enabled needed"));
        }

        if (fromDate == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "fromDate needed"));
        }

        if (toDate == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "toDate needed"));
        }

        if (toDate.compareTo(fromDate) < 0) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "toDate should bigger than fromDate"));
        }

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        List<Integer> exists = template.queryForList("select 1 from ASSIST_WHITELIST where id=?",
                new Object[]{id}, Integer.class);
        if (exists == null || exists.size() == 0) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "data not found for whitelistId: "+id));
        }

        exists = template.queryForList("select 1 from ASSIST_WHITELIST where id=? and FLIGHT_DATE_FROM <= ? and FLIGHT_DATE_TO >= ? and FLIGHT_DATE_FROM <= ? and FLIGHT_DATE_TO >= ?",
                new Object[]{id, fromDate, fromDate, toDate, toDate}, Integer.class);
        if (exists == null || exists.size() == 0) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "fromDate and toDate beyond time range"));
        }

        StringBuffer sql = new StringBuffer("UPDATE ASSIST_WHITELIST_DETAIL SET ");
        if (enabled != null) {
            sql.append("ENABLED=").append(enabled).append(",");
        }
        if (yRatio != null) {
            sql.append("Y_RATIO=").append(yRatio).append(",");
        }
        if (cRatio != null) {
            sql.append("C_RATIO=").append(cRatio).append(",");
        }
        if (minExDif != null) {
            sql.append("MIN_EX_DIF=").append(minExDif).append(",");
        }
        sql.append("UPDATE_TIME=sysdate WHERE WHITELIST_ID=? and FLIGHT_DATE between ? and ?");
        if (StringUtils.isNotBlank(flightNo)) {
            sql.append(" and FLIGHT_NO='").append(flightNo).append("'");
        }

        template.update(sql.toString(), id, fromDate, toDate);

        if (enabled != null && enabled == 1 && yRatio != null) {
            List<Map<String, Object>> list = template.queryForList("select * from ASSIST_WHITELIST where ID=?", id);
            if (list == null || list.size() == 0) {
                // 白名单已被删除
                return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "data not found for id: "+id));
            }

            Map<String, Object> curr = list.get(0);
            String dep = (String)curr.get("DEP");
            String arr = (String)curr.get("ARR");

            List<String> flights = new ArrayList<>();
            if (StringUtils.isNotBlank(flightNo)) {
                flights.add(flightNo);
            } else {
                List<Map<String, Object>> list1 = template.queryForList("select distinct FLIGHT_NO from ASSIST_WHITELIST_DETAIL where WHITELIST_ID=? and FLIGHT_DATE between ? and ?",
                        id, fromDate, toDate);
                flights = list1.stream().map(x -> (String)x.get("FLIGHT_NO")).collect(Collectors.toList());
            }

            Calendar cal = Calendar.getInstance();
            cal.setTime(DateTimeHelper.getDate(fromDate, AirlinkConst.TIME_DATE_FORMAT));

            // 乐观指数变化后，调用相关接口使乐观指数起作用
            while (true) {
                String flightDate = DateTimeHelper.date2String(cal.getTime(), AirlinkConst.TIME_DATE_FORMAT);
                for (String flight : flights) {
                    ratioChanged(template, flight, flightDate, dep, arr, cRatio, yRatio, null);
                }
                if (flightDate.equals(toDate)) {
                    break;
                }

                cal.add(Calendar.DAY_OF_YEAR, 1);
            }
        }

        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    /**
     * 设置乐观指数
     *
     * @param params
     * @return
     */
    @PostMapping("/whitelist/detail/set")
    public ResponseEntity<?> setWhiteListDetail(@RequestBody() String params) {
        JSONObject json = JSON.parseObject(params);
        if (json == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "invalid JSON"));
        }

        Integer id = json.getInteger("id");
        String dep = json.getString("dep");
        String arr = json.getString("arr");
        String flightNo = json.getString("flightNo");
        String flightDate = json.getString("flightDate");
        Integer enabled = json.getInteger("enabled");
        Float yRatio = json.getFloat("y_ratio");
        Float cRatio = json.getFloat("c_ratio");
        String forecastTime = json.getString("forecastTime");

        // 参数检查
        if (enabled == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "enabled needed"));
        }

        if (enabled == 1) {
            if (yRatio == null) {
                return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "y_ratio needed"));
            }

            if (cRatio == null) {
                return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "c_ratio needed"));
            }
        }

        if (id == null) {
            if (StringUtils.isBlank(flightDate)) {
                return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "flightDate needed"));
            }

            if (StringUtils.isBlank(flightNo)) {
                return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "flightNo needed"));
            }

            if (StringUtils.isBlank(dep)) {
                return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "dep needed"));
            }

            if (StringUtils.isBlank(arr)) {
                return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "arr needed"));
            }
        }

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        StringBuffer sql = new StringBuffer("UPDATE ASSIST_WHITELIST_DETAIL SET Y_RATIO=?, C_RATIO=?, ENABLED=?, UPDATE_TIME=sysdate WHERE ");
        if (id != null) {
            sql.append("ID=?");
        } else {
            sql.append("FLIGHT_DATE=? and FLIGHT_NO=? and WHITELIST_ID in (select ID from ASSIST_WHITELIST where DEP=? and ARR=?)");
        }

        int changed = 0;
        if (id != null) {
            changed = template.update(sql.toString(), yRatio, cRatio, enabled, id);
        } else {
            changed = template.update(sql.toString(), yRatio, cRatio, enabled, flightDate, flightNo, dep, arr);
        }

        if (changed == 0) {
            // 应该是白名单被删除
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "No data changed"));
        }

        if (enabled == 1) {
            // 调用外部接口需要航班号等参数
            if (StringUtils.isBlank(flightNo)) {
                List<Map<String, Object>> list = template.queryForList("select * from ASSIST_WHITELIST where ID in (select WHITELIST_ID from ASSIST_WHITELIST_DETAIL where ID=?)", id);
                if (list != null && list.size() > 0) {
                    Map<String, Object> curr = list.get(0);
                    dep = (String) curr.get("DEP");
                    arr = (String) curr.get("ARR");
                }

                flightNo = template.queryForObject("select FLIGHT_NO from ASSIST_WHITELIST_DETAIL where ID="+id, String.class);
            }

            // 航班号为空说明白名单已被删除
            if (StringUtils.isNotBlank(flightNo)) {
                // 乐观指数变化后，调用相关接口使乐观指数起作用
                ratioChanged(template, flightNo, flightDate, dep, arr, cRatio, yRatio, forecastTime);
            }
        }

        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    /**
     * 批量设置乐观指数
     *
     * @param params
     * @return
     */
    @PostMapping("/whitelist/detail/batch_set")
    public ResponseEntity<?> batchSetWhiteListDetail(@RequestBody() String params) {
        JSONArray jsons = JSON.parseArray(params);
        if (jsons == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "invalid JSON"));
        }

        JSONObject json;
        for (int i = 0; i < jsons.size(); i++) {
            json = jsons.getJSONObject(i);
            setWhiteListDetail(JSONObject.toJSONString(json));
        }

        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    /**
     * 调用相关接口使乐观指数起作用
     *
     * @param template
     * @param flightNo
     * @param flightDate
     * @param dep
     * @param arr
     * @param cRatio
     * @param yRatio
     * @param forecastTime
     */
    private void ratioChanged(JdbcTemplate template, String flightNo, String flightDate, String dep, String arr, Float cRatio, Float yRatio, String forecastTime) {
        // 可以通过将这个属性置为空来停止调用外部接口
        if (StringUtils.isBlank(emsrbUrl)) {
            return;
        }

        // 如果不提供预测时间，就读取最近的预测时间
        if (forecastTime == null) {
            String sql = "select max(forecast_time) as forecast_time from PREDICT_FLIGHT_HE_FORECAST " +
                    "WHERE fltno='%s' and flight_date='%s' and dep='%s' and arr='%s'";
            sql = String.format(sql, flightNo, flightDate, dep, arr);
            List<Map<String, Object>> times = template.queryForList(sql);
            if (times != null && times.size() > 0) {
                forecastTime = (String) times.get(0).get("forecast_time");
            }
        }

        String url = emsrbUrl + "/change_coff_prices";

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        Map<String, Object> map = new HashMap<>();
        map.put("fltno", flightNo);
        map.put("flight_date", flightDate);
        map.put("dep", dep);
        map.put("arr", arr);
        map.put("clz_type", "'Y'");
        map.put("coff", yRatio);
        map.put("update_db", 1);
        map.put("forecast_time", forecastTime);

        try {
//            HttpEntity<Map<String, Object>> httpEntity = new HttpEntity<>(map, headers);
//            ResponseEntity<String> responseEntity = restTemplate.postForEntity(url, httpEntity, String.class);
//            if (!responseEntity.getStatusCode().is2xxSuccessful()) {
//                return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.RUNTIME_FAIL.getCode(), CommonCodeEnum.RUNTIME_FAIL.getDesc(), "call change_coff_prices failed"));
//            }

        map.put("clz_type", "'C'");
        map.put("coff", cRatio);
//        httpEntity = new HttpEntity<>(map, headers);
//        responseEntity = restTemplate.postForEntity(url, httpEntity, String.class);
//        if (!responseEntity.getStatusCode().is2xxSuccessful()) {
//            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.RUNTIME_FAIL.getCode(), CommonCodeEnum.RUNTIME_FAIL.getDesc(), "call change_coff_prices failed"));
//        }
        } catch (Exception e) {
            e.printStackTrace();
            // 短时间调用失败可以忽略
        }
    }

    /**
     *
     * 读取同环比列表。可以分页读取。
     *
     * @param pageNo 第几页，从1开始。
     * @param pageSize 每天条数，缺省值为20.
     * @return
     */
    @GetMapping("/wyratio/list")
    public ResponseEntity<?> getWYRatioList(@RequestParam(value = "flightNo", required = false) String flightNo,
                                          @RequestParam(value = "matchMethod", required = false) String matchMethod,
                                          @RequestParam(value = "pageNo", defaultValue = "1") Integer pageNo,
                                          @RequestParam(value = "pageSize", defaultValue = "20") Integer pageSize) {
        int startRow = (pageNo - 1) * pageSize;
        int endRow = startRow + pageSize;

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        StringBuffer sql = new StringBuffer("SELECT A.*, ROWNUM RN FROM ASSIST_WYRATIO A where 1=1 ");
        if (StringUtils.isNotBlank(flightNo)) {
            sql.append("and flight_no='"+flightNo+"'");
        }
        if (StringUtils.isNotBlank(matchMethod)) {
            sql.append("and MATCH_METHOD='"+matchMethod+"'");
        }
        Integer total = template.queryForObject("select count(*) from (" + sql + ")", Integer.class);
        if (total == null) {
            total = 0;
        }

        sql.append(" order by FLIGHT_NO, ELINE, FLIGHT_DATE_FROM) WHERE RN >= " + startRow+" and RN < "+endRow);
        List<Map<String, Object>> list = template.queryForList("SELECT * FROM ("+sql.toString());

        list = list.stream().map(x -> {
            x.put("CREATE_TIME", DateTimeHelper.date2String((Date)x.get("CREATE_TIME"), AirlinkConst.TIME_FULL_FORMAT));
            x.put("UPDATE_TIME", DateTimeHelper.date2String((Date)x.get("UPDATE_TIME"), AirlinkConst.TIME_FULL_FORMAT));
            x.remove("RN");
            return x;
        }).collect(Collectors.toList());

        Map<String, Object> result = new HashMap<>();
        result.put("list", list);
        result.put("total", total);
        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), result));
    }

    /**
     * 删除同环比
     *
     * @param id ID值
     * @return
     */
    @PostMapping("/wyratio/delete")
    public ResponseEntity<?> deleteWYRatio(@RequestParam("id") int id) {
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);
        template.update("DELETE FROM ASSIST_WYRATIO WHERE ID=?", id);
        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    /**
     * 更新同环比
     *
     * @param params
     * @return
     */
    @PostMapping("/wyratio/update")
    public ResponseEntity<?> updateWYRatio(@RequestBody() String params) {
        JSONObject json = JSON.parseObject(params);
        if (json == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "invalid JSON"));
        }

        Integer id = json.getInteger("id");
        String flightNo = json.getString("flightNo");
        String eline = json.getString("eline");
        String fromDate = json.getString("fromDate");
        String toDate = json.getString("toDate");
        String peerFlightNo = json.getString("peerFlightNo");
        String peerEline = json.getString("peerEline");
        String peerFromDate = json.getString("peerFromDate");
        String peerToDate = json.getString("peerToDate");
        String matchMethod = json.getString("matchMethod");
        String memo = json.getString("memo");

        // 参数检查
        if (id == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "id needed"));
        }

//        if (StringUtils.isBlank(flightNo)) {
//            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "flightNo needed"));
//        }

//        if (StringUtils.isBlank(eline)) {
//            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "eline needed"));
//        }
//
//        if (StringUtils.isNotBlank(eline) && eline.length() != 6) {
//            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "invalid eline"));
//        }

        if (StringUtils.isBlank(fromDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "fromDate needed"));
        }

        if (StringUtils.isBlank(toDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "toDate needed"));
        }

        if (toDate.compareTo(fromDate) < 0) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "toDate should bigger than fromDate"));
        }

        if (StringUtils.isBlank(peerFlightNo)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "peerFlightNo needed"));
        }

        if (StringUtils.isBlank(peerEline)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "peerEline needed"));
        }

        if (StringUtils.isNotBlank(peerEline) && peerEline.length() != 6) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "invalid peerEline"));
        }

        if (StringUtils.isBlank(peerFromDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "peerFromDate needed"));
        }

        if (StringUtils.isBlank(peerToDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "peerToDate needed"));
        }

        if (peerToDate.compareTo(peerFromDate) < 0) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "peerToDate should bigger than peerFromDate"));
        }

        int diff = DateTimeHelper.daysBetween(DateTimeHelper.getDate(fromDate, AirlinkConst.TIME_DATE_FORMAT), DateTimeHelper.getDate(toDate, AirlinkConst.TIME_DATE_FORMAT));
        if (diff < 300 && diff !=
                DateTimeHelper.daysBetween(DateTimeHelper.getDate(peerFromDate, AirlinkConst.TIME_DATE_FORMAT), DateTimeHelper.getDate(peerToDate, AirlinkConst.TIME_DATE_FORMAT))) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "日期间隔应该一样"));
        }

        if (StringUtils.isBlank(matchMethod)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "matchMethod needed"));
        }

        if (!"DOW".equals(matchMethod) && !"SEQ".equals(matchMethod)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "matchMethod should be DOW or SEQ"));
        }

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        List<Map<String, Object>> currs = template.queryForList("select * from ASSIST_WYRATIO where id=?", id);
        if (currs == null || currs.size() == 0) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "data not found for: " + id));
        }

        Map<String, Object> curr = currs.get(0);

        if (StringUtils.isBlank(flightNo)) {
            flightNo = (String)curr.get("FLIGHT_NO");
        }

        if (StringUtils.isBlank(eline)) {
            eline = (String)curr.get("ELINE");
        }

        if (StringUtils.isNotBlank(fromDate)) {
            List<Integer> exists = template.queryForList("select 1 from ASSIST_WYRATIO where FLIGHT_NO=? and ELINE=? and id != ? and " +
                    "FLIGHT_DATE_FROM <= ? and FLIGHT_DATE_TO >= ? ", new Object[]{flightNo, eline, id, fromDate, fromDate}, Integer.class);
            if (exists != null && exists.size() > 0) {
                return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "time range conflict"));
            }
        }

        if (StringUtils.isNotBlank(toDate)) {
            List<Integer> exists = template.queryForList("select 1 from ASSIST_WYRATIO where FLIGHT_NO=? and ELINE=? and id != ? and " +
                    "FLIGHT_DATE_FROM <= ? and FLIGHT_DATE_TO >= ?", new Object[]{id, flightNo, eline, toDate, toDate}, Integer.class);
            if (exists != null && exists.size() > 0) {
                return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "time range conflict"));
            }
        }

        StringBuffer sql = new StringBuffer("UPDATE ASSIST_WYRATIO SET ");
        if (StringUtils.isNotBlank(flightNo)) {
            sql.append("FLIGHT_NO='").append(flightNo).append("',");
        }
        if (StringUtils.isNotBlank(eline)) {
            sql.append("ELINE='").append(eline).append("',");
        }
        if (StringUtils.isNotBlank(fromDate)) {
            sql.append("FLIGHT_DATE_FROM='").append(fromDate).append("',");
        }
        if (StringUtils.isNotBlank(toDate)) {
            sql.append("FLIGHT_DATE_TO='").append(toDate).append("',");
        }
        if (StringUtils.isNotBlank(peerFlightNo)) {
            sql.append("PEER_FLIGHT_NO='").append(peerFlightNo).append("',");
        }
        if (StringUtils.isNotBlank(peerEline)) {
            sql.append("PEER_ELINE='").append(peerEline).append("',");
        }
        if (StringUtils.isNotBlank(peerFromDate)) {
            sql.append("PEER_FLIGHT_DATE_FROM='").append(peerFromDate).append("',");
        }
        if (StringUtils.isNotBlank(peerToDate)) {
            sql.append("PEER_FLIGHT_DATE_TO='").append(peerToDate).append("',");
        }
        if (StringUtils.isNotBlank(memo)) {
            sql.append("MEMO='").append(memo).append("',");
        }
        if (StringUtils.isNotBlank(matchMethod)) {
            sql.append("MATCH_METHOD='").append(matchMethod).append("',");
        }
        sql.append("UPDATE_TIME=SYSDATE WHERE ID=?");

        template.update(sql.toString(), id);
        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    /**
     * 生成新的同环比。
     *
     * @param params
     * @return
     */
    @PostMapping("/wyratio/create")
    public ResponseEntity<?> createWYRatio(@RequestBody() String params) {
        JSONObject json = JSON.parseObject(params);
        if (json == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "invalid JSON"));
        }

        String flightNo = json.getString("flightNo");
        String eline = json.getString("eline");
        String fromDate = json.getString("fromDate");
        String toDate = json.getString("toDate");
        String peerFlightNo = json.getString("peerFlightNo");
        String peerEline = json.getString("peerEline");
        String peerFromDate = json.getString("peerFromDate");
        String peerToDate = json.getString("peerToDate");
        String matchMethod = json.getString("matchMethod");
        String memo = json.getString("memo");

        // 参数检查
        if (StringUtils.isBlank(flightNo)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "flightNo needed"));
        }

        if (StringUtils.isBlank(eline)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "eline needed"));
        }

        if (StringUtils.isNotBlank(eline) && eline.length() != 6) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "invalid eline"));
        }

        if (StringUtils.isBlank(fromDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "fromDate needed"));
        }

        if (StringUtils.isBlank(toDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "toDate needed"));
        }

        if (toDate.compareTo(fromDate) < 0) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "toDate should bigger than fromDate"));
        }

        if (StringUtils.isBlank(peerFlightNo)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "peerFlightNo needed"));
        }

        if (StringUtils.isBlank(peerEline)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "peerEline needed"));
        }

        if (StringUtils.isNotBlank(peerEline) && peerEline.length() != 6) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "invalid peerEline"));
        }

        if (StringUtils.isBlank(peerFromDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "peerFromDate needed"));
        }

        if (StringUtils.isBlank(peerToDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "peerToDate needed"));
        }

        if (peerToDate.compareTo(peerFromDate) < 0) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "peerToDate should bigger than peerFromDate"));
        }

        int diff = DateTimeHelper.daysBetween(DateTimeHelper.getDate(fromDate, AirlinkConst.TIME_DATE_FORMAT), DateTimeHelper.getDate(toDate, AirlinkConst.TIME_DATE_FORMAT));
        if (diff < 300 && diff !=
                DateTimeHelper.daysBetween(DateTimeHelper.getDate(peerFromDate, AirlinkConst.TIME_DATE_FORMAT), DateTimeHelper.getDate(peerToDate, AirlinkConst.TIME_DATE_FORMAT))) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "日期间隔应该一样"));
        }

        if (StringUtils.isBlank(matchMethod)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "matchMethod needed"));
        }

        if (!"DOW".equals(matchMethod) && !"SEQ".equals(matchMethod)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "matchMethod should be DOW or SEQ"));
        }

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        List<Map<String, Object>> exists = template.queryForList("select 1 from ASSIST_WYRATIO where FLIGHT_NO=? and ELINE=? and " +
                        "(FLIGHT_DATE_FROM <= ? and FLIGHT_DATE_TO >= ? or FLIGHT_DATE_FROM <= ? and FLIGHT_DATE_TO >= ?)",
                flightNo, eline, fromDate, fromDate, toDate, toDate);
        if (exists != null && exists.size() > 0) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "time range conflict"));
        }

        Integer max = template.queryForObject("select max(ID) from ASSIST_WYRATIO", Integer.class);
        int id = max == null ? 1 : max + 1;

        template.update("INSERT INTO ASSIST_WYRATIO\n" +
                "(ID, FLIGHT_NO, ELINE, FLIGHT_DATE_FROM, FLIGHT_DATE_TO, PEER_FLIGHT_NO, PEER_ELINE, " +
                "PEER_FLIGHT_DATE_FROM, PEER_FLIGHT_DATE_TO, MATCH_METHOD, MEMO, CREATE_TIME, UPDATE_TIME)\n" +
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, sysdate, sysdate)",
                id, flightNo, eline, fromDate, toDate, peerFlightNo, peerEline, peerFromDate, peerToDate, matchMethod, memo);

        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    /**
     *
     * 读取节假日列表。可以分页读取。
     *
     * @param pageNo 第几页，从1开始。
     * @param pageSize 每天条数，缺省值为20.
     * @return
     */
    @GetMapping("/holiday/list")
    public ResponseEntity<?> getHolidayList(@RequestParam(value = "memo", required = false) String memo,
                                            @RequestParam(value = "pageNo", defaultValue = "1") Integer pageNo,
                                            @RequestParam(value = "pageSize", defaultValue = "20") Integer pageSize) {
        int startRow = (pageNo - 1) * pageSize;
        int endRow = startRow + pageSize;
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        StringBuffer sql = new StringBuffer("SELECT A.*, ROWNUM RN FROM ASSIST_HOLIDAY_CFG A where 1=1 ");
        if (StringUtils.isNotBlank(memo)) {
            sql.append("and memo like '%"+memo+"%'");
        }

        Integer total = template.queryForObject("select count(*) from (" + sql + ")", Integer.class);
        if (total == null) {
            total = 0;
        }

        sql.append("order by MEMO, HOLIDAY) WHERE RN >= " + startRow+" and RN < "+endRow);
        List<Map<String, Object>> list = template.queryForList("SELECT * FROM ("+ sql);
        Map<String, Object> result = new HashMap<>();
        result.put("list", list);
        result.put("total", total);
        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), result));
    }

    /**
     * 为节假日编辑页面提供编辑数据
     *
     * @param id ID值
     * @return
     */
    @GetMapping("/holiday/info")
    public ResponseEntity<?> getHolidayInfo(@RequestParam(value = "id") int id) {
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        List<Map<String, Object>> list = template.queryForList("select min(seq) as min_seq, max(seq) as max_seq, max(days) as days, count(*) as count, max(HOLIDAY) as max_day, " +
                "min(HOLIDAY) as min_day, max(memo) as memo, max(eline) as eline, max(exclude_eline) as exclude_eline\n" +
                "from ASSIST_HOLIDAY_CFG where HOLIDAY_SEQ=(select HOLIDAY_SEQ from ASSIST_HOLIDAY_CFG where id=?)", id);
        if (list == null || list.size() == 0 || list.get(0).get("min_seq") == null) {
            // 数据不存在，可能是被删除了
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "No data for id: "+id));
        }

        Map<String, Object> map = list.get(0);

        int min_seq = ((Number)map.get("min_seq")).intValue();
        int max_seq = ((Number)map.get("max_seq")).intValue();
        int days = ((Number)map.get("days")).intValue();
        int count = ((Number)map.get("count")).intValue();
        String min_day = (String)map.get("min_day");
        String max_day = (String)map.get("max_day");
        String memo = (String)map.get("memo");
        String eline = (String)map.get("eline");
        String exclude_eline = (String)map.get("exclude_eline");

        int overflowHead = min_seq < 0 ? -min_seq : 0;
        int overflowTail = count - days - overflowHead;
        String fromDate = DateTimeHelper.getDateStrAfter(min_day, overflowHead, AirlinkConst.TIME_DATE_FORMAT);
        String toDate = DateTimeHelper.getDateStrAfter(max_day, -overflowTail, AirlinkConst.TIME_DATE_FORMAT);

        Map<String, Object> result = new HashMap<>();
        result.put("memo", memo);
        result.put("eline", eline);
        result.put("excludeEline", exclude_eline);
        result.put("fromDate", fromDate);
        result.put("toDate", toDate);
        result.put("overflowHead", overflowHead);
        result.put("overflowTail", overflowTail);

        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), result));
    }

    /**
     * 删除节假日
     *
     * @param id ID值
     * @return
     */
    @PostMapping("/holiday/delete")
    public ResponseEntity<?> deleteHoliday(@RequestParam("id") Integer id) {
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);
        int changed = template.update("delete from ASSIST_HOLIDAY_CFG where HOLIDAY_SEQ=(select HOLIDAY_SEQ from ASSIST_HOLIDAY_CFG where id=?)", id);

        if (changed > 0) {
            holidayTransitService.addHolidayDate(ConstantUtil.RESET);
        }
        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    /**
     * 更新节假日
     *
     * @param params
     * @return
     */
    @PostMapping("/holiday/update")
    public ResponseEntity<?> updateHoliday(@RequestBody() String params) {
        JSONObject json = JSON.parseObject(params);
        if (json == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "invalid JSON"));
        }

        Integer id = json.getInteger("id");
        String fromDate = json.getString("fromDate");
        String toDate = json.getString("toDate");
        String memo = json.getString("memo");
        String eline = json.getString("eline");
        String excludeEline = json.getString("excludeEline");
        Integer overflowHead = json.getInteger("overflowHead");
        Integer overflowTail = json.getInteger("overflowTail");

        if (StringUtils.isBlank(eline)) {
            eline = "*";
        }

        if (excludeEline == null) {
            excludeEline = "";
        }

        // 参数检查
        if (id == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "id needed"));
        }

        if (overflowHead == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "overflowHead needed"));
        }

        if (overflowTail == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "overflowTail needed"));
        }

        if (StringUtils.isBlank(fromDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "fromDate needed"));
        }

        if (StringUtils.isBlank(toDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "toDate needed"));
        }

        if (StringUtils.isBlank(memo)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "memo needed"));
        }

        if (StringUtils.isBlank(toDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "toDate needed"));
        }

        if (StringUtils.isBlank(toDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "toDate needed"));
        }

        if (toDate.compareTo(fromDate) < 0) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "to_date should bigger than from_date"));
        }

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        List<Map<String, Object>> holidays = template.queryForList("select * from ASSIST_HOLIDAY_CFG where HOLIDAY_SEQ=(select HOLIDAY_SEQ from ASSIST_HOLIDAY_CFG where id=?)", id);
        if (holidays == null || holidays.size() == 0) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "No data for id: "+id));
        }

        // 计算可用的ID值
        Integer max = template.queryForObject("select max(ID) from ASSIST_HOLIDAY_CFG", Integer.class);
        int mid = max == null ? 1 : max + 1;

        int holidaySeq = ((Number)holidays.get(0).get("HOLIDAY_SEQ")).intValue();

        Date _fromDate = DateTimeHelper.getDate(fromDate, AirlinkConst.TIME_DATE_FORMAT);
        Date _toDate = DateTimeHelper.getDate(toDate, AirlinkConst.TIME_DATE_FORMAT);
        int days = DateTimeHelper.daysBetween(_fromDate, _toDate) + 1;

        Calendar cal = Calendar.getInstance();
        cal.setTime(_fromDate);

        // 删除多余的数据
        cal.add(Calendar.DAY_OF_YEAR, -overflowHead);
        for ( ; ; ) {
            cal.add(Calendar.DAY_OF_YEAR, -1);
            String date = DateTimeHelper.date2String(cal.getTime(), AirlinkConst.TIME_DATE_FORMAT);
            Map<String, Object> myday = holidays.stream().filter(x -> date.equals(x.get("HOLIDAY"))).findFirst().orElse(null);
            if (myday == null) {
                break;
            }

            template.update("DELETE FROM ASSIST_HOLIDAY_CFG WHERE ID=?", myday.get("ID"));
        }

        cal.setTime(_fromDate);
        if (overflowHead > 0) {
            cal.add(Calendar.DAY_OF_YEAR, -overflowHead);
            for (int i = overflowHead; i > 0 ; i--) {
                String date = DateTimeHelper.date2String(cal.getTime(), AirlinkConst.TIME_DATE_FORMAT);
                Map<String, Object> myday = holidays.stream().filter(x -> date.equals(x.get("HOLIDAY"))).findFirst().orElse(null);
                if (myday != null) {
                    if (((Number)myday.get("OVERFLOW")).intValue() != 1 || ((Number)myday.get("SEQ")).intValue() != -i) {
                        template.update("UPDATE ASSIST_HOLIDAY_CFG SET OVERFLOW=1, SEQ=? WHERE ID=?", -i, myday.get("ID"));
                    }
                } else {
                    template.update("INSERT INTO ASSIST_HOLIDAY_CFG\n" +
                                    "(ID, HOLIDAY, MEMO, ELINE, EXCLUDE_ELINE, DAYS, OVERFLOW, SEQ, HOLIDAY_SEQ)\n" +
                                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            mid++, date, memo, eline, excludeEline, days, 1, -i, holidaySeq);
                }
                cal.add(Calendar.DAY_OF_YEAR, 1);
            }
        }

        for (int i = 1; i <= days ; i++) {
            String date = DateTimeHelper.date2String(cal.getTime(), AirlinkConst.TIME_DATE_FORMAT);
            Map<String, Object> myday = holidays.stream().filter(x -> date.equals(x.get("HOLIDAY"))).findFirst().orElse(null);
            if (myday != null) {
                if (((Number)myday.get("OVERFLOW")).intValue() != 0 || ((Number)myday.get("SEQ")).intValue() != i ) {
                    template.update("UPDATE ASSIST_HOLIDAY_CFG SET OVERFLOW=0, SEQ=? WHERE ID=?", i, myday.get("ID"));
                }
            } else {
                template.update("INSERT INTO ASSIST_HOLIDAY_CFG\n" +
                                "(ID, HOLIDAY, MEMO, ELINE, EXCLUDE_ELINE, DAYS, OVERFLOW, SEQ, HOLIDAY_SEQ)\n" +
                                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        mid++, date, memo, eline, excludeEline, days, 0, i, holidaySeq);
            }
            cal.add(Calendar.DAY_OF_YEAR, 1);
        }

        if (overflowTail > 0) {
            for (int i = 1; i <= overflowTail ; i++) {
                String date = DateTimeHelper.date2String(cal.getTime(), AirlinkConst.TIME_DATE_FORMAT);
                Map<String, Object> myday = holidays.stream().filter(x -> date.equals(x.get("HOLIDAY"))).findFirst().orElse(null);
                if (myday != null) {
                    if (((Number)myday.get("OVERFLOW")).intValue() != 1 || ((Number)myday.get("SEQ")).intValue() != days + i) {
                        template.update("UPDATE ASSIST_HOLIDAY_CFG SET OVERFLOW=1, SEQ=? WHERE ID=?", days + i, myday.get("ID"));
                    }
                } else {
                    template.update("INSERT INTO ASSIST_HOLIDAY_CFG\n" +
                                    "(ID, HOLIDAY, MEMO, ELINE, EXCLUDE_ELINE, DAYS, OVERFLOW, SEQ, HOLIDAY_SEQ)\n" +
                                    "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            mid++, date, memo, eline, excludeEline, days, 1, days + i, holidaySeq);
                }
                cal.add(Calendar.DAY_OF_YEAR, 1);
            }
        }

        // 删除多余的数据
        for ( ; ; ) {
            String date = DateTimeHelper.date2String(cal.getTime(), AirlinkConst.TIME_DATE_FORMAT);
            Map<String, Object> myday = holidays.stream().filter(x -> date.equals(x.get("HOLIDAY"))).findFirst().orElse(null);
            if (myday == null) {
                break;
            }

            template.update("DELETE FROM ASSIST_HOLIDAY_CFG WHERE ID=?", myday.get("ID"));
            cal.add(Calendar.DAY_OF_YEAR, 1);
        }

        int changed = template.update("UPDATE ASSIST_HOLIDAY_CFG SET MEMO=?, ELINE=?, EXCLUDE_ELINE=?, DAYS=? WHERE HOLIDAY_SEQ=?", memo, eline, excludeEline, days, holidaySeq);

        if (changed > 0) {
            // 修改相关的表
            holidayTransitService.addHolidayDate(ConstantUtil.RESET);
        }

        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    /**
     * 生成新的节假日。
     *
     * @param params
     * @return
     */
    @PostMapping("/holiday/create")
    public ResponseEntity<?> createHoliday(@RequestBody() String params) {
        JSONObject json = JSON.parseObject(params);
        if (json == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "invalid JSON"));
        }

        String fromDate = json.getString("fromDate");
        String toDate = json.getString("toDate");
        String memo = json.getString("memo");
        String eline = json.getString("eline");
        String excludeEline = json.getString("excludeEline");
        Integer overflowHead = json.getInteger("overflowHead");
        Integer overflowTail = json.getInteger("overflowTail");

        if (StringUtils.isBlank(eline)) {
            eline = "*";
        }

        if (excludeEline == null) {
            excludeEline = "";
        }

        // 参数检查
        if (overflowHead == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "overflowHead needed"));
        }

        if (overflowTail == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "overflowTail needed"));
        }

        if (StringUtils.isBlank(fromDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "fromDate needed"));
        }

        if (StringUtils.isBlank(toDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "toDate needed"));
        }

        if (StringUtils.isBlank(memo)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "memo needed"));
        }

        if (StringUtils.isBlank(toDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "toDate needed"));
        }

        if (StringUtils.isBlank(toDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "toDate needed"));
        }

        if (toDate.compareTo(fromDate) < 0) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "toDate should bigger than fromDate"));
        }

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        // 计算可用的ID值和节假日序号。
        // 节假日序号用来表示一个节假日。
        Map<String, Object> max = template.queryForMap("select max(ID) as ID, max(HOLIDAY_SEQ) as HOLIDAY_SEQ from ASSIST_HOLIDAY_CFG");
        int id = max.get("ID") == null ? 1 : ((Number)max.get("ID")).intValue() + 1;
        int holidaySeq = max.get("HOLIDAY_SEQ") == null ? 1 : ((Number)max.get("HOLIDAY_SEQ")).intValue() + 1;

        Date _fromDate = DateTimeHelper.getDate(fromDate, AirlinkConst.TIME_DATE_FORMAT);
        Date _toDate = DateTimeHelper.getDate(toDate, AirlinkConst.TIME_DATE_FORMAT);
        int days = DateTimeHelper.daysBetween(_fromDate, _toDate) + 1;

        Calendar cal = Calendar.getInstance();
        cal.setTime(_fromDate);
        // 生成前溢出记录
        if (overflowHead > 0) {
            cal.add(Calendar.DAY_OF_YEAR, -overflowHead);
            for (int i = overflowHead; i > 0 ; i--) {
                template.update("INSERT INTO ASSIST_HOLIDAY_CFG\n" +
                                "(ID, HOLIDAY, MEMO, ELINE, EXCLUDE_ELINE, DAYS, OVERFLOW, SEQ, HOLIDAY_SEQ)\n" +
                                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        id++, DateTimeHelper.date2String(cal.getTime(), AirlinkConst.TIME_DATE_FORMAT), memo, eline, excludeEline, days, 1, -i, holidaySeq);
                cal.add(Calendar.DAY_OF_YEAR, 1);
            }
        }

        for (int i = 1; i <= days ; i++) {
            template.update("INSERT INTO ASSIST_HOLIDAY_CFG\n" +
                            "(ID, HOLIDAY, MEMO, ELINE, EXCLUDE_ELINE, DAYS, OVERFLOW, SEQ, HOLIDAY_SEQ)\n" +
                            "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    id++, DateTimeHelper.date2String(cal.getTime(), AirlinkConst.TIME_DATE_FORMAT), memo, eline, excludeEline, days, 0, i, holidaySeq);
            cal.add(Calendar.DAY_OF_YEAR, 1);
        }

        // 生成后溢出记录
        if (overflowTail > 0) {
            for (int i = 1; i <= overflowTail ; i++) {
                template.update("INSERT INTO ASSIST_HOLIDAY_CFG\n" +
                                "(ID, HOLIDAY, MEMO, ELINE, EXCLUDE_ELINE, DAYS, OVERFLOW, SEQ, HOLIDAY_SEQ)\n" +
                                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        id++, DateTimeHelper.date2String(cal.getTime(), AirlinkConst.TIME_DATE_FORMAT), memo, eline, excludeEline, days, 1, days + i, holidaySeq);
                cal.add(Calendar.DAY_OF_YEAR, 1);
            }
        }

        // 修改相关的表
        holidayTransitService.addHolidayDate(ConstantUtil.APPEND);

        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    /**
     *
     * 读取航季列表。可以分页读取。
     *
     * @param pageNo 第几页，从1开始。
     * @param pageSize 每天条数，缺省值为20.
     * @return
     */
    @GetMapping("/flightSeason/list")
    public ResponseEntity<?> getFlightSeasonList(@RequestParam(value = "name", required = false) String name,
                                            @RequestParam(value = "pageNo", defaultValue = "1") Integer pageNo,
                                            @RequestParam(value = "pageSize", defaultValue = "20") Integer pageSize) {
        int startRow = (pageNo - 1) * pageSize;
        int endRow = startRow + pageSize;

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        StringBuffer sql = new StringBuffer("SELECT A.*, ROWNUM RN FROM ASSIST_SEASON A where 1=1 ");
        if (StringUtils.isNotBlank(name)) {
            sql.append("and name like '%"+name+"%'");
        }
        Integer total = template.queryForObject("select count(*) from (" + sql + ")", Integer.class);
        if (total == null) {
            total = 0;
        }

        sql.append(" order by START_DATE) WHERE RN >= " + startRow+" and RN < "+endRow);
        List<Map<String, Object>> list = template.queryForList("SELECT * FROM ("+sql);
        Map<String, Object> result = new HashMap<>();
        result.put("list", list);
        result.put("total", total);
        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), result));
    }

    /**
     * 删除航季
     *
     * @param id ID值
     * @return
     */
    @PostMapping("/flightSeason/delete")
    public ResponseEntity<?> deleteFlightSeason(@RequestParam("id") int id) {
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);
        template.execute("delete from ASSIST_SEASON where id="+id);
        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    /**
     * 修改航季
     *
     * @param params
     * @return
     */
    @PostMapping("/flightSeason/update")
    public ResponseEntity<?> updateFlightSeason(@RequestBody() String params) {
        JSONObject json = JSON.parseObject(params);
        if (json == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "invalid JSON"));
        }

        Integer id = json.getInteger("id");
        String name = json.getString("name");
        String startDate = json.getString("startDate");
        String endDate = json.getString("endDate");

        // 检查参数
        if (id == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "id needed"));
        }

        if (StringUtils.isBlank(startDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "startDate needed"));
        }

        if (StringUtils.isBlank(endDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "endDate needed"));
        }

        if (StringUtils.isBlank(name)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "name needed"));
        }

        if (endDate.compareTo(startDate) < 0) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "endDate should bigger than startDate"));
        }

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        template.update("UPDATE ASSIST_SEASON SET START_DATE=?, END_DATE=?, NAME=? WHERE ID=?",
                startDate, endDate, name, id);

        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    /**
     * 生成新的航季。
     *
     * @param params
     * @return
     */
    @PostMapping("/flightSeason/create")
    public ResponseEntity<?> createFlightSeason(@RequestBody() String params) {
        JSONObject json = JSON.parseObject(params);
        if (json == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "invalid JSON"));
        }

        String name = json.getString("name");
        String startDate = json.getString("startDate");
        String endDate = json.getString("endDate");

        // 参数检查
        if (StringUtils.isBlank(startDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "startDate needed"));
        }

        if (StringUtils.isBlank(endDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "endDate needed"));
        }

        if (StringUtils.isBlank(name)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "name needed"));
        }

        if (endDate.compareTo(startDate) < 0) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "endDate should bigger than startDate"));
        }

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        // 计算可用的ID值
        Integer max = template.queryForObject("select max(ID) from ASSIST_SEASON", Integer.class);
        int id = max == null ? 1 : max + 1;

        template.update("INSERT INTO ASSIST_SEASON (ID, START_DATE, END_DATE, NAME) VALUES(?, ?, ?, ?)",
                id, startDate, endDate, name);

        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    /**
     * 设置春运起止时间
     * @param startDate
     * @param endDate
     * @return
     */
    @PostMapping("/springFestivalTransport/set")
    public ResponseEntity<?> setSpringFestivalTransportRange(@RequestParam("startDate") String startDate, @RequestParam("endDate")String endDate) {
        // should be 12-16,01-25
        setKeyValue("spring_festival_transport.range", startDate+","+endDate);
        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    /**
     * 读取春运起止时间
     *
     * @return
     */
    @GetMapping("/springFestivalTransport/get")
    public ResponseEntity<?> getSpringFestivalTransportRange() {
        String value = getKeyValue("spring_festival_transport.range");
        if (StringUtils.isBlank(value)) {
            value = "12-16,01-25";
        }
        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), value == null ? "" : value));
    }

    /**
     * 设置全局控制与博弈开关
     * @param enable
     * @return
     */
    @PostMapping("/challenge/set")
    public ResponseEntity<?> setChallengeEnable(@RequestParam("enable") boolean enable) {
        setKeyValue("challenge.enable", Boolean.toString(enable));
        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    /**
     * 读取全局控制与博弈开关
     *
     * @return
     */
    @GetMapping("/challenge/get")
    public ResponseEntity<?> getChallengeEnable() {
        String value = getKeyValue("challenge.enable");
        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), value == null ? "true" : value));
    }

    private void setKeyValue(String key, String value) {
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);
        Integer max = template.queryForObject("select max(ID) from ASSIST_KEY_VALUE", Integer.class);
        int id = max == null ? 1 : max + 1;

        template.update("MERGE INTO ASSIST_KEY_VALUE a\n" +
                "USING (SELECT ? as ID, ? as C_KEY, ? as C_VALUE FROM dual) b\n" +
                "ON ( a.C_KEY=b.C_KEY)\n" +
                "WHEN MATCHED THEN\n" +
                "  UPDATE SET a.C_VALUE=b.C_VALUE\n" +
                "WHEN NOT MATCHED THEN\n" +
                "  INSERT (ID, C_KEY, C_VALUE) VALUES(b.ID, b.C_KEY, b.C_VALUE)", id, key, value);
    }

    private String getKeyValue(String key) {
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);
        List<Map<String, Object>> list = template.queryForList("select C_VALUE from ASSIST_KEY_VALUE where C_KEY=?", key);
        if (list == null || list.size() == 0) {
            return null;
        }
        return (String) list.get(0).get("C_VALUE");
    }

    /**
     *
     * 读取航线列表。可以分页读取。
     *
     * @param pageNo 第几页，从1开始。
     * @param pageSize 每天条数，缺省值为20.
     * @return
     */
    @GetMapping("/flightLine/list")
    public ResponseEntity<?> getFlightLineList(@RequestParam(value = "eline", required = false) String eline,
                                               @RequestParam(value = "flightNo", required = false) String flightNo,
                                               @RequestParam(value = "flyType", required = false) String flyType,
                                               @RequestParam(value = "challenge", required = false) String challenge,
                                               @RequestParam(value = "pageNo", defaultValue = "1") Integer pageNo,
                                               @RequestParam(value = "pageSize", defaultValue = "20") Integer pageSize) {
        int startRow = (pageNo - 1) * pageSize;
        int endRow = startRow + pageSize;

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        StringBuffer sql = new StringBuffer("SELECT A.*, ROWNUM RN FROM ASSIST_LINE_CFG A where 1=1 ");
        if (StringUtils.isNotBlank(eline)) {
            sql.append("and ELINE='"+eline+"'");
        }
        if (StringUtils.isNotBlank(flightNo)) {
            sql.append("and FLIGHT_NO='"+flightNo+"'");
        }
        if (StringUtils.isNotBlank(flyType)) {
            sql.append("and FLY_TYPE='"+flyType+"'");
        }
        if (StringUtils.isNotBlank(challenge) && ("0".equals(challenge) || "1".equals(challenge))) {
            sql.append("and CHALLENGE="+challenge);
        }
        Integer total = template.queryForObject("select count(*) from (" + sql + ")", Integer.class);
        if (total == null) {
            total = 0;
        }

        sql.append(" order by FLIGHT_DATE, FLIGHT_NO, DEP, ARR) WHERE RN >= " + startRow+" and RN < "+endRow);
        List<Map<String, Object>> list = template.queryForList("SELECT * FROM ("+sql);

        if (list != null && list.size() > 0) {
            sql.setLength(0);
            sql.append("select * from ASSIST_LINE_CFG_PEER where LINE_CFG_ID in (");
            for (Map<String, Object> item : list) {
                sql.append(item.get("ID")).append(",");
            }
            sql.setLength(sql.length() - 1);
            sql.append(") order by LINE_CFG_ID, SEQ");

            List<Map<String, Object>> peers = template.queryForList(sql.toString());
            for (Map<String, Object> flt : list) {
                flt.put("peers", peers.stream().map(x -> {x.remove("SEQ"); return x;}).filter(x -> flt.get("ID").equals(x.get("LINE_CFG_ID"))).collect(Collectors.toList()));
            }
        }

        list = list.stream().map(x -> {
            x.put("CREATE_TIME", DateTimeHelper.date2String((Date)x.get("CREATE_TIME"), AirlinkConst.TIME_FULL_FORMAT));
            x.put("UPDATE_TIME", DateTimeHelper.date2String((Date)x.get("UPDATE_TIME"), AirlinkConst.TIME_FULL_FORMAT));
            x.remove("RN");
            return x;
        }).collect(Collectors.toList());

        Map<String, Object> result = new HashMap<>();
        result.put("list", list);
        result.put("total", total);
        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), result));
    }

    /**
     * 删除航线
     *
     * @param id ID值
     * @return
     */
    @PostMapping("/flightLine/delete")
    public ResponseEntity<?> deleteFlightLine(@RequestParam("id") Integer id) {
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);
        template.execute("delete from ASSIST_LINE_CFG_PEER where LINE_CFG_ID="+id);
        template.execute("delete from ASSIST_LINE_CFG where id="+id);
        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    /**
     * 修改航线价格红线
     *
     * @param params
     * @return
     */
    @PostMapping("/flightLine/redPrice/update")
    public ResponseEntity<?> updateFlightLineRedPrice(@RequestBody() String params) {
        JSONObject json = JSON.parseObject(params);
        if (json == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "invalid JSON"));
        }

        String eline = json.getString("eline");
        String flightNo = json.getString("flightNo");
        String startDate = json.getString("startDate");
        String endDate = json.getString("endDate");
        Integer redPrice = json.getInteger("redPrice");

        if (StringUtils.isBlank(eline)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "eline needed"));
        }

        if (eline.length() != 6) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "invalid eline"));
        }

        if (StringUtils.isBlank(flightNo)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "flightNo needed"));
        }

        if (StringUtils.isBlank(startDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "startDate needed"));
        }

        if (StringUtils.isBlank(endDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "endDate needed"));
        }

        if (redPrice == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "redPrice needed"));
        }

        if (endDate.compareTo(startDate) < 0) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "endDate should bigger than startDate"));
        }

        String dep = eline.substring(0, 3);
        String arr = eline.substring(3);

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        List<Map<String, Object>> exists = template.queryForList("select max(FLIGHT_DATE) as max_date, min(FLIGHT_DATE) as min_date from ASSIST_LINE_CFG where FLIGHT_NO=? and DEP=? and ARR=?", flightNo, dep, arr);
        if (exists != null && exists.size() > 0) {
            String maxDate = (String)exists.get(0).get("max_date");
            String minDate = (String)exists.get(0).get("min_date");
            if (startDate.compareTo(minDate) < 0 || endDate.compareTo(maxDate) > 0) {
                return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "日期超出范围"));
            }
        }

        String sql = "UPDATE ASSIST_LINE_CFG SET RED_PRICE=? WHERE FLIGHT_NO=? and DEP=? and ARR=? and FLIGHT_DATE between ? and ?\n";

        template.update(sql, redPrice, flightNo, dep, arr, startDate, endDate);

        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    /**
     * 修改航线控制与博弈开关
     *
     * @param params
     * @return
     */
    @PostMapping("/flightLine/challenge/update")
    public ResponseEntity<?> updateFlightLineChallenge(@RequestBody() String params) {
        JSONObject json = JSON.parseObject(params);
        if (json == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "invalid JSON"));
        }

        String eline = json.getString("eline");
        String flightNo = json.getString("flightNo");
        String startDate = json.getString("startDate");
        String endDate = json.getString("endDate");
        Integer challenge = json.getInteger("challenge");

        if (StringUtils.isBlank(eline)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "eline needed"));
        }

        if (eline.length() != 6) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "invalid eline"));
        }

        if (StringUtils.isBlank(flightNo)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "flightNo needed"));
        }

        if (StringUtils.isBlank(startDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "startDate needed"));
        }

        if (StringUtils.isBlank(endDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "endDate needed"));
        }

        if (challenge == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "challenge needed"));
        }

        if (endDate.compareTo(startDate) < 0) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "endDate should bigger than startDate"));
        }

        String dep = eline.substring(0, 3);
        String arr = eline.substring(3);

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        List<Map<String, Object>> exists = template.queryForList("select max(FLIGHT_DATE) as max_date, min(FLIGHT_DATE) as min_date from ASSIST_LINE_CFG where FLIGHT_NO=? and DEP=? and ARR=?", flightNo, dep, arr);
        if (exists != null && exists.size() > 0) {
            String maxDate = (String)exists.get(0).get("max_date");
            String minDate = (String)exists.get(0).get("min_date");
            if (startDate.compareTo(minDate) < 0 || endDate.compareTo(maxDate) > 0) {
                return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "日期超出范围"));
            }
        }

        String sql = "UPDATE ASSIST_LINE_CFG SET CHALLENGE=? WHERE FLIGHT_NO=? and DEP=? and ARR=? and FLIGHT_DATE between ? and ?\n";

        template.update(sql, challenge, flightNo, dep, arr, startDate, endDate);

        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    /**
     * 修改航线
     *
     * @param params
     * @return
     */
    @PostMapping("/flightLine/update")
    public ResponseEntity<?> updateFlightLine(@RequestBody() String params) {
        JSONObject json = JSON.parseObject(params);
        if (json == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "invalid JSON"));
        }

        Integer id = json.getInteger("id");
        Integer challenge = json.getInteger("challenge");
        Integer redPrice = json.getInteger("redPrice");
        Integer locked = json.getInteger("locked");
        Integer priceStep = json.getInteger("priceStep");
        if (priceStep == null) {
            priceStep = 0;
        }
        String flyType = json.getString("flyType");

        if (id == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "id needed"));
        }

        if (locked == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "locked needed"));
        }

        if (redPrice == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "redPrice needed"));
        }

        if (StringUtils.isBlank(flyType)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "flyType needed"));
        }

        if (!"ALONE".equals(flyType) && !"COMPETE".equals(flyType)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "flyType should be ALONE or COMPETE"));
        }

        if (challenge == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "challenge needed"));
        }

        JSONArray peers = json.getJSONArray("peers");

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        String sql = "UPDATE ASSIST_LINE_CFG SET CHALLENGE=?, FLY_TYPE=?, UPDATE_TIME=sysdate, RED_PRICE=?, LOCKED=?, PRICE_STEP=? WHERE id=?\n";

        template.update(sql, challenge, flyType, redPrice, locked, priceStep, id);

        if (peers != null && peers.size() > 0) {
            List<Map<String, Object>> mypeers = template.queryForList("SELECT * FROM ASSIST_LINE_CFG_PEER WHERE LINE_CFG_ID=?", id);

            if (mypeers.size() > peers.size()) {
                // 删除多余的
                for (int i = mypeers.size() ; i > peers.size() ; i--) {
                    template.update("DELETE FROM ASSIST_LINE_CFG_PEER WHERE LINE_CFG_ID=? and SEQ=?", id, i);
                }
            } else if (mypeers.size() < peers.size()) {
                Integer max = template.queryForObject("select max(ID) from ASSIST_LINE_CFG_PEER", Integer.class);
                int pid = max == null ? 1 : max + 1;

                // 补上缺少的
                sql = "INSERT INTO ASSIST_LINE_CFG_PEER\n" +
                        "(ID, LINE_CFG_ID, FLIGHT_NO, DEP, ARR, DEP_TIME, ARR_TIME, SEQ)\n" +
                        "VALUES(?, ?, ?, ?, ?, ?, ?, ?)";
                for (int i = mypeers.size() + 1 ; i <= peers.size(); i++) {
                    JSONObject peer = peers.getJSONObject(i - 1);
                    template.update(sql, pid++, id, peer.getString("flightNo"), peer.getString("dep"),
                            peer.getString("arr"), peer.getString("dep_time"), peer.getString("arr_time"), i);
                }
            }

            // 修改其他的
            sql = "UPDATE ASSIST_LINE_CFG_PEER set FLIGHT_NO=?, DEP=?, ARR=?, DEP_TIME=?, ARR_TIME=? " +
                    "where LINE_CFG_ID=? and SEQ=?";
            for (int i = 0; i < Math.min(peers.size(), mypeers.size()) ; i++) {
                JSONObject peer = peers.getJSONObject(i);
                template.update(sql, peer.getString("flightNo"), peer.getString("dep"),
                        peer.getString("arr"), peer.getString("dep_time"), peer.getString("arr_time"), id, i + 1);
            }
        }

        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    /**
     * 生成新的航线。
     *
     * @param params
     * @return
     */
    @PostMapping("/flightLine/create")
    public ResponseEntity<?> createFlightLine(@RequestBody() String params) {
        JSONObject json = JSON.parseObject(params);
        if (json == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "invalid JSON"));
        }

        String eline = json.getString("eline");
        String flightNo = json.getString("flightNo");
        String startDate = json.getString("startDate");
        String endDate = json.getString("endDate");
        Integer challenge = json.getInteger("challenge");
        Integer redPrice = json.getInteger("redPrice");
        Integer locked = json.getInteger("locked");
        Integer priceStep = json.getInteger("priceStep");
        if (priceStep == null) {
            priceStep = 0;
        }
        String flyType = json.getString("flyType");

        if (StringUtils.isBlank(eline)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "eline needed"));
        }

        if (eline.length() != 6) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "invalid eline"));
        }

        if (StringUtils.isBlank(flightNo)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "flightNo needed"));
        }

        if (StringUtils.isBlank(startDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "startDate needed"));
        }

        if (StringUtils.isBlank(endDate)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "endDate needed"));
        }

        if (locked == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "locked needed"));
        }

        if (redPrice == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "redPrice needed"));
        }

        if (StringUtils.isBlank(flyType)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "flyType needed"));
        }

        if (!"ALONE".equals(flyType) && !"COMPETE".equals(flyType)) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "flyType should be ALONE or COMPETE"));
        }

        if (challenge == null) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_NEEDED, "challenge needed"));
        }

        String dep = eline.substring(0, 3);
        String arr = eline.substring(3);

        if (endDate.compareTo(startDate) < 0) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "endDate should bigger than startDate"));
        }

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        List<Map<String, Object>> exists = template.queryForList("select 1 from ASSIST_LINE_CFG where FLIGHT_NO=? and DEP=? and ARR=? and FLIGHT_DATE>=? and FLIGHT_DATE<=?", flightNo, dep, arr, startDate, endDate);
        if (exists != null && exists.size() > 0) {
            return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.PARAM_INVALID, "日期已存在"));
        }

        // 计算可用的ID值
        Integer max = template.queryForObject("select max(ID) from ASSIST_LINE_CFG", Integer.class);
        int id = max == null ? 1 : max + 1;

        List<Map<String, Object>> times = template.queryForList("select distinct to_char(FLIGHT_DATE, 'yyyy-MM-dd') as FLIGHT_DATE, " +
                "(substr(DEPTIME, 1, 2) || ':' || substr(DEPTIME, 3)) as DEP_TIME, (substr(ARRTIME, 1, 2) || ':' || substr(ARRTIME, 3)) as ARR_TIME " +
                "from ANALY_AOGRAPH_AIR_FOR_MODEL where FLTNO=? and DEP=? and ARR=? and FLIGHT_DATE between to_date(?, 'yyyy-MM-dd') and to_date(?, 'yyyy-MM-dd')",
                flightNo, dep, arr, startDate, endDate);

        String sql = "INSERT INTO ASSIST_LINE_CFG\n" +
                "(ID, FLIGHT_NO, DEP, ARR, FLIGHT_DATE, DEP_TIME, ARR_TIME, CHALLENGE, FLY_TYPE, CREATE_TIME, UPDATE_TIME, RED_PRICE, LOCKED, PRICE_STEP)\n" +
                "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, sysdate, sysdate, ?, ?, ?)";

        String flightDate = startDate;
        for ( ; ; ) {
            String finalFlightDate = flightDate;
            Map<String, Object> time = times.stream().filter(x -> finalFlightDate.equals(x.get("FLIGHT_DATE"))).findFirst().orElse(null);
            template.update(sql, id++, flightNo, dep, arr, flightDate, time == null ? "" : time.get("DEP_TIME"), time == null ? "" : time.get("ARR_TIME"),
                    challenge, flyType, redPrice, locked, priceStep);

            if (flightDate.equals(endDate)) {
                break;
            }

            flightDate = DateTimeHelper.getDateStrAfter(flightDate, 1, AirlinkConst.TIME_DATE_FORMAT);
        }

        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), "OK"));
    }

    /**
     * 读取竞飞列表
     *
     * @param id
     * @return
     */
    @GetMapping("/flightLine/peer/list")
    public ResponseEntity<?> getFlightLinePeerList(@RequestParam("id") int id) {
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);

        Map<String, Object> flt = template.queryForMap("select * from ASSIST_LINE_CFG where id=?", id);
        String flightNo = (String)flt.get("FLIGHT_NO");
        String flightDate = (String)flt.get("FLIGHT_DATE");
        String dep = (String)flt.get("DEP");
        String arr = (String)flt.get("ARR");

        String sql = "select distinct DEP, DEP_CITY from ASSIST_AIRPORT_OD_PAIR where DEP in (?, ?)";
        List<Map<String, Object>> cities = template.queryForList(sql.toString(), dep, arr);

        // 所在城市
        String depCity = cities.stream().filter(x -> dep.equals(x.get("DEP"))).map(x -> (String)x.get("DEP_CITY")).findFirst().orElse(null);
        String arrCity = cities.stream().filter(x -> arr.equals(x.get("DEP"))).map(x -> (String)x.get("DEP_CITY")).findFirst().orElse(null);

        sql = "select distinct DEP, DEP_CITY from ASSIST_AIRPORT_OD_PAIR where DEP_CITY in (?, ?)";
        cities = template.queryForList(sql.toString(), depCity, arrCity);

        // 一市两场的机场
        String deps = cities.stream().filter(x -> depCity.equals(x.get("DEP_CITY"))).map(x -> (String) x.get("DEP")).collect(Collectors.joining("','", "'", "'"));
        String arrs = cities.stream().filter(x -> arrCity.equals(x.get("DEP_CITY"))).map(x -> (String) x.get("DEP")).collect(Collectors.joining("','", "'", "'"));

        sql = "WITH\n" +
                "t2 as (select FLTNO as FLIGHT_NO, DEP, ARR, DEPTIME as DEP_TIME, ARRTIME as ARR_TIME, INSERT_DATE from ANALY_AOGRAPH_AIR_FOR_MODEL\n" +
                "  where DEP in ("+deps+") and ARR in ("+arrs+") and FLIGHT_DATE=to_date(?, 'yyyy-MM-dd') and FLTNO != ?)\n" +
                "SELECT * FROM (SELECT t2.*,row_number() OVER (PARTITION BY FLIGHT_NO, DEP, ARR ORDER BY INSERT_DATE DESC) limit_order FROM t2) WHERE limit_order <= 1";
        List<Map<String, Object>> peers = template.queryForList(sql, flightDate, flightNo);
        for (Map<String, Object> peer : peers) {
            String s = (String)peer.get("DEP_TIME");
            peer.put("DEP_TIME", s.substring(0, 2) + ":" + s.substring(2));
            s = (String)peer.get("ARR_TIME");
            peer.put("ARR_TIME", s.substring(0, 2) + ":" + s.substring(2));
            peer.remove("INSERT_DATE");
            peer.remove("limit_order");
        }
        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), peers));
    }

    /**
     *
     * 返回RO数据用于前台显示
     *
     * @param flightNo 航班号
     * @param flightDate 航班日期
     * @param dep 出发机场
     * @param arr 到达机场
     * @param exDate 采样时间
     * @return
     */
    @GetMapping("/ro")
    public ResponseEntity<?> getRoView(@RequestParam("flight_no") String flightNo, @RequestParam("flight_date")String flightDate,
                                       @RequestParam("dep")String dep, @RequestParam("arr")String arr, @RequestParam(value="ex_date", required=false)String exDate) {
        if (exDate == null) {
            exDate = DateTimeHelper.date2String(new Date(), AirlinkConst.TIME_DATE_FORMAT);
        }

        Map<String, Object> resp = new HashMap<>();

        // 读取最新的数据
        StringBuffer sql = new StringBuffer("WITH\n" +
                "t1 as (SELECT * " +
                "from ORG_INV_LEG \n" +
//                "where dttm > addMinutes(now(), -30) and flight_no='").
                "where （airline_code || flight_number || suffix)='").append(flightNo).append("' " +
                "and leg_dep_date=DATE '").append(flightDate).append("'");
        if (StringUtils.isNotBlank(exDate)) {
            sql.append(" and trunc(dttm)=DATE '").append(exDate).append("' ");
        }
        sql.append("),\n" +
                "t2 as (SELECT * FROM (" +
                "  SELECT t1.*, row_number() OVER (PARTITION BY (airline_code || flight_number || suffix), leg_dep, leg_arr ORDER BY dttm DESC) limit_order FROM t1) WHERE limit_order <= 1) \n" +
                "select t2.* from t2");

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);
        List<Map<String, Object>> legs = template.queryForList(sql.toString());
        if (legs.size() == 0) {
            return ResponseEntity.ok().build();
        }

        List<Map<String, Object>> cities = template.queryForList("select distinct CITY_CODE, CITY_CH_NAME from ASSIST_AIRPORT where CITY_CODE in (?, ?)", dep, arr);
        String depCity = dep;
        String arrCity = arr;
        for (Map<String, Object> city : cities) {
            String code = (String) city.get("CITY_CODE");
            String name = (String) city.get("CITY_CH_NAME");
            if (dep.equals(code)) {
                depCity = name;
            } else {
                arrCity = name;
            }
        }

        sql.setLength(0);
        // 读取最新的数据
        sql.append("WITH\n" +
                "t1 as (SELECT * " +
                "from ORG_INV_SEGMENT \n" +
//                "where dttm > addMinutes(now(), -30) and flight_no='").
                "where （airline_code || flight_number || suffix)='").append(flightNo).append("' " +
                "and seg_dep_date=DATE '").append(flightDate).append("'");
        if (StringUtils.isNotBlank(exDate)) {
            sql.append(" and trunc(dttm)=DATE '").append(exDate).append("' ");
        }
        sql.append("),\n" +
                "t2 as (SELECT * FROM (" +
                "  SELECT t1.*, row_number() OVER (PARTITION BY (airline_code || flight_number || suffix), seg_dep, seg_arr ORDER BY dttm DESC) limit_order FROM t1) WHERE limit_order <= 1) \n" +
                "select t2.* from t2");
        List<Map<String, Object>> segs = template.queryForList(sql.toString());

        List<String[]> ods = segs.stream().map(x -> x.get("seg_dep") + "," + x.get("seg_arr")).distinct().
                sorted().map(x -> x.split(",")).collect(Collectors.toList());

        // AB, AC, BC or BC, AB, AC
        String[] sites = null;
        if (ods.size() == 3) {
            sites = new String[3];
            if (ods.get(0)[0].equals(ods.get(1)[0])) {
                sites[0] = ods.get(0)[0];
                sites[1] = ods.get(2)[0];
                sites[2] = ods.get(2)[1];
            } else {
                sites[0] = ods.get(1)[0];
                sites[1] = ods.get(0)[0];
                sites[2] = ods.get(0)[1];
            }
        }

        Map<String, Object> record = legs.get(0);

        String cabins = "";     // 所有的舱位，从高到低

        StringBuffer header = new StringBuffer();
        header.append((String) record.get("airline_code")+record.get("flight_number")).append(" ");
        Date date = DateTimeHelper.getDate(flightDate, AirlinkConst.TIME_DATE_FORMAT);
        header.append(DateTimeHelper.date2String(date, "ddMMM").toUpperCase()).append(" ").
                append("DOMESTIC".equals(record.get("type")) ? "D" : "I").append(" ").
                append(dep).append(arr).append(" ").append(DateTimeHelper.daysBetween(DateTimeHelper.getDate(exDate, AirlinkConst.TIME_DATE_FORMAT), date)).append(" ").
                append(record.get("leg_equip")).append("^");
        List<String> clsList = new ArrayList<>();
        List<String> cosList = new ArrayList<>();
        for (int i = 0 ; i < 4 ; i++) {
            String cls = (String)record.get("leg_c"+i+"_clz");
            if (StringUtils.isNotBlank(cls)) {
                header.append(cls).append(record.get("leg_c"+i+"_cos")).append("/");
                clsList.add(cls);
                cosList.add((String)record.get("leg_c"+i+"_cos"));
                cabins += cls + record.get("leg_c"+i+"_cos");
            }
        }
        header.setLength(header.length() - 1);
        resp.put("header", header.toString());
        resp.put("insert_time", DateTimeHelper.date2String((Date)record.get("dttm"), AirlinkConst.TIME_FULL_FORMAT));

        List<Map<String, Object>> list = new ArrayList<>();
        for (Map<String, Object> leg : legs) {
            leg.put("dep_city", depCity);
            leg.put("arr_city", arrCity);

            String legDep = (String) leg.get("leg_dep");
            String legArr = (String) leg.get("leg_arr");
            for (int i = 0; i < 4; i++) {
                String cls = (String) leg.get("leg_c" + i + "_clz");
                if (StringUtils.isBlank(cls)) {
                    continue;
                }

                Map<String, Object> info = new HashMap<>();
                info.put("dep", legDep);
                info.put("arr", legArr);
                info.put("dep_city", leg.get("dep_city"));
                info.put("arr_city", leg.get("arr_city"));
                info.put("CLASS", cls + "/" + leg.get("leg_c" + i + "_cos"));
                info.put("OPEN", leg.get("leg_c" + i + "_opn"));
                info.put("MAX", leg.get("leg_c" + i + "_max"));
                info.put("CAP", leg.get("leg_c" + i + "_cap"));
                info.put("T/B", leg.get("leg_c" + i + "_tb"));
                info.put("GRO", leg.get("leg_c" + i + "_gro"));
                info.put("GRS", leg.get("leg_c" + i + "_grs"));
                info.put("GT", leg.get("leg_c" + i + "_gt"));
                info.put("AVAIL", Integer.parseInt((String) leg.get("leg_c" + i + "_max")) - Integer.parseInt((String)leg.get("leg_c" + i + "_tb")));
//                info.put("RATIO", Double.parseDouble((String) leg.get("leg_c" + i + "_tb")) / Integer.parseInt((String) leg.get("leg_c" + i + "_cap")));
                info.put("BLK", leg.get("leg_c" + i + "_blk"));
//                        info.put("CT", record.get("leg_c"+i+"_ct"));
                info.put("SMT", leg.get("leg_c" + i + "_smt"));
                info.put("IND", leg.get("leg_c" + i + "_ind"));
                list.add(info);
            }
        }
        resp.put("legs", list);

        sql.setLength(0);
        sql.append("WITH\n" +
                "t1 as (select departure_3code, arrival_3code, ow_price, cabin, insert_date\n" +
                "  from ASSIST_TBL_PAT_INFO\n" +
                "  where fltno='%s' and flight_date_start <= '%s' and flight_date_end >= '%s' and start_date=DATE '").append(exDate).append("'),\n" +
                "t2 as (SELECT * FROM (" +
                "  SELECT t1.*, row_number() OVER (PARTITION BY departure_3code, arrival_3code, cabin ORDER BY insert_date DESC) limit_order FROM t1) WHERE limit_order <= 1) \n" +
                "select t2.* from t2");
        List<Map<String, Object>> prices = template.queryForList(String.format(sql.toString(), flightNo, flightDate, flightDate));

        sql.setLength(0);
        sql.append("select * from (select dep, arr, allocate_detail, forecast_time, ROWNUM RN " +
                "from PREDICT_FLIGHT_HE_FORECAST WHERE fltno='%s' and flight_date='%s' " +
//                "and forecast_date >= date_sub(now(), INTERVAL 30 minute) and forecast_date=" +
                "and trunc(forecast_time)=DATE '").append(exDate).append("' and status!=0 " +
                "order by dep, arr, forecast_time desc) where RN <= "+ods.size());
        template.setDataSource(dataSource);
        List<Map<String, Object>> forcasts = template.queryForList(String.format(sql.toString(), flightNo, flightDate, flightNo, flightDate));

        List<Map<String, Object>> forcastCabins = new ArrayList<>();
        if (forcasts != null && forcasts.size() > 0) {
            String forecastDate;
            if (forcasts.size() > 0 && forcasts.get(0).get("forecast_time") instanceof Date) {
                forecastDate = DateTimeHelper.date2String((Date) forcasts.get(0).get("forecast_time"), AirlinkConst.TIME_FULL_FORMAT);
            } else if (forcasts.size() > 0 && forcasts.get(0).get("forecast_time") instanceof LocalDateTime) {
                Date d = DateTimeHelper.toDate((LocalDateTime) forcasts.get(0).get("forecast_time"));
                forecastDate = DateTimeHelper.date2String(d, AirlinkConst.TIME_FULL_FORMAT);
            } else {
                forecastDate = "";
            }
            resp.put("forecast_time", forecastDate);

            String detail = (String) forcasts.get(0).get("allocate_detail");
            JSONArray details = JSON.parseArray(detail);
            Set<String> usedCabins = new HashSet<>();
            for (int i = details.size() - 1 ; i >= 0 ; i--) {
                JSONObject obj = details.getJSONObject(i);
                String code = obj.getString("code");
                if (i < details.size() - 1 && code.contains("(")) {
                    // G(S)这种随动舱只能出现在最低位
                    continue;
                }

                // K, K1, K2这种只取最低舱
//                code = code.substring(0, 1);
                if (usedCabins.contains(code.substring(0, 1))) {
                    continue;
                }
                usedCabins.add(code.substring(0, 1));

                Map<String, Object> item = new HashMap<>();
                item.put("code", code);
                item.put("cap", obj.getInteger("cap"));
                item.put("price",obj.getInteger("price"));
                forcastCabins.add(item);
            }
        }

//        List<String> cmds = null;
//        CabinAdjustService.Adjust adjust = cabinAdjustService.createForecastAdjust(flightNo, flightDate, dep, arr);
//        if (adjust != null) {
//            cmds = cabinAdjustService.createAdjustCabinCommands(adjust);
//        }

        list = new ArrayList<>();

        String classY = cabins.substring(cabins.indexOf("Y"));

        for (Map<String, Object> seg : segs) {
            String segDep = (String) seg.get("seg_dep");
            String segArr = (String) seg.get("seg_arr");

            Map<String, Object> forecast = forcasts.stream().filter(x -> segDep.equals(x.get("org_city_code")) && segArr.equals(x.get("dst_city_code"))).
                    limit(1).findFirst().orElse(null);

//            if (ods.size() == 1 || ods.size() == 3 && (dep.equals(sites[0]) && arr.equals(sites[1])) && segDep.equals(dep) && segArr.equals(arr) ||
//                    dep.equals(sites[1]) && arr.equals(sites[2]) && segDep.equals(dep) && segArr.equals(arr) ||
//                    dep.equals(sites[0]) && arr.equals(sites[2])) {
//
            for (int i = 0; i < cabins.length(); i++) {
                Map<String, Object> info = new HashMap<>();
                info.put("dep", segDep);
                info.put("arr", segArr);
                info.put("dep_city", seg.get("dep_city"));
                info.put("arr_city", seg.get("arr_city"));
                String cls = cabins.substring(i, i + 1);
                info.put("CLS", cls);
                String finalCls = cls;
                info.put("FARE", prices.stream().filter(x -> segDep.equals(x.get("departure_3code")) && segArr.equals(x.get("arrival_3code")) && finalCls.equals(x.get("cabin"))).
                        map(x -> ((Number)x.get("ow_price")).intValue()).min((x, y) -> ((Integer)x).compareTo(y)).orElse(0));

                cls = cls.toLowerCase();

                info.put("BKD", seg.get("seg_c" + cls + "_bkd"));
                info.put("GRS", seg.get("seg_c" + cls + "_grs"));
                info.put("BLK", seg.get("seg_c" + cls + "_blk"));
                info.put("WL", seg.get("seg_c" + cls + "_wl"));
                info.put("LSV(curr)", seg.get("seg_c" + cls + "_lsv"));
//                    String rcmd = (String) info.get("LSV(curr)");
//                    String v = getLsv(cmds, cls.toUpperCase());
//                    if (v != null) {
//                        rcmd = v;
//                    }
                String rcmd = null;
                Integer price = 0;
                if (forcastCabins != null) {
                    for (int j = 0; j < forcastCabins.size(); j++) {
                        Map<String, Object> cabin = forcastCabins.get(j);
                        String code = (String)cabin.get("code");
                        if (code.startsWith(cls.toUpperCase())) {
                            // this is LSS
                            int cap = (Integer) cabin.get("cap");
                            price = (Integer) cabin.get("price");
//                            int nextLss = 0;
//                            for (int pos = cabins.indexOf(cls.toUpperCase()) + 1 ; pos < cabins.length() ; pos ++) {
//                                String nextCls = cabins.substring(pos, pos + 1).toLowerCase();
//                                String nextInd = (String) seg.get("seg_c" + nextCls + "_ind");
//                                if (nextInd != null && nextInd.contains("L")) {
//                                    nextLss = Integer.parseInt((String) seg.get("seg_c" + nextCls + "_lss"));
//                                    break;
//                                }
//                            }
                            int lsv = cap + Integer.parseInt((String) seg.get("seg_c" + cls + "_bkd"));
                            rcmd = Integer.toString(lsv);
//                        } else if (code.endsWith(cls.toUpperCase()+")")) {
//                            // G(S)这种随动舱
//                            int lsv = 10 + Integer.parseInt((String) seg.get("seg_c" + cls + "_bkd"));
//                            rcmd = Integer.toString(lsv);
                        }
                    }
                }
                if (rcmd == null) {
                    rcmd = (String) info.get("LSV(curr)");
                }
                info.put("FARE(rcmd)", price);
                info.put("LSV(rcmd)", rcmd);

                String adviseCls = "";
                if (forcastCabins != null) {
                    for (int j = 0; j < forcastCabins.size(); j++) {
                        Map<String, Object> cabin = forcastCabins.get(j);
                        adviseCls += ((String) cabin.get("code")).substring(0, 1);
                    }
                }
                info.put("ADVISE_CLS", adviseCls);

                String lss = (String) seg.get("seg_c" + cls + "_lss");
                if ("#".equals(lss)) {
                    lss = "0";
                }
                info.put("LSS", lss);
                info.put("LT", seg.get("seg_c" + cls + "_lt"));
                info.put("SMT", seg.get("seg_c" + cls + "_smt"));

                String ind = (String) seg.get("seg_c" + cls + "_ind");
                if (ind == null) {
                    ind = "";
                }
                ind = sortInd(ind);
                info.put("IND", ind);
                if (!ind.contains("P") && "#".equals(seg.get("seg_c" + cls + "_lsv"))) {
                    ind += "P";
                }
                if (adviseCls.contains(cls.toUpperCase())) {
                    if(ind.contains("P")) {
                        ind = ind.replace("P", "");
                    }
                    if (!ind.contains("L")) {
                        ind += "L";
                    }
                    ind = sortInd(ind);
                }
                info.put("IND(rcmd)", ind);
                list.add(info);
            }

            List<CabinAdjustService.Seg > all = list.stream().
                    filter(x -> classY.contains((String) x.get("CLS"))).
                    map(x -> {
                        CabinAdjustService.Seg _seg = new CabinAdjustService.Seg();
                        String ind = (String) x.get("IND(rcmd)");
                        _seg.setCls((String) x.get("CLS"));
                        _seg.setOpen(ind.contains("P") ? "N" : "Y");
                        _seg.setLsv((String) x.get("LSV(rcmd)"));
                        return _seg;
                    }).
                    collect(Collectors.toList());

            String forcastCabinCodes = forcastCabins.stream().map(x -> ((String)x.get("code")).substring(0, 1)).collect(Collectors.joining());
            List<CabinAdjustService.Seg> _segs = all.stream().filter(x -> forcastCabinCodes.contains(x.getCls())).collect(Collectors.toList());

            cabinAdjustService.changeAdjustByRo(seg, all, _segs, classY, forcastCabins.size() == 0 ? null : (String)forcastCabins.get(forcastCabins.size() - 1).get("code"), true);

            for (CabinAdjustService.Seg _seg : all) {
                Map<String, Object> item = list.stream().filter(x -> _seg.getCls().equals(x.get("CLS"))).findFirst().get();
                String ind = (String) item.get("IND(rcmd)");
                if ("Y".equals(_seg.getOpen())) {
                    if (ind.contains("P")) {
                        ind = ind.replace("P", "");
                    }
                } else if ("N".equals(_seg.getOpen())) {
                    if (!ind.contains("P")) {
                        ind += "P";
                    }
                }

                item.put("LSV(rcmd)", _seg.getLsv());
                ind = sortInd(ind);
                item.put("IND(rcmd)", ind);
            }
        }

        String finalCabins = cabins;
        Map<String, List<Map<String, Object>>> clsMap = list.stream().collect(Collectors.groupingBy(l -> l.get("CLS").toString()));
        list.stream().forEach(l->{
            String rcmd = (String) l.get("IND(rcmd)");
            String cls = (String) l.get("CLS");
            String lsvrcmd = (String) l.get("LSV(rcmd)");//预测lsv
            if("-".equals(lsvrcmd)){
                l.put("LSS(rcmd)","-");
                return;
            }
            if("#".equals(lsvrcmd)){
                lsvrcmd = "0";
            }

            String bkd = (String) l.get("BKD");
            int lssrcmd = Integer.valueOf(lsvrcmd) - Integer.valueOf(bkd);
            if(!rcmd.contains("L")){
                l.put("LSS(rcmd)",lssrcmd);
                return;
            }

            String elkStr = finalCabins.substring(finalCabins.indexOf(cls)+1);
            for(int k=0; k<elkStr.length(); k++){
                String elkcabin = elkStr.substring(k, k + 1);
                List<Map<String, Object>> maps = clsMap.get(elkcabin);
                Map<String, Object> info = maps.get(0);
                String cabinlsvrcmd = info.get("LSV(rcmd)").toString();//预测lsv
                String cabinbkd = info.get("BKD").toString();
                String cabinindrcmd = (String) info.get("IND(rcmd)");

                if(!cabinindrcmd.contains("L")){
                    continue;
                }
                if(!StringUtils.isNumeric(cabinlsvrcmd)){
                    continue;
                }

                Integer elklssrcmd = Integer.valueOf(cabinlsvrcmd) - Integer.valueOf(cabinbkd);

                lssrcmd += elklssrcmd;
            }
            l.put("LSS(rcmd)",lssrcmd);
        });

        resp.put("segs", list);

        return ResponseEntity.ok().body(new ResponseVo<>(CommonCodeEnum.SUCCESS.getCode(), CommonCodeEnum.SUCCESS.getDesc(), resp));
    }

    // 排序， 使ind以ELPK的顺序出现
    private String sortInd(String ind) {
        StringBuffer sb = new StringBuffer("E");
        if (ind.contains("L")) {
            sb.append("L");
        }
        if (ind.contains("P")) {
            sb.append("P");
        }
        sb.append("K");
        return sb.toString();
    }

    private String getSql(String file) throws IOException {
        return IOUtils.toString(getClass().getResourceAsStream(file), "UTF-8");
    }

    public static String convert(String content, Map<String, ?> values) throws IOException {
        return FreeMarkHelper.convert(content, values);
    }
}
