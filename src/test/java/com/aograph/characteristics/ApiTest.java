package com.aograph.characteristics;

import com.aograph.characteristics.control.ApiController;
import com.aograph.characteristics.control.CommonCodeEnum;
import com.aograph.characteristics.control.ResponseVo;
import com.aograph.characteristics.utils.SpringContextUtil;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.annotation.Resource;
import javax.sql.DataSource;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ApiTest {
    @Autowired
    ApiController controller;

    @Test
    @Order(1)
    public void init() {
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(SpringContextUtil.getBean("oracleDataSource"));

        template.execute("truncate table ASSIST_WHITELIST");
        template.execute("truncate table ASSIST_WHITELIST_DETAIL");
        template.execute("truncate table ASSIST_WYRATIO");
        template.execute("truncate table ASSIST_HOLIDAY_CFG");
        template.execute("truncate table ASSIST_SEASON");
        template.execute("truncate table ASSIST_KEY_VALUE");
        template.execute("truncate table ASSIST_LINE_CFG");
        template.execute("truncate table ASSIST_LINE_CFG_PEER");
    }

    @Test
    @Order(2)
    public void testWhitelistList1() {
        String dep = null;
        String arr = null;
        String flightDateFrom = null;
        String flightDateTo = null;
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.getWhiteList(dep, arr, flightDateFrom, flightDateTo, 1, 20);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode(), resp.toString());
    }

    @Test
    @Order(3)
    public void testWhitelistCreate1() {
        // 参数缺乏
        String params = "{\"dep\": \"CTU\", \"arr\":\"PEK\", \"fromDate\":\"2022-11-27\", \"toDate\":\"2022-11-20\", \"exDifFrom\":1, \"exDifTo\":10, \"memo\":\"test\"}";
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.createWhiteList(params);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.PARAM_NEEDED.getCode(), resp.toString());
    }

    @Test
    @Order(4)
    public void testWhitelistCreate2() {
        // 参数不对
        String params = "{\"flightNo\":\"MU9999\", \"dep\": \"CTU\", \"arr\":\"PEK\", \"fromDate\":\"2022-11-27\", \"toDate\":\"2022-11-20\", \"exDifFrom\":1, \"exDifTo\":10, \"memo\":\"test\"}";
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.createWhiteList(params);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.PARAM_INVALID.getCode(), resp.toString());
    }

    @Test
    @Order(5)
    public void testWhitelistCreate3() {
        // 正常创建
        String params = "{\"flightNo\":\"MU9999\", \"dep\": \"CTU\", \"arr\":\"PEK\", \"fromDate\":\"2022-11-17\", \"toDate\":\"2022-11-20\", \"exDifFrom\":1, \"exDifTo\":10, \"memo\":\"test\"}";
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.createWhiteList(params);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode(), resp.toString());
    }

    @Test
    @Order(6)
    public void testWhitelistCreate4() {
        // 时间冲突
        String params = "{\"flightNo\":\"MU9999\", \"dep\": \"CTU\", \"arr\":\"PEK\", \"fromDate\":\"2022-11-10\", \"toDate\":\"2022-11-18\", \"exDifFrom\":1, \"exDifTo\":10, \"memo\":\"test\"}";
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.createWhiteList(params);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().toString().contains("时间范围有冲突"), resp.toString());
    }

    @Test
    @Order(7)
    public void testWhitelistCreate5() {
        // 参数缺乏
        String params = "{\"dep\": \"CTU\", \"arr\":\"PEK\", \"fromDate\":\"2022-11-10\", \"toDate\":\"2022-11-15\", \"exDifFrom\":1, \"exDifTo\":10, \"memo\":\"test\"}";
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.createWhiteList(params);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode(), resp.toString());

        String dep = null;
        String arr = null;
        String flightDateFrom = null;
        String flightDateTo = null;
        resp = (ResponseEntity<ResponseVo>) controller.getWhiteList(dep, arr, flightDateFrom, flightDateTo, 1, 20);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode() && resp.getBody().toString().contains("MU8888"), resp.toString());
    }

    @Test
    @Order(8)
    public void testWhitelistList2() {
        String dep = null;
        String arr = null;
        String flightDateFrom = null;
        String flightDateTo = null;
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.getWhiteList(dep, arr, flightDateFrom, flightDateTo, 1, 20);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode() && resp.getBody().toString().contains("MU9999"), resp.toString());
    }

    @Test
    @Order(9)
    public void testWhitelistUpdate1() {
        // 参数缺失
        String params = "{\"dep\": \"CTU\", \"arr\":\"PEK\", \"fromDate\":\"2022-11-10\", \"toDate\":\"2022-11-20\", \"exDifFrom\":1, \"exDifTo\":10, \"memo\":\"test\"}";
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.updateWhiteList(params);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.PARAM_NEEDED.getCode(), resp.toString());
    }

    @Test
    @Order(10)
    public void testWhitelistUpdate2() {
        // 参数不对
        String params = "{\"dep\": \"CTU\", \"arr\":\"PEK\", \"fromDate\":\"2022-11-27\", \"toDate\":\"2022-11-20\", \"exDifFrom\":1, \"exDifTo\":10, \"memo\":\"test\"}";
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.updateWhiteList(params);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.PARAM_NEEDED.getCode(), resp.toString());
    }

    @Test
    @Order(11)
    public void testWhitelistDelete() {
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.deleteWhiteList("1");
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode(), resp.toString());

        String dep = null;
        String arr = null;
        String flightDateFrom = null;
        String flightDateTo = null;
        resp = (ResponseEntity<ResponseVo>) controller.getWhiteList(dep, arr, flightDateFrom, flightDateTo, 1, 20);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode() && !resp.getBody().toString().contains("MU9999"), resp.toString());
    }

    @Test
    @Order(12)
    public void testGetWhiteListDetail() {
        String dep = null;
        String arr = null;
        String carrier = null;
        String flightNo = null;
        String flightDateFrom = null;
        String flightDateTo = null;
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.getWhiteListDetail(dep, arr, flightNo, flightDateFrom, flightDateTo, 1, 20);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode() && resp.getBody().toString().contains("8888"), resp.toString());
    }

    @Test
    @Order(13)
    public void testUpdateWhiteListDetail1() {
        // data not found
        String params = "{\"whitelistId\":1, \"enabled\":0, \"y_ratio\":0.8, \"c_ratio\":0.8, \"fromDate\":\"2022-11-14\", \"toDate\":\"2022-11-16\", \"minExDif\":2}";
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.updateWhiteListDetail(params);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.PARAM_INVALID.getCode(), resp.toString());
    }

    @Test
    @Order(13)
    public void testUpdateWhiteListDetail2() {
        // beyond time range
        String params = "{\"whitelistId\":2, \"enabled\":0, \"y_ratio\":0.8, \"c_ratio\":0.8, \"fromDate\":\"2022-11-14\", \"toDate\":\"2022-11-16\", \"minExDif\":2}";
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.updateWhiteListDetail(params);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.PARAM_INVALID.getCode(), resp.toString());
    }

    @Test
    @Order(14)
    public void testUpdateWhiteListDetail3() {
        String params = "{\"whitelistId\":2, \"enabled\":0, \"y_ratio\":0.8, \"c_ratio\":0.8, \"fromDate\":\"2022-11-12\", \"toDate\":\"2022-11-15\", \"minExDif\":2}";
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.updateWhiteListDetail(params);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode(), resp.toString());
    }

    @Test
    @Order(15)
    public void testSetWhiteListDetail1() {
        String params = "{\"id\":5, \"enabled\":0}";
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.setWhiteListDetail(params);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode(), resp.toString());

        String dep = null;
        String arr = null;
        String flightNo = "MU8888";
        String flightDateFrom = "2022-11-10";
        String flightDateTo = "2022-11-10";
        resp = (ResponseEntity<ResponseVo>) controller.getWhiteListDetail(dep, arr, flightNo, flightDateFrom, flightDateTo, 1, 20);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode() && resp.getBody().toString().contains("ENABLED=0"), resp.toString());
    }

    @Test
    @Order(16)
    public void testSetWhiteListDetail2() {
        String params = "{\"id\":5, \"enabled\":1, \"y_ratio\":0.81, \"c_ratio\":0.82, \"forecastTime\":\"2022-11-14 10:21:00\"}";
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.setWhiteListDetail(params);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode(), resp.toString());

        String dep = null;
        String arr = null;
        String flightNo = "MU8888";
        String flightDateFrom = "2022-11-10";
        String flightDateTo = "2022-11-10";
        resp = (ResponseEntity<ResponseVo>) controller.getWhiteListDetail(dep, arr, flightNo, flightDateFrom, flightDateTo, 1, 20);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode() && resp.getBody().toString().contains("Y_RATIO=0.81, C_RATIO=0.82"), resp.toString());
    }

    @Test
    @Order(17)
    public void testSetWhiteListDetail3() {
        String params = "{\"flightNo\":\"MU8888\", \"dep\":\"CTU\", \"arr\":\"PEK\", \"flightDate\":\"2022-11-12\", \"enabled\":0}";
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.setWhiteListDetail(params);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode(), resp.toString());

        String dep = null;
        String arr = null;
        String flightNo = "MU8888";
        String flightDateFrom = "2022-11-12";
        String flightDateTo = "2022-11-12";
        resp = (ResponseEntity<ResponseVo>) controller.getWhiteListDetail(dep, arr, flightNo, flightDateFrom, flightDateTo, 1, 20);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode() && resp.getBody().toString().contains("ENABLED=0"), resp.toString());
    }

    @Test
    @Order(18)
    public void testSetWhiteListDetail4() {
        String params = "{\"flightNo\":\"MU8888\", \"dep\":\"CTU\", \"arr\":\"PEK\", \"flightDate\":\"2022-11-12\", \"enabled\":1, \"y_ratio\":0.8, \"c_ratio\":0.8, \"forecastTime\":\"2022-11-14 10:21:00\"}";
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.setWhiteListDetail(params);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode(), resp.toString());

        String dep = null;
        String arr = null;
        String flightNo = "MU8888";
        String flightDateFrom = "2022-11-12";
        String flightDateTo = "2022-11-12";
        resp = (ResponseEntity<ResponseVo>) controller.getWhiteListDetail(dep, arr, flightNo, flightDateFrom, flightDateTo, 1, 20);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode() && resp.getBody().toString().contains("Y_RATIO=0.8, C_RATIO=0.8"), resp.toString());
    }

    @Test
    @Order(19)
    public void testBatchSetWhiteListDetail() {
        String params = "[{\"flightNo\":\"MU8888\", \"dep\":\"CTU\", \"arr\":\"PEK\", \"flightDate\":\"2022-11-12\", \"enabled\":1, \"y_ratio\":0.83, \"c_ratio\":0.84, \"forecastTime\":\"2022-11-14 10:21:00\"}]";
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.batchSetWhiteListDetail(params);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode(), resp.toString());

        String dep = null;
        String arr = null;
        String flightNo = "MU8888";
        String flightDateFrom = "2022-11-12";
        String flightDateTo = "2022-11-12";
        resp = (ResponseEntity<ResponseVo>) controller.getWhiteListDetail(dep, arr, flightNo, flightDateFrom, flightDateTo, 1, 20);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode() && resp.getBody().toString().contains("Y_RATIO=0.83, C_RATIO=0.84"), resp.toString());
    }

    @Test
    @Order(30)
    public void testGetWYRatioList1() {
        String flightNo = null;
        String matchMethod = null;
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.getWYRatioList(flightNo, matchMethod, 1, 20);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode(), resp.toString());
    }

    @Test
    @Order(31)
    public void testCreateWYRatio() {
        // 参数缺乏
        String params = "{\"flightNo\":\"MU3102\", \"eline\": \"CTUPEK\", \"fromDate\":\"2022-11-17\", \"toDate\":\"2022-11-20\", " +
                "\"peerFlightNo\":\"3U6608\", \"peerEline\": \"CTUPEK\", \"peerFromDate\":\"2022-11-17\", \"peerToDate\":\"2022-11-20\", \"matchMethod\":\"DOW\", \"memo\":\"test\"}";
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.createWYRatio(params);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode(), resp.toString());
    }

    @Test
    @Order(32)
    public void testGetWYRatioList2() {
        String flightNo = null;
        String matchMethod = "";
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.getWYRatioList(flightNo, matchMethod, 1, 20);
        System.out.println(resp.toString());
        assertTrue(resp.toString().contains("MU3102"), resp.toString());
    }

    @Test
    @Order(33)
    public void testUpdateWYRatio() {
        String params = "{\"id\":1, \"fromDate\":\"2022-11-14\", \"toDate\":\"2022-11-18\", " +
                "\"peerFlightNo\":\"3U6308\", \"peerEline\": \"CTUPEK\", \"peerFromDate\":\"2022-11-14\", \"peerToDate\":\"2022-11-18\", \"matchMethod\":\"DOW\", \"memo\":\"test\"}";
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.updateWYRatio(params);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode(), resp.toString());

        String flightNo = null;
        String matchMethod = "";
        resp = (ResponseEntity<ResponseVo>) controller.getWYRatioList(flightNo, matchMethod, 1, 20);
        System.out.println(resp.toString());
        assertTrue(resp.toString().contains("PEER_FLIGHT_DATE_FROM=2022-11-14, PEER_FLIGHT_DATE_TO=2022-11-18"), resp.toString());
    }

    @Test
    @Order(34)
    public void testDeleteWYRatio() {
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.deleteWYRatio(1);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode(), resp.toString());

        String flightNo = null;
        String matchMethod = "";
        resp = (ResponseEntity<ResponseVo>) controller.getWYRatioList(flightNo, matchMethod, 1, 20);
        System.out.println(resp.toString());
        assertTrue(resp.toString().contains("[]"), resp.toString());
    }

    @Test
    @Order(40)
    public void testGetHolidayList() {
        String memo = null;
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.getHolidayList(memo, 1, 20);
        System.out.println(resp.toString());
        assertTrue(resp.toString().contains("[]"), resp.toString());
    }

    @Test
    @Order(41)
    public void testCreateHoliday() {
        String params = "{\"excludeEline\":\"CTUPEK,XIYCAN\", \"eline\": \"*\", \"fromDate\":\"2022-10-01\", \"toDate\":\"2022-10-07\", \"overflowHead\":1, \"overflowTail\":1, \"memo\":\"2022年国庆节\"}";
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.createHoliday(params);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode(), resp.toString());

        String memo = null;
        resp = (ResponseEntity<ResponseVo>) controller.getHolidayList(memo, 1, 20);
        System.out.println(resp.toString());
        assertTrue(resp.toString().contains("SEQ=8"), resp.toString());
    }

    @Test
    @Order(42)
    public void testUpdateHoliday1() {
        String params = "{\"id\":10, \"excludeEline\":\"CTUPEK,XIYCAN\", \"eline\": \"*\", \"fromDate\":\"2022-10-01\", \"toDate\":\"2022-10-07\", \"overflowHead\":1, \"overflowTail\": 1, \"memo\":\"2022年国庆节\"}";
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.updateHoliday(params);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.PARAM_INVALID.getCode(), resp.toString());
    }

    @Test
    @Order(43)
    public void testUpdateHoliday2() {
        String params = "{\"id\":1, \"excludeEline\":\"XIYCAN\", \"eline\": \"*\", \"fromDate\":\"2022-10-01\", \"toDate\":\"2022-10-07\", \"overflowHead\":1, \"overflowTail\":0, \"memo\":\"2022年国庆节\"}";
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.updateHoliday(params);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.SUCCESS.getCode(), resp.toString());

        String memo = null;
        resp = (ResponseEntity<ResponseVo>) controller.getHolidayList(memo, 1, 20);
        System.out.println(resp.toString());
        assertTrue(!resp.toString().contains("SEQ=8"), resp.toString());
    }

    @Test
    @Order(44)
    public void testGetHolidayInfo1() {
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.getHolidayInfo(10);
        System.out.println(resp.toString());
        assertTrue(resp.getBody().getCode() == CommonCodeEnum.PARAM_INVALID.getCode(), resp.toString());
    }

    @Test
    @Order(45)
    public void testGetHolidayInfo2() {
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.getHolidayInfo(1);
        System.out.println(resp.toString());
        assertTrue(resp.toString().contains("fromDate=2022-10-01, overflowHead=1, toDate=2022-10-07"), resp.toString());
    }

    @Test
    @Order(46)
    public void testDeleteHoliday1() {
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.deleteHoliday(100);
        System.out.println(resp.toString());
        assertTrue(resp.toString().contains("fromDate=2022-10-01, overflowHead=1, toDate=2022-10-07"), resp.toString());
    }

    @Test
    @Order(47)
    public void testDeleteHoliday2() {
        ResponseEntity<ResponseVo> resp = (ResponseEntity<ResponseVo>) controller.deleteHoliday(1);
        System.out.println(resp.toString());
        assertTrue(resp.toString().contains("fromDate=2022-10-01, overflowHead=1, toDate=2022-10-07"), resp.toString());
    }
}
