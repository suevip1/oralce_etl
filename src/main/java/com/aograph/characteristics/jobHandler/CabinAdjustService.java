package com.aograph.characteristics.jobHandler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aograph.characteristics.utils.AirlinkConst;
import com.aograph.characteristics.utils.DateTimeHelper;
import com.aograph.characteristics.utils.SpringContextUtil;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class CabinAdjustService {
    private String invLeg = "ORG_INV_LEG";
    private String invSeg = "ORG_INV_SEGMENT";

    private String fdlTable = "ANALY_AOGRAPH_AIR_FOR_MODEL";

    @Value("${cabin.adjust.ignore}")
    private String cabinIgnore;        // 这些舱不需要处理

    @Resource(name = "oracleDataSource")
    private DataSource dataSource;

    @Resource
    private TravelSkyEtl travelSkyEtl;

    private void initCabins() {
    }

    // 生成调舱对象
    public Adjust createForecastAdjust(String flightNo, String flightDate, String dep, String arr, String detail, String forecastTime) {
        initCabins();

        Adjust adjust = new Adjust();
        adjust.setFlightNo(flightNo);
        adjust.setDep(dep);
        adjust.setArr(arr);
        adjust.setFlightDate(flightDate);
        adjust.setForecastTime(DateTimeHelper.getDate(forecastTime, AirlinkConst.TIME_DATE_FORMAT));

        List<Leg> legs = new ArrayList<>();
        List<Seg> segs = new ArrayList<>();

        JSONArray forcastCabins = JSON.parseArray(detail);

        for (int i = 0 ; i < forcastCabins.size() ; i++) {
            JSONObject forcastCabin = forcastCabins.getJSONObject(i);

            Seg seg = new Seg();
            seg.setDep(dep);
            seg.setArr(arr);
            seg.setCls(((String)forcastCabin.get("code")).substring(0, 1));
            seg.setLsv(forcastCabin.get("cap").toString());
            seg.setOpen("Y");
            segs.add(seg);
        }

        adjust.setLegs(legs);
        adjust.setSegs(segs);

        return adjust;
    }

    // 收舱：将当前舱位以下舱位（明折明扣、特价舱，产品舱不需要）进行锁舱
    // 放舱：将当前舱位以上舱位进行解锁舱，并投放合适数量，并进行嵌套。
    // 销售舱位嵌套后的库存数量之和必须小于(MAX-BKD之和)
    // 舱位投放数量不可以为0（Y舱除外）
    public List<String> createAdjustCabinCommands(Adjust adjust, String cabins, Map<String, Object> ro, boolean optimize) {
        initCabins();

        List<String> dataList = new ArrayList<>();

        if (cabins == null || ro == null) {
            return dataList;
        }

        String flightDate = adjust.getFlightDate();
        String flight = adjust.getFlightNo();

        String exTime = DateTimeHelper.date2String(new Date(), "yyyy-MM-dd HH:mm:ss")+("."+System.nanoTime()).substring(0, 7);
        // 对于同一航班，每行前半部分都一样
        String line = flight.substring(0, 2)+","+flight+","+adjust.getDep()+","+adjust.getArr()+","+flightDate+","+
                exTime.substring(0, exTime.indexOf(" "))+","+exTime.substring(exTime.indexOf(" ")+1)+","+
                DateTimeHelper.daysBetween(new Date(), DateTimeHelper.getDate(flightDate, AirlinkConst.TIME_DATE_FORMAT))+",,,";

        if (adjust.getSegs() != null && adjust.getSegs().size() > 0) {
            // cabins格式为JCDIO/WS/YPBMHKUALQEVZTNRGX
            List<Seg> all = new ArrayList<>(adjust.getSegs());
            for (String bigCabin : cabins.split("/")) {
                // 各舱等分别处理
                changeAdjustByRo(
                        ro,
                        all,
                        adjust.getSegs(),
                        bigCabin,
                        null, optimize);
            }

            // 生成调舱数据
            for (Seg seg : all) {
                dataList.add(line + seg.getCls() + "," + ("#".equals(seg.getLsv()) ? "0" : seg.getLsv()) + "," + ("Y".equals(seg.getOpen()) ? 1 : 0));
            }
        }

        return dataList;
    }

    // 销售舱位嵌套后的库存数量之和必须小于(MAX-BKD之和)
    public List<Seg> changeAdjustByRo(Map<String, Object> ro, List<Seg> all, List<Seg> segs, String cabins, String forecastCabin, boolean optimize) {
        if (ro == null) {
            return all;
        }

        initCabins();

        // 只处理本舱等的数据
        segs = segs.stream().filter(x -> cabins.contains(x.getCls())).
                sorted((x, y) -> Integer.valueOf(cabins.indexOf(y.getCls())).compareTo(Integer.valueOf(cabins.indexOf(x.getCls())))).
                collect(Collectors.toList());

        // 各舱等可以单独设置阈值
        int minDiff = SpringContextUtil.getInt("cabin.adjust."+cabins.substring(0, 1)+".min_diff", 0);
        // 如果最新的RO和要调整的数差别太小，忽略它
        if (optimize && minDiff > 0) {
            for (int i = segs.size() - 1; i >= 0; i--) {
                Seg seg = segs.get(i);
                String ind = (String) ro.get("seg_c" + seg.getCls().toLowerCase() + "_ind");
                String bkdStr = (String) ro.get("seg_c" + seg.getCls().toLowerCase() + "_bkd");
                String lsvStr = (String) ro.get("seg_c" + seg.getCls().toLowerCase() + "_lsv");
                if (ind == null) {
                    segs.remove(i);
                    all.remove(seg);
                } else if (!ind.contains("P") && !"#".equals(lsvStr) && !"-".equals(lsvStr)) {
                    int lsv = Integer.parseInt(lsvStr);
                    int bkd = Integer.parseInt(bkdStr);
                    if (Math.abs(lsv - Double.parseDouble(seg.getLsv()) - bkd) <= minDiff) {
                        segs.remove(i);
                        all.remove(seg);
                    }
                }
            }
        }

        if (segs.size() == 0) {
            return all;
        }

        // 那些需要调舱的舱位
        List<String> lsvCls = segs.stream().filter(x -> StringUtils.isNotBlank(x.getLsv()) && cabins.contains(x.getCls())).map(x -> x.getCls()).collect(Collectors.toList());

        for (Seg seg : segs) {
            if (cabinIgnore.contains(seg.getCls())) {
                continue;
            }

            if ("Y".equals(seg.getOpen())) {
                int bkd = 0;
                try {
                    bkd = Integer.parseInt((String) ro.get("seg_c" + seg.getCls().toLowerCase() + "_bkd"));
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
                seg.setLsv(Integer.toString(bkd + Integer.parseInt(seg.getLsv())));
            }
        }

        // 放舱：将当前舱位以上舱位进行解锁舱，并投放合适数量，并进行嵌套。
        // 舱位投放数量不可以为0（Y舱除外）
        if (lsvCls.size() > 0) {
            for (int i = cabins.indexOf(lsvCls.get(lsvCls.size() - 1)); i > 0; i--) {
                String cls = cabins.substring(i, i + 1);
                if ("Y".equals(cls) || lsvCls.contains(cls) || cabinIgnore.contains(cls)) {
                    continue;
                }

                Seg seg = all.stream().filter(x -> cls.equals(x.getCls())).findFirst().orElse(null);
                if (seg == null) {
                    seg = new Seg();
                    seg.setDep(segs.get(0).getDep());
                    seg.setArr(segs.get(0).getArr());
                    seg.setCls(cls);
                    all.add(seg);
                }

                seg.setOpen("Y");
                String lsv = StringUtils.defaultString((String) ro.get("seg_c" + cls.toLowerCase() + "_lsv"), "0");
                String ind = StringUtils.defaultString((String) ro.get("seg_c" + cls.toLowerCase() + "_ind"), "");
                int bkd = 0;
                try {
                    bkd = Integer.parseInt((String) ro.get("seg_c" + cls.toLowerCase() + "_bkd"));
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
                if (ind.contains("P") || lsv.equals("#")) {     // 如果原为锁舱
                    if (bkd == 0) {
                        bkd = 1;    // 最小为1, 不能为0
                    }
                    seg.setLsv(Integer.toString(bkd));
                } else {
                    seg.setLsv(lsv);
                }
            }

            // 收舱：将当前舱位以下舱位（明折明扣、特价舱位，产品舱不需要）进行锁舱
            // 如果有舱位需锁舱，且其上舱位都设有嵌套，需要将其bkd加到本舱位上
            for (int i = cabins.indexOf(lsvCls.get(0)) + 1; i < cabins.length(); i++) {
                String cls = cabins.substring(i, i + 1);
                if (cabinIgnore.contains(cls)) {
                    continue;
                }

                String ind = (String) ro.get("seg_c" + cls.toLowerCase() + "_ind");
                Seg seg = all.stream().filter(x -> cls.equals(x.getCls())).findFirst().orElse(null);
                if (seg == null) {
                    seg = new Seg();
                    all.add(seg);

                    seg.setDep(segs.get(0).getDep());
                    seg.setArr(segs.get(0).getArr());
                    seg.setCls(cls);
                    seg.setLsv((String) ro.get("seg_c" + cls.toLowerCase() + "_lsv"));
                }

                seg.setOpen("N");
                seg.setLsv("#");
            }

            // 多个舱位，舱位间的舱位（明折明扣、特价舱位，产品舱不需要）进行开舱
            for (int i = 0; i < lsvCls.size() - 1; i++) {
                String cls1 = lsvCls.get(i);
                String cls2 = lsvCls.get(i + 1);
                int p1 = cabins.indexOf(cls1);
                int p2 = cabins.indexOf(cls2);
                if (p1 == p2 + 1) {
                    continue;
                }

                for (int p = p2 + 1; p < p1; p++) {
                    String cls = cabins.substring(p, p + 1);
                    if (cabinIgnore.contains(cls)) {
                        continue;
                    }

                    String ind = StringUtils.defaultString((String) ro.get("seg_c" + cls.toLowerCase() + "_ind"), "");
                    String lsv = StringUtils.defaultString((String) ro.get("seg_c" + cls.toLowerCase() + "_lsv"), "0");

                    Seg seg = all.stream().filter(x -> cls.equals(x.getCls())).findFirst().orElse(null);
                    if (seg == null) {
                        seg = new Seg();
                        all.add(seg);

                        seg.setDep(segs.get(0).getDep());
                        seg.setArr(segs.get(0).getArr());
                        seg.setCls(cls);
                        seg.setLsv(lsv);
                    }

                    seg.setOpen("Y");
                    int bkd = 0;
                    try {
                        bkd = Integer.parseInt(StringUtils.defaultString((String) ro.get("seg_c" + cls.toLowerCase() + "_bkd"), "0"));
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                    }
                    if (ind.contains("P") || lsv.equals("#")) {     // 如果原为锁舱
                        if (bkd == 0) {
                            bkd = 1;    // 最小为1, 不能为0
                        }
                        seg.setLsv(Integer.toString(bkd));
                    } else {
                        seg.setLsv(lsv);
                    }
                }
            }

            // 预测舱位是G(T)这种联动舱位
            if (forecastCabin != null && forecastCabin.endsWith(")")) {
                String mainCabin = forecastCabin.substring(0, 1);
                String followingCabin = forecastCabin.substring(forecastCabin.indexOf("(") + 1, forecastCabin.length() - 1);
                // 这两个舱位间的要关舱
                for (int i = cabins.indexOf(followingCabin) + 1; i < cabins.indexOf(mainCabin); i++) {
                    String cls = cabins.substring(i, i + 1);
                    if (cabinIgnore.contains(cls)) {
                        continue;
                    }

                    String ind = StringUtils.defaultString((String) ro.get("seg_c" + cls.toLowerCase() + "_ind"), "");
                    String lsv = StringUtils.defaultString((String) ro.get("seg_c" + cls.toLowerCase() + "_lsv"), "0");

                    Seg seg = all.stream().filter(x -> cls.equals(x.getCls())).findFirst().orElse(null);
                    if (seg == null) {
                        seg = new Seg();
                        all.add(seg);

                        seg.setDep(segs.get(0).getDep());
                        seg.setArr(segs.get(0).getArr());
                        seg.setCls(cls);
                        seg.setLsv(lsv);
                    }

                    seg.setOpen("N");
                    if (!"#".equals(lsv)) {
                        seg.setLsv("#");
                    }
                }

                // 被跟随的舱位要开舱
                Seg seg = all.stream().filter(x -> followingCabin.equals(x.getCls())).findFirst().orElse(null);
                if (seg == null) {
                    seg = new Seg();
                    all.add(seg);

                    seg.setDep(segs.get(0).getDep());
                    seg.setArr(segs.get(0).getArr());
                    seg.setCls(followingCabin);
                    seg.setLsv((String) ro.get("seg_c" + followingCabin.toLowerCase() + "_lsv"));
                }
                seg.setOpen("Y");
                seg.setLsv("10");
            }
        }

        // 锁舱的没必要嵌套了
        int size = all.size();
        for (int idx = 0; idx < size ; idx++) {
            Seg seg = all.get(idx);

            if ("N".equals(seg.getOpen())) {
                seg.setLsv("#");
            }

            // 原为嵌套且开放的舱位，现需要锁舱的，需要把bkd加到前面的现为嵌套的且开放的舱位中
            String ind = StringUtils.defaultString((String) ro.get("seg_c" + seg.getCls().toLowerCase() + "_ind"), "");
            String lsv = StringUtils.defaultString((String) ro.get("seg_c" + seg.getCls().toLowerCase() + "_lsv"), "0");
            int bkd = Integer.parseInt(StringUtils.defaultString((String) ro.get("seg_c" + seg.getCls().toLowerCase() + "_bkd"), "0"));
            if (bkd > 0 && ind.contains("L") && (!ind.contains("P") && !"#".equals(lsv)) && "N".equals(seg.getOpen())) {
                // 寻找前面的嵌套的且开放的舱位，将bkd加上去
                for (int i = cabins.indexOf(seg.getCls()) - 1 ; i > 0 ; i--) {
                    String _cls = cabins.substring(i, i+1);
                    String _ind = StringUtils.defaultString((String) ro.get("seg_c" + _cls.toLowerCase() + "_ind"), "");
                    String _lsv = StringUtils.defaultString((String) ro.get("seg_c" + _cls.toLowerCase() + "_lsv"), "0");
                    Seg _seg = all.stream().filter(x -> x.getCls().equals(_cls)).distinct().findFirst().orElse(null);
                    if (_seg == null) {
                        // 嵌套且开放
                        if (_ind.contains("L") && (!_ind.contains("P") && !"#".equals(_lsv))) {
                            seg = new Seg();
                            all.add(seg);

                            seg.setDep(segs.get(0).getDep());
                            seg.setArr(segs.get(0).getArr());
                            seg.setCls(_cls);
                            seg.setOpen("Y");
                            seg.setLsv(Integer.toString(Integer.parseInt(_lsv) + bkd));
                            break;
                        }
                    } else if ("Y".equals(_seg.getOpen())){
                        _seg.setLsv(Integer.toString(Integer.parseInt(_seg.getLsv()) + bkd));
                        break;
                    }
                }
            }
        }

        // 删掉多余的调舱舱位
        for (int i = all.size() - 1 ; i >= 0 ; i--) {
            Seg seg = all.get(i);
            String ind = StringUtils.defaultString((String) ro.get("seg_c"+seg.getCls().toLowerCase()+"_ind"), "");
            String lsv = StringUtils.defaultString((String) ro.get("seg_c"+seg.getCls().toLowerCase()+"_lsv"), "0");

            boolean isSame = false;
            // 原来就是锁舱，现在还是锁舱的
            if ("N".equals(seg.getOpen()) && (ind.contains("P") || "#".equals(lsv))) {
                isSame = true;
            }
            // 原来就是开舱，且lsv都一样的
            if ("Y".equals(seg.getOpen()) && !ind.contains("P") && seg.getLsv().equals(lsv)) {
                isSame = true;
            }
            if (isSame) {
                all.remove(i);
            }
        }

        return all;
    }

    public String queryMaxForecastDate(int from, int to){
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);
//        String sql = "select max(FORECAST_TIME) from PREDICT_FLIGHT_HE_FORECAST where to_date(FORECAST_TIME)=to_date(sysdate) and status!=0";
        String sql = "select max(FORECAST_TIME) from PREDICT_FLIGHT_HE_FORECAST where FLIGHT_DATE - trunc(SYSDATE) between "+from+" and "+to;
        return template.queryForObject(sql,String.class);
    }

    public List<Map<String, Object>> queryFlights(String forecastDate, int from, int to){
        if(StringUtils.isBlank(forecastDate)){
            return null;
        }

        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);
        String sql = "select distinct fltno, flight_date, dep, arr, ALLOCATE_DETAIL, C_ALLOCATE_DETAIL, F_ALLOCATE_DETAIL " +
                "from PREDICT_FLIGHT_HE_FORECAST where forecast_time=? and FLIGHT_DATE - trunc(SYSDATE) between "+from+" and "+to;
        //查询全部规则
        List<Map<String, Object>> list = template.queryForList(sql, new Timestamp(DateTimeHelper.getDate(forecastDate, AirlinkConst.TIME_FULL_FORMAT).getTime()));
        return list;
    }

    public static class Adjust {
        @JsonFormat(pattern = AirlinkConst.TIME_FULL_FORMAT)
        private Date forecastTime;
        private String flightNo;
        private String flightDate;
        private String dep;
        private String arr;
        private List<Leg> legs;
        private List<Seg> segs;

        public Date getForecastTime() {
            return forecastTime;
        }

        public void setForecastTime(Date forecastTime) {
            this.forecastTime = forecastTime;
        }

        public String getFlightNo() {
            return flightNo;
        }

        public void setFlightNo(String flightNo) {
            this.flightNo = flightNo;
        }

        public String getFlightDate() {
            return flightDate;
        }

        public void setFlightDate(String flightDate) {
            this.flightDate = flightDate;
        }

        public String getDep() {
            return dep;
        }

        public void setDep(String dep) {
            this.dep = dep;
        }

        public String getArr() {
            return arr;
        }

        public void setArr(String arr) {
            this.arr = arr;
        }

        public List<Leg> getLegs() {
            return legs;
        }

        public void setLegs(List<Leg> legs) {
            this.legs = legs;
        }

        public List<Seg> getSegs() {
            return segs;
        }

        public void setSegs(List<Seg> segs) {
            this.segs = segs;
        }
    }

    public static class Leg {
        private String dep;
        private String arr;
        private String cls;
        private int max;

        public String getDep() {
            return dep;
        }

        public void setDep(String dep) {
            this.dep = dep;
        }

        public String getArr() {
            return arr;
        }

        public void setArr(String arr) {
            this.arr = arr;
        }

        public String getCls() {
            return cls;
        }

        public void setCls(String cls) {
            this.cls = cls;
        }

        public int getMax() {
            return max;
        }

        public void setMax(int max) {
            this.max = max;
        }
    }

    public static class Seg {
        private String dep;
        private String arr;
        private String cls;
        private String lsv;
        private String open;    // Y/N

        public String getDep() {
            return dep;
        }

        public void setDep(String dep) {
            this.dep = dep;
        }

        public String getArr() {
            return arr;
        }

        public void setArr(String arr) {
            this.arr = arr;
        }

        public String getCls() {
            return cls;
        }

        public void setCls(String cls) {
            this.cls = cls;
        }

        public String getLsv() {
            return lsv;
        }

        public void setLsv(String lsv) {
            this.lsv = lsv;
        }

        public String getOpen() {
            return open;
        }

        public void setOpen(String open) {
            this.open = open;
        }
    }
}
