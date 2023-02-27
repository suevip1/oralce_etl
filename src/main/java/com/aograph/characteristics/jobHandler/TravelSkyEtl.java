package com.aograph.characteristics.jobHandler;

import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
@Slf4j
public class TravelSkyEtl {
    private static final Logger logger = LoggerFactory.getLogger(TravelSkyEtl.class);

    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    private static SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static Map<String, String> invSegmentColumns = new HashMap<>();     // 映射到字段名
    private static Map<String, String> invLegColumns = new HashMap<>();         // 映射到字段名
    static {
        initInvSegmentNames();
        initInvLegNames();
    }

    public void insertInvData(JdbcTemplate template, List<Map<String, Object>> legs, List<Map<String, Object>> segs) {
        // 批量插入
        for (int i = 0 ; i < legs.size() ; i += 10000) {
            insertData(template, "ORG_INV_LEG", legs.subList(i, Math.min(i + 10000, legs.size())), invLegColumns);
        }

        // 批量插入
        for (int i = 0 ; i < segs.size() ; i += 10000) {
            insertData(template, "ORG_INV_SEGMENT", segs.subList(i, Math.min(i + 10000, segs.size())), invSegmentColumns);
        }
    }

    private void insertData(JdbcTemplate template, String table, List<Map<String, Object>> data, Map<String, String> columns) {
        List<String> names = new ArrayList<>(columns.keySet());
        StringBuffer sb = new StringBuffer("insert into ").append(table).append( "(ID");
        for (int i = 0; i < names.size(); i++) {
            sb.append(", \"").append(columns.get(names.get(i)).toUpperCase()).append("\"");
        }
        sb.append(") values ("+table+"_SEQ.nextval,");
        for (int i = 0; i < names.size(); i++) {
            sb.append("?,");
        }
        sb.setLength(sb.length() -1);
        sb.append(")");

        SimpleDateFormat format2 = new SimpleDateFormat("yyyy-MM-dd");
        template.batchUpdate(sb.toString(), new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement stat, int i) throws SQLException {
                Map<String, Object> m = data.get(i);
                int k = 1;
                for (int n = 0; n < names.size(); n++) {
                    String name = names.get(n);
                    if (m.get(name) instanceof java.sql.Date) {
                        stat.setDate(k++, (java.sql.Date)m.get(name));
                    } else if (m.get(name) instanceof java.sql.Timestamp) {
                        stat.setTimestamp(k++, (java.sql.Timestamp)m.get(name));
                    } else {
                        String val = m.get(name) == null ? null : m.get(name).toString();
                        if (val == null || "null".equals(val)) {
                            val = "";
                        }
                        stat.setString(k++, val);
                    }
                }
            }

            @Override
            public int getBatchSize() {
                return data.size();
            }
        });
    }

    // 初始化leg表字段名
    private static void initInvLegNames() {
        // 头信息
//        invLegColumns.put("ID", "ID");
        invLegColumns.put("file", "file");              // CSV文件名
        invLegColumns.put("hdr-SNDR", "sndr");
        invLegColumns.put("hdr-VER", "ver");
        invLegColumns.put("hdr-DTTM", "dttm");
        invLegColumns.put("hdr-NEWVAL", "newval");
        invLegColumns.put("hdr-OLDVAL", "oldval");
        invLegColumns.put("hdr-EVENT", "event");
        invLegColumns.put("hdr-CHANGETYPE", "changetype");
        invLegColumns.put("hdr-STAMP", "stamp");
        invLegColumns.put("hdr-FLAG", "flag");
        invLegColumns.put("hdr-SEQNUM", "seqnum");

        invLegColumns.put("AirlineCode", "airline_code");   // 航司编号
        invLegColumns.put("FlightNumber", "flight_number"); // 航班号
        invLegColumns.put("Suffix", "suffix");              // 航班后缀
//        invLegColumns.put("Date", "date");
        invLegColumns.put("SubControlOffice", "sub_control_office");
        invLegColumns.put("ControlOffice", "control_office");
        invLegColumns.put("OriginAirport", "origin_airport");
        invLegColumns.put("Type", "type");
        invLegColumns.put("CodeShareType", "code_share_type");
        invLegColumns.put("BorrowMethod", "borrow_method");

        invLegColumns.put("carrier-AirlineCode", "carrier_airline_code");   // 承运商航司编号
        invLegColumns.put("carrier-FlightNumber", "carrier_flight_number"); // 承运商航班号
        invLegColumns.put("carrier-Suffix", "carrier_suffix");              // 承运商航班后缀

        invLegColumns.put("leg-CarrierCode", "leg_carrier_code");
        invLegColumns.put("leg-FlightNumber", "leg_flight_number");
        invLegColumns.put("leg-Suffix", "leg_suffix");
        invLegColumns.put("leg-LegMiles", "leg_miles");

        invLegColumns.put("leg-Equipment", "leg_equip");                    // 飞机型号
        invLegColumns.put("leg-ScheduledDeparture-AirportCode", "leg_dep");
        invLegColumns.put("leg-ScheduledDeparture-DepartureDate", "leg_dep_date");
        invLegColumns.put("leg-ScheduledDeparture-Time", "leg_dep_time");
        invLegColumns.put("leg-ScheduledDeparture-ChangeOfDay", "leg_dep_change_of_day");
        invLegColumns.put("leg-ScheduledArrival-AirportCode", "leg_arr");
        invLegColumns.put("leg-ScheduledArrival-ArrivalDate", "leg_arr_date");
        invLegColumns.put("leg-ScheduledArrival-Time", "leg_arr_time");
        invLegColumns.put("leg-ScheduledArrival-ChangeOfDay", "leg_arr_change_of_day");

        // 序号为舱等序号
        for (int i = 0 ; i < 6 ; i++) {
            invLegColumns.put("leg-cabin"+i+"-class", "leg_c"+i+"_clz");
            invLegColumns.put("leg-cabin"+i+"-CAP", "leg_c"+i+"_cap");
            invLegColumns.put("leg-cabin"+i+"-OPN", "leg_c"+i+"_opn");
            invLegColumns.put("leg-cabin"+i+"-MAX", "leg_c"+i+"_max");
            invLegColumns.put("leg-cabin"+i+"-TB", "leg_c"+i+"_tb");
            invLegColumns.put("leg-cabin"+i+"-GRO", "leg_c"+i+"_gro");
            invLegColumns.put("leg-cabin"+i+"-GRS", "leg_c"+i+"_grs");
            invLegColumns.put("leg-cabin"+i+"-BLK", "leg_c"+i+"_blk");
            invLegColumns.put("leg-cabin"+i+"-AV", "leg_c"+i+"_av");
            invLegColumns.put("leg-cabin"+i+"-NoShow", "leg_c"+i+"_no_show");
            invLegColumns.put("leg-cabin"+i+"-GoShow", "leg_c"+i+"_go_show");
            invLegColumns.put("leg-cabin"+i+"-IND", "leg_c"+i+"_ind");
            invLegColumns.put("leg-cabin"+i+"-PCF", "leg_c"+i+"_pcf");
            invLegColumns.put("leg-cabin"+i+"-SMT", "leg_c"+i+"_smt");
            invLegColumns.put("leg-cabin"+i+"-CT", "leg_c"+i+"_ct");
            invLegColumns.put("leg-cabin"+i+"-CNC", "leg_c"+i+"_cnc");
            invLegColumns.put("leg-cabin"+i+"-ClassOfService", "leg_c"+i+"_cos");
            invLegColumns.put("leg-cabin"+i+"-GT", "leg_c"+i+"_gt");
            invLegColumns.put("leg-cabin"+i+"-LT", "leg_c"+i+"_lt");
            invLegColumns.put("leg-cabin"+i+"-LSS", "leg_c"+i+"_lss");
            invLegColumns.put("leg-cabin"+i+"-PT", "leg_c"+i+"_pt");
            invLegColumns.put("leg-cabin"+i+"-AT", "leg_c"+i+"_at");
        }

        // 各子舱
        for (char c = 'A' ; c <= 'Z' ; c++) {
            String sc = Character.toString(c);
            invLegColumns.put("leg-cabin"+sc+"-Ls", "leg_c"+sc.toUpperCase()+"_ls");
            invLegColumns.put("leg-cabin"+sc+"-Opn", "leg_c"+sc.toUpperCase()+"_opn");
            invLegColumns.put("leg-cabin"+sc+"-Lsn", "leg_c"+sc.toUpperCase()+"_lsn");
        }
    }

    // 初始化seg表字段名
    private static void initInvSegmentNames() {
        // 头信息
//        invSegmentColumns.put("ID", "ID");
        invSegmentColumns.put("file", "file");              // CSV文件名
        invSegmentColumns.put("hdr-SNDR", "sndr");
        invSegmentColumns.put("hdr-VER", "ver");
        invSegmentColumns.put("hdr-DTTM", "dttm");
        invSegmentColumns.put("hdr-NEWVAL", "newval");
        invSegmentColumns.put("hdr-OLDVAL", "oldval");
        invSegmentColumns.put("hdr-EVENT", "event");
        invSegmentColumns.put("hdr-CHANGETYPE", "changetype");
        invSegmentColumns.put("hdr-STAMP", "stamp");
        invSegmentColumns.put("hdr-FLAG", "flag");
        invSegmentColumns.put("hdr-SEQNUM", "seqnum");

        invSegmentColumns.put("AirlineCode", "airline_code");               // 航司编号
        invSegmentColumns.put("FlightNumber", "flight_number");             // 航班号
        invSegmentColumns.put("Suffix", "suffix");                          // 航班后缀
//        invSegmentColumns.put("Date", "date");
        invSegmentColumns.put("SubControlOffice", "sub_control_office");
        invSegmentColumns.put("ControlOffice", "control_office");
        invSegmentColumns.put("OriginAirport", "origin_airport");
        invSegmentColumns.put("Type", "type");
        invSegmentColumns.put("CodeShareType", "code_share_type");
        invSegmentColumns.put("BorrowMethod", "borrow_method");

        // 承运商航班
        invSegmentColumns.put("carrier-AirlineCode", "carrier_airline_code");       // 承运商航司编号
        invSegmentColumns.put("carrier-FlightNumber", "carrier_flight_number");     // 承运商航班号
        invSegmentColumns.put("carrier-Suffix", "carrier_suffix");                  // 承运商航班后缀

        invSegmentColumns.put("segment-OriginAirport", "seg_dep");
        invSegmentColumns.put("segment-DestinationAirport", "seg_arr");
        invSegmentColumns.put("segment-DepartureDate", "seg_dep_date");
        invSegmentColumns.put("segment-DepIndex", "seg_dep_index");
        invSegmentColumns.put("segment-ArrIndex", "seg_arr_index");
        invSegmentColumns.put("segment-FlyTime", "seg_flytime");
        invSegmentColumns.put("segment-eline", "seg_eline");
        invSegmentColumns.put("segment-Distance", "seg_distance");
        invSegmentColumns.put("segment-Max", "seg_max");

        // 序号为舱等序号
        for (int i = 0 ; i < 6 ; i++) {
            invSegmentColumns.put("segment-cabin"+i+"-class", "seg_c"+i+"_clz");
            invSegmentColumns.put("segment-cabin"+i+"-SMT", "seg_c"+i+"_smt");
            invSegmentColumns.put("segment-cabin"+i+"-ClassOfService", "seg_c"+i+"_cos");
        }

        // 各子舱
        for (char c = 'A' ; c <= 'Z' ; c++) {
            String sc = Character.toString(c);
            invSegmentColumns.put("segment-cabin"+sc+"-BKD", "seg_c"+sc.toLowerCase()+"_bkd");                  // 售票量
            invSegmentColumns.put("segment-cabin"+sc+"-GRS", "seg_c"+sc.toLowerCase()+"_grs");                  // 团体订座数
            invSegmentColumns.put("segment-cabin"+sc+"-BLK", "seg_c"+sc.toLowerCase()+"_blk");                  // 锁定座位数
            invSegmentColumns.put("segment-cabin"+sc+"-WL", "seg_c"+sc.toLowerCase()+"_wl");                    // 候补旅客数
            invSegmentColumns.put("segment-cabin"+sc+"-LSV", "seg_c"+sc.toLowerCase()+"_lsv");                  // 限制销售座位数
            invSegmentColumns.put("segment-cabin"+sc+"-LSN", "seg_c"+sc.toLowerCase()+"_lsn");                  // 是否被关舱
            invSegmentColumns.put("segment-cabin"+sc+"-LSS", "seg_c"+sc.toLowerCase()+"_lss");                  // 限制销售组合后，还可销售数
            invSegmentColumns.put("segment-cabin"+sc+"-LT", "seg_c"+sc.toLowerCase()+"_lt");                    // 限制销售表号
            invSegmentColumns.put("segment-cabin"+sc+"-NC", "seg_c"+sc.toLowerCase()+"_nc");                    // 嵌套子舱
            invSegmentColumns.put("segment-cabin"+sc+"-NoShow", "seg_c"+sc.toLowerCase()+"_noshow");
            invSegmentColumns.put("segment-cabin"+sc+"-GoShow", "seg_c"+sc.toLowerCase()+"_goshow");
            invSegmentColumns.put("segment-cabin"+sc+"-IND", "seg_c"+sc.toLowerCase()+"_ind");
            invSegmentColumns.put("segment-cabin"+sc+"-Avail", "seg_c"+sc.toLowerCase()+"_avail");              // 剩余可售
            invSegmentColumns.put("segment-cabin"+sc+"-AvailStatus", "seg_c"+sc.toLowerCase()+"_avail_stat");   // 剩余可售状态
        }
    }
}
