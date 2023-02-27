package com.aograph.characteristics.properties;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @Package: com.aograph.characteristics.properties
 * @Author：tangqipeng
 * @CreateTime: 2022/9/16 16:14
 * @Description: 辅助表表名配置
 */
@Setter
@Getter
@Data
@Component
@ConfigurationProperties(prefix = "spring.datasource.assist")
public class AssistDataBaseProperties {

    private String predict_flight_no;
    private String airport;
    private String comp;
    private String eqt;
    private String hx;
    private String od;
    private String fltno;
    private String city;
    private String discount;
    private String fly_season;
    private String holiday;
    private String holiday_cfg;
    private String single_leg_time;
    private String ac_single_leg_time;
    private String od_distance;
    private String wy_flight;
    private String reference_flight;
    private String db_flight;

}
