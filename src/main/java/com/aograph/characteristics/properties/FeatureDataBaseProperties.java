package com.aograph.characteristics.properties;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @Package: com.aograph.characteristics.properties
 * @Author：tangqipeng
 * @CreateTime: 2022/9/16 16:16
 * @Description:模型特征及其中间表配置
 */
@Setter
@Getter
@Data
@Component
@ConfigurationProperties(prefix = "spring.datasource.model")
public class FeatureDataBaseProperties {

    private String ota_feature;
    private String ota_merge_feature;

    private String trc_hour_model_feature;
    private String trc_model_feature;

    private String trc_basic_feature;
    private String trc_cap_feature;
    private String trc_kzl_feature;
    private String trc_pat_feature;
    private String trc_diff_feature;
    private String trc_yoy_feature;
    private String trc_wow_pat_feature;
    private String trc_label_feature;

    private String price_basic_feature;
    private String price_top_feature;
    private String price_fly_feature;
    private String price_low_feature;

    private String final_omf_feature;
    private String final_jf_feature;

}
