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
@ConfigurationProperties(prefix = "spring.datasource.pnr")
public class PnrDataBaseProperties {

    private String pnr_derivation_price;
    private String pnr_price_for_flt_no;

}
