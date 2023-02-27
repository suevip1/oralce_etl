package com.aograph.characteristics.properties;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * @Package: com.aograph.characteristics.properties
 * @Author：tangqipeng
 * @CreateTime: 2022/9/16 16:02
 * @Description:数据源表的配置
 */
@Setter
@Getter
@Data
@Component
@ConfigurationProperties(prefix = "spring.datasource.source")
public class SourceDataBaseProperties {
    private String fdl;
    private String ota;
    private String aograph_tbl_pat;
}
