package com.aograph.characteristics.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import javax.sql.DataSource;

/**
 * @Package: com.aograph.characteristics.databaseconfig
 * @Author：tangqipeng
 * @CreateTime: 2022/9/14 17:49
 * @Description:
 */
@Configuration
public class DataSourceConfig {

    /**
     * 数据源配置 oracle数据源
     */
    @Primary
    @Bean(name = "oracleDataSourceProperties")
    @ConfigurationProperties(prefix = "spring.datasource.oracle")
    public DataSourceProperties oracleDataSourceProperties() {
        return new DataSourceProperties();
    }

    /**
     * 数据源 oracle数据源
     */
    @Primary
    @Bean(name = "oracleDataSource")
    @ConfigurationProperties(prefix = "spring.datasource.oracle.hikari")
    public DataSource oracleDataSource(@Qualifier("oracleDataSourceProperties") DataSourceProperties dataSourceProperties) {
        return dataSourceProperties.initializeDataSourceBuilder().build();
    }

}
