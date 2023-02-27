package com.aograph.characteristics.dao;

import com.aograph.characteristics.utils.LogHelper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.util.List;
import java.util.Map;

/**
 * @Package: com.aograph.characteristics.dao
 * @Author：tangqipeng
 * @CreateTime: 2022/9/14 18:01
 * @Description: 数据库操作类
 */
@Service
public class DataBaseDao {

    @Resource(name = "oracleDataSource")
    private DataSource dataSource;

    /**
     * 执行不需要返回的sql
     * @param sql 执行的sql
     */
    public void executeSql(String sql){
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);
        template.execute(sql);
    }

    /**
     * 查询出Map结果
     * @param sql 执行的sql
     * @return Map
     */
    public Map<String, Object> selectMapBySql(String sql){
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);
        return template.queryForMap(sql);
    }

    /**
     * 查询单条结果
     * @param sql 执行的sql
     * @param requiredType 结果对象，或者类型
     * @param <T> 返回类型或对象
     * @return T
     */
    public <T> T selectObjectBySql(String sql, Class<T> requiredType){
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);
        return template.queryForObject(sql, requiredType);
    }

    /**
     * 查询数据集合
     * @param sql 执行的sql
     * @param requiredType 结果对象，或者类型
     * @param <T> 返回类型或对象
     * @return List<T>
     */
    public <T> List<T> selectListBySql(String sql, Class<T> requiredType){
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);
        return template.queryForList(sql, requiredType);
    }

    /**
     * 查询数据集合
     * @param sql
     * @return
     */
    public List<Map<String, Object>> queryForList(String sql){
        JdbcTemplate template = new JdbcTemplate();
        template.setDataSource(dataSource);
        return template.queryForList(sql);
    }

    /**
     * 清理表数据
     * @param table 表明
     */
    public void truncateTable(String table){
        Map<String, Object> map = selectMapBySql("select * from user_tables t where table_name= '" + table + "'");
        if (map != null && map.get("TABLE_NAME").equals(table)) {
            LogHelper.log("clear " + table);
            executeSql("TRUNCATE TABLE " + table);
        }
    }

}
