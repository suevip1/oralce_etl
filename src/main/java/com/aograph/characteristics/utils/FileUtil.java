package com.aograph.characteristics.utils;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * @Package: com.aograph.characteristics.utils
 * @Author: tangqipeng
 * @CreateTime: 2022/9/23 18:37
 * @Description:
 */
public class FileUtil {

    /**
     * 以行为单位读取文件，常用于读面向行的格式化文件
     */
    public static String readFileByLines(String filePath) {
        StringBuilder str = new StringBuilder();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(filePath), StandardCharsets.UTF_8));
            String tempString;
            // 一次读入一行，直到读入null为文件结束
            while ((tempString = reader.readLine()) != null) {
                str.append(tempString).append("\n");
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return str.toString();
    }

    /**
     * 读取指定路径的文件
     * @param sqlFileName 文件地址
     * @return String
     */
    public static String getSqlPath(String sqlFileName){
        File directory = new File("src/main/resources/sql/" + sqlFileName);
        String sql = readFileByLines(directory.getPath());
        if (sql.equals(""))
            return null;
        return sql;
    }

    /**
     * 读取文件
     * @param parentPath 文件夹地址
     * @param sqlFileName 文件名
     * @return String
     */
    public static String readSql(String parentPath, String sqlFileName){
        File directory = new File(parentPath + sqlFileName);
        String sql = readFileByLines(directory.getPath());
        if (sql.equals(""))
            return null;
        return sql;
    }

    /**
     * 读取resource下的文件
     * @param parentPath 文件夹名
     * @param sqlFileName 文件名
     * @return String
     */
    public static String readResources(String parentPath, String sqlFileName) {
        StringBuilder str = new StringBuilder();
        BufferedReader reader = null;
        InputStream is = Util.class.getClassLoader().getResourceAsStream(parentPath + "/" +sqlFileName);
        try {
            if (is != null) {
                reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
                String tempString;
                // 一次读入一行，直到读入null为文件结束
                while ((tempString = reader.readLine()) != null) {
                    str.append(tempString).append("\n");
                }
                reader.close();
                is.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }
        }
        return str.toString();
    }


}
