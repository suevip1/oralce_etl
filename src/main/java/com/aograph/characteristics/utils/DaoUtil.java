package com.aograph.characteristics.utils;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @Package: com.aograph.characteristics.utils
 * @Author: tangqipeng
 * @CreateTime: 2022/9/19 14:47
 * @Description:
 */
public class DaoUtil {

    /**
     * @param strDate 返回java.sql.Date格式的
     */
    public static java.sql.Date strToDate(String strDate) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        if (strDate != null && !strDate.equals("")) {
            Date d;
            try {
                d = format.parse(strDate);
                return new java.sql.Date(d.getTime());
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
        return null;
    }

    /**
     * @param strDate 返回java.sql.Timestamp格式的
     */
    public static java.sql.Timestamp strToTimestamp(String strDate) {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String starDate = strDate;
        if (strDate != null && !strDate.equals("")) {
            if (starDate.length() == 10) {
                starDate = starDate + " 00:00:00";
            }
            Date d;
            try {
                d = format.parse(starDate);
                return new java.sql.Timestamp(d.getTime());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }


    /**
     * 将对象转为创建表的语句
     */
    public static Map<String, String> filedParams(Class<?> clazz) {
        Map<String, String> fieldMap = new HashMap<>();
        Field[] fields = clazz.getDeclaredFields();
        System.out.println(Arrays.stream(fields).toString());
        for (Field field : fields) {
            field.setAccessible(true);
//            String fieldName = upperCharToUnderLine(field.getName());
            String fieldName = field.getName();
            fieldMap.put(fieldName, field.getType().getName());
        }
        return fieldMap;
    }

    /**
     * 将对象转为创建表的语句
     */
    public static String filedParamToInsertSQL(Class<?> clazz, int dType) {
        StringBuilder fieldSB = new StringBuilder("(");
        StringBuilder xSB = new StringBuilder(" VALUES (");
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            String fieldName = (field.getName());
            if (dType == 1)
                fieldSB.append(fieldName).append(",");
            else
                fieldSB.append("\"").append(fieldName.toUpperCase()).append("\"").append(",");
            xSB.append("?").append(",");
        }
        fieldSB.deleteCharAt(fieldSB.toString().length() - 1);
        xSB.deleteCharAt(xSB.toString().length() - 1);
        fieldSB.append(")");
        xSB.append(")");
        return fieldSB.toString() + xSB.toString();
    }

    /**
     * 将对象转为创建表的语句
     */
    public static <T> void insertValueSQLStatement(T clazz, PreparedStatement statement, Class<?> modelClass, int dType) throws NoSuchFieldException, IllegalAccessException, SQLException {
        Class<?> classValue = clazz.getClass();
        int index = 0;
        Field[] fields = modelClass.getDeclaredFields();
        for (Field field : fields) {
            index++;
            Field fieldName = classValue.getDeclaredField(field.getName());
            fieldName.setAccessible(true);
            Object fieldValue = fieldName.get(clazz);
            if (field.getName().equals("flight_date")) {
                if (fieldValue == null || fieldValue.equals("")) {
                    statement.setDate(index, DaoUtil.strToDate("1970-01-01"));
                } else {
                    statement.setDate(index, DaoUtil.strToDate(fieldValue.toString()));
                }
            } else if (field.getName().equals("insert_date")) {
                if (fieldValue == null || fieldValue.equals("")) {
                    statement.setTimestamp(index, DaoUtil.strToTimestamp("1970-01-01 00:00:00"));
                } else {
                    statement.setTimestamp(index, DaoUtil.strToTimestamp(fieldValue.toString()));
                }
            } else {
                if (field.getType().getName().contains("List")) {
                    if (dType == 1) {
                        statement.setObject(index, fieldValue);
                    } else {
                        if (fieldValue != null) {
                            statement.setObject(index, fieldValue.toString());
                        } else
                            statement.setObject(index, "");
                    }
                } else {
                    statement.setObject(index, fieldValue);
                }
            }
        }
    }

    /**
     * 将对象转为创建表的语句
     *
     * @param clazz
     * @param dType 1为ck，2为oracle, 3mysql
     * @return
     */
    public static String filedParamToCreateSQL(Class<?> clazz, String[] dateFields, String[] dateTimeFields, int dType) {
        StringBuilder sb = new StringBuilder();
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            String type = "String";
            if (dType != 1)
                type = "VARCHAR2(30)";
            if (field.getType().getName().contains("Integer") || field.getType().getName().contains("int")) {
                if (dType == 1) {
                    type = "Int32";
                } else if (dType == 2) {
                    type = "number(10)";
                } else {
                    type = "int(10)";
                }
            } else if (field.getType().getName().contains("Float") || field.getType().getName().contains("float")) {
                if (dType == 1) {
                    type = "Float32";
                } else if (dType == 2) {
                    type = "number(10, 3)";
                } else {
                    type = "double";
                }
            } else if (field.getType() == java.util.List.class) {
                if (dType == 1) {
                    Type genericType = field.getGenericType();
                    if(genericType instanceof ParameterizedType){
                        ParameterizedType pt = (ParameterizedType) genericType;
                        //得到泛型里的class类型对象
                        for (Type arg : pt.getActualTypeArguments()) {
                            if (arg == java.lang.Integer.class){
                                type = "Array(Int32)";
                            } else if (arg == java.lang.Float.class){
                                type = "Array(Float32)";
                            } else if (arg == java.lang.String.class){
                                type = "Array(String)";
                            } else {
                                ParameterizedType tpt = (ParameterizedType) arg;
                                for (Type targ : tpt.getActualTypeArguments()) {
                                    if (targ == java.lang.Integer.class){
                                        type = "Array(Array(Int32))";
                                    } else if (targ == java.lang.Float.class){
                                        type = "Array(Array(Float32))";
                                    } else if (targ == java.lang.String.class){
                                        type = "Array(Array(String))";
                                    }
                                }
                            }
                        }
                    } else {
                        type = "String";
                    }
                } else {
                    type = "CLOB";
                }
            }
            String fieldName = field.getName();
            if (dateFields != null && dateFields.length > 0 && Arrays.asList(dateFields).contains(field.getName())) {
                type = "Date";
            } else if (dateTimeFields != null && dateTimeFields.length > 0 && Arrays.asList(dateTimeFields).contains(field.getName())) {
                if (dType == 1) {
                    type = "DateTime";
                } else if (dType == 2) {
                    type = "Date";
                } else {
                    type = "DateTime";
                }
            }
            if(dType == 1) {
                sb.append("\n").append(fieldName).append(" ").append(type).append(",");
            } else {
                sb.append("\n").append("\"").append(fieldName.toUpperCase()).append("\"").append(" ").append(type.toUpperCase()).append(",");
            }
        }
        sb.deleteCharAt(sb.toString().length() - 1);
        return sb.toString();
    }


    /**
     * 将对象转为创建表的语句
     *
     * @param clazz
     * @param dType 1为ck，2为oracle, 3mysql
     * @return
     */
    public static String filedParamToCreateSQL(Class<?> clazz, int dType) {
        StringBuilder sb = new StringBuilder();
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            String type = "String";
            if (dType != 1)
                type = "VARCHAR2(30)";
            if (field.getType().getName().contains("Integer") || field.getType().getName().contains("int")) {
                if (dType == 1) {
                    type = "Int32";
                } else if (dType == 2) {
                    type = "number(10)";
                } else {
                    type = "int(10)";
                }
            } else if (field.getType().getName().contains("Float") || field.getType().getName().contains("float")) {
                if (dType == 1) {
                    type = "Float32";
                } else if (dType == 2) {
                    type = "number(10, 3)";
                } else {
                    type = "double";
                }
            } else if (field.getType() == java.util.List.class) {
                if (dType == 1) {
                    Type genericType = field.getGenericType();
                    if(genericType instanceof ParameterizedType){
                        ParameterizedType pt = (ParameterizedType) genericType;
                        //得到泛型里的class类型对象
                        for (Type arg : pt.getActualTypeArguments()) {
                            if (arg == java.lang.Integer.class){
                                type = "Array(Int32)";
                            } else if (arg == java.lang.Float.class){
                                type = "Array(Float32)";
                            } else if (arg == java.lang.String.class){
                                type = "Array(String)";
                            } else {
                                ParameterizedType tpt = (ParameterizedType) arg;
                                for (Type targ : tpt.getActualTypeArguments()) {
                                    if (targ == java.lang.Integer.class){
                                        type = "Array(Array(Int32))";
                                    } else if (targ == java.lang.Float.class){
                                        type = "Array(Array(Float32))";
                                    } else if (targ == java.lang.String.class){
                                        type = "Array(Array(String))";
                                    }
                                }
                            }
                        }
                    } else {
                        type = "String";
                    }
                } else {
                    type = "CLOB";
                }
            } else if (field.getType() == java.sql.Date.class) {
                type = "Date";
            } else if (field.getType() == java.sql.Timestamp.class) {
                if (dType == 1) {
                    type = "DateTime";
                } else if (dType == 2) {
                    type = "Date";
                } else {
                    type = "DateTime";
                }
            }
            String fieldName = field.getName();
            if(dType == 1) {
                sb.append("\n").append(fieldName).append(" ").append(type).append(",");
            } else {
                sb.append("\n").append("\"").append(fieldName.toUpperCase()).append("\"").append(" ").append(type.toUpperCase()).append(",");
            }
        }
        sb.deleteCharAt(sb.toString().length() - 1);
        return sb.toString();
    }

}
