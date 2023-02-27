package com.aograph.characteristics.utils;

import com.xxl.job.core.context.XxlJobHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Package: com.aograph.characteristics.utils
 * @Author：tangqipeng
 * @CreateTime: 2022/9/14 15:43
 * @Description:
 */
public class Util {

    /**
     * 将时间戳转换为时间
     */
    public static String stampToDateString(long s) {
        return stampToDateString(s, "yyyy-MM-dd");
    }

    /**
     * 将时间戳转换为时间
     */
    public static String stampToDateTimeString(long s) {
        return stampToDateString(s, "yyyy-MM-dd HH:mm:ss");
    }

    /**
     * 时间戳转字符串
     * @param s 时间戳
     * @param pattern 字符串格式
     * @return String
     */
    private static String stampToDateString(long s, String pattern) {
        String res;
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
        Date date = new Date(s);
        res = simpleDateFormat.format(date);
        return res;
    }

    /**
     * 提取日期中的小时数
     * @param s 时间字符串
     * @return 小时数
     */
    public static int stringTimeToHour(String s) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar cal = Calendar.getInstance();
        cal.setTime(sdf.parse(s));
        return cal.get(Calendar.HOUR_OF_DAY);
    }

    /**
     * 计算两个日期差
     * @param fromDate 起始时间
     * @param toDate 结束时间
     * @return int
     * @throws ParseException 异常
     */
    public static int daysBetween(String fromDate, String toDate) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            return daysBetween(sdf.parse(fromDate), sdf.parse(toDate));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 计算两个日期差
     * @param fromDate 起始时间
     * @param toDate 结束时间
     * @return int
     */
    public static int daysBetween(Date fromDate, Date toDate) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar cal = Calendar.getInstance();
        cal.setTime(fromDate);
        long time1 = cal.getTimeInMillis();
        cal.setTime(toDate);
        long time2 = cal.getTimeInMillis();
        long betweenDays = (time2 - time1) / (1000 * 3600 * 24);
        return (int) betweenDays;
    }

    /**
     * 小时差
     * @param fromDate 起始时间
     * @param toDate 结束时间
     * @return int
     */
    public static int hoursBetween(String fromDate, String toDate) {
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            return hoursBetween(sdf.parse(fromDate), sdf.parse(toDate));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 小时差
     * @param fromDate 起始时间
     * @param toDate 结束时间
     * @return int
     */
    public static int hoursBetween(Date fromDate, Date toDate) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(fromDate);
        long time1 = cal.getTimeInMillis();
        cal.setTime(toDate);
        long time2 = cal.getTimeInMillis();
        long diff = (time2 - time1) / (1000 * 3600);
        return (int) diff;
    }

    /**
     * 字符串转时间戳
     * @param s 字符串日期
     * @return long
     */
    public static long stringToStamp(String s) {
        return stringToStamp(s, "yyyy-MM-dd");
    }

    /**
     * 字符串转时间戳
     * @param s 字符串日期
     * @param pattern 字符串日期格式
     * @return long
     */
    public static long stringToStamp(String s, String pattern) {
        try {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
            Date date = simpleDateFormat.parse(s);
            return date.getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 将属性名转化为数据库表属性命名格式
     */
    public static String upperCharToUnderLine(String param) {
        Pattern p = Pattern.compile("[A-Z]");
        if (param == null || param.equals("")) {
            return "";
        }
        StringBuilder builder = new StringBuilder(param);
        Matcher mc = p.matcher(param);
        int i = 0;
        while (mc.find()) {
            builder.replace(mc.start() + i, mc.end() + i, "_" + mc.group().toLowerCase());
            i++;
        }
        if ('_' == builder.charAt(0)) {
            builder.deleteCharAt(0);
        }
        return builder.toString();
    }

    /**
     * 集合根据另一个集合排序
     * @param orderList 排序集合
     * @param targetList 目标集合
     */
    public static void listSort(List<String> orderList, List<String> targetList) {
        targetList.sort(((o1, o2) -> {
            int io1 = orderList.indexOf(o1);
            int io2 = orderList.indexOf(o2);
            if (io1 != -1) {
                io1 = targetList.size() - io1;
            }
            if (io2 != -1) {
                io2 = targetList.size() - io2;
            }
            return io2 - io1;
        }));
    }

    private static final long oneDays = 24 * 60 * 60 * 1000;

    /**
     * 日期加上天数
     * @param startDate 起始的日期
     * @param days 要加的天数
     * @return 得到日期
     */
    public static String addDays(String startDate, int days){
        long startStamp = stringToStamp(startDate);
        long endStamp = startStamp + days * oneDays;
        return stampToDateString(endStamp);
    }
}
