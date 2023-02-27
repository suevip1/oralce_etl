package com.aograph.characteristics.utils;

import org.apache.commons.lang3.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

public class DateTimeHelper {
    private static final String TAG=DateTimeHelper.class.getSimpleName();
    private static final String[] daysOfWeek = {"星期天", "星期一", "星期二", "星期三", "星期四", "星期五", "星期六"};

    public static Date toDate(LocalDateTime dt) {
        return Date.from(dt.atZone(ZoneId.systemDefault()).toInstant());
    }

    public static List<Date> getDaysInRange(Date startDay, Date endDay) {
        List<Date> dateList = new ArrayList<>();
        if (startDay.after(endDay)) {
            return dateList;
        }

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(startDay);
        Date day = calendar.getTime();
        while(day.before(endDay)){
            dateList.add(day);
            calendar.add(Calendar.DATE, 1);
            day = calendar.getTime();
        }

        dateList.add(endDay);
        return dateList;
    }

    public static Date getDate(String strDate, String format) {
        if (StringUtils.isBlank(strDate)) {
            return null;
        }

        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Date date = null;
        try {
            // 注意格式需要与上面一致，不然会出现异常
            date = sdf.parse(strDate);
        } catch (Exception e) {
//            LogHelper.error(TAG, e);
            throw new RuntimeException(e);
        }

        return date;
    }

    public static String date2String(LocalDateTime date, String format) {
        if (date == null) {
            return "";
        }

        return date2String(toDate(date), format);
    }

    public static String date2String(Date date, String format) {
        SimpleDateFormat sdf = new SimpleDateFormat(format, Locale.US);
        try {
            if (date == null) {
                return "";
            }

            return sdf.format(date);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static String getDateStrAfter(String date, int days, String format) {
        Date day = getDate(date, format);
        day = getDateAfter(day, days);
        return date2String(day, format);
    }

    public static Date getDateAfter(Date startDay, int days) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(startDay);
        calendar.add(Calendar.DAY_OF_MONTH, days);
        return calendar.getTime();
    }

    public static int getDayOfMonth(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.DAY_OF_MONTH);
    }

    public static String getDayStringOfWeek(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return daysOfWeek[calendar.get(Calendar.DAY_OF_WEEK) - Calendar.SUNDAY];
    }

    public static String getDayOfWeek(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int day = calendar.get(Calendar.DAY_OF_WEEK);
        if (day == Calendar.SUNDAY) {
            return "7";
        } else {
            return String.valueOf(day - Calendar.SUNDAY);
        }
    }

    public static int getYear(String dateStr, String format) {
        Date date = getDate(dateStr, format);
        return getYear(date);
    }

    public static int getYear(Date date) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        return calendar.get(Calendar.YEAR);
    }

    public static int daysBetween(Date sdate,Date bdate) {
        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd");
        try {
            sdate = sdf.parse(sdf.format(sdate));
            bdate = sdf.parse(sdf.format(bdate));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(sdate);
        long time1 = cal.getTimeInMillis();
        cal.setTime(bdate);
        long time2 = cal.getTimeInMillis();
        long between_days=(time2-time1)/(1000*3600*24);

        return Integer.parseInt(String.valueOf(between_days));
    }

    public static Date set2ZeroHour(Date date) {
        Calendar start = Calendar.getInstance();
        if (date != null) {
            start.setTime(date);
        }
        start.set( Calendar.HOUR_OF_DAY, 0);
        start.set( Calendar.MINUTE, 0);
        start.set( Calendar.SECOND, 0);
        start.set( Calendar.MILLISECOND, 0);
        return start.getTime();
    }

    public static Date getCurrTime() {
        Calendar start = Calendar.getInstance();
        return start.getTime();
    }

    public static Date getCurrDay() {
        return set2ZeroHour(null);
    }

    public static Date getSameDayOfWeek(Date date, int years) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        int weekNum = calendar.get(Calendar.WEEK_OF_MONTH);
        int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
        int month = calendar.get(Calendar.MONTH);
        calendar.add(Calendar.YEAR, years);
        int newDayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
        int newMonth = calendar.get(Calendar.MONTH);

        if (dayOfWeek == newDayOfWeek) {
            if (month < newMonth) {
                while(calendar.get(Calendar.MONTH) != month) {
                    calendar.add(Calendar.DAY_OF_YEAR, -7);
                }
            } else if (month > newMonth) {
                while(calendar.get(Calendar.MONTH) != month) {
                    calendar.add(Calendar.DAY_OF_YEAR, 7);
                }
            }
            return calendar.getTime();
        }

        dayOfWeek = dayOfWeek == 1 ? 7 : dayOfWeek - 1;
        newDayOfWeek = newDayOfWeek == 1 ? 7 : newDayOfWeek - 1;

        int diff;
        if (Math.abs(dayOfWeek - newDayOfWeek) <= 3) {
            diff = dayOfWeek - newDayOfWeek;
        } else if (dayOfWeek < newDayOfWeek) {
            diff = 7 - (newDayOfWeek - dayOfWeek);
        } else {
            diff = - (7 + newDayOfWeek - dayOfWeek);
        }

        calendar.add(Calendar.DAY_OF_YEAR, diff);
        newMonth = calendar.get(Calendar.MONTH);
        if (month < newMonth) {
            calendar.add(Calendar.DAY_OF_YEAR, -7);
        } else if (month > newMonth) {
            calendar.add(Calendar.DAY_OF_YEAR, 7);
        }
        return calendar.getTime();
    }

    public static String getSameDayOfWeek(String date, int years, String format) {
        Date day = getDate(date, format);
        day = getSameDayOfWeek(day, years);
        return date2String(day, format);
    }

    public static void main(String[] args) {
        Date day = getDate("2021-07-01", "yyyy-MM-dd");
        Date day1 = getSameDayOfWeek(day, -3);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(day);
        int w1 = calendar.get(Calendar.DAY_OF_WEEK);
        calendar.setTime(day1);
        int w2 = calendar.get(Calendar.DAY_OF_WEEK);
        System.out.println(day1+", "+w1+", "+w2);
    }
}
