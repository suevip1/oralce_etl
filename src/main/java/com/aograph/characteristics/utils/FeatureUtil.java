package com.aograph.characteristics.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @Package: com.aograph.characteristics.utils
 * @Author：tangqipeng
 * @CreateTime: 2022/9/15 19:20
 * @Description:
 */
public class FeatureUtil {

    /**
     * 分割出时间的小时数
     *
     * @param time
     * @return
     */
    public static int splitTimeToHour(String time) {
        if (!time.trim().equals("")) {
            if (time.trim().length() == 3) {
                return Integer.parseInt(time.substring(0, 1));
            } else if (time.trim().length() == 4) {
                return Integer.parseInt(time.substring(0, 2));
            } else {
                return 0;
            }
        }
        return -1;
    }

    /**
     * 获取时间的日期号
     *
     * @param time
     * @return
     */
    public static int dateTimeToDay(String time) throws ParseException {
        if (time != null && !time.trim().equals("")) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
            Calendar ca = Calendar.getInstance();
            ca.setTime(sdf.parse(time));
            return ca.get(Calendar.DAY_OF_MONTH);
        }
        return -1;
    }

    /**
     * 时间分段
     *
     * @param t
     * @return
     * @throws ParseException
     */
    public static int timePiecewise(String t) throws ParseException {
        long oneGrading = Util.stringToStamp("2018-02-02 00:00:00", "yyyy-MM-dd HH:mm:ss");
        long twoGrading = Util.stringToStamp("2018-02-02 07:31:00", "yyyy-MM-dd HH:mm:ss");
        long threeGrading = Util.stringToStamp("2018-02-02 08:31:00", "yyyy-MM-dd HH:mm:ss");
        long fourGrading = Util.stringToStamp("2018-02-02 20:00:00", "yyyy-MM-dd HH:mm:ss");
        long fiveGrading = Util.stringToStamp("2018-02-02 22:00:00", "yyyy-MM-dd HH:mm:ss");
        if (t == null || t.equals("")) {
            return -1;
        } else {
            String ti = t.trim();
            if (ti.contains(":")) {
                ti = ti.replace(":", "");
            }
            if (ti.length() == 4) {
                ti = ti.substring(0, 2) + ":" + ti.substring(2) + ":00";
            } else if (ti.length() == 3) {
                ti = "0" + ti.charAt(0) + ":" + ti.substring(2) + ":00";
            } else if (ti.length() == 2) {
                ti = "00:" + ti + ":00";
            } else {
                ti = "00:00:00";
            }
            String timeStr = "2018-02-02 " + ti;
            long timeStamp = Util.stringToStamp(timeStr, "yyyy-MM-dd HH:mm:ss");
            if (timeStamp >= oneGrading && timeStamp < twoGrading) {
                return 1;
            } else if (timeStamp >= twoGrading && timeStamp < threeGrading) {
                return 2;
            } else if (timeStamp >= threeGrading && timeStamp < fourGrading) {
                return 3;
            } else if (timeStamp >= fourGrading && timeStamp < fiveGrading) {
                return 4;
            } else if (timeStamp >= fiveGrading) {
                return 5;
            } else {
                return 0;
            }
        }
    }


    /**
     * 航线标识
     *
     * @param upLocation
     * @param disLocation
     * @param eline
     * @return
     */
    public static int elineType(String upLocation, String disLocation, String eline) {
        if (eline != null && eline.contains("-")) {
            String[] elines = eline.split("-");
            if (elines.length == 2) {
                return 0;
            } else if (elines.length > 3) {
                return -1;
            } else {
                String odLocation = upLocation + "-" + disLocation;
                if (!eline.contains(odLocation)) {
                    return 3;
                } else {
                    if ((elines[0] + "-" + elines[1]).equals(odLocation)) {
                        return 1;
                    } else {
                        return 2;
                    }
                }
            }
        }
        return -1;
    }


    /**
     * 时间按分钟分级
     *
     * @param time
     * @return
     */
    public static int typeMinutePiecewise(String time) {
        if (time != null && !time.trim().equals("")) {
            if (time.contains(":")) {
                time = time.trim().replace(":", "");
            }
            if (time.length() == 3) {
                time = "0" + time;
            } else if (time.length() == 2) {
                time = "00" + time;
            } else if (time.length() == 1) {
                time = "000" + time;
            }
            int hour = Integer.parseInt(time.substring(0, 2));
            int minute = Integer.parseInt(time.substring(2));
            return (hour * 60 + minute) / 5;
        }
        return -1;
    }

    /**
     * 计算两个四位数代表时间的数之间的差
     *
     * @param depTime
     * @param arrTime
     * @return
     */
    public static float flyhours(String depTime, String arrTime) throws ParseException {
        float timeCha = 0f;
        if (depTime != null && arrTime != null && depTime.length() == 4 && arrTime.length() == 4) {
            if (depTime.contains(":")) {
                depTime = depTime.trim().replace(":", "");
            }
            if (depTime.length() == 3) {
                depTime = "0" + depTime;
            } else if (depTime.length() == 2) {
                depTime = "00" + depTime;
            } else if (depTime.length() == 1) {
                depTime = "000" + depTime;
            }
            if (arrTime.contains(":")) {
                arrTime = arrTime.trim().replace(":", "");
            }
            if (arrTime.length() == 3) {
                arrTime = "0" + arrTime;
            } else if (arrTime.length() == 2) {
                arrTime = "00" + arrTime;
            } else if (arrTime.length() == 1) {
                arrTime = "000" + arrTime;
            }
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String date1 = "2008-02-02 " + depTime.substring(0, 2) + ":" + depTime.substring(2) + ":00";
            long beginUseTime = sdf.parse(date1).getTime();
            String date2 = "2008-02-02 " + arrTime.substring(0, 2) + ":" + arrTime.substring(2) + ":00";
            if (date1.compareTo(date2) <= 0) {
                String arrDate = "2008-02-03" + " " + arrTime.substring(0, 2) + ":" + arrTime.substring(2) + ":00";
                long endUseTime = sdf.parse(arrDate).getTime();
                timeCha = (float) (endUseTime - beginUseTime) / 1000 / 60 / 60;
            } else {
                long endUseTime = sdf.parse(date2).getTime();
                timeCha = (float) (endUseTime - beginUseTime) / 1000 / 60 / 60;
            }
        }
        return timeCha;
    }

    /**
     * 通过经纬度计算距离
     * @param longitude
     * @param latitude
     * @param longitude2
     * @param latitude2
     * @return
     */
    public static int countDistance(float longitude, float latitude, float longitude2, float latitude2) {
        if (longitude != 0 && latitude != 0 && longitude2 != 0 && latitude2 != 0) {
            int r = 6371;
            float lon1 = rad(latitude);
            float lat1 = rad(longitude);

            float lon2 = rad(latitude2);
            float lat2 = rad(longitude2);

            float dlon = lon2 - lon1;
            float dlat = lat2 - lat1;

            float a = (float) (Math.pow(Math.sin(dlat / 2), 2) + Math.cos(lat1) * Math.cos(lat2) * Math.pow(Math.sin(dlon / 2), 2));
            float c = (float) (2 * Math.asin(Math.sqrt(a)));
            return (int) (c * r);
        }
        return 0;
    }

    private static float rad(float d) {
        return (float) ((float) (d * Math.PI) / 180.0);
    }

    /**
     * 飞行日期所在的航季
     * @param flightDate
     * @return
     */
    public static int typeFlightDate(String flightDate){
        if((flightDate.compareTo("2019-03-31") >= 0 && flightDate.compareTo("2019-10-26") <= 0) ||
                (flightDate.compareTo("2020-03-29") >= 0 && flightDate.compareTo("2020-10-31") <= 0) ||
                (flightDate.compareTo("2021-03-28") >= 0 && flightDate.compareTo("2021-10-30") <= 0) ||
                (flightDate.compareTo("2022-03-27") >= 0 && flightDate.compareTo("2022-10-29") <= 0) ||
                (flightDate.compareTo("2023-03-26") >= 0 && flightDate.compareTo("2023-10-28") <= 0) ||
                (flightDate.compareTo("2024-03-31") >= 0 && flightDate.compareTo("2024-10-26") <= 0)){
            return 1;
        } else {
            return 0;
        }
    }
}
