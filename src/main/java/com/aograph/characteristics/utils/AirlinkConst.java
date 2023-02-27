package com.aograph.characteristics.utils;

public interface AirlinkConst {
    // date time format
    String TIME_DATE_FORMAT = "yyyy-MM-dd";
    String TIME_FULL_FORMAT = "yyyy-MM-dd HH:mm:ss";
    String TIME_OTA_TASK_FORMAT = "yyyy-MM-ddHH:mm:ss";
    String TIME_OTA_TABLE_FORMAT = "yyyy_MM_dd";
    String POLICY_CRAWL_DATE_FORMAT = "yyyyMMdd";
    String POLICY_DATE_FORMAT = "yyyy/MM/dd HH:mm";
    String TIME_EP_TABLE_FORMAT = "yyyy_MM_dd";
    String TIME_OTA_TRAIN_TABLE_FORMAT = "yyyy_MM_dd";

    // data sync id for rm_etl_offset record
    int RM_SKY_TRAVEL_RECORD = 1;
    int RM_OTA_RECORD = 2;


    // data transform id for rm_etl_offset record
    int RM_SALE_RECORD = 11;
    int RM_SCHEDULE_RECORD = 12;

    // history progress
    int RM_HISTORY_BASE = 100;
    int RM_OTA_HISTORY_RECORD = RM_HISTORY_BASE + RM_OTA_RECORD;

    int RM_OTA_TRAIN_RECORD = 3;
    int RM_OTA_TRAIN_HISTORY_RECORD = RM_HISTORY_BASE + RM_OTA_TRAIN_RECORD;

}
