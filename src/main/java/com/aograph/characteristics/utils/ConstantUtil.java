package com.aograph.characteristics.utils;

/**
 * @Package: com.aograph.characteristics.utils
 * @Author: tangqipeng
 * @CreateTime: 2023/1/6 18:26
 * @Description:
 */
public class ConstantUtil {

//    数据重制
    public static final int RESET = 1;
    public static final int APPEND = 0;

//    是训练还是预测
    public static final int TRAINING = 1;
    public static final int PREDICT = 0;

//    是否增加白名单的选择条件
    public static final int ADD = 1;
    public static final int NO = 0;

//    是否追加
    public static final int ADD_ON = 1;
    public static final int DELETE = 0;

//    选用模型表
    public static final int TRAFFIC_FEATURE = 0;
    public static final int PRICE_FEATURE = 1;

    //是否增加航线
    public static final int ADD_HX = 1;
    public static final int DEFAULT = 0;

    //是否是共有运价
    public static final int IS_PNR = 0;
    public static final int IS_PRIVATE = 1;
    public static final int IS_PUBLIC = 2;

    //oracle版本
    public static final int ORACLE_VERSION = 12;

}
