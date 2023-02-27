package com.aograph.characteristics.transform;

import com.aograph.characteristics.assist.AssistData;
import com.aograph.characteristics.dao.DataBaseDao;
import com.aograph.characteristics.properties.AssistDataBaseProperties;
import com.aograph.characteristics.properties.FeatureDataBaseProperties;
import com.aograph.characteristics.properties.PnrDataBaseProperties;
import com.aograph.characteristics.properties.SourceDataBaseProperties;
import com.aograph.characteristics.utils.ConstantUtil;
import com.aograph.characteristics.utils.FileUtil;
import com.aograph.characteristics.utils.LogHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * @Package: com.aograph.characteristics.transform
 * @Author: tangqipeng
 * @CreateTime: 2022/10/16 09:36
 * @Description:
 */
@Component
public class FeatureProduct {

    // 数据库操作
    @Autowired
    private DataBaseDao mDataBaseDao;

    //辅助表检查类
    @Autowired
    AssistData assistData;

    //数据源配置
    @Resource
    private SourceDataBaseProperties mSourceDataBaseProperties;

    //辅助表配置
    @Resource
    private AssistDataBaseProperties mAssistDataBaseProperties;

    //模型特征配置
    @Resource
    private FeatureDataBaseProperties mFeatureDataBaseProperties;

    @Resource
    private PnrDataBaseProperties mPnrDataBaseProperties;

    //运行航司
    @Value("${spring.etl.airline_type}")
    private String mAirlineType;

    //是否执行某一些选中航班
    @Value("${spring.predict.choose_type}")
    private int choiceType;

    //价格模型的航班执飞的特征运行天数
    @Value("${spring.etl.price.fly.days}")
    private int priceFlyPredictDays;

    //预测舱位
    @Value("${spring.predict.cabins}")
    private String pnrCabins;

    //配置运行航司的的所有航司
    private String mRunAir;

    //销售源表
    private String mFdlSourceTable;
    //ota数据源
    private String mOtaTable;

    //需要预测数据的数据库
    private String mPredictFlightNo;
    //运价表
    private String mTblPatTable;
    //假期表
    private String mHolidayTable;
    //航司表
    private String mCompTable;
    //机型表
    private String mEqtTable;
    //航班表
    private String mFltNoTable;
    //城市表
    private String mCityTable;
    //航线表
    private String mHxTable;
    //航段表
    private String mOdTable;
    //航季表
    private String mFlySeasonTable;
    //同环比航班信息
    private String mWYRatioTable;
    //航班和对标航班信息
    private String mReferenceTable;
    private String mDuiBiaoTable;
    //自定义配置的辅助表
    private String mSingleLegTimeTable;
    private String mAcSingleLegTimeTable;
    //航段距离表
    private String mOdDistanceTable;

    //模型特征基础表
    private String mBasicFeatureTable;
    //模型特征运力表
    private String mCapFeatureTable;
    //模型特征历史客座率表
    private String mKzlHisFeatureTable;
    //模型特征运价表
    private String mPatFeatureTable;
    //模型特征增量表
    private String mDiffFeatureTable;
    //模型特征同期表
    private String mYoyFeatureTable;
    //模型特征同期表
    private String mWowPriceFeatureTable;
    //模型特征增量label表
    private String mLabelFeatureTable;
    //销售数据的特征表
    private String mAirlineModelHour;

    //ota数据的基础特征表
    private String mOtaPriceBasicFeature;
    //ota数据中预测航班同航段最小6个价格
    private String mOtaPriceTopFeature;
    //ota数据中预测航班同航段航班数
    private String mOtaPriceFlyFeature;
    //ota数据中预测航班同航段最低价航班
    private String mOtaPriceLowFeature;
    //ota数据中特征
    private String mOtaFeature;

    //ota数据特征找出需要融合销售特征的时间
    private String mFinalOMFFeature;
    //ota数据竞飞航班
    private String mFinalJFFeature;

    //ota数据特征融合销售特征
    private String mOtaMergeFeature;
    //销售特征融合ota数据特征
    private String mAirlineModel;

    private String mPnrPriceDerivationAll;
    private String mPnrPriceForOtaTable;

    private int mStartExDif;
    private int mEndExDif;

    //选择条件
    private String mPredictCondition;
    private String mTrainingCondition;

    private final static String allCabins = "A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z";
    private final static String predictCabins = "B,C,D,E,F,H,I,J,K,L,M,N,P,Q,R,S,T,U,V,W,Y,Z";

    private List<String> allCabinList = new ArrayList<>();
    private List<String> predictCabinList = new ArrayList<>();
    private List<String> pnrCabinList = new ArrayList<>();

    /**
     * 初始化各种表
     *
     * @param mode 模型用途，0为预测，1为训练
     */
    public void init(int mode, int bExDif, int eExDif) {
        //辅助表
        mFdlSourceTable = mSourceDataBaseProperties.getFdl().toUpperCase();
        mOtaTable = mSourceDataBaseProperties.getOta().toUpperCase();

        mTblPatTable = mSourceDataBaseProperties.getAograph_tbl_pat().toUpperCase();
        mPredictFlightNo = mAssistDataBaseProperties.getPredict_flight_no();
        mHolidayTable = mAssistDataBaseProperties.getHoliday().toUpperCase();
        mCompTable = mAssistDataBaseProperties.getComp().toUpperCase();
        mEqtTable = mAssistDataBaseProperties.getEqt().toUpperCase();
        mFltNoTable = mAssistDataBaseProperties.getFltno().toUpperCase();
        mCityTable = mAssistDataBaseProperties.getCity().toUpperCase();
        mHxTable = mAssistDataBaseProperties.getHx().toUpperCase();
        mOdTable = mAssistDataBaseProperties.getOd().toUpperCase();
        mFlySeasonTable = mAssistDataBaseProperties.getFly_season().toUpperCase();
        mSingleLegTimeTable = mAssistDataBaseProperties.getSingle_leg_time().toUpperCase();
        mAcSingleLegTimeTable = mAssistDataBaseProperties.getAc_single_leg_time().toUpperCase();
        mOdDistanceTable = mAssistDataBaseProperties.getOd_distance().toUpperCase();
        mWYRatioTable = mAssistDataBaseProperties.getWy_flight().toUpperCase();
        mReferenceTable = mAssistDataBaseProperties.getReference_flight().toUpperCase();
        mDuiBiaoTable = mAssistDataBaseProperties.getDb_flight().toUpperCase();

        String suffix = "";
        String modelSuffix = "";
        if (mode == ConstantUtil.TRAINING) {
            suffix = "_TRAIN";
            modelSuffix = "_ALL";
            choiceType = ConstantUtil.NO;
        }
        //流量特征表
        mAirlineModelHour = mFeatureDataBaseProperties.getTrc_hour_model_feature().toUpperCase() + modelSuffix;
        mAirlineModel = mFeatureDataBaseProperties.getTrc_model_feature().toUpperCase() + modelSuffix;
        //价格特征表
        mOtaFeature = mFeatureDataBaseProperties.getOta_feature().toUpperCase() + modelSuffix;
        mOtaMergeFeature = mFeatureDataBaseProperties.getOta_merge_feature().toUpperCase() + modelSuffix;
        //流量中间表
        mBasicFeatureTable = mFeatureDataBaseProperties.getTrc_basic_feature().toUpperCase() + suffix;
        mCapFeatureTable = mFeatureDataBaseProperties.getTrc_cap_feature().toUpperCase() + suffix;
        mKzlHisFeatureTable = mFeatureDataBaseProperties.getTrc_kzl_feature().toUpperCase() + suffix;
        mPatFeatureTable = mFeatureDataBaseProperties.getTrc_pat_feature().toUpperCase() + suffix;
        mDiffFeatureTable = mFeatureDataBaseProperties.getTrc_diff_feature().toUpperCase() + suffix;
        mYoyFeatureTable = mFeatureDataBaseProperties.getTrc_yoy_feature().toUpperCase() + suffix;
        mLabelFeatureTable = mFeatureDataBaseProperties.getTrc_label_feature().toUpperCase() + suffix;
        mWowPriceFeatureTable = mFeatureDataBaseProperties.getTrc_wow_pat_feature().toUpperCase() + suffix;
        //价格中间表
        mOtaPriceBasicFeature = mFeatureDataBaseProperties.getPrice_basic_feature().toUpperCase() + suffix;
        mOtaPriceTopFeature = mFeatureDataBaseProperties.getPrice_top_feature().toUpperCase() + suffix;
        mOtaPriceFlyFeature = mFeatureDataBaseProperties.getPrice_fly_feature().toUpperCase() + suffix;
        mOtaPriceLowFeature = mFeatureDataBaseProperties.getPrice_low_feature().toUpperCase() + suffix;
        //最终中间表
        mFinalOMFFeature = mFeatureDataBaseProperties.getFinal_omf_feature().toUpperCase() + suffix;
        mFinalJFFeature = mFeatureDataBaseProperties.getFinal_jf_feature().toUpperCase() + suffix;

        mPnrPriceDerivationAll = mPnrDataBaseProperties.getPnr_derivation_price();
        mPnrPriceForOtaTable = mPnrDataBaseProperties.getPnr_price_for_flt_no();

        if (mAirlineType.equals("MU"))
            mRunAir = "'MU', 'FM', 'KN'";
        else
            mRunAir = "'" + mAirlineType + "'";

        mStartExDif = bExDif;
        mEndExDif = eExDif;

        mPredictCondition = " AND concat(DEP, ARR) IN (\n" +
                "\tSELECT DISTINCT concat(DEP, ARR) AS OD\n" +
                "\tFROM " + mPredictFlightNo + " t \n" +
                ")";

        mTrainingCondition = " AND concat(DEP, ARR) NOT IN (\n" +
                "\tSELECT DISTINCT concat(DEP, ARR) AS OD\n" +
                "\tFROM " + mAirlineModel + " t \n" +
                "\tWHERE to_char(FLIGHT_DATE, 'yyyy-MM-dd') = '%1$s'\n" +
                ")";

        allCabinList = Arrays.asList(allCabins.split(",").clone());
        predictCabinList = Arrays.asList(predictCabins.split(",").clone());
        pnrCabinList = Arrays.asList(pnrCabins.split(",").clone());
    }

    /**
     * 清理模型特征表
     *
     * @param add 是否为追加模式，1：是；0：否
     */
    public void clearFeatureTable(int add, int add_hx) {
        // 确保不是新增航线，只有在不新增航线时，非追加模式才将价差模型和流量模型清空重做，否则也只是继续追加
        if (add == ConstantUtil.DELETE && add_hx == ConstantUtil.DEFAULT) {
            mDataBaseDao.truncateTable(mAirlineModel);
            mDataBaseDao.truncateTable(mOtaMergeFeature);
        }
        LogHelper.log("清理流量模型特征表.");
    }

    /**
     * 清理流量模型的中间表和特征表
     */
    @Async("taskExecutor")
    public void clearTrafficMidTable(CountDownLatch cd) {
        mDataBaseDao.truncateTable(mBasicFeatureTable);
        mDataBaseDao.truncateTable(mCapFeatureTable);
        mDataBaseDao.truncateTable(mKzlHisFeatureTable);
        mDataBaseDao.truncateTable(mPatFeatureTable);
        mDataBaseDao.truncateTable(mDiffFeatureTable);
        mDataBaseDao.truncateTable(mYoyFeatureTable);
        mDataBaseDao.truncateTable(mLabelFeatureTable);
        mDataBaseDao.truncateTable(mWowPriceFeatureTable);
        mDataBaseDao.truncateTable(mAirlineModelHour);
        cd.countDown();
        LogHelper.log("清理流量模型特征表，以及中间表");
    }

    /**
     * 清理价格模型的中间表和特征表
     */
    @Async("taskExecutor")
    public void clearPriceMidTable(CountDownLatch cd) {
        mDataBaseDao.truncateTable(mOtaPriceBasicFeature);
        mDataBaseDao.truncateTable(mOtaPriceTopFeature);
        mDataBaseDao.truncateTable(mOtaPriceFlyFeature);
        mDataBaseDao.truncateTable(mOtaPriceLowFeature);
        mDataBaseDao.truncateTable(mFinalOMFFeature);
        mDataBaseDao.truncateTable(mFinalJFFeature);
        mDataBaseDao.truncateTable(mOtaFeature);
        mDataBaseDao.truncateTable(mPnrPriceForOtaTable);
        cd.countDown();
        LogHelper.log("清理价格模型特征表，以及中间表");
    }

    /**
     * comp辅助表检查
     *
     * @param cd  CountDownLatch
     * @param add 是否是累增，0：会搜索整个数据源表，1：从昨天开始搜索
     */
    @Async("taskExecutor")
    public void checkComp(CountDownLatch cd, int add) {
        if (add == ConstantUtil.DELETE) {
            mDataBaseDao.truncateTable(mAssistDataBaseProperties.getComp());
            String basicSeq = mAssistDataBaseProperties.getComp() + "_CODE";
            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);
        }
        assistData.checkComp(mFdlSourceTable, "comp", "insert_date", add);
        assistData.checkComp(mOtaTable, "air_code", "create_time", add);
        cd.countDown();
    }

    /**
     * 机型辅助表检查
     *
     * @param cd  CountDownLatch
     * @param add 是否是累增，0：会搜索整个数据源表，1：从昨天开始搜索
     */
    @Async("taskExecutor")
    public void checkEqt(CountDownLatch cd, int add) {
        if (add == ConstantUtil.DELETE) {
            mDataBaseDao.truncateTable(mAssistDataBaseProperties.getEqt());
            String basicSeq = mAssistDataBaseProperties.getEqt() + "_CODE";
            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);
        }
        assistData.checkEqt(mFdlSourceTable, "insert_date", add);
        cd.countDown();
    }

    /**
     * 航班辅助表检查
     *
     * @param cd  CountDownLatch
     * @param add 是否是累增，0：会搜索整个数据源表，1：从昨天开始搜索
     */
    @Async("taskExecutor")
    public void checkFltNo(CountDownLatch cd, int add) {
        if (add == ConstantUtil.DELETE) {
            mDataBaseDao.truncateTable(mAssistDataBaseProperties.getFltno());
            String basicSeq = mAssistDataBaseProperties.getFltno() + "_CODE";
            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);
        }
        assistData.checkFltno(mFdlSourceTable, "fltno", "insert_date", add);
        assistData.checkFltno(mOtaTable, "flight_no", "create_time", add);
        cd.countDown();
    }

    /**
     * 机场三字吗辅助表检查
     *
     * @param cd  CountDownLatch
     * @param add 是否是累增，0：会搜索整个数据源表，1：从昨天开始搜索
     */
    @Async("taskExecutor")
    public void checkCity(CountDownLatch cd, int add) {
        if (add == ConstantUtil.DELETE) {
            mDataBaseDao.truncateTable(mAssistDataBaseProperties.getCity());
            String basicSeq = mAssistDataBaseProperties.getCity() + "_CODE";
            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);
        }
        assistData.checkCity(mFdlSourceTable, "insert_date", add);
        assistData.checkCity(mOtaTable, "create_time", add);
        cd.countDown();
    }

    /**
     * 航段辅助表检查
     *
     * @param cd  CountDownLatch
     * @param add 是否是累增，0：会搜索整个数据源表，1：从昨天开始搜索
     */
    @Async("taskExecutor")
    public void checkOd(CountDownLatch cd, int add) {
        if (add == ConstantUtil.DELETE) {
            mDataBaseDao.truncateTable(mAssistDataBaseProperties.getOd());
            String basicSeq = mAssistDataBaseProperties.getOd() + "_CODE";
            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);
        }
        assistData.checkOd(mFdlSourceTable, "insert_date", add);
        cd.countDown();
    }

    /**
     * 航线辅助表检查
     *
     * @param cd  CountDownLatch
     * @param add 是否是累增，0：会搜索整个数据源表，1：从昨天开始搜索
     */
    @Async("taskExecutor")
    public void checkHx(CountDownLatch cd, int add) {
        if (add == ConstantUtil.DELETE) {
            mDataBaseDao.truncateTable(mAssistDataBaseProperties.getHx());
            String basicSeq = mAssistDataBaseProperties.getHx() + "_CODE";
            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);
        }
        assistData.checkHx(mFdlSourceTable, "insert_date", add);
        cd.countDown();
    }

    /**
     * singleLegTime辅助表检查
     *
     * @param cd  CountDownLatch
     * @param add 是否是累增，0：会搜索整个数据源表，1：从昨天开始搜索
     */
    @Async("taskExecutor")
    public void checkSingleLegTime(CountDownLatch cd, int add) {
        if (add == ConstantUtil.DELETE) {
            mDataBaseDao.truncateTable(mAssistDataBaseProperties.getSingle_leg_time());
            String basicSeq = mAssistDataBaseProperties.getSingle_leg_time() + "_CODE";
            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);
        }
        assistData.checkSingleLegTime(mFdlSourceTable, "insert_date", add);
        assistData.checkSingleLegTime(mOtaTable, "create_time", add);
        cd.countDown();
    }

    /**
     * AcSingleLegTime辅助表检查
     *
     * @param cd  CountDownLatch
     * @param add 是否是累增，0：会搜索整个数据源表，1：从昨天开始搜索
     */
    @Async("taskExecutor")
    public void checkAcSingleLegTime(CountDownLatch cd, int add) {
        if (add == ConstantUtil.DELETE) {
            mDataBaseDao.truncateTable(mAssistDataBaseProperties.getAc_single_leg_time());
            String basicSeq = mAssistDataBaseProperties.getAc_single_leg_time() + "_CODE";
            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);
        }
        assistData.checkAcSingleLegTime(mFdlSourceTable, "insert_date", add);
        assistData.checkAcSingleLegTime(mOtaTable, "create_time", add);
        cd.countDown();
    }

    /**
     * 基础特征
     *
     * @param mode    模型用途，0为预测，1为训练
     * @param runDate 运行日期
     * @param cd      CountDownLatch
     */
    @Async("taskExecutor")
    public void trafficBasicFeature(int mode, int add_hx, String runDate, CountDownLatch cd) {
        LogHelper.log("productBasicFeature start");
        long startTime = System.currentTimeMillis();
        String fdlBasicFeatureSql = FileUtil.readResources("traffic", "traffic_feature_basic.sql");
        if (!fdlBasicFeatureSql.equals("")) {
            String conditionStr = "to_char(FLIGHT_DATE, 'yyyy-MM-dd') = '" + runDate + "'";
            String basicSeq = "TRC_BASIC_FEA_SEQ";
            if (mode == ConstantUtil.PREDICT) {
                conditionStr = "INSERT_DATE BETWEEN to_date('" + runDate + " 00:00:00','yyyy-MM-dd hh24:mi:ss') - 1" +
                        " AND to_date('" + runDate + " 23:59:59','yyyy-MM-dd hh24:mi:ss')";
            }
            if (choiceType == ConstantUtil.ADD) {
                conditionStr += mPredictCondition;
            }

            if (add_hx == ConstantUtil.ADD_HX) {
                String chooseCondition = String.format(mTrainingCondition, runDate);
                conditionStr += chooseCondition;
            }

            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);

            StringBuilder affirmBkg = new StringBuilder("(");
            StringBuilder cancelBkg = new StringBuilder("(");
            StringBuilder numOfBkg = new StringBuilder();
            for (String cab : allCabinList) {
                String cabin = cab + "_BKG";
                if (predictCabinList.contains(cab)) {
                    affirmBkg.append(cabin).append("+");
                    numOfBkg.append(cabin).append("||','||");
                } else {
                    cancelBkg.append(cabin).append("+");
                }
            }
            affirmBkg.deleteCharAt(affirmBkg.length() - 1);
            cancelBkg.deleteCharAt(cancelBkg.length() - 1);
            numOfBkg.delete(numOfBkg.length() - 7, numOfBkg.length());
            affirmBkg.append(") AS affirm_bkg,\n");
            cancelBkg.append(") AS cancel_bkg,\n");
            numOfBkg.append(" AS numOfSold");
            String sqlStr = affirmBkg.toString() + cancelBkg.toString() + numOfBkg.toString();
            fdlBasicFeatureSql = String.format(fdlBasicFeatureSql, mBasicFeatureTable, mFdlSourceTable, mRunAir,
                    mStartExDif, mEndExDif, conditionStr, mOdDistanceTable, sqlStr, mFlySeasonTable, basicSeq);
            LogHelper.log("fdlBasicFeatureSql is " + fdlBasicFeatureSql);
            mDataBaseDao.executeSql(fdlBasicFeatureSql);
        }
        cd.countDown();
        long costTime = (System.currentTimeMillis() - startTime);
        LogHelper.log("productBasicFeature costTime is " + costTime / 1000 / 60 + "m");
    }

    /**
     * 运力统计
     *
     * @param mode    模型用途，0为预测，1为训练
     * @param runDate 运行日期
     * @param cd      CountDownLatch
     */
    @Async("taskExecutor")
    public void trafficCapFeature(int mode, int add_hx, String runDate, CountDownLatch cd) {
        LogHelper.log("productCapFeature start");
        long startTime = System.currentTimeMillis();
        String capFeatureSql = FileUtil.readResources("traffic", "traffic_feature_cap.sql");
        if (!capFeatureSql.equals("")) {
            String conditionStr = "to_char(FLIGHT_DATE, 'yyyy-MM-dd') = '" + runDate + "'";
            String basicSeq = "TRC_CAP_FEA_SEQ";
            if (mode == ConstantUtil.PREDICT) {
                conditionStr = "INSERT_DATE BETWEEN to_date('" + runDate + " 00:00:00','yyyy-MM-dd hh24:mi:ss') - 1" +
                        " AND to_date('" + runDate + " 23:59:59','yyyy-MM-dd hh24:mi:ss')";
            }
            if (choiceType == ConstantUtil.ADD) {
                conditionStr += mPredictCondition;
            }
            if (add_hx == ConstantUtil.ADD_HX) {
                String chooseCondition = String.format(mTrainingCondition, runDate);
                conditionStr += chooseCondition;
            }
            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);

            capFeatureSql = String.format(capFeatureSql, mCapFeatureTable, mFdlSourceTable, mStartExDif, mEndExDif, conditionStr, basicSeq);
            LogHelper.log("capFeatureSql is " + capFeatureSql);
            mDataBaseDao.executeSql(capFeatureSql);
        }
        cd.countDown();
        long costTime = (System.currentTimeMillis() - startTime);
        LogHelper.log("productCapFeature costTime is " + costTime / 1000 / 60 + "m");
    }

    /**
     * 计算历史客座率
     *
     * @param mode    模型用途，0为预测，1为训练
     * @param runDate 运行日期
     * @param cd      CountDownLatch
     */
    @Async("taskExecutor")
    public void trafficKzlHisFeature(int mode, int add_hx, String runDate, CountDownLatch cd) {
        LogHelper.log("productKzlHisFeature start");
        long startTime = System.currentTimeMillis();
        String kzlHisFeatureSql = FileUtil.readResources("traffic", "traffic_feature_hiz_kzl.sql");
        if (!kzlHisFeatureSql.equals("")) {
            String conditionStr1 = "to_char(FLIGHT_DATE, 'yyyy-MM-dd') = '" + runDate + "'";
            String conditionStr2 = "EX_DIF >= " + mStartExDif + " AND EX_DIF <= " + mEndExDif;
            String basicSeq = "TRC_KZL_FEA_SEQ";
            if (mode == ConstantUtil.PREDICT) {
                conditionStr1 = "INSERT_DATE BETWEEN to_date('" + runDate +
                        " 00:00:00','yyyy-MM-dd hh24:mi:ss')-7 AND to_date('" + runDate + " 23:59:59','yyyy-MM-dd hh24:mi:ss')";
                conditionStr2 = "INSERT_DATE BETWEEN to_date('" + runDate +
                        " 00:00:00','yyyy-MM-dd hh24:mi:ss')-1 AND to_date('" + runDate + " 23:59:59','yyyy-MM-dd hh24:mi:ss')";
            }
            if (choiceType == ConstantUtil.ADD) {
                conditionStr1 += mPredictCondition;
            }

            if (add_hx == ConstantUtil.ADD_HX) {
                String chooseCondition = String.format(mTrainingCondition, runDate);
                conditionStr1 += chooseCondition;
            }
            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);

            kzlHisFeatureSql = String.format(kzlHisFeatureSql, mKzlHisFeatureTable, mFdlSourceTable, mRunAir, mStartExDif,
                    (mEndExDif + 7), conditionStr1, conditionStr2, basicSeq);
            LogHelper.log("kzlHisFeatureSql is " + kzlHisFeatureSql);
            mDataBaseDao.executeSql(kzlHisFeatureSql);
        }
        cd.countDown();
        long costTime = (System.currentTimeMillis() - startTime);
        LogHelper.log("productKzlHisFeature costTime is " + costTime / 1000 / 60 + "m");
    }

    /**
     * 运价表
     *
     * @param mode    模型用途，0为预测，1为训练
     * @param runDate 运行日期
     * @param cd      CountDownLatch
     */
    @Async("taskExecutor")
    public void trafficPat(int mode, int add_hx, String runDate, CountDownLatch cd) {
        long startTime = System.currentTimeMillis();
        String patFeatureSql = FileUtil.readResources("traffic", "traffic_feature_tbl_pat.sql");
        if (!patFeatureSql.equals("")) {
            String conditionStr1 = "to_char(FLIGHT_DATE, 'yyyy-MM-dd') = '" + runDate + "'";
            String conditionStr2 = "START_DATE BETWEEN to_date('" + runDate + " 00:00:00','yyyy-MM-dd hh24:mi:ss') - " +
                    (mEndExDif - mStartExDif + 1) + " AND to_date('" + runDate + " 23:59:59','yyyy-MM-dd hh24:mi:ss')";
            String basicSeq = "TRC_PAT_FEA_SEQ";
            if (mode == ConstantUtil.PREDICT) {
                conditionStr1 = "INSERT_DATE BETWEEN to_date('" + runDate + " 00:00:00','yyyy-MM-dd hh24:mi:ss')-1 AND to_date('" +
                        runDate + " 23:59:59','yyyy-MM-dd hh24:mi:ss')";
                conditionStr2 = "START_DATE BETWEEN to_date('" + runDate + " 00:00:00','yyyy-MM-dd hh24:mi:ss') - 1" +
                        " AND to_date('" + runDate + " 23:59:59','yyyy-MM-dd hh24:mi:ss')";
            }
            if (choiceType == ConstantUtil.ADD) {
                conditionStr1 += mPredictCondition;

                String patChooseCon = " AND CONCAT(DEPARTURE_3CODE, ARRIVAL_3CODE) IN (SELECT DISTINCT CONCAT(DEP, ARR) FROM " + mPredictFlightNo + " )";
                conditionStr2 += patChooseCon;
            }
            if (add_hx == ConstantUtil.ADD_HX) {
                String chooseCondition = String.format(mTrainingCondition, runDate);
                conditionStr1 += chooseCondition;
            }
            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);

            StringBuilder cabinPriceSql = new StringBuilder();
            for (String cab : allCabinList) {
                cabinPriceSql.append("\n(SELECT tp.PRICE FROM table(PATINFOS) tp WHERE tp.CABIN = '").append(cab).append("') as ").append(cab).append("_cabin_price,");
            }
            cabinPriceSql.deleteCharAt(cabinPriceSql.length() - 1);
            patFeatureSql = String.format(patFeatureSql, mPatFeatureTable, mFdlSourceTable, mRunAir, mStartExDif, mEndExDif,
                    conditionStr1, mTblPatTable, conditionStr2, allCabins, cabinPriceSql.toString(), basicSeq);
            LogHelper.log("patFeatureSql is " + patFeatureSql);
            mDataBaseDao.executeSql(patFeatureSql);
        }
        cd.countDown();
        long costTime = (System.currentTimeMillis() - startTime);
        LogHelper.log("productPat costTime is " + costTime / 1000 / 60 + "m");
    }

    /**
     * 环比运价表
     *
     * @param mode    模型用途，0为预测，1为训练
     * @param runDate 运行日期
     */
    public void trafficWowPat(int mode, int add_hx, String runDate) {
        long startTime = System.currentTimeMillis();
        String wowPatFeatureSql = FileUtil.readResources("traffic", "traffic_feature_wow_pat.sql");
        if (!wowPatFeatureSql.equals("")) {
            String conditionStr1 = "to_date(FLIGHT_DATE) = to_date('" + runDate + "', 'yyyy-MM-dd')-7";
            String conditionStr2 = "START_DATE BETWEEN to_date('" + runDate + " 00:00:00','yyyy-MM-dd hh24:mi:ss') - " +
                    (mEndExDif - mStartExDif - 7) + " AND to_date('" + runDate + " 23:59:59','yyyy-MM-dd hh24:mi:ss') - 7";
            String basicSeq = "TRC_WOWPAT_FEA_SEQ";
            if (mode == ConstantUtil.PREDICT) {
                conditionStr1 = "INSERT_DATE BETWEEN to_date('" + runDate + " 00:00:00','yyyy-MM-dd hh24:mi:ss')-9 AND to_date('" +
                        runDate + " 23:59:59','yyyy-MM-dd hh24:mi:ss') - 7";
                conditionStr2 = "START_DATE BETWEEN to_date('" + runDate + " 00:00:00','yyyy-MM-dd hh24:mi:ss') - 9" +
                        " AND to_date('" + runDate + " 23:59:59','yyyy-MM-dd hh24:mi:ss') - 7";
            }

            if (choiceType == ConstantUtil.ADD) {
                conditionStr1 += mPredictCondition;

                String patChooseCon = " AND CONCAT(DEPARTURE_3CODE, ARRIVAL_3CODE) IN (SELECT DISTINCT CONCAT(DEP, ARR) FROM " + mPredictFlightNo + " )";
                conditionStr2 += patChooseCon;
            }

            if (add_hx == ConstantUtil.ADD_HX) {
                String chooseCondition = String.format(mTrainingCondition, runDate);
                conditionStr1 += chooseCondition;
            }

            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);

            wowPatFeatureSql = String.format(wowPatFeatureSql, mWowPriceFeatureTable, mFdlSourceTable, mRunAir, mStartExDif, mEndExDif,
                    conditionStr1, mTblPatTable, conditionStr2, allCabins, basicSeq);
            LogHelper.log("wowPatFeatureSql is " + wowPatFeatureSql);
            mDataBaseDao.executeSql(wowPatFeatureSql);
        }
        long costTime = (System.currentTimeMillis() - startTime);
        LogHelper.log("productWowPat costTime is " + costTime / 1000 / 60 + "m");
    }

    /**
     * diff级别的特征
     *
     * @param mode    模型用途，0为预测，1为训练
     * @param runDate 运行日期
     * @param cd      CountDownLatch
     */
    @Async("taskExecutor")
    public void trafficDiff(int mode, int add_hx, String runDate, CountDownLatch cd) {
        long startTime = System.currentTimeMillis();
        String diffFeatureSql = FileUtil.readResources("traffic", "traffic_feature_diff.sql");
        if (!diffFeatureSql.equals("")) {
            String conditionStr1 = "to_char(FLIGHT_DATE, 'yyyy-MM-dd') = '" + runDate + "'";
            String conditionStr2 = "EX_DIF >= " + mStartExDif + " AND EX_DIF <= " + mEndExDif;
            String basicSeq = "TRC_DIFF_FEA_SEQ";
            if (mode == ConstantUtil.PREDICT) {
                conditionStr1 = "INSERT_DATE BETWEEN to_date('" + runDate +
                        " 00:00:00','yyyy-MM-dd hh24:mi:ss')-8 AND to_date('" + runDate + " 23:59:59','yyyy-MM-dd hh24:mi:ss')";
                conditionStr2 = "INSERT_DATE BETWEEN to_date('" + runDate + " 00:00:00','yyyy-MM-dd hh24:mi:ss') - 1" +
                        " AND to_date('" + runDate + " 23:59:59','yyyy-MM-dd hh24:mi:ss')";
            }
            if (choiceType == ConstantUtil.ADD) {
                conditionStr1 += mPredictCondition;
            }

            if (add_hx == ConstantUtil.ADD_HX) {
                String chooseCondition = String.format(mTrainingCondition, runDate);
                conditionStr1 += chooseCondition;
            }
            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);

            StringBuilder diffBkgStr = new StringBuilder();
            StringBuilder detDiffBkgStr = new StringBuilder();
            StringBuilder detailBkgStr = new StringBuilder();
            StringBuilder bkdSumStr = new StringBuilder();
            for (String cab : allCabinList) {
                String cabin = cab + "_BKG";
                diffBkgStr.append("(CASE WHEN t1.").append(cabin).append(" IS NULL THEN -1 ELSE (t1.").append(cabin).append(" - t.").append(cabin).append(") END) AS diff").append(cab).append("BKG,\n");
                detDiffBkgStr.append("(CASE WHEN diff").append(cab).append("Bkg < 0 THEN 0 ELSE diff").append(cab).append("Bkg END) AS det").append(cab).append("bkg,\n");
            }
            for (String cab : predictCabinList) {
                detailBkgStr.append("diff").append(cab).append("Bkg||','||");
                bkdSumStr.append("det").append(cab).append("bkg+");
            }
            detailBkgStr.delete(detailBkgStr.length() - 7, detailBkgStr.length());
            detailBkgStr.append(" AS detail_diff_bkd");
            String detailStr = detDiffBkgStr.toString() + detailBkgStr.toString();
            bkdSumStr.deleteCharAt(bkdSumStr.length() - 1);
            bkdSumStr.append(" as bkd_sum");
            diffFeatureSql = String.format(diffFeatureSql, mDiffFeatureTable, mFdlSourceTable, mRunAir, mStartExDif, (mEndExDif + 4),
                    conditionStr1, conditionStr2, diffBkgStr.toString(), detailStr, bkdSumStr.toString(), basicSeq);
            LogHelper.log("diffFeatureSql is " + diffFeatureSql);
            mDataBaseDao.executeSql(diffFeatureSql);
        }
        cd.countDown();
        long costTime = (System.currentTimeMillis() - startTime);
        LogHelper.log("productDiff costTime is " + costTime / 1000 / 60 + "m");
    }

    /**
     * 环比数据
     *
     * @param mode    模型用途，0为预测，1为训练
     * @param runDate 运行日期
     * @param cd      CountDownLatch
     */
    @Async("taskExecutor")
    public void trafficYoy(int mode, int add_hx, String runDate, CountDownLatch cd) {
        trafficWowPat(mode, add_hx, runDate);
        long startTime = System.currentTimeMillis();
        String yoyFeatureSql = FileUtil.readResources("traffic", "traffic_feature_yoy.sql");
        if (!yoyFeatureSql.equals("")) {
            String conditionStr = "to_char(FLIGHT_DATE, 'yyyy-MM-dd') = '" + runDate + "'";
            String basicSeq = "TRC_YOY_FEA_SEQ";
            if (mode == ConstantUtil.PREDICT) {
                conditionStr = "INSERT_DATE BETWEEN to_date('" + runDate + " 00:00:00','yyyy-MM-dd hh24:mi:ss') - 1" +
                        " AND to_date('" + runDate + " 23:59:59','yyyy-MM-dd hh24:mi:ss')";
            }
            String conditionStr2 = "ltrim(rtrim(COMP)) in (" + mRunAir + ")";
            if (choiceType == ConstantUtil.ADD) {
                conditionStr += mPredictCondition;
                conditionStr2 += mPredictCondition;
            }

            if (add_hx == ConstantUtil.ADD_HX) {
                String chooseCondition = String.format(mTrainingCondition, runDate);
                conditionStr += chooseCondition;
                conditionStr2 += chooseCondition;
            }

            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);

            StringBuilder numOfBkg = new StringBuilder();
            StringBuilder diffBkgStr = new StringBuilder();
            StringBuilder detDiffBkgStr = new StringBuilder();
            StringBuilder detailBkgStr = new StringBuilder();
            StringBuilder bkdSumStr = new StringBuilder();
            StringBuilder yoyDiffDemandStr = new StringBuilder();
            StringBuilder yoyDemandStr = new StringBuilder();
            for (String cab : predictCabinList) {
                String cabin = cab + "_BKG";
                numOfBkg.append(cabin).append("||','||");
                diffBkgStr.append("\n(CASE WHEN t1.").append(cabin).append(" IS NULL THEN -1 ELSE (t1.").append(cabin).append(" - t.").append(cabin).append(") END) AS diff").append(cab).append("BKG,");
                detDiffBkgStr.append("(CASE WHEN diff").append(cab).append("Bkg < 0 THEN 0 ELSE diff").append(cab).append("Bkg END) AS det").append(cab).append("bkg,\n");
                detailBkgStr.append("diff").append(cab).append("Bkg||','||");
                bkdSumStr.append("det").append(cab).append("bkg+");
                yoyDiffDemandStr.append("\n(CASE WHEN t1.").append(cab).append("_BKG IS NULL or t.").append(cab).append("_BKG IS NULL THEN -1 ELSE (t1.").append(cab).append("_BKG - t.").append(cab).append("_BKG) END) AS diff").append(cab).append("Bkg,");
                yoyDemandStr.append("diff").append(cab).append("Bkg||','||");
            }
            numOfBkg.delete(numOfBkg.length() - 7, numOfBkg.length());
            diffBkgStr.deleteCharAt(diffBkgStr.length() - 1);
            numOfBkg.append(" AS numOfSold");
            detailBkgStr.delete(detailBkgStr.length() - 7, detailBkgStr.length());
            detailBkgStr.append(" AS detail_diff_bkd");
            String detailStr = detDiffBkgStr.toString() + detailBkgStr.toString();
            bkdSumStr.deleteCharAt(bkdSumStr.length() - 1);
            bkdSumStr.append(" as bkd_sum");
            yoyDemandStr.delete(yoyDemandStr.length() - 7, yoyDemandStr.length());
            yoyDemandStr.append(" AS YOY_DEMAND");
            yoyFeatureSql = String.format(yoyFeatureSql, mYoyFeatureTable, mFdlSourceTable, mRunAir, mStartExDif, mEndExDif,
                    conditionStr, mHolidayTable, conditionStr2, numOfBkg.toString(), diffBkgStr.toString(), detailStr,
                    bkdSumStr.toString(), yoyDiffDemandStr.toString(), yoyDemandStr.toString(), mWowPriceFeatureTable,
                    mWYRatioTable, basicSeq);
            LogHelper.log("yoyFeatureSql is " + yoyFeatureSql);
            mDataBaseDao.executeSql(yoyFeatureSql);
        }
        cd.countDown();
        long costTime = (System.currentTimeMillis() - startTime);
        LogHelper.log("productYoy costTime is " + costTime / 1000 / 60 + "m");
    }

    /**
     * 计算label级别的特征
     */
    private void trafficLabel() {
        long startTime = System.currentTimeMillis();
        String labelFeatureSql = FileUtil.readResources("traffic", "traffic_feature_label.sql");
        if (!labelFeatureSql.equals("")) {
            String basicSeq = "TRC_LABEL_FEA_SEQ";

            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);

            String diffBkgStr = "DIFF_BKG_PRICE_FILTER";
            if (oracleVersion() < ConstantUtil.ORACLE_VERSION) {
                diffBkgStr = "DIFF_BKG_PRICE_FILTER_ELEVEN";
            }
            StringBuilder filterSql = new StringBuilder(diffBkgStr + "(DIFF_BKG_PRICE_TABLE(");
            StringBuilder sumIncomeSql = new StringBuilder();
            for (String cab : predictCabinList) {
                sumIncomeSql.append(cab).append("_BKG*").append(cab).append("_CABIN_PRICE+");
                filterSql.append("DIFF_BKG_PRICE_TUPLE(det").append(cab).append("bkg, ").append(cab).append("_CABIN_PRICE),");
            }
            sumIncomeSql.deleteCharAt(sumIncomeSql.length() - 1);
            sumIncomeSql.append(" as sum_income_new,\n");
            filterSql.deleteCharAt(filterSql.length() - 1);
            filterSql.append(")) as ddff");
            String sqlStr = sumIncomeSql.toString() + filterSql.toString();
            labelFeatureSql = String.format(labelFeatureSql, mLabelFeatureTable, mDiffFeatureTable, mPatFeatureTable, sqlStr, basicSeq);
            LogHelper.log("labelFeatureSql is " + labelFeatureSql);
            mDataBaseDao.executeSql(labelFeatureSql);
        }
        long costTime = (System.currentTimeMillis() - startTime);
        LogHelper.log("productLabel costTime is " + costTime / 1000 / 60 + "m");
    }

    /**
     * 流量模型的最后一步
     */
    @Async("taskExecutor")
    public void trafficAirlineHourModel(int mode, CountDownLatch cd) {
        trafficLabel();
        long startTime = System.currentTimeMillis();
        String modelFeatureSql = FileUtil.readResources("traffic", "traffic_feature_final.sql");
        if (!modelFeatureSql.equals("")) {
            StringBuilder numOfBkg = new StringBuilder();
            StringBuilder demandSql = new StringBuilder("(case when t1.B_BKG is null THEN NULL ELSE ");
            for (String cab : predictCabinList) {
                String cabin = cab + "_BKG";
                numOfBkg.append(cabin).append("||','||");
                demandSql.append("(t1.").append(cabin).append("-t.").append(cabin).append(")||','||");
            }
            numOfBkg.delete(numOfBkg.length() - 7, numOfBkg.length());
            demandSql.delete(demandSql.length() - 7, demandSql.length());
            numOfBkg.append(" AS numOfSold");
            demandSql.append(" END) AS demand");

            String d0PriceArr;
            String d0infoSql;
            String basicSeq = "AIRLINE_MODEL_HOUR_SEQ";
            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);
            if (mode == ConstantUtil.PREDICT) {
                d0PriceArr = "null AS D0_PRICEARR";
                d0infoSql = FileUtil.readResources("traffic", "traffic_d0_info.sql");
                d0infoSql = String.format(d0infoSql, mFdlSourceTable, mRunAir, numOfBkg.toString(), mBasicFeatureTable);
            } else {
                d0PriceArr = "t1.PRICEARR AS D0_PRICEARR";
                d0infoSql = FileUtil.readResources("traffic", "traffic_d0_info_train.sql");
            }
            modelFeatureSql = String.format(modelFeatureSql, mAirlineModelHour, mBasicFeatureTable, mCompTable, mCityTable,
                    mEqtTable, mFltNoTable, mOdTable, mHxTable, mYoyFeatureTable, mKzlHisFeatureTable, mCapFeatureTable,
                    mLabelFeatureTable, d0infoSql, d0PriceArr, demandSql.toString(), basicSeq);
            LogHelper.log("modelFeatureSql is " + modelFeatureSql);
            mDataBaseDao.executeSql(modelFeatureSql);
        }
        cd.countDown();
        long costTime = (System.currentTimeMillis() - startTime);
        LogHelper.log("productAirlineHourModel costTime is " + costTime / 1000 / 60 + "m");
    }

    /**
     * 基础特征，只运行mRunAir所属航司的数据
     *
     * @param runDate 运行日期
     * @param cd      CountDownLatch
     */
    @Async("taskExecutor")
    public void priceBasicFeature(int mode, int add_hx, String runDate, CountDownLatch cd) {
        long startTime = System.currentTimeMillis();
        String priceBasicFeatureSql = FileUtil.readResources("price", "price_feature_basic.sql");
        if (!priceBasicFeatureSql.equals("")) {
            String basicSeq = "PRI_BASIC_FEA_SEQ";
            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);

            String conditionStr = "CREATE_TIME BETWEEN to_date('" + runDate + " 00:00:00','yyyy-MM-dd hh24:mi:ss') - 1" +
                    " AND to_date('" + runDate + " 23:59:59','yyyy-MM-dd hh24:mi:ss')";
            if (mode == ConstantUtil.TRAINING)
                conditionStr = "to_char(flight_date, 'yyyy-mm-dd') = '" + runDate + "'";
            if (choiceType == ConstantUtil.ADD) {
                conditionStr += mPredictCondition;
            }

            if (add_hx == ConstantUtil.ADD_HX) {
                String chooseCondition = String.format(mTrainingCondition, runDate);
                conditionStr += chooseCondition;
            }
            priceBasicFeatureSql = String.format(priceBasicFeatureSql, mOtaPriceBasicFeature, mOtaTable, conditionStr, mStartExDif,
                    mEndExDif, mCompTable, mFltNoTable, mOdDistanceTable, mSingleLegTimeTable, mAcSingleLegTimeTable, mCityTable, basicSeq);
            LogHelper.log("priceBasicFeatureSql is " + priceBasicFeatureSql);
            mDataBaseDao.executeSql(priceBasicFeatureSql);
        }
        cd.countDown();
        long costTime = (System.currentTimeMillis() - startTime);
        LogHelper.log("priceBasicFeature costTime is " + costTime / 1000 / 60 + "m");
    }

    /**
     * 计算6个最低价格，最终结果也只要mRunAir所属航司数据
     *
     * @param runDate 运行日期
     * @param cd      CountDownLatch
     */
    @Async("taskExecutor")
    public void priceTopFeature(int mode, int add_hx, String runDate, CountDownLatch cd) {
        long startTime = System.currentTimeMillis();
        String priceTopFeatureSql = FileUtil.readResources("price", "price_feature_top.sql");
        if (!priceTopFeatureSql.equals("")) {

            String basicSeq = "PRI_TOP_FEA_SEQ";
            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);

            String conditionStr = "CREATE_TIME BETWEEN to_date('" + runDate + " 00:00:00','yyyy-MM-dd hh24:mi:ss') - 1" +
                    " AND to_date('" + runDate + " 23:59:59','yyyy-MM-dd hh24:mi:ss')";
            if (mode == ConstantUtil.TRAINING)
                conditionStr = "to_char(flight_date, 'yyyy-mm-dd') = '" + runDate + "'";
            if (choiceType == ConstantUtil.ADD) {
                conditionStr += mPredictCondition;
            }

            if (add_hx == ConstantUtil.ADD_HX) {
                String chooseCondition = String.format(mTrainingCondition, runDate);
                conditionStr += chooseCondition;
            }
            priceTopFeatureSql = String.format(priceTopFeatureSql, mOtaPriceTopFeature, mOtaTable, mStartExDif, mEndExDif,
                    conditionStr, mRunAir, basicSeq);
            LogHelper.log("priceTopFeatureSql is " + priceTopFeatureSql);
            mDataBaseDao.executeSql(priceTopFeatureSql);
        }
        cd.countDown();
        long costTime = (System.currentTimeMillis() - startTime);
        LogHelper.log("priceTopFeature costTime is " + costTime / 1000 / 60 + "m");
    }

    /**
     * 统计航班数量，最终结果也只要mRunAir所属航司数据
     *
     * @param rDate 运行日期
     * @param cd    CountDownLatch
     */
    @Async("taskExecutor")
    public void priceFlyFeature(int mode, int add_hx, String rDate, CountDownLatch cd) {
        long startTime = System.currentTimeMillis();

        String basicSeq = "PRI_FLY_FEA_SEQ";
        String dropSeq = "DROP SEQUENCE " + basicSeq;
        mDataBaseDao.executeSql(dropSeq);
        String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
        mDataBaseDao.executeSql(createSeq);

        String priceFlyFeatureSql;
        if (oracleVersion() < ConstantUtil.ORACLE_VERSION)
            priceFlyFeatureSql = FileUtil.readResources("price", "price_feature_fly_2.sql");
        else
            priceFlyFeatureSql = FileUtil.readResources("price", "price_feature_fly.sql");
        if (!priceFlyFeatureSql.equals("")) {
            String conditionStr = "FLIGHT_DATE BETWEEN to_date('" + rDate + " 00:00:00','yyyy-MM-dd hh24:mi:ss') + " + (mStartExDif - priceFlyPredictDays) +
                    " AND to_date('" + rDate + " 23:59:59','yyyy-MM-dd hh24:mi:ss') + " + (mEndExDif + priceFlyPredictDays) + "\n" +
                    "    AND TO_DATE(CREATE_TIME) <= TO_DATE('" + rDate + "', 'yyyy-MM-dd')";
            String conditionStr2 = "CREATE_TIME BETWEEN to_date('" + rDate + " 00:00:00','yyyy-MM-dd hh24:mi:ss') - 1" +
                    " AND to_date('" + rDate + " 23:59:59','yyyy-MM-dd hh24:mi:ss')";
            if (mode == ConstantUtil.TRAINING) {
                conditionStr = "FLIGHT_DATE BETWEEN to_date('" + rDate + " 00:00:00','yyyy-MM-dd hh24:mi:ss') - " + priceFlyPredictDays +
                        "AND to_date('" + rDate + " 23:59:59','yyyy-MM-dd hh24:mi:ss') + " + priceFlyPredictDays;
//                conditionStr2 = "FLIGHT_DATE BETWEEN to_date('" + rDate + " 00:00:00','yyyy-MM-dd hh24:mi:ss') " +
//                        "AND to_date('" + rDate + " 23:59:59','yyyy-MM-dd hh24:mi:ss')";
                conditionStr2 = "to_char(flight_date, 'yyyy-mm-dd') = '" + rDate + "'";
            }

            if (choiceType == ConstantUtil.ADD) {
                conditionStr += mPredictCondition;
            }

            if (add_hx == ConstantUtil.ADD_HX) {
                String chooseCondition = String.format(mTrainingCondition, rDate);
                conditionStr += chooseCondition;
            }
            priceFlyFeatureSql = String.format(priceFlyFeatureSql, mOtaPriceFlyFeature, mOtaTable, mStartExDif,
                    mEndExDif, conditionStr, conditionStr2, basicSeq);
            LogHelper.log("priceFlyFeatureSql is " + priceFlyFeatureSql);
            mDataBaseDao.executeSql(priceFlyFeatureSql);
        }
        cd.countDown();
        long costTime = (System.currentTimeMillis() - startTime);
        LogHelper.log("priceFlyFeature costTime is " + costTime / 1000 / 60 + "m");
    }

    /**
     * 最低价航班
     *
     * @param rDate 运行日期
     * @param cd    CountDownLatch
     */
    @Async("taskExecutor")
    public void priceLowestFeature(int mode, int add_hx, String rDate, CountDownLatch cd) {
        long startTime = System.currentTimeMillis();
        String priceLowestFeatureSql = FileUtil.readResources("price", "price_feature_low.sql");
        if (!priceLowestFeatureSql.equals("")) {
            String basicSeq = "PRI_LOW_FEA_SEQ";
            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);

            String conditionStr = "CREATE_TIME BETWEEN to_date('" + rDate + " 00:00:00','yyyy-MM-dd hh24:mi:ss') - 1" +
                    " AND to_date('" + rDate + " 23:59:59','yyyy-MM-dd hh24:mi:ss')";
            if (mode == ConstantUtil.TRAINING)
                conditionStr = "to_char(flight_date, 'yyyy-mm-dd') = '" + rDate + "'";
            if (choiceType == ConstantUtil.ADD) {
                conditionStr += mPredictCondition;
            }

            if (add_hx == ConstantUtil.ADD_HX) {
                String chooseCondition = String.format(mTrainingCondition, rDate);
                conditionStr += chooseCondition;
            }
            String bSql = "FIND_LOW_FLIGHT(flightTuple, COMP) AS lowestFlight";
            priceLowestFeatureSql = String.format(priceLowestFeatureSql, mOtaPriceLowFeature, mOtaTable, mStartExDif,
                    mEndExDif, conditionStr, mRunAir, bSql, basicSeq);
            LogHelper.log("priceLowestFeatureSql is " + priceLowestFeatureSql);
            mDataBaseDao.executeSql(priceLowestFeatureSql);
        }
        cd.countDown();
        long costTime = (System.currentTimeMillis() - startTime);
        LogHelper.log("priceLowestFeature costTime is " + costTime / 1000 / 60 + "m");
    }

    /**
     * 价格模型基础版本
     *
     * @param cd CountDownLatch
     */
    @Async("taskExecutor")
    public void priceFeature(CountDownLatch cd) {
        long startTime = System.currentTimeMillis();
        String priceFeatureSql = FileUtil.readResources("price", "price_feature_final.sql");
        if (!priceFeatureSql.equals("")) {
            String basicSeq = "PRI_OTA_FEATURE_SEQ";
            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);

            priceFeatureSql = String.format(priceFeatureSql, mOtaFeature, mOtaPriceBasicFeature,
                    mOtaPriceTopFeature, mOtaPriceFlyFeature, mOtaPriceLowFeature, mRunAir, basicSeq);
            LogHelper.log("priceFeature is " + priceFeatureSql);
            mDataBaseDao.executeSql(priceFeatureSql);
        }
        cd.countDown();
        long costTime = (System.currentTimeMillis() - startTime);
        LogHelper.log("priceFeature costTime is " + costTime / 1000 / 60 + "m");
    }

    /**
     * 查找竞飞航班
     *
     * @param cd CountDownLatch
     */
    @Async("taskExecutor")
    public void priceFindJfFlight(CountDownLatch cd) {
        long startTime = System.currentTimeMillis();
        String findJfFlightFeatureSql = FileUtil.readResources("final", "ota_find_jf_flight.sql");
        if (!findJfFlightFeatureSql.equals("")) {

            String basicSeq = "PRI_JF_FEATURE_SEQ";
            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);

            String jfFun = "FIND_JF_FLIGHT";
            if (oracleVersion() < ConstantUtil.ORACLE_VERSION)
                jfFun = "FIND_JF_FLIGHT_ELEVEN";
            findJfFlightFeatureSql = String.format(findJfFlightFeatureSql, mFinalJFFeature, mOtaPriceBasicFeature, mRunAir,
                    jfFun, mReferenceTable, mDuiBiaoTable, basicSeq);
            LogHelper.log("findJfFlightFeatureSql is " + findJfFlightFeatureSql);
            mDataBaseDao.executeSql(findJfFlightFeatureSql);
        }
        cd.countDown();
        long costTime = (System.currentTimeMillis() - startTime);
        LogHelper.log("findJfFlight costTime is " + costTime / 1000 / 60 + "m");
    }

    /**
     * ota合并fdl
     */
    public void priceOtaMergeFdl() {
        long startTime = System.currentTimeMillis();
        String sqlFile = "ota_merge_fdl_11.sql";
        String otaMergeFdlFeatureSql = FileUtil.readResources("final", sqlFile);
        if (!otaMergeFdlFeatureSql.equals("")) {

            String basicSeq = "PRI_OMF_FEATURE_SEQ";
            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);

            otaMergeFdlFeatureSql = String.format(otaMergeFdlFeatureSql, mFinalOMFFeature, mOtaFeature, mAirlineModelHour, mHolidayTable, basicSeq);
            LogHelper.log("otaMergeFdlFeatureSql is " + otaMergeFdlFeatureSql);
            mDataBaseDao.executeSql(otaMergeFdlFeatureSql);
        }
        long costTime = (System.currentTimeMillis() - startTime);
        LogHelper.log("otaMergeFdl costTime is " + costTime / 1000 / 60 + "m");
    }

    /**
     * 整理pnr数据
     */
    @Async("taskExecutor")
    public void priceDealPnrForOta(String runDate, CountDownLatch cd) {
        long startTime = System.currentTimeMillis();
        String dealPnrForOtaSql = FileUtil.readResources("pnr", "pnr_price_for_ota.sql");
        if (!dealPnrForOtaSql.equals("")) {

            String basicSeq = "PNRPRICEFOROTATRAIN_SEQ";
            String dropSeq = "DROP SEQUENCE " + basicSeq;
            mDataBaseDao.executeSql(dropSeq);
            String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
            mDataBaseDao.executeSql(createSeq);

            StringBuilder notPredictCabinStr = new StringBuilder();
            for (String cabin : allCabinList) {
                if (!pnrCabinList.contains(cabin)) {
                    notPredictCabinStr.append("'").append(cabin).append("',");
                }
            }
            notPredictCabinStr.deleteCharAt(notPredictCabinStr.length() - 1);
            dealPnrForOtaSql = String.format(dealPnrForOtaSql, mPnrPriceForOtaTable, mPnrPriceDerivationAll, runDate,
                    mEndExDif, mStartExDif, notPredictCabinStr, basicSeq);
            LogHelper.log("dealPnrForOtaSql is " + dealPnrForOtaSql);
            mDataBaseDao.executeSql(dealPnrForOtaSql);
        }
        cd.countDown();
        long costTime = (System.currentTimeMillis() - startTime);
        LogHelper.log("airlineModel costTime is " + costTime / 1000 / 60 + "m");
    }

    /**
     * 合并所有特征生成最终的特征
     *
     * @param runDate 运行日期
     * @param mode    模式
     */
    public void priceOtaMergeFeature(String runDate, int add, int add_hx, int mode) {
        long startTime = System.currentTimeMillis();
        String priSqlFile = "ota_merge_feature.sql";
        if (mode == ConstantUtil.TRAINING)
            priSqlFile = "ota_merge_feature_all.sql";
        String jFMergeInfoSql = FileUtil.readResources("final", priSqlFile);
        if (!jFMergeInfoSql.equals("")) {

            String basicSeq = "OTA_MERGE_FEATURE_ALL_SEQ";
            if (mode == ConstantUtil.PREDICT) {
                basicSeq = "OTA_MERGE_FEATURE_SEQ";
            }
            if (add == ConstantUtil.DELETE && add_hx == ConstantUtil.DEFAULT) {
                String dropSeq = "DROP SEQUENCE " + basicSeq;
                mDataBaseDao.executeSql(dropSeq);
                String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
                mDataBaseDao.executeSql(createSeq);
            }

            String conditionStr = "to_char(FLIGHT_DATE, 'yyyy-MM-dd') = '" + runDate + "'";
            if (mode == ConstantUtil.PREDICT) {
                conditionStr = "INSERT_DATE BETWEEN to_date('" + runDate + " 00:00:00','yyyy-MM-dd hh24:mi:ss') - 1" +
                        " AND to_date('" + runDate + " 23:59:59','yyyy-MM-dd hh24:mi:ss')";
                jFMergeInfoSql = String.format(jFMergeInfoSql, mOtaMergeFeature, mFinalJFFeature, mOtaPriceBasicFeature,
                        mFinalOMFFeature, mCapFeatureTable, mFdlSourceTable, mStartExDif, mEndExDif, conditionStr, basicSeq);
            } else
                jFMergeInfoSql = String.format(jFMergeInfoSql, mOtaMergeFeature, mFinalJFFeature, mOtaPriceBasicFeature,
                        mFinalOMFFeature, mCapFeatureTable, mPnrPriceForOtaTable, mFdlSourceTable, mStartExDif, mEndExDif,
                        conditionStr, basicSeq);
            LogHelper.log("otaMergeFdlFeatureSql is " + jFMergeInfoSql);
            mDataBaseDao.executeSql(jFMergeInfoSql);
        }
        long costTime = (System.currentTimeMillis() - startTime);
        LogHelper.log("otaMergeFdl costTime is " + costTime / 1000 / 60 + "m");
    }

    /**
     * airlineModel
     */
    public void trafficAirlineModel(int add, int add_hx, int mode) {
        long startTime = System.currentTimeMillis();
        String airlineModelSql = FileUtil.readResources("final", "airline_model.sql");
        if (!airlineModelSql.equals("")) {
            String basicSeq = "ANALY_AIRLINE_MODEL_ALL_SEQ";
            if (mode == ConstantUtil.PREDICT) {
                basicSeq = "ANALY_AIRLINE_MODEL_SEQ";
            }
            if (add == ConstantUtil.DELETE && add_hx == ConstantUtil.DEFAULT) {
                String dropSeq = "DROP SEQUENCE " + basicSeq;
                mDataBaseDao.executeSql(dropSeq);
                String createSeq = "CREATE SEQUENCE " + basicSeq + " INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999999999999999999999 NOCYCLE CACHE 200 NOORDER";
                mDataBaseDao.executeSql(createSeq);
            }

            String ovSql = "FDL_FIND_OTA_TIME(CREATE_TIMES, t.INSERT_DATE) AS neartime";
            airlineModelSql = String.format(airlineModelSql, mAirlineModel, ovSql, mAirlineModelHour, mOtaFeature, mOtaMergeFeature, basicSeq);
            LogHelper.log("airlineModelSql is " + airlineModelSql);
            mDataBaseDao.executeSql(airlineModelSql);
        }
        long costTime = (System.currentTimeMillis() - startTime);
        LogHelper.log("otaMergeFdl costTime is " + costTime / 1000 / 60 + "m");
    }

    /**
     * 获取当前基础特征运行到了哪一天，以insert_date为单位
     *
     * @param featureTag 特征表
     * @return 返回模型特征上次运行到的时间
     */
    public String getMaxDate(int featureTag) {
        String featureTable = mAirlineModel;
        if (featureTag == ConstantUtil.PRICE_FEATURE)
            featureTable = mOtaMergeFeature;
        String selSql = "SELECT to_char(max(FLIGHT_DATE), 'yyyy-MM-dd') as MAX_DATE FROM " + featureTable;
        LogHelper.log("getMaxInsertDate sql is " + selSql);
        Map<String, Object> map = mDataBaseDao.selectMapBySql(selSql);
        if (map != null && map.get("MAX_DATE") != null) {
            return map.get("MAX_DATE").toString();
        }
        return null;
    }

    public String getStartDate(int featureTag) {
        String featureTable = mAirlineModel;
        if (featureTag == ConstantUtil.PRICE_FEATURE)
            featureTable = mOtaMergeFeature;
        String selSql = "SELECT to_char(min(FLIGHT_DATE), 'yyyy-MM-dd') as START_DATE FROM " + featureTable;
        LogHelper.log("getStartDate sql is " + selSql);
        Map<String, Object> map = mDataBaseDao.selectMapBySql(selSql);
        if (map != null && map.get("START_DATE") != null) {
            return map.get("START_DATE").toString();
        }
        return null;
    }

    /**
     * 根据flight_date删除数据
     *
     * @param startDate 删除起始日期
     * @param endDate   删除结束日期
     */
    public void deleteData(String startDate, String endDate) {
        String delSql = "DELETE FROM " + mOtaMergeFeature + " WHERE FLIGHT_DATE BETWEEN TO_DATE('" + startDate +
                "', 'yyyy-MM-dd') + 1 AND TO_DATE('" + endDate + "','yyyy-MM-dd')";
        mDataBaseDao.executeSql(delSql);
    }

    /**
     * oracle版本判断
     *
     * @return int
     */
    public int oracleVersion() {
        String sql = "select BANNER from v$version";
        List<String> list = mDataBaseDao.selectListBySql(sql, String.class);
        if (list != null && list.size() > 0) {
            String banner = list.get(0);
            if (banner.contains("11")) {
                return 11;
            } else if (banner.contains("12")) {
                return 12;
            } else if (banner.contains("18")) {
                return 18;
            }
        }
        return 12;
    }


}
