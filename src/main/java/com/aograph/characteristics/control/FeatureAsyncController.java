package com.aograph.characteristics.control;

import com.aograph.characteristics.transform.FeatureProduct;
import com.aograph.characteristics.utils.ConstantUtil;
import com.aograph.characteristics.utils.LogHelper;
import com.aograph.characteristics.utils.Util;
import com.xxl.job.core.context.XxlJobHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.CountDownLatch;

/**
 * @Package: com.aograph.characteristics.control
 * @Author: tangqipeng
 * @CreateTime: 2022/10/16 10:45
 * @Description:
 */
@Component
public class FeatureAsyncController {

    @Autowired
    private FeatureProduct featureProduct;

    private static final long oneDays = 24 * 60 * 60 * 1000;

    public void assistStart(int add) throws InterruptedException {
        init(0, 0, 60);
        checkAssist(add);
    }

    /**
     *  启动预测任务
     * @param add 是否追加；1：是；0：不是
     * @param runDate 预测日期
     * @param bExDif 起始ex_dif
     * @param eExDif 结束ex_dif
     * @throws Exception 抛出异常
     */
    public void predictFeatureStart(int add, String runDate, int bExDif, int eExDif) throws Exception {
        init(0, bExDif, eExDif);
        checkAssist(1);
        runFeature(0, add, ConstantUtil.DEFAULT, runDate);
    }


    /**
     * 模型特征训练数据任务
     * @param add 是否追加；1：是；0：不是
     * @param beginDate 起始运行日期
     * @param endDate 结束日期
     * @param bExDif 起始ex_dif
     * @param eExDif 结束ex_dif
     * @throws Exception 抛出异常
     */
    public void trainingFeatureStart(int add, int add_hx, String beginDate, String endDate, int bExDif, int eExDif) throws Exception {
        init(1, bExDif, eExDif);
        checkAssist(add);
        runTrainingFeature(add, add_hx, beginDate, endDate);
    }


    /**
     * 初始化
     * @param mode 模式：1：训练数据；0：预测数据
     * @param bExDif 起始ex_dif
     * @param eExDif 结束ex_dif
     */
    private void init(int mode, int bExDif, int eExDif) {
        featureProduct.init(mode, bExDif, eExDif);
        LogHelper.log("runFeature init");
    }

    /**
     * 辅助表运行
     * @param add 是否是累增，0：会搜索整个数据源表，1：从昨天开始搜索
     * @throws InterruptedException 异常
     */
    private void checkAssist(int add) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        CountDownLatch trafficCD = new CountDownLatch(8);
        featureProduct.checkComp(trafficCD, add);
        featureProduct.checkEqt(trafficCD, add);
        featureProduct.checkCity(trafficCD, add);
        featureProduct.checkFltNo(trafficCD, add);
        featureProduct.checkOd(trafficCD, add);
        featureProduct.checkHx(trafficCD, add);
        featureProduct.checkSingleLegTime(trafficCD, add);
        featureProduct.checkAcSingleLegTime(trafficCD, add);
        trafficCD.await();
        long costTime = (System.currentTimeMillis() - startTime);
        LogHelper.log(" checkAssist costTime is " + costTime/1000/60 + "m");
    }


    /**
     * 运行模型特征
     * @param mode 模式：1：训练数据；0：预测数据
     * @param add 0：清空模型特征数据，重新构建数据；1：追加数据，不清空特征表
     * @param runDate 运行数据的日期
     * @throws Exception 异常
     */
    private void runFeature(int mode, int add, int add_hx, String runDate) throws Exception {

        if (runDate == null || runDate.equals(""))
            runDate = Util.stampToDateString(System.currentTimeMillis());

        long startTime = System.currentTimeMillis();

        clearTable(add, add_hx);
        Thread.sleep(3000);

        LogHelper.log("runFeature start");

        CountDownLatch trafficCD = new CountDownLatch(10);
        featureProduct.trafficBasicFeature(mode, add_hx, runDate, trafficCD);
        featureProduct.trafficCapFeature(mode, add_hx, runDate, trafficCD);
        featureProduct.trafficKzlHisFeature(mode, add_hx, runDate, trafficCD);
        featureProduct.trafficPat(mode, add_hx, runDate, trafficCD);
        featureProduct.trafficDiff(mode, add_hx, runDate, trafficCD);
        featureProduct.trafficYoy(mode, add_hx, runDate, trafficCD);

        featureProduct.priceBasicFeature(mode, add_hx, runDate, trafficCD);
        featureProduct.priceTopFeature(mode, add_hx, runDate, trafficCD);
        featureProduct.priceFlyFeature(mode, add_hx, runDate, trafficCD);
        featureProduct.priceLowestFeature(mode, add_hx, runDate, trafficCD);
        trafficCD.await();
        LogHelper.log("CountDownLatch 1 end");

        int countDown = 3;
        if (mode == 1)
            countDown = 4;
        CountDownLatch traffic2CD = new CountDownLatch(countDown);
        featureProduct.priceFeature(traffic2CD);
        featureProduct.priceFindJfFlight(traffic2CD);
        featureProduct.trafficAirlineHourModel(mode, traffic2CD);
        if (mode == 1)
            featureProduct.priceDealPnrForOta(runDate, traffic2CD);
        traffic2CD.await();
        LogHelper.log("CountDownLatch 2 end");

        featureProduct.priceOtaMergeFdl();
        featureProduct.priceOtaMergeFeature(runDate, add, add_hx, mode);
        featureProduct.trafficAirlineModel(add, add_hx, mode);
        long costTime = (System.currentTimeMillis() - startTime);
        LogHelper.log(runDate + " Feature costTime is " + (costTime > 60 * 1000 ? costTime/1000/60 + "m " + (costTime-costTime/1000/60)/1000 + "s"
                :(costTime > 1000 ? costTime/1000 + "s" : costTime + "ms")));
    }

    /**
     *  训练逻辑
     * @param add 0：清空模型特征数据，重新构建数据；1：追加数据，不清空特征表
     * @param beginDate 训练数据的起始时间
     * @param endDate 训练数据的结束时间
     * @throws Exception 异常
     */
    private void runTrainingFeature(int add, int add_hx, String beginDate, String endDate) throws Exception {
        // 结束日期可以为空，默认到今天，但实际运行只会到昨天
        String currentDate = Util.stampToDateString(System.currentTimeMillis());
        if (endDate == null || endDate.equals("") || endDate.compareTo(currentDate) > 0)
            endDate = currentDate;

        if (add == ConstantUtil.DELETE){
            if (beginDate == null || beginDate.equals("")) {
                beginDate = featureProduct.getStartDate(0);
            }
        } else {
            String maxAFlightDate = featureProduct.getMaxDate(0);
            String maxOFlightDate = featureProduct.getMaxDate(1);
            if (maxAFlightDate != null && maxOFlightDate != null && maxOFlightDate.compareTo(maxAFlightDate) > 0) {
                featureProduct.deleteData(maxOFlightDate, maxAFlightDate);
            }
            if (maxAFlightDate != null && !maxAFlightDate.equals("")){
                long bStamp = Util.stringToStamp(maxAFlightDate) + oneDays;
                if ((beginDate == null || beginDate.equals("")) || beginDate.compareTo(maxAFlightDate) < 0) {
                    beginDate = Util.stampToDateString(bStamp);
                }
            }
        }
        if (beginDate == null)
            throw new Exception("起始日期为空");

        //  两个日期的差
        int diffDays = Util.daysBetween(beginDate, endDate);
        long startStamp = Util.stringToStamp(beginDate);
        XxlJobHelper.log("训练数据运行总天数 == " + diffDays);

        for (int i = 0; i < diffDays; i ++) {
            long runStamp = startStamp + i * oneDays;
            String runDate = Util.stampToDateString(runStamp);
            XxlJobHelper.log("运行日期 == " + runDate);
            XxlJobHelper.log("是否追加 == " + add);
            runFeature(1, add, add_hx, runDate);
            add = 1;
        }
    }

    /**
     * 清理两个模型的特征表和中间表
     * @param add 是否为追加模式，1：是；0：否
     * @throws InterruptedException 异常
     */
    private void clearTable(int add, int add_hx) throws InterruptedException {
        CountDownLatch trafficCD = new CountDownLatch(2);
        featureProduct.clearTrafficMidTable(trafficCD);
        featureProduct.clearPriceMidTable(trafficCD);
        trafficCD.await();
        featureProduct.clearFeatureTable(add, add_hx);
        LogHelper.log("清理完模型特征表，以及中间表");
    }

    public void runTest(String runDate, int bExDif, int eExDif) throws Exception {
        featureProduct.init(0, bExDif, eExDif);
        featureProduct.priceOtaMergeFdl();
//        CountDownLatch trafficCD = new CountDownLatch(1);
//        featureProduct.trafficBasicFeature(0, runDate, trafficCD);
//        featureProduct.priceFindJfFlight(trafficCD);
//        trafficCD.await();
//        System.out.println(featureProduct.oracleVersion());
//        String beginDate = featureProduct.getStartDate(0);
    }
}
