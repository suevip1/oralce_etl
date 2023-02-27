package com.aograph.characteristics;

import com.aograph.characteristics.control.FeatureAsyncController;
import com.aograph.characteristics.control.HolidayTransitService;
import com.aograph.characteristics.control.SupplementHisFareService;
import com.aograph.characteristics.jobHandler.*;
import com.aograph.characteristics.utils.ConstantUtil;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@SpringBootTest
class CharacteristicsApplicationTests {


    @Autowired
    FeatureAsyncController controller;

    @Autowired
    SupplementHisFareService fareController;

    @Autowired
    ReadDepArrPairJob pairJob;

    @Autowired
    ReadAvJob avJob;

    @Autowired
    ReadFlightInfoJob flightInfoJob;

    @Autowired
    ReadFlightScheduleJob flightScheduleJob;

    @Autowired
    ReadFareFdJob fdJob;

    @Autowired
    ReadFarePrivateJob privateJob;

    @Autowired
    ReadFlpJob flpJob;

    @Autowired
    ReadPnrTicketJob pnrTicketJob;

    @Autowired
    ReadCtripPriceJob ctripPriceJob;

    @Autowired
    ReadHisInvJob hisInvJob;

    @Autowired
    HolidayTransitService transitService;

    @Resource
    private SupplementHisFareService supplementHisFareService;

    private final static String allCabins = "A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,P,Q,R,S,T,U,V,W,X,Y,Z";
    private final static String predictCabins = "A,B,C,D,E,F,G,H,I,J,K,L,M,N,O,Q,R,S,T,V,W,X,Y,Z";

    private List<String> allCabinList = new ArrayList<>();
    private List<String> predictCabinList = new ArrayList<>();


    @Test
    void contextLoads() throws Exception {

//        readDate();

//        controller.assistStart(0);
//        controller.runTest("2021-11-10", 0, 60);
        controller.predictFeatureStart(0, "2022-09-20", 0, 60);
//        controller.predictFeatureStart(0, "2020-10-01", 0, 360);
//        controller.predictFeatureStart(0, "2021-01-24", 0, 60);
//        controller.trainingFeatureStart(0, "2021-12-12", null, 0, 60);
//        controller.trainingFeatureStart(0, "2022-02-07", "2022-02-09", 0, 60);

//        supplementHisFareService.supplementHisFare(ConstantUtil.IS_PRIVATE, 1);

//        transitService.addHolidayDate(0);

//        System.out.println("2021-12-30".compareTo("2022-10-01"));
//        fareController.SupplementHisFare("2021-06-12", "2022-08-08", 0);
//
//        String fdlBasicFeatureSql = FileUtil.readResources("traffic", "traffic_feature_basic.sql");
//        System.out.println(fdlBasicFeatureSql);

    }

    private void readDate() throws IOException {
        boolean history = true;
        pairJob.processFiles(new ContainFilter("od_pair"), history);
        avJob.processFiles(new ContainFilter("av"), history);
        flightInfoJob.processFiles(new ContainFilter("info"), history);
        flightScheduleJob.processFiles(new ContainFilter("schedule"), history);
        fdJob.processFiles(new ContainFilter("fd"), history);
        privateJob.processFiles(new ContainFilter("fulture"), history);
        flpJob.processFiles(new ContainFilter("flp"), history);
        pnrTicketJob.processFiles(new ContainFilter("pnr"), history);
        ctripPriceJob.processFiles(new ContainFilter("ctrip"), history);
        hisInvJob.processFiles(new ReadHisInvJob.InvFilter(), history);
    }

    //打印方法
    private static void showArr(int []arr) {
        //增强for循环打印
        for(int a:arr) {
            System.out.print(a+"\t");

        }
        System.out.println();

    }

}
