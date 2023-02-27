package com.aograph.characteristics.jobHandler;

import com.alibaba.fastjson.JSONObject;
import com.aograph.characteristics.control.FeatureAsyncController;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Objects;

/**
 * @Package: com.aograph.characteristics.jobHandler
 * @Author: tangqipeng
 * @CreateTime: 2022/10/18 13:06
 * @Description:
 */
@Component
public class FeatureRunnerJob {

    @Autowired
    private FeatureAsyncController controller;

    /**
     * 辅助表自增任务
     * @throws InterruptedException 异常
     */
    @XxlJob(value = "checkAssist")
    public void checkAssistTable() throws InterruptedException {
        XxlJobHelper.log("checkAssistTable start...");

        String[] args = Objects.requireNonNull(XxlJobHelper.getJobParam()).split(",");

        controller.assistStart(Integer.parseInt(args[0]));

        XxlJobHelper.log("checkAssistTable finish...");
    }

    /**
     * 预测特征表任务
     * @throws Exception 异常
     */
    @XxlJob(value = "featureEtlJob")
    public void featureEtlJob() throws Exception {
        XxlJobHelper.log("featureEtlJob start...");

        String param = XxlJobHelper.getJobParam();

        JSONObject map = new JSONObject();
        if (StringUtils.isNotBlank(param)) {
            map = JSONObject.parseObject(param);
        }
        if (param == null || map == null)
            throw new Exception("没有参数");

        String runDate = map.getString("runDate");

        controller.predictFeatureStart(map.getIntValue("add"), runDate, map.getIntValue("startExDif"), map.getIntValue("endExDif"));

        XxlJobHelper.log("featureEtlJob finish...");
    }

    /**
     * 训练特征表
     * @throws Exception 异常
     */
    @XxlJob(value = "featureEtlAllJob")
    public void featureEtlAllJob() throws Exception {
        XxlJobHelper.log("featureEtlAllJob start...");

        // 获取参数
        String param = XxlJobHelper.getJobParam();

        //解析参数
        JSONObject map = new JSONObject();
        if (StringUtils.isNotBlank(param)) {
            map = JSONObject.parseObject(param);
        }
        if (param == null || map == null)
            throw new Exception("没有参数");

        //起始日期---结束日期
        String startDate = map.getString("beginDate");
        String endDate = map.getString("endDate");

        //是否追加
        int add = map.getIntValue("add");
        int add_hx = map.getIntValue("add_hx");

        // 预测航天的天数
        int startExDif = map.getIntValue("startExDif");
        int endExDif = map.getIntValue("endExDif");

        controller.trainingFeatureStart(add, add_hx, startDate, endDate, startExDif, endExDif);

        XxlJobHelper.log("featureEtlAllJob finish...");
    }

}
