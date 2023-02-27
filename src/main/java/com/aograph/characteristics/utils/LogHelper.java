package com.aograph.characteristics.utils;

import com.xxl.job.core.context.XxlJobHelper;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LogHelper {
    public static boolean log(String appendLogPattern, Object... appendLogArguments) {
        log.info(appendLogPattern, appendLogArguments);
        return XxlJobHelper.log(appendLogPattern, appendLogArguments);
    }

    public static boolean error(String tag, Throwable e) {
        log.error(tag,e);
        return XxlJobHelper.log(e);
    }
}
