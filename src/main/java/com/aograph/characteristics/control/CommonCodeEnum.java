package com.aograph.characteristics.control;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * response里使用的状态常量
 */
@AllArgsConstructor
@Getter
public enum CommonCodeEnum {
    SUCCESS(200,"成功"),
    RUNTIME_FAIL(500,"运行异常"),
    CREATE_FAIL(501, "创建失败"),
    UPDATE_FAIL(502, "更新失败"),
    DELETE_FAIL(503, "删除失败"),
    PARAM_NEEDED(504,"参数缺失"),
    PARAM_INVALID(505,"参数非法");

    private Integer code;
    private String desc;
}
