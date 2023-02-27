package com.aograph.characteristics.control;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ResponseVo<T> implements Serializable {

    private Integer code;
    private String message;
    private T data;

    public ResponseVo(CommonCodeEnum e, T data) {
        this.code = e.getCode();
        this.message = e.getDesc();
        this.data = data;
    }

    public ResponseVo(Integer code, String message, T data) {
        this.code = code;
        this.message = message;
        this.data = data;
    }

    public ResponseVo(Integer code, String message) {
        this(code, message, null);
    }

    public ResponseVo(Integer code, T data) {
        this(code, null, data);
    }

}
