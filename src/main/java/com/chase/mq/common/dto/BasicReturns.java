package com.chase.mq.common.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

/**
 * 这个类表示各个远程调用的方法的返回值的公共信息
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class BasicReturns extends BasicArguments implements Serializable {

    /**
     * 表示当前这个远程嗲用方法的返回值
     */
    private boolean ok;

}


