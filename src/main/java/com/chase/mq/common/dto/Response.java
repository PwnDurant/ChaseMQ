package com.chase.mq.common.dto;

import lombok.Data;

/**
 * 这个对象表示一个响应
 */
@Data
public class Response {

    /**
     * 类型
     */
    private int type;

    /**
     * 载荷长度
     */
    private int length;

    /**
     * 载荷
     */
    private byte[] payload;

}
