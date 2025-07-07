package com.chase.mq.common.server;


/**
 * 自定义异常类
 * 在构造方法中指定异常原因信息
 */
public class MQException extends RuntimeException {
    public MQException(String message) {
        super(message);
    }
}
