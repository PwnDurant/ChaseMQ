package com.chase.mq.common.server;

import com.chase.mq.mqserver.core.BasicProperties;

import java.io.IOException;

/**
 * 指示一个单纯的函数式接口（回调函数）
 * 收到消息之后要处理消息时调用的方法
 */
@FunctionalInterface
public interface Consumer {

    /**
     * Delivery 的意思是“投递”，这个方法预期是在每次服务器收到消息之后，来调用
     * 通过这个方法把消息推送给对应的消费者
     * @param consumerTag 一次消费的标识
     * @param basicProperties 基本参数
     * @param body 消息体
     * @throws IOException IO 异常
     */
    void handleDelivery(String consumerTag, BasicProperties basicProperties ,byte[] body) throws IOException;

}
