package com.chase.mq.common;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 表示一个消费者（完整的执行环境）
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ConsumerEnv {

    /**
     * 一次消费的标识
     */
    private String consumerTag;

    /**
     * 消费的队列
     */
    private String queueName;

    /**
     * 是否自动应答
     */
    private boolean autoAck;

    /**
     * 通过这个回调函数来处理收到的消息
     */
    private Consumer consumer;

}
