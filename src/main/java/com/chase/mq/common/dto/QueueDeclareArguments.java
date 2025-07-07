package com.chase.mq.common.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.Map;

/**
 * 用来声明创建一个队列的参数
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class QueueDeclareArguments extends BasicArguments implements Serializable {

    /**
     * 创建的队列名
     */
    private String queueName;

    /**
     * 创建的队列是否可持久化
     */
    private boolean durable;

    /**
     * 创建的队列能否可以给其他人使用
     */
    private boolean exclusive;

    /**
     * 创建的队列是否是自动删除的
     */
    private boolean autoDelete;

    /**
     * 其他参数
     */
    private Map<String,Object> arguments;

}
