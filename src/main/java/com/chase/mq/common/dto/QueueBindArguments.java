package com.chase.mq.common.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

/**
 * 用来声明绑定队列的一些参数
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class QueueBindArguments extends BasicArguments implements Serializable {

    /**
     * 被绑定的队列名
     */
    private String queueName;

    /**
     * 被绑定的交换机名
     */
    private String exchangeName;

    /**
     * 绑定键
     */
    private String bindingKey;

}
