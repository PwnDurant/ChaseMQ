package com.chase.mq.common.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

/**
 * 用来声明解绑的一些参数
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class QueueUnbindArguments extends BasicArguments implements Serializable {

    /**
     * 指定被解绑的队列名
     */
    private String queueName;

    /**
     * 指定被解绑的交换机名
     */
    private String exchangeName;

}
