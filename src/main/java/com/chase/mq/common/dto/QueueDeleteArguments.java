package com.chase.mq.common.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

/**
 * 用来声明删除指定队列的参数
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class QueueDeleteArguments extends BasicArguments implements Serializable {

    private String queueName;

}
