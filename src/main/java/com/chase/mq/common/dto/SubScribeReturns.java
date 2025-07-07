package com.chase.mq.common.dto;


import com.chase.mq.mqserver.core.BasicProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

/**
 * 服务器向客户端推送消息
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class SubScribeReturns extends BasicReturns implements Serializable {

    /**
     * 消费的唯一标识
     */
    private String consumerTag;

    /**
     * message 的元数据
     */
    private BasicProperties basicProperties;

    /**
     * 消息体
     */
    private byte[] body;

}
