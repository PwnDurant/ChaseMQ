package com.chase.mq.common.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

/**
 * 针对某条消息进行 ack 确认的参数
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class BasicAckArguments extends BasicArguments implements Serializable {

    private String queueName;

    private String messageId;

}
