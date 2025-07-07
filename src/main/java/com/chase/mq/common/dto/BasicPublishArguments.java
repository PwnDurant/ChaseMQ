package com.chase.mq.common.dto;


import com.chase.mq.mqserver.core.BasicProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

@Data
@EqualsAndHashCode(callSuper = true)
public class BasicPublishArguments extends BasicArguments implements Serializable {

    private String exchangeName;

    private String routingKey;

    private BasicProperties basicProperties;

    private byte[] body;

}
