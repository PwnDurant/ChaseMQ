package com.chase.mq.common.dto;

import com.chase.mq.mqserver.core.ExchangeType;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;
import java.util.Map;

/**
 * 用来创建交换机的声明参数
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class ExchangeDeclareArguments extends BasicArguments implements Serializable {

    /**
     * 交换机名字
     */
    private String exchangeName;

    /**
     * 交换机类型
     */
    private ExchangeType exchangeType;

    /**
     * 交换机是否持久化
     */
    private boolean durable;

    /**
     * 交换机是否是自动删除的
     */
    private boolean autoDelete;

    /**
     * 其他参数
     */
    private Map<String,Object> arguments;

}
