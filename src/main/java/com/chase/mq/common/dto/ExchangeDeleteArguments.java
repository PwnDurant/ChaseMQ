package com.chase.mq.common.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;

/**
 * 用来删除交换机的声明参数
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class ExchangeDeleteArguments extends BasicArguments implements Serializable {

    private String exchangeName;


}
