package com.chase.mq.common.dto;


import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * 使用这个类表示方法的公共参数/辅助的字段
 * 后续每个方法又会又一些不同的参数，不同的参数再分别使用不同的子类来表示
 */
@Setter
@Getter
public class BasicArguments implements Serializable {

    /**
     * 表示一次请求/响应的身份标识，可以把请求和响应对上
     */
    private String rid;

    /**
     * 这次通讯使用的 channel 的身份标识
     */
    private String channelId;

}
