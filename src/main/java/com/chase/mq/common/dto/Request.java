package com.chase.mq.common.dto;


import lombok.Data;

/**
 * 表示一个网络通讯中的请求对象，按照自定义格式展开
 */
@Data
public class Request {

    /**
     * 0x1:创建 channel
     * 0x2:关闭 channel
     * 0x3:创建 exchange
     * 0x4:删除 exchange
     * 0x5:创建 queue
     * 0x6:删除 queue
     * 0x7:创建 binding
     * 0x8:删除 binding
     * 0x9:发送 message
     * 0xa:订阅 message
     * 0xb:返回 ack
     * 0xc:服务器给客户端推送消息。（被订阅的消息）响应独有的
     */
    private int type;

    /**
     * 表示携带数据（载荷）长度
     */
    private int length;

    /**
     * 真正携带的数据
     */
    private byte[] payload;

}
