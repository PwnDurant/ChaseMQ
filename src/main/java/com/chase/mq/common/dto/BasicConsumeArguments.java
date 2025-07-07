package com.chase.mq.common.dto;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.io.Serializable;


/**
 * 这个类对应的 basicConsume 方法中，还有一个参数，就是回调函数。（如何处理消息）
 * 这个回调函数是不能通过网络传输的
 * 站在 broker server 角度，针对消息的处理回调，其实是统一的。（把消息返回给客户端）
 * 客户端这边收到消息之后，再在客户端这边执行一个用户自定义回调就行
 * 此时，客户端也不需要把自身的回调告诉服务器来
 * 因此这个类也不需要 consumer 成员
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class BasicConsumeArguments extends BasicArguments implements Serializable {

    private String consumerTag;

    private String queueName;

    private boolean autoAck;

}
