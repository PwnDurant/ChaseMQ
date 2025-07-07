package com.chase.mq.mqserver;

import com.chase.mq.common.server.Consumer;
import com.chase.mq.common.server.MQException;
import com.chase.mq.mqserver.core.*;
import com.chase.mq.mqserver.datacenter.DiskDataCenter;
import com.chase.mq.mqserver.datacenter.MemoryDataCenter;
import lombok.Getter;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 通过这个类来表示虚拟主机
 * 每个虚拟主机啊下面都管理着自己的 交换机，队列，绑定，消息，数据
 * 同时提供 API 供上层调用
 * 针对 VirtualHost 这个类，作为业务逻辑的整合，就需要对于代码中抛出的异常进行处理了
 */
@Getter
public class VirtualHost {

    /**
     * 虚拟主机名
     */
    private final String virtualHostName;

    /**
     * 操作内存数据的核心类
     */
    private final MemoryDataCenter memoryDataCenter = new MemoryDataCenter();

    /**
     * 操作磁盘数据的核心类
     */
    private final DiskDataCenter diskDataCenter = new DiskDataCenter();

    /**
     * 用来比对路由的核心类
     */
    private final Router router = new Router();

    /**
     * 消费核心类
     */
    private final ConsumerManager consumerManager = new ConsumerManager(this);

    /**
     * 操作交换机的锁对象
     */
    private final Object exchangeLocker = new Object();

    /**
     * 操作队列的锁对象
     */
    private final Object queueLocker = new Object();

    /**
     * 在创建虚拟主机的时候指定主机名就行
     * 对于 MemoryDataCenter 来说，不需要额外的初始化操作，只需要 new 出来就行
     * 对于 DiskDataCenter 来说，需要进行初始化操作，建库建表和初始数据约定
     * 并且要对硬盘中的数据进行恢复到内存中去
     * @param name 指定虚拟主机的主机名
     */
    public VirtualHost (String name) {
        this.virtualHostName = name;
        diskDataCenter.init();
        try{
            memoryDataCenter.recovery(diskDataCenter);
        } catch (IOException | ClassNotFoundException e){
            System.out.println("[VirtualHost] 恢复内存数据失败! 错误信息:" + e.getMessage());
        }
    }

    /**
     * 用来声明一个指定的交换机:
     * 1，通过内存查询该交换机是否已经存在
     * 2，真正创建交换机，先构造 Exchange 对象
     * 3，把交换机写入硬盘中
     * 4，把交换机对象写入内存中
     * 注：先硬盘，后内存，目的是为了硬盘容易写失败，如果硬盘失败了就直接报错，内存也不用再写了
     * @param exchangeName 指定交换机的名字 -> 会加上虚拟主机作为前缀
     * @param exchangeType 指定交换机的类型
     * @param durable 是否持久化
     * @param autoDelete 是否自动删除
     * @param arguments 其他参数
     * @return 是否创建成功
     */
    public boolean exchangeDeclare(String exchangeName, ExchangeType exchangeType,
                                   boolean durable, boolean autoDelete, Map<String,Object> arguments){
        exchangeName = virtualHostName + exchangeName;
        try{
            synchronized (exchangeLocker){
                Exchange existstExchange = memoryDataCenter.getExchange(exchangeName);
                if(existstExchange != null){
                    System.out.println("[VirtualHost] 交换机已经存在! exchangeName=" + exchangeName);
                    return true;
                }
                Exchange exchange = new Exchange();
                exchange.setName(exchangeName);
                exchange.setType(exchangeType);
                exchange.setDurable(durable);
                exchange.setAutoDelete(autoDelete);
                exchange.setArguments(arguments);
                if(durable) diskDataCenter.insertExchange(exchange);
                memoryDataCenter.insertExchange(exchange);
                System.out.println("[VirtualHost] 交换机创建完成! exchangeName=" + exchangeName);
            }
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost] 交换机创建失败! exchangeName=" + exchangeName +
                    " 错误信息：" + e.getMessage());
            return false;
        }
    }

    /**
     * 删除交换机:
     * 1，先找对应的交换机
     * 2，删除硬盘上的数据
     * 3，删除内存中的交换机数据
     * @param exchangeName 被删除交换机的名字
     * @return 是否删除成功
     */
    public boolean exchangeDelete(String exchangeName){
        exchangeName = virtualHostName + exchangeName;
        try{
            synchronized (exchangeLocker){
                Exchange toDelete = memoryDataCenter.getExchange(exchangeName);
                if(toDelete == null)
                    throw new MQException("[VirtualHost] 交换机不存在无法删除!");
                if(toDelete.isDurable())
                    diskDataCenter.deleteExchange(exchangeName);
                memoryDataCenter.deleteExchange(exchangeName);
                System.out.println("[VirtualHost] 交换机删除成功! exchangeName=" + exchangeName);
            }
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost] 交换机删除失败! exchangeName=" + exchangeName +
                    "错误原因:" + e.getMessage());
            return false;
        }
    }

    /**
     * 用来声明一个队列
     * 1，判定队列是否存在
     * 2，创建队列对象
     * 3，写入硬盘
     * 4，写入内存
     * @param queueName 队列名 -> 前面需要拼接上虚拟主机名字
     * @param durable 是否持久化
     * @param exclusive true:只能被一个消费者使用
     *                  false:可以被所有消费者使用
     * @param autoDelete 如果该队列没有人使用的话就删除
     * @param arguments 其他拓展参数
     * @return 是否创建成功
     */
    public boolean queueDeclare(String queueName,boolean durable,boolean exclusive,
                                boolean autoDelete,Map<String,Object> arguments){
        queueName = virtualHostName + queueName;
        try{
            synchronized (queueLocker){
                MSGQueue existsQueue = memoryDataCenter.getQueue(queueName);
                if (existsQueue != null) {
                    System.out.println("[VirtualHost] 队列已经存在! queueName=" + queueName);
                    return true;
                }
                MSGQueue queue = new MSGQueue();
                queue.setName(queueName);
                queue.setDurable(durable);
                queue.setExclusive(exclusive);
                queue.setAutoDelete(autoDelete);
                queue.setArguments(arguments);
                if(durable)
                    diskDataCenter.insertQueue(queue);
                memoryDataCenter.insertQueue(queue);
                System.out.println("[VirtualHost] 队列创建成功! queueName=" + queueName);
            }
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost] 队列创建失败! queueName=" + queueName +
                    "错误原因：" + e.getMessage());
            return false;
        }
    }

    /**
     * 删除指定队列
     * @param queueName 队列名
     * @return 是否删除成功
     */
    public boolean queueDelete(String queueName){
        queueName = virtualHostName + queueName;
        try{
            synchronized (queueLocker){
                MSGQueue toDelete = memoryDataCenter.getQueue(queueName);
                if(toDelete == null)
                    throw new MQException("[VirtualHost] 交换机不存在无法删除!");
                if(toDelete.isDurable())
                    diskDataCenter.deleteQueue(queueName);
                memoryDataCenter.deleteQueue(queueName);
                System.out.println("[VirtualHost] 删除队列成功! queueName=" + queueName);
            }
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost] 删除队列失败! queueName=" + queueName +
                    "错误原因：" + e.getMessage());
            return false;
        }
    }

    /**
     * 绑定队列:
     * 1，判断当前绑定关系是否已经存在
     * 2，验证 bindingKey 是否合法
     * 3，创建 Binding 对象
     * 4，获取对应的交换机和队列查看是否存在
     * 5，先写入硬盘再写入内存
     * @param queueName 被绑定的队列名
     * @param exchangeName 被绑定的交换机名
     * @param bindingKey 指定路由键
     * @return 是否绑定成功
     */
    public boolean queueBind(String queueName,String exchangeName,String bindingKey){
        queueName = virtualHostName + queueName;
        exchangeName = virtualHostName + exchangeName;
        try{
            synchronized (exchangeLocker){
                synchronized (queueLocker){
                    Binding existsBinding = memoryDataCenter.getBinding(exchangeName, queueName);
                    if(existsBinding != null)
                        throw new MQException("[VirtualHost] binding 已经存在! queueName=" + queueName
                                + ", exchangeName=" + exchangeName);
                    if(!router.checkBindingKey(bindingKey))
                        throw new MQException("[VirtualHost] bindingKey 非法! bindingKey=" + bindingKey);
                    Binding binding = new Binding();
                    binding.setExchangeName(exchangeName);
                    binding.setQueueName(queueName);
                    binding.setBindingKey(bindingKey);
                    MSGQueue queue = memoryDataCenter.getQueue(queueName);
                    if (queue == null)
                        throw new MQException("[VirtualHost] 队列不存在! queueName=" + queueName);
                    Exchange exchange = memoryDataCenter.getExchange(exchangeName);
                    if (exchange == null)
                        throw new MQException("[VirtualHost] 交换机不存在! exchangeName=" + exchangeName);
                    if(queue.isDurable() && exchange.isDurable())
                        diskDataCenter.insertBinding(binding);
                    memoryDataCenter.insertBinding(binding);
                }
            }
            System.out.println("[VirtualHost] 绑定创建成功! exchangeName=" + exchangeName
                    + ", queueName=" + queueName);
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost] 绑定创建失败! exchangeName=" + exchangeName
                    + ", queueName=" + queueName);
            return false;
        }
    }

    /**
     * 解除绑定关系
     * 1，查看绑定关系是否已经存在
     * 2，无论绑定是否持久化，都要从硬盘中删除一份
     * 3，删除内存中的数据
     * @param queueName 被解除的绑定队列
     * @param exchangeName 被解除的绑定交换机
     * @return 是否解除成功
     */
    public boolean queueUnbind(String queueName,String exchangeName){
        queueName = virtualHostName + queueName;
        exchangeName = virtualHostName + exchangeName;
        try{
            synchronized (exchangeLocker){
                synchronized (virtualHostName){
                    Binding binding = memoryDataCenter.getBinding(exchangeName, queueName);
                    if(binding == null)
                        throw new MQException("[VirtualHost] 删除绑定失败! 绑定不存在! exchangeName=" + exchangeName + ", queueName=" + queueName);
                    diskDataCenter.deleteBinding(binding);
                    memoryDataCenter.deleteBinding(binding);
                    System.out.println("[VirtualHost] 删除绑定成功!");
                }
            }
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost] 删除绑定失败!" +
                    "错误原因：" + e.getMessage());
            return false;
        }

    }

    /**
     * 发送消息到指定队列中:
     * 1，转换名字并检查 routerKey 是否合法
     * 2，查找交换机对象并判断交换机类型
     * 3，构造消息对象
     * 4，查找该队列名对应的对象
     * 5，队列存在，直接给队列中写入消息
     * 6，找到该交换机关联的所有绑定，并遍历这些绑定对象
     * @param exchangeName 通过指定交换机发送
     * @param routingKey 密钥
     * @param basicProperties 元数据信息
     * @return 是否发送成功
     */
    public boolean basicPublish(String exchangeName, String routingKey, BasicProperties basicProperties,byte[] body){
        try{
            exchangeName = virtualHostName + exchangeName;
            if(!router.checkBindingKey(routingKey))
                throw new MQException("[VirtualHost] routingKey 非法! routingKey=" + routingKey);
            Exchange exchange = memoryDataCenter.getExchange(exchangeName);
            if(exchange == null)
                throw new MQException("[VirtualHost] 交换机不存在! exchangeName=" + exchangeName);
            if(exchange.getType() == ExchangeType.DIRECT){
                String queueName = virtualHostName + routingKey;
                Message message = Message.createMessageWithId(routingKey, basicProperties, body);
                MSGQueue queue = memoryDataCenter.getQueue(queueName);
                if(queue == null)
                    throw new MQException("[VirtualHost] 队列不存在! queueName=" + queueName);
                sendMessage(queue,message);
            }else{
                ConcurrentHashMap<String, Binding> bindings = memoryDataCenter.getBindings(exchangeName);
                for (Map.Entry<String,Binding> entry : bindings.entrySet()){
                    Binding binding = entry.getValue();
                    MSGQueue queue = memoryDataCenter.getQueue(binding.getQueueName());
                    if(queue == null){
                        System.out.println("[VirtualHost] basicPublish 发送消息时, 发现队列不存在! queueName=" + binding.getQueueName());
                        continue;
                    }
                    Message message = Message.createMessageWithId(routingKey, basicProperties, body);
                    if(!router.route(exchange.getType(),binding,message))
                        continue;
                    sendMessage(queue,message);
                }
            }
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost] 消息发送失败!" +
                    "错误原因：" + e.getMessage());
            return false;
        }
    }

    /**
     * 发送消息到指定队列：
     * 这里发送消息就是写入到 硬盘 和 内存上面
     * deliverMode : 交付模式 1 -> 不持久化  2 -> 持久化存储
     * @param queue 指定队列
     * @param message 所发送的消息
     */
    private void sendMessage(MSGQueue queue, Message message) throws IOException, InterruptedException {
        int deliverModel = message.getDeliverModel();
        if(deliverModel == 2)
            diskDataCenter.sendMessage(queue,message);
        memoryDataCenter.sendMessage(queue,message);
        consumerManager.notifyConsume(queue.getName());
    }

    /**
     * 订阅消息
     * @param consumerTag 消费者的身份标识
     * @param queueName 订阅消息的队列
     * @param autoAck 是否是自动应当
     * @param consumer 回调函数，此处类型设置为函数式接口，这样后续调用 basicConsume 传入实参的时候，就可以写作 lambda 样子
     * @return 是否订阅成功
     */
    public boolean basicConsume(String consumerTag, String queueName, boolean autoAck, Consumer consumer){
        queueName = virtualHostName + queueName;
        try{
            consumerManager.addConsumer(consumerTag,queueName,autoAck,consumer);
            System.out.println("[VirtualHost] basicConsume 成功! queueName=" + queueName);
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost] basicConsume 失败! queueName=" + queueName);
            return false;
        }
    }

    /**
     * 针对哪个队列的哪个消息进行 ack 确认：
     * 1,获取到消息和队列
     * 2，删除硬盘上的数据
     * 3，删除消息中心的数据
     * 4，删除待确认的集合中的数据
     */
    public boolean basicAck(String queueName,String messageId){
        queueName = virtualHostName + queueName;
        try{
            Message message = memoryDataCenter.getMessage(messageId);
            if(message == null) throw new MQException("[VirtualHost] 要确认的消息不存在! messageId=" + messageId);
            MSGQueue queue = memoryDataCenter.getQueue(queueName);
            if(queue == null) throw new MQException("[VirtualHost] 要确认的队列不存在! queueName=" + queueName);
            if(message.getDeliverModel() == 2) diskDataCenter.deleteMessage(queue,message);
            memoryDataCenter.removeMessage(messageId);
            memoryDataCenter.removeMessageWaitAck(queueName,messageId);
            System.out.println("[VirtualHost] basicAck 成功! 消息被成功确认! queueName=" + queueName
                    + ", messageId=" + messageId);
            return true;
        }catch (Exception e){
            System.out.println("[VirtualHost] basicAck 失败! 消息确认失败! queueName=" + queueName
                    + ", messageId=" + messageId);
            return false;
        }
    }

}
