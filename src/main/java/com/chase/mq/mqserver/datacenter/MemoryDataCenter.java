package com.chase.mq.mqserver.datacenter;


import com.chase.mq.common.server.MQException;
import com.chase.mq.mqserver.core.Binding;
import com.chase.mq.mqserver.core.Exchange;
import com.chase.mq.mqserver.core.MSGQueue;
import com.chase.mq.mqserver.core.Message;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 这个类来统一管理内存中的所有数据
 * 该类后续提供一些方法，可能会在多线程环境下被使用，因此需要注意线程安全问题
 * @Author Chase
 */
public class MemoryDataCenter {

    /**
     * 交换机映射表 :
     * key : exchangeName
     * value : Exchange 对象
     * 使用 final 关键字修饰的引用类型变量，表示这个变量的的引用地址不能变，
     * 但是它引用对象的内容是可以变化的
     */
    private final ConcurrentHashMap<String, Exchange> exchangeMap = new ConcurrentHashMap<>();

    /**
     * 消息队列映着表 :
     * key : queueName
     * value : MSGQueue 对象
     */
    private final ConcurrentHashMap<String, MSGQueue> queueMap = new ConcurrentHashMap<>();

    /**
     * 绑定关系映射表 :
     * key1 : exchangeName
     * key2 : queueName
     * value : binding 对象
     */
    private final ConcurrentHashMap<String,ConcurrentHashMap<String, Binding>> bindingsMap = new ConcurrentHashMap<>();

    /**
     * 消息映射表 :
     * key : messageId
     * value : Message 对象
     */
    private final ConcurrentHashMap<String, Message> messageMap = new ConcurrentHashMap<>();

    /**
     * 队列消息映射表 :
     * key1 : queueName
     * value : Message 链表
     */
    private final ConcurrentHashMap<String, LinkedList<Message>> queueMessageMap = new ConcurrentHashMap<>();

    /**
     * 等效确认消息映射表 :
     * key1 : queueName
     * key2 : messageId
     * value : Message 对象（未确认）
     */
    private final ConcurrentHashMap<String,ConcurrentHashMap<String,Message>> queueMessageWaitAckMap = new ConcurrentHashMap<>();


    /**
     * 新增交换机
     * @param exchange 需要新增的交换机对象
     */
    public void insertExchange(Exchange exchange){
        exchangeMap.put(exchange.getName(),exchange);
        System.out.println("[MemoryDataCenter] 新交换机添加成功! exchangeName=" + exchange.getName());
    }

    /**
     * 获取指定交换机
     * @param exchangeName 要获取的交换机的名字（主键）
     * @return 如果存在则返回指定交换机
     */
    public Exchange getExchange(String exchangeName) {
        return exchangeMap.get(exchangeName);
    }

    /**
     * 删除指定交换机
     * @param exchangeName 要被删除的交换机的名字
     */
    public void deleteExchange(String exchangeName){
        exchangeMap.remove(exchangeName);
        System.out.println("[MemoryDataCenter] 交换机删除成功! exchangeName=" + exchangeName);
    }

    /**
     * 新增消息队列
     * @param queue 要新增的队列
     */
    public void insertQueue(MSGQueue queue){
        queueMap.put(queue.getName(),queue);
        System.out.println("[MemoryDataCenter] 新队列添加成功! queueName=" + queue.getName());
    }

    /**
     * 删除指定消息队列
     * @param queueName 要被删除的队列名
     */
    public void deleteQueue(String queueName){
        queueMap.remove(queueName);
        System.out.println("[MemoryDataCenter] 队列删除成功! queueName = " + queueName);
    }

    /**
     * 获取指定队列
     * @param queueName 要获取的队列名
     * @return 返回指定队列
     */
    public MSGQueue getQueue(String queueName){
        return queueMap.get(queueName);
    }

    /**
     * 新增绑定关系
     * computeIfAbsent : 是 Java8 中引入 Map 接口中方法，
     * 用来判断某个 key 是否存在，不存在就自动计算并插入对应的值
     * @param binding 需要新增的绑定关系
     */
    public void insertBinding(Binding binding){
        ConcurrentHashMap<String, Binding> bindingMap =
                bindingsMap.computeIfAbsent(binding.getExchangeName(),
                k -> new ConcurrentHashMap<>());
        synchronized (bindingMap){
            if(bindingMap.get(binding.getQueueName()) != null)
                throw new MQException("[MemoryDataCenter] 绑定已经存在! exchangeName=" +
                        binding.getExchangeName() + ", queueName=" + binding.getQueueName());
            bindingMap.put(binding.getQueueName(),binding);
        }
        System.out.println("[MemoryDataCenter] 新绑定添加成功! exchangeName=" +
                binding.getExchangeName() + ", queueName=" + binding.getQueueName());
    }

    /**
     * 获取绑定关系对象
     * @param exchangeName 指定的交换机
     * @param queueName 指定的队列
     * @return 根据 exchangeName 和 queueName 确定唯一一个 Binding
     */
    public Binding getBinding(String exchangeName,String queueName){
        ConcurrentHashMap<String, Binding> bindingMap = bindingsMap.get(exchangeName);
        if(bindingMap == null) return null;
        return bindingMap.get(queueName);
    }

    /**
     * 获取所有绑定关系
     * @param exchangeName 指定的 exchange
     * @return 获取指定 exchange 下的所有绑定对象
     */
    public ConcurrentHashMap<String,Binding> getBindings(String exchangeName){
        return bindingsMap.get(exchangeName);
    }

    /**
     * 删除绑定关系
     * @param binding 要删除的 Binding 对象
     */
    public void deleteBinding(Binding binding){
        ConcurrentHashMap<String, Binding> bindingMap = bindingsMap.get(binding.getExchangeName());
//        该交换机没有绑定任何队列
        if(bindingMap == null)
            throw new MQException("[MemoryDataCenter] 绑定不存在! exchangeName=" + binding.getExchangeName()
                    + ", queueName=" + binding.getQueueName());
        bindingMap.remove(binding.getQueueName());
        System.out.println("[MemoryDataCenter] 绑定删除成功! exchangeName=" + binding.getExchangeName()
                + ", queueName=" + binding.getQueueName());
    }

    /**
     * 添加消息
     * @param message 需要被添加的消息对象
     */
    public void addMessage(Message message){
        messageMap.put(message.getMessageId(), message);
        System.out.println("[MemoryDataCenter] 新消息添加成功! messageId=" + message.getMessageId());
    }

    /**
     * 获取指定消息
     * @param messageId 指定消息的消息 ID
     * @return 返回对应 messageId 的 Message 对象
     */
    public Message getMessage(String messageId){
        if(messageMap.get(messageId) == null)
            throw new MQException("[MemoryDataCenter] 对应消息不存在! messageId = " + messageId);
        return messageMap.get(messageId);
    }

    /**
     * 根据 Id 删除消息
     * @param messageId 要被删除的消息 ID
     */
    public void removeMessage(String messageId){
        messageMap.remove(messageId);
        System.out.println("[MemoryDataCenter] 消息被移除! messageId=" + messageId);
    }

    /**
     * 发动消息到指定队列
     * 在插入完之后还需要在消息中心也插入一下
     * @param queue 指定队列
     * @param message 要发送的消息
     */
    public void sendMessage(MSGQueue queue,Message message){
        LinkedList<Message> messages = queueMessageMap.computeIfAbsent(queue.getName(),
                k -> new LinkedList<>());
        synchronized (messages) {
            messages.add(message);
        }
        addMessage(message);
        System.out.println("[MemoryDataCenter] 消息被投递到队列中! messageId = " + message.getMessageId());
    }

    /**
     * 从队列中取出消息 (FIFO)
     * @param queueName 需要被消费的队列
     * @return 返回队列的第一个元素
     */
    public Message pollMessage(String queueName){
        LinkedList<Message> messages = queueMessageMap.get(queueName);
        if(messages == null || messages.isEmpty()) return null;
        synchronized (messages){
            Message message = messages.remove(0);
            System.out.println("[MemoryDataCenter] 消息从队列中取出! messageId=" + message.getMessageId());
            return message;
        }
    }

    /**
     * 获取指定队列的消息个数
     * @param queueName 指定的队列名
     * @return 返回队列中的消息个数
     */
    public int getMessageCount(String queueName){
        LinkedList<Message> messages = queueMessageMap.get(queueName);
        if(messages == null) return 0;
        synchronized (messages){
            return messages.size();
        }
    }

    /**
     * 添加未确认的消息
     * @param queueName 指定的队列名
     * @param message 要添加的消息
     */
    public void addMessageWaitAck(String queueName,Message message){
        ConcurrentHashMap<String, Message> messagesHashMap = queueMessageWaitAckMap.computeIfAbsent(queueName,
                k -> new ConcurrentHashMap<>());
        messagesHashMap.put(message.getMessageId(),message);
        System.out.println("[MemoryDataCenter] 消息进入待确认队列! messageId=" + message.getMessageId());
    }

    /**
     * 移除未确认消息
     * @param queueName 要移除消息的队列名
     * @param messageId 消息 ID
     */
    public void removeMessageWaitAck(String queueName,String messageId){
        ConcurrentHashMap<String, Message> messagesHashMap = queueMessageWaitAckMap.get(queueName);
        if(messagesHashMap == null) return;
        messagesHashMap.remove(messageId);
        System.out.println("[MemoryDataCenter] 消息从待确认队列删除! messageId=" + messageId);
    }

    /**
     * 获取未确认的消息
     * @param queueName 指定获取消息的队列
     * @param messageId 指定的消息 id
     * @return 返回指定的消息
     */
    public Message getMessageWaitAck(String queueName,String messageId){
        ConcurrentHashMap<String, Message> messagesHashMap = queueMessageWaitAckMap.get(queueName);
        if(messagesHashMap == null) return null;
        return messagesHashMap.get(messageId);
    }

    /**
     * 从硬盘上读取数据，把硬盘中之前持久化存储的各个维度的数据恢复到内存中
     * 1,清空之前的所有数据
     * 2,恢复所有交换机的数据
     * 3,恢复所有队列的数据
     * 4,恢复所有绑定数据
     * 5,恢复所有消息数据
     * 注意:针对“未确认的消息”这部分内存中的数据，不需要从硬盘中恢复
     *      一旦在等待 ack 的过程中，服务器重启了，此时这些“未被确认的消息”，就恢复成“未被取走的消息”
     * @param diskDataCenter 操作硬盘信息的工具类
     */
    public void recovery(DiskDataCenter diskDataCenter) throws IOException, ClassNotFoundException {
        clear();
        List<Exchange> exchanges = diskDataCenter.selectAllExchanges();
        for (Exchange exchange : exchanges)
            exchangeMap.put(exchange.getName(),exchange);
        List<MSGQueue> msgQueues = diskDataCenter.selectAllQueues();
        for (MSGQueue msgQueue : msgQueues)
            queueMap.put(msgQueue.getName(), msgQueue);
        List<Binding> bindings = diskDataCenter.selectAllBindings();
        for (Binding binding : bindings) {
            ConcurrentHashMap<String, Binding> bindingMap = bindingsMap.computeIfAbsent(binding.getExchangeName(),
                    k -> new ConcurrentHashMap<>());
            bindingMap.put(binding.getQueueName(),binding);
        }
        for (MSGQueue msgQueue : msgQueues) {
            LinkedList<Message> messages = diskDataCenter.loadAllMessageFromQueue(msgQueue.getName());
            queueMessageMap.put(msgQueue.getName(),messages);
            for (Message message : messages)
                messageMap.put(message.getMessageId(),message);
        }
    }

    /**
     * 清空内存数据
     */
    private void clear(){
        exchangeMap.clear();
        queueMap.clear();
        bindingsMap.clear();
        messageMap.clear();
        queueMessageMap.clear();
    }



}
