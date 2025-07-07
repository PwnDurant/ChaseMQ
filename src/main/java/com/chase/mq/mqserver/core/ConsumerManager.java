package com.chase.mq.mqserver.core;


import com.chase.mq.common.server.Consumer;
import com.chase.mq.common.server.ConsumerEnv;
import com.chase.mq.common.server.MQException;
import com.chase.mq.mqserver.VirtualHost;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * 通过这个类来实现消费消息的核心逻辑
 */
public class ConsumerManager {

    /**
     * 持有上层的 virtualHost 对象的引用，用来操作数据库
     */
    private VirtualHost parent;

    /**
     * 指定一个线程池，负责执行具体的回调任务
     */
    private final ExecutorService workPool =  Executors.newFixedThreadPool(4);

    /**
     * 存放令牌的队列
     */
    private final BlockingQueue<String> tokenQueue = new LinkedBlockingDeque<>();

    /**
     * 扫描线程
     */
    private Thread scannerThread = null;

    /**
     * 构造方法
     */
    public ConsumerManager (VirtualHost parent){
        this.parent = parent;
        scannerThread = new Thread(() -> {
            while(true){
                try{
                    String queueName = tokenQueue.take();
                    MSGQueue queue = parent.getMemoryDataCenter().getQueue(queueName);
                    if(queue == null)
                        throw new MQException("[ConsumerManager] 取令牌后发现, 该队列名不存在! queueName=" + queueName);
                    synchronized (queue){
                        consumeMessage(queue);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        scannerThread.setDaemon(true);
        scannerThread.start();
    }

    /**
     * 通过这个方法来消费一个数据:
     * 1，按照轮询的方式找出一个消费者
     *      当前队列没有消费者，暂时不消费，等后面出现消费者再说
     * 2，从队列中提取一个消息
     *      当前队列没有消息，也不需要消费
     * 3，把消息带入到消费者的回调方法中，丢给线程池执行
     *      把消息放到待确认的集合中，这个操作势必在执行回调之前
     *      真正执行回调操作
     *      如果当前是“自动应答”，就可以直接把消息删除了
     *      如果当前是“手动应答”，就先不处理，交给后续消费者调用 basicAck 方法处理
     *          删除硬盘上的数据
     *          删除上面待确认集合中消息
     *          删除内存中消息中心的消息
     * @param queue 要被消费消息的队列
     */
    private void consumeMessage (MSGQueue queue){
        ConsumerEnv luckDog = queue.chooseConsumer();
        if(luckDog == null) return;
        Message message = parent.getMemoryDataCenter().pollMessage(queue.getName());
        if(message == null) return;
        workPool.submit(() -> {
            try{
                parent.getMemoryDataCenter().addMessageWaitAck(queue.getName(),message);
                luckDog.getConsumer().handleDelivery(luckDog.getConsumerTag(), message.getBasicProperties(),
                        message.getBody());
                if(luckDog.isAutoAck()){
                    if(message.getDeliverModel() == 2)
                        parent.getDiskDataCenter().deleteMessage(queue,message);
                    parent.getMemoryDataCenter().removeMessageWaitAck(queue.getName(),message.getMessageId());
                    parent.getMemoryDataCenter().removeMessage(message.getMessageId());
                    System.out.println("[ConsumerManager] 消息被成功消费! queueName=" + queue.getName());
                }
            }catch (Exception e){
                throw new RuntimeException();
            }
        });
    }

    /**
     * 这个方法的调用时机就是发送消息的时候
     */
    public void notifyConsume(String queueName) throws InterruptedException {
        tokenQueue.put(queueName);
    }

    /**
     * 添加消费者：
     * 1，找到对应队列
     * 2，如果当前队列中已经有一些消息了，需要立即消费掉
     * 3，这个方法调用一次就消费一条消息
     */
    public void addConsumer(String consumerTag, String queueName, boolean autoAck, Consumer consumer){
        MSGQueue queue = parent.getMemoryDataCenter().getQueue(queueName);
        if(queue == null)
            throw new MQException("[ConsumerManager] 队列不存在! queueName=" + queueName);
        ConsumerEnv consumerEnv = new ConsumerEnv(consumerTag, queueName, autoAck, consumer);
        synchronized (queue){
            queue.addConsumerEnv(consumerEnv);
            int messageCount = parent.getMemoryDataCenter().getMessageCount(queueName);
            for (int i = 0; i < messageCount; i++) {
                consumeMessage(queue);
            }
        }
    }

}
