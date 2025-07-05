package com.chase.mq.mqserver.datacenter;


import com.chase.mq.mqserver.core.Binding;
import com.chase.mq.mqserver.core.Exchange;
import com.chase.mq.mqserver.core.MSGQueue;
import com.chase.mq.mqserver.core.Message;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * 使用这个类来管理硬盘上的数据
 * 1,数据库：交换机，绑定，队列
 * 2,数据文件：消息
 * 上层逻辑如果需要操作硬盘，统一通过这个类来使用
 * @Author Chase
 */
public class DiskDataCenter {

    /**
     * 这个实例来管理数据库中的数据
     */
    private final DataBaseManager dataBaseManager = new DataBaseManager();

    /**
     * 这个实例来管理数据文件中的数据
     */
    private final MessageFileManager messageFileManager = new MessageFileManager();

    /**
     * 针对上述操作进行初始化
     * dataBaseManager : 初始化数据库
     * messageFileManager : 初始化消息文件（目前还没有需要操作）
     */
    public void init() {
        dataBaseManager.init();
        messageFileManager.init();
    }

    /**
     * 以下是对交换机的封装操作
     */
    public void insertExchange(Exchange exchange) {
        dataBaseManager.insertExchange(exchange);
    }

    public void deleteExchange(String exchangeName){
        dataBaseManager.deleteExchange(exchangeName);
    }

    public List<Exchange> selectAllExchanges(){
        return dataBaseManager.selectAllExchanges();
    }

    /**
     * 以下是对队列的封装操作
     * 创建队列的同时还需要把队列对象写到数据库中，还需要创建出对应的目录和文件
     * 删除的同时也需要把文件删除
     */
    public void insertQueue(MSGQueue queue) throws IOException {
        dataBaseManager.insertQueue(queue);
        messageFileManager.createQueueFiles(queue.getName());
    }

    public void deleteQueue(String queueName) throws IOException {
        dataBaseManager.deleteQueue(queueName);
        messageFileManager.destroyQueueFiles(queueName);
    }

    public List<MSGQueue> selectAllQueues(){
        return dataBaseManager.selectAllQueue();
    }

    /**
     * 封装绑定操作
     */
    public void insertBinding(Binding binding){
        dataBaseManager.insertBinding(binding);
    }

    public void deleteBinding(Binding binding){
        dataBaseManager.deleteBinding(binding);
    }

    public List<Binding> selectAllBindings(){
        return dataBaseManager.selectAllBindings();
    }

    /**
     * 封装消息操作
     * 在删除消息的时候，检查是否达到 GC 的标准，如果达到了就开始 GC
     */
    public void sendMessage(MSGQueue queue, Message message) throws IOException {
        messageFileManager.sendMessage(queue,message);
    }

    public void deleteMessage(MSGQueue queue,Message message) throws IOException, ClassNotFoundException {
        messageFileManager.deleteMessage(queue,message);
        if(messageFileManager.checkGC(queue.getName()))
            messageFileManager.gc(queue);
    }

    public LinkedList<Message> loadAllMessageFromQueue(String queueName) throws IOException, ClassNotFoundException {
        return messageFileManager.loadAllMessageFromQueue(queueName);
    }

}
