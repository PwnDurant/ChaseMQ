package com.chase.mq.mqserver.core;

import com.chase.mq.common.ConsumerEnv;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 消息队列
 */
@Data
public class MSGQueue {

    private static ObjectMapper obj = new ObjectMapper();

//    标识队列的唯一身份标识
    private String name;

//    队列是否持久化
    private boolean durable;

//    这个属性为 true，表示这个队列只能被一个消费者所使用，如果为 false 表示大家都可以使用（后续拓展）
    private boolean exclusive;

//    如果该队列没有人使用的话就自动删除（后续拓展）
    private boolean autoDelete = false;

//    创建队列时指定的一些额外的选项（后续拓展）
    private Map<String,Object> arguments =  new HashMap<>();

//    当前队列有哪些消费者订阅了
    private List<ConsumerEnv> consumerEnvList = new ArrayList<>();

//    添加一个新的订阅者
    private AtomicInteger consumerSeq = new AtomicInteger(0);

    /**
     * 添加一个新的订阅者
     * @param consumerEnv 填加的新的订阅者
     */
    public void addConsumerEnv(ConsumerEnv consumerEnv){
        consumerEnvList.add(consumerEnv);
    }

    /**
     * 挑选这个订阅者，用来处理当前消息（按照轮询方式）
     */
    public ConsumerEnv chooseConsumer() {
        if(consumerEnvList.isEmpty())
            return null;
        int index = consumerSeq.get() % consumerEnvList.size();
        consumerSeq.getAndIncrement();
        return consumerEnvList.get(index);
    }

    public String getArguments(){
        try{
            return obj.writeValueAsString(arguments);
        }catch (JsonProcessingException e){
            e.printStackTrace();
        }
        return "{}";
    }

    public Object getArguments(String key){
        return arguments.get(key);
    }

    public void setArguments(String arguments){
        try{
            this.arguments = obj.readValue(arguments, new TypeReference<Map<String, Object>>() {});
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public void setArguments(Map<String,Object> arguments){
        this.arguments = arguments;
    }

    public void setArguments(String key,Object value){
        arguments.put(key, value);
    }

}
