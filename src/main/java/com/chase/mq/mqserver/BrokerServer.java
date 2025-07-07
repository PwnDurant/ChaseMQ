package com.chase.mq.mqserver;


import com.chase.mq.common.dto.*;
import com.chase.mq.common.server.BinaryTool;
import com.chase.mq.common.server.Consumer;
import com.chase.mq.common.server.MQException;
import com.chase.mq.mqserver.core.BasicProperties;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 这个 BrokerServer 就是消息队列本体服务器
 * 本纸上就是一个 TCP 服务器
 */
public class BrokerServer {

    /**
     * socket 套接字
     */
    private ServerSocket serverSocket = null;

    /**
     * 一个 broker server 上可以存在多个 virtualHost ，本服务器只实现了一个 可以拓展
     */
    private VirtualHost virtualHost = new VirtualHost("default");

    /**
     * 使用这个来代替一个 connection 中存在的多个 channel （也就是说有哪些客户端正在和服务器通讯）
     * 此处的 key 是 channelId，value 为对应的 Socket 对象
     */
    private ConcurrentHashMap<String, Socket> sessions = new ConcurrentHashMap<>();

    /**
     * 引入一个线程池，来处理多个客户端请求
     */
    private ExecutorService executorService = null;

    /**
     * 引入 boolean 变量控制服务器是否继续运行
     */
    private volatile boolean runnable = true;

    /**
     * 构造方法
     * @param port 指定运行端口
     */
    public BrokerServer(int port) throws IOException {
        serverSocket = new ServerSocket(port);
    }

    /**
     * 启动服务器
     */
    public void start() throws IOException{
        System.out.println("[BrokerServer] 启动！");
        executorService = Executors.newCachedThreadPool();
        try{
            while(runnable){
                Socket clientSocket = serverSocket.accept();
//                把处理连接的逻辑放在这个线程池中
                System.out.println("[BrokerServer] 接收到请求");
                executorService.submit(() ->{
                    processConnection(clientSocket);
                });
            }
        }catch (SocketException e){
            System.out.println("[BrokerServer] 服务器停止运行！");
        }
    }

    /**
     * 一般来说停止服务器直接 kill 对应的 PID 就行
     * 此处单独创建一个方法用于后续的单元测试操作
     */
    public void stop() throws IOException {
        runnable = false;
//        把线程池中的任务放弃，让线程都销毁
        executorService.shutdownNow();
        serverSocket.close();
    }

    /**
     * 通过这个方法，来处理一个客户端的连接
     * 在这一个连接中可能会涉及到多个请求和响应
     * 由于这里需要按照指定格式来读取，所以需要使用到 DataInputStream /DataOutputStream
     * 来读取指定长度
     * 1，读取请求并指定长度
     * 2，根据请求计算响应
     * 3，把响应写回客户端
     * @param clientSocket 一个客户端连接
     */
    private void processConnection(Socket clientSocket) {
        try(InputStream inputStream = clientSocket.getInputStream();
            OutputStream outputStream = clientSocket.getOutputStream()){
            try (DataInputStream dataInputStream = new DataInputStream(inputStream);
                 DataOutputStream dataOutputStream = new DataOutputStream(outputStream)){
                while(true){
                    Request request = readRequest(dataInputStream);
                    Response response = process(request,clientSocket);
                    writeResponse(dataOutputStream, response);
                }
            }
        }catch (EOFException | SocketException exception){
            System.out.println("[BrokerServer] connection 关闭! 客户端的地址: " + clientSocket.getInetAddress().toString()
                    + ":" + clientSocket.getPort());
        }catch (IOException | ClassNotFoundException | MQException exception){
            System.out.println("[BrokerServer] connection 出现异常! 错误原因： "+exception.getMessage());
        }finally {
            try{
//                当前连接处理完，关闭 socket
                clientSocket.close();
//                一个 TCP 连接，可能包含多个 channel
//                因此需要把当前这个 socket 对应的所有 channel 也顺便清理完
                clearClosedSession(clientSocket);
            }catch (IOException e){
                System.out.println("[BrokerServer] connection 关闭异常！错误原因 : " + e.getMessage());
            }
        }
    }

    /**
     * 读取请求并指定长度
     */
    private Request readRequest(DataInputStream dataInputStream) throws IOException {
        System.out.println("[BrokerServer] 读取到请求");
        Request request = new Request();
        request.setType(dataInputStream.readInt());
        request.setLength(dataInputStream.readInt());
        byte[] payload = new byte[request.getLength()];
        int n = dataInputStream.read(payload);
        if(n != request.getLength())
            throw new IOException("[BrokerServer] 读取请求格式出错！");
        request.setPayload(payload);
        return request;
    }

    /**
     * 根据请求计算响应
     * @see com.chase.mq.common.dto.Request
     */
    private Response process(Request request, Socket clientSocket) throws IOException, ClassNotFoundException {
        System.out.println("[BrokerServer] 根据请求计算响应");
//        1，把 request 中的 payload 做个初步解析
        BasicArguments basicArguments = (BasicArguments) BinaryTool.fromBytes(request.getPayload());
        System.out.println("[Request] rid = " + basicArguments.getRid() + ", channelId = " + basicArguments.getChannelId()
        + ", type = " + request.getType() + ",length = " + request.getLength());
//        2，根据 type 值，来进一步区分接下来这次请求要干嘛
        boolean ok = true;
        if(request.getType() == 0x1) {
//            创建 channel
            sessions.put(basicArguments.getChannelId(), clientSocket);
            System.out.println("[BrokerServer] 创建 channel 完成! channelId= " + basicArguments.getChannelId());
        } else if (request.getType() == 0x2) {
//            销毁 channel
            sessions.remove(basicArguments.getChannelId());
            System.out.println("[BrokerServer] 销毁 channel 完成! channelId= " + basicArguments.getChannelId());
        } else if (request.getType() == 0x3) {
//            创建交换机，此时 payload 就是 ExchangeDeclareArguments 对象
            ExchangeDeclareArguments arguments = (ExchangeDeclareArguments) basicArguments;
            ok = virtualHost.exchangeDeclare(arguments.getExchangeName(), arguments.getExchangeType(),
                    arguments.isDurable(),arguments.isAutoDelete(),arguments.getArguments());
        } else if (request.getType() == 0x4) {
//            销毁交换机
            ExchangeDeleteArguments arguments = (ExchangeDeleteArguments) basicArguments;
            ok = virtualHost.exchangeDelete(arguments.getExchangeName());
        } else if (request.getType() == 0x5){
//            创建队列
            QueueDeclareArguments arguments = (QueueDeclareArguments) basicArguments;
            ok = virtualHost.queueDeclare(arguments.getQueueName(),arguments.isDurable(), arguments.isExclusive(),
                    arguments.isAutoDelete(),arguments.getArguments());
        } else if (request.getType() == 0x6) {
//            删除队列
            QueueDeleteArguments arguments = (QueueDeleteArguments) basicArguments;
            ok = virtualHost.queueDelete(arguments.getQueueName());
        } else if (request.getType() == 0x7) {
//            绑定队列
            QueueBindArguments arguments = (QueueBindArguments) basicArguments;
            ok = virtualHost.queueBind(arguments.getQueueName(), arguments.getExchangeName(), arguments.getBindingKey());
        } else if (request.getType() == 0x8) {
//            解绑队列
            QueueUnbindArguments arguments = (QueueUnbindArguments) basicArguments;
            ok = virtualHost.queueUnbind(arguments.getQueueName(),arguments.getQueueName());
        } else if (request.getType() == 0x9) {
//            发送消息
            BasicPublishArguments arguments = (BasicPublishArguments) basicArguments;
            ok = virtualHost.basicPublish(arguments.getExchangeName(),arguments.getRoutingKey(),arguments.getBasicProperties(),arguments.getBody());
        } else if (request.getType() == 0xa) {
//            订阅消息 -> 服务端如果接受到某个客户端的订阅之后要将该队列的消息推送给指定客户端
            BasicConsumeArguments arguments = (BasicConsumeArguments) basicArguments;
            ok = virtualHost.basicConsume(arguments.getConsumerTag(), arguments.getQueueName(), arguments.isAutoAck(),
                    new Consumer() {
//                            这个回调函数要做的就是把服务器收到的消息直接推送回对应的客户端
                        @Override
                        public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) throws IOException {
//                            先知道当前这个收到的消息，要发送给哪个客户端
//                            这里的 consumerTag 就是 channelId ，根据 channelId 去 sessions 中查询对应 socket 对象，再往这个里面写东西就行了
//                            1,根据 channelId 找到 socket 对象
                            Socket socket = sessions.get(consumerTag);
                            if(socket == null || socket.isClosed())
                                throw new MQException("[BrokerServer] 订阅消息的客户端已经关闭！");
//                            2,构造响应数据
                            SubScribeReturns subScribeReturns = new SubScribeReturns();
                            subScribeReturns.setChannelId(consumerTag);
                            subScribeReturns.setRid(""); // 由于这里只有响应, 没有请求, 不需要去对应. rid 暂时不需要.
                            subScribeReturns.setOk(true);
                            subScribeReturns.setConsumerTag(consumerTag);
                            subScribeReturns.setBasicProperties(basicProperties);
                            subScribeReturns.setBody(body);
                            byte[] payload = BinaryTool.toBytes(subScribeReturns);
                            Response response = new Response();
                            response.setType(0xc);
                            response.setLength(payload.length);
                            response.setPayload(payload);
//                            把数据写回客户端
//                            注意！此处的 dataOutputStream 这个对象不能 close
//                            如果把 dataOutputStream 关闭，就会直接把 clientSocket 里的 outputStream 关闭
                            DataOutputStream dataOutputStream = new DataOutputStream(clientSocket.getOutputStream());
                            writeResponse(dataOutputStream,response);
                        }
                    });
        } else if (request.getType() == 0xb) {
//            调用 basicAck 确认消息
            BasicAckArguments arguments = (BasicAckArguments) basicArguments;
            ok = virtualHost.basicAck(arguments.getQueueName(),arguments.getMessageId());
        }else {
//            当前 type 是非法的
            throw new MQException("[BrokerServer] 未知的 type！type = " + request.getType());
        }
//        构造响应
        BasicReturns basicReturns = new BasicReturns();
        basicReturns.setChannelId(basicArguments.getChannelId());
        basicReturns.setRid(basicArguments.getRid());
        basicReturns.setOk(ok);
        byte[] payload = BinaryTool.toBytes(basicReturns);
        Response response = new Response();
        response.setType(request.getType());
        response.setLength(payload.length);
        response.setPayload(payload);
        System.out.println("[Response] rid=" + basicReturns.getRid() + ", channelId=" + basicReturns.getChannelId()
                + ", type=" + response.getType() + ", length=" + response.getLength());
        return response;
    }

    /**
     * 把响应写回客户端
     */
    private void writeResponse(DataOutputStream dataOutputStream, Response response) throws IOException {
        System.out.println("写回响应中");
        dataOutputStream.writeInt(response.getType());
        dataOutputStream.writeInt(response.getLength());
        dataOutputStream.write(response.getPayload());
//        注意！需要刷新缓冲区才能及时把消息发出去！！！
        dataOutputStream.flush();
    }

    /**
     * 把当前 socket 对应所有的 channel 也顺便清理
     */
    private void clearClosedSession(Socket clientSocket) {
//        这里主要就是遍历上述的 sessions hash 表，把应该被关闭的 socket 对应的键值对删掉
        List<String> toDeleteChannelId = new ArrayList<>();
        for(Map.Entry<String,Socket> entry : sessions.entrySet()){
            if(entry.getValue() == clientSocket){
//                不能一边遍历一遍删除，将需要删除的保存起来放在链表中统一删除
                toDeleteChannelId.add(entry.getKey());
            }
        }
        for (String channelId : toDeleteChannelId) {
            sessions.remove(channelId);
        }
        System.out.println("[BrokerServer] 清理 session 完成！ 被清理的 channelId = " + toDeleteChannelId);
    }
}
