package com.chase.mq.mqclient;

import com.chase.mq.common.dto.BasicReturns;
import com.chase.mq.common.dto.Request;
import com.chase.mq.common.dto.Response;
import com.chase.mq.common.dto.SubScribeReturns;
import com.chase.mq.common.server.BinaryTool;
import com.chase.mq.common.server.MQException;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.*;
import java.net.Socket;
import java.net.SocketException;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 一个连接
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Connection {

    /**
     * 一个连接套接字
     */
    private Socket socket = null;

    /**
     * 当前 connection 中存在多个 channel ，使用 hash 表把 channel 组织起来
     * 相比于 brokerServer 中的 hash 表，表示的范围更小一点
     */
    private ConcurrentHashMap<String,Channel> channelMap = new ConcurrentHashMap<>();

    /**
     * 文件输出流
     */
    private InputStream inputStream;

    /**
     * 文件输入流
     */
    private OutputStream outputStream;

    /**
     * 指定长度输出流
     */
    private DataOutputStream dataOutputStream;

    /**
     * 指定长度输入流
     */
    private DataInputStream dataInputStream;

    /**
     * 引入线程池来处理回调请求
     */
    private ExecutorService callbackPool = null;

    /**
     * 连接构造方法，初始化连接之前需要做的事情
     */
    public Connection(String host,int port) throws IOException {
        socket = new Socket(host,port);
        inputStream = socket.getInputStream();
        outputStream = socket.getOutputStream();
        dataInputStream = new DataInputStream(inputStream);
        dataOutputStream = new DataOutputStream(outputStream);
        callbackPool = Executors.newFixedThreadPool(4);
//        创建一个扫描消除，这个线程负责不停的从 socket 中读取响应数据，把这个响应数据在交给对应的 channel 负责处理
        Thread t = new Thread(() -> {
            try{
                while(!socket.isClosed()){
                    Response response = readResponse();
                    dispatchResponse(response);
                }
            }catch (SocketException e){
                // 连接正常断开的. 此时这个异常直接忽略.
                System.out.println("[Connection] 连接正常断开!");
            }catch (IOException | ClassNotFoundException | MQException e) {
                System.out.println("[Connection] 连接异常断开!");
                e.printStackTrace();
            }
        });
        t.start();
    }

    /**
     * 关闭 Connection 并释放上述资源
     */
    public void close(){
        try{
            callbackPool.shutdownNow();
            channelMap.clear();
            inputStream.close();
            outputStream.close();
            socket.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    /**
     * 读取响应
     */
    public Response readResponse() throws IOException {
        Response response = new Response();
        try {
            response.setType(dataInputStream.readInt());
            response.setLength(dataInputStream.readInt());
        } catch (EOFException e){
            System.out.println("[Connection] 读取响应失败");
        }
        byte[] payload = new byte[response.getLength()];
        int n = dataInputStream.read(payload);
        if(n != response.getLength())
            throw new IOException("[Connection] 读取响应数据不完整");
        response.setPayload(payload);
        System.out.println("[Connection] 收到响应！type = " + response.getType());
        return response;
    }

    /**
     * 使用这个方法来分别处理，当前响应是一个针对控制请求的响应，还是服务器推送的消息
     */
    private void dispatchResponse(Response response) throws IOException, ClassNotFoundException {
        if(response.getType() == 0xc){
//            服务器推动过来的消息数据
            SubScribeReturns subScribeReturns = (SubScribeReturns) BinaryTool.fromBytes(response.getPayload());
//            根据 channelId 找到对应的 channel 对象
            Channel channel = channelMap.get(subScribeReturns.getChannelId());
            if(channel == null){
                throw new MQException("[Connection] 该消息对应的 channel 在客户端中不存在! channelId=" + channel.getChannelId());
            }
//            执行该 channel 对象内部的回调.
            callbackPool.submit(() -> {
                try {
                    channel.getConsumer().handleDelivery(subScribeReturns.getConsumerTag(), subScribeReturns.getBasicProperties(),
                            subScribeReturns.getBody());
                } catch (MQException | IOException e) {
                    e.printStackTrace();
                }
            });
        } else {
//            当前响应是针对之前的控制请求的响应
            BasicReturns basicReturns = (BasicReturns) BinaryTool.fromBytes(response.getPayload());
//            把这个结果放到对应 channel 的 hash 表中
            Channel channel = channelMap.get(basicReturns.getChannelId());
            if (channel == null) {
                throw new MQException("[Connection] 该消息对应的 channel 在客户端中不存在! channelId=" + channel.getChannelId());
            }
            channel.putReturns(basicReturns);
        }
    }

    /**
     * 发送请求
     */
    public void writeRequest(Request request) throws IOException {
        dataOutputStream.writeInt(request.getType());
        dataOutputStream.writeInt(request.getLength());
        dataOutputStream.write(request.getPayload());
        dataOutputStream.flush();
        System.out.println("[Connection] 发送请求! type=" + request.getType() + ", length=" + request.getLength());
    }

    /**
     * 在 connection 中创建出一个 channel
     */
    public Channel createChannel() throws IOException {
        String channelId = "C-" + UUID.randomUUID().toString();
        Channel channel = new Channel(channelId, this);
        // 把这个 channel 对象放到 Connection 管理 channel 的 哈希表 中.
        channelMap.put(channelId, channel);
        // 同时也需要把 "创建 channel" 的这个消息也告诉服务器.
        boolean ok = channel.createChannel();
        if (!ok) {
            // 服务器这里创建失败了!! 整个这次创建 channel 操作不顺利!!
            // 把刚才已经加入 hash 表的键值对, 再删了.
            System.out.println("[Connection] 创建 channel 失败！");
            channelMap.remove(channelId);
            return null;
        }
        return channel;
    }

}
