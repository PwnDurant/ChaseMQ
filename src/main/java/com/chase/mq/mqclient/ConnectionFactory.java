package com.chase.mq.mqclient;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.IOException;

/**
 * 连接工厂类
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ConnectionFactory {

    /**
     * broker server IP address
     */
    private String host;

    /**
     * broker server port
     */
    private int port;

    /**
     * 拓展信息:访问 broker server 的那个虚拟主机
     */
//    private String virtualHostname;
//    private String username;
//    private String password;

    public Connection newConnection() throws IOException {
        return new Connection(host,port);
    }

}
