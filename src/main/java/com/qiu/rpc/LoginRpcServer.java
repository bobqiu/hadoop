package com.qiu.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;

/**
 * Created by Administrator on 2016/10/18.
 */
public class LoginRpcServer {
    public static void main(String[] args) throws IOException {
        RPC.Builder builder = new RPC.Builder(new Configuration());

        builder.setBindAddress("172.16.18.104").setPort(10000).setInstance(new LoginServiceImpl()).setProtocol(LoginServiceInterface.class);

        RPC.Server server = builder.build();

        server.start();
    }
}
