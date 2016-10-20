package com.qiu.rpc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by Administrator on 2016/10/18.
 */
public class LoginController {
    public static void main(String[] args) throws IOException {
        LoginServiceInterface service = RPC.getProxy(LoginServiceInterface.class, 1L, new InetSocketAddress("172.16.18.104", 10000), new Configuration());
        String result = service.login("qiubo");
        System.out.println("result:" + result);
        RPC.stopProxy(service);
    }
}
