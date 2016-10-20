package com.qiu.rpc;

/**
 * Created by Administrator on 2016/10/18.
 */
public class LoginServiceImpl implements LoginServiceInterface {
    public String login(String name) {
        return "hello," + name + ".welcome";
    }
}
