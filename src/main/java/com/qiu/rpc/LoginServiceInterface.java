package com.qiu.rpc;

/**
 * Created by Administrator on 2016/10/18.
 */
public interface LoginServiceInterface {

    static final long versionID=1L;

    String login(String name);
}
