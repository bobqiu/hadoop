package com.qiu.hbase.retrievemeta;


import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: bob
 * Date: 14-10-29
 * Time: 上午10:08
 * To change this template use File | Settings | File Templates.
 */
public class TableMeta {

    private TableMeta() {
    }

    public static void main(String[] args) throws IOException {
    /*  CloseableHttpClient httpClient = null;
        httpClient= HttpClients.createDefault();
        HttpGet httpGet=new HttpGet("http://damhadoop1:8000/");
        System.out.println(httpGet.getRequestLine());
        CloseableHttpResponse response=httpClient.execute(httpGet);
        HttpEntity httpEntity=response.getEntity();
        System.out.println(httpEntity.getContentLength());*/
      getTableNameList();
    }

    public static String getTableNameList() throws IOException {
        HttpClient httpClient=new HttpClient();
        HttpMethod getMethod = new GetMethod("http://damhadoop1:8000");
        getMethod.setRequestHeader("Accept","application/json");
        getMethod.setRequestHeader("Content-Type","application/json;charset=utf-8");
        httpClient.executeMethod(getMethod);
        String response = getMethod.getResponseBodyAsString();
        System.out.println(response);
        System.out.println(getMethod.getStatusLine());
        return response;
    }
}
