package com.qiu.mptest.getdatafromconf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.*;

/**
 * Created by Administrator on 2016/7/18.
 * source:<a href="http://www.369bi.com">http://www.369bi.com</a>
 */
public class ObjectSerializer {
    private static final Log log = LogFactory.getLog(ObjectSerializer.class);

    public static String serialize(Serializable obj) throws IOException {
        if (obj == null) {
            return "";
        }
        try {
            ByteArrayOutputStream serialObj = new ByteArrayOutputStream();
            ObjectOutputStream objStream = new ObjectOutputStream(serialObj);
            objStream.writeObject(obj);
            objStream.close();

            return encodeBytes(serialObj.toByteArray());
        } catch (IOException e) {
            e.printStackTrace();
            throw new IOException("Serialization error:" + e.getMessage(), e);
        }

    }

    public static Object deserialize(String string) throws IOException, ClassNotFoundException {
        if (string == null || string.length() == 0) {
            return null;
        }
        try {
            ByteArrayInputStream serialObj = new ByteArrayInputStream(decodeBytes(string));
            ObjectInputStream objectInputStream = new ObjectInputStream(serialObj);
            return objectInputStream.readObject();
        } catch (IOException e) {
            e.printStackTrace();
            throw new IOException("Deserialization eroor:" + e.getMessage(), e);
        }
    }

    private static byte[] decodeBytes(String string) {
        byte[] bytes = new byte[string.length() / 2];
        for(int i=0;i<string.length();i+=2) {
            char c = string.charAt(i);
            bytes[i / 2] = (byte) ((c - 'a') << 4);
            c = string.charAt(i + 1);
            bytes[i / 2] += (c - 'a');
        }

        return bytes;
    }

    private static String encodeBytes(byte[] bytes) {
        StringBuffer sb = new StringBuffer();
        for(int i=0;i<bytes.length;i++) {
            sb.append((char) (((bytes[i] >> 4) & 0xF) + ((int) 'a')));
            sb.append((char) ((bytes[i]) & 0xF) + ((int) 'a'));
        }
        return sb.toString();
    }

}
