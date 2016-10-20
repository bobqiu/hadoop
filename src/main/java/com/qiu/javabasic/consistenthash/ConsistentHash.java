package com.qiu.javabasic.consistenthash;

import com.sun.jdi.IntegerType;
import com.sun.org.apache.xml.internal.security.encryption.CipherReference;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/**
 * Created by Administrator on 2016/10/20.
 */
public class ConsistentHash {

    private HashFunc hashFunc;

    private int numberOfReplicate = 0;

    private final SortedMap<Integer, Node> circle = new TreeMap<Integer, Node>();

    public ConsistentHash(int numberOfReplicate,Collection<Node> nodes) {
        this.numberOfReplicate = numberOfReplicate;
        this.hashFunc=new HashFunc() {
            public Integer hash(Object key) {
                String data = key.toString();
                //默认使用FNV1hash算法
                final int p = 16777619;
                int hash = (int) 2166136261L;
                for (int i = 0; i < data.length(); i++)
                    hash = (hash ^ data.charAt(i)) * p;
                hash += hash << 13;
                hash ^= hash >> 7;
                hash += hash << 3;
                hash ^= hash >> 17;
                hash += hash << 5;
                return hash;
            }
        };
    }

    public ConsistentHash(HashFunc hashFunc, int numberOfReplicas, Collection<Node> nodes) {
        this.numberOfReplicate = numberOfReplicas;
        this.hashFunc = hashFunc;
        //初始化节点
        for (Node node : nodes) {
            add(node);
        }
    }

    private void add(Node node) {
        for (int i = 0; i < numberOfReplicate; i ++) {
            System.out.println(node.getName()+":"+hashFunc.hash(node.toString()+i)+":"+node);
            circle.put(hashFunc.hash(node.toString() + i), node);
        }
    }

    private void remove(Node node) {
        for (int i = 0; i < numberOfReplicate; i ++) {
            circle.remove(hashFunc.hash(node.toString() + i));
        }
    }

    private Node get(Object key) {
        if (circle.isEmpty()) {
            return null;
        }
        int hash = hashFunc.hash(key);
        if (!circle.containsKey(hash)) {
            SortedMap<Integer, Node> tailMap = circle.tailMap(hash);
            hash=tailMap.isEmpty()?circle.firstKey():tailMap.firstKey();
        }
        return circle.get(hash);
    }


    public static void main(String[] args) {
        List<Node> nodes = new ArrayList<Node>();
        nodes.add(new Node("10.1.32.1"));
        nodes.add(new Node("10.1.32.2"));
        nodes.add(new Node("10.1.32.3"));
        nodes.add(new Node("10.1.32.4"));
        nodes.add(new Node("10.1.32.5"));

        int replicates=10;

        HashFunc hashFunc=new HashFunc() {
            public Integer hash(Object key) {
                String data = key.toString();
                //默认使用FNV1hash算法
                final int p = 16777619;
                int hash = (int) 2166136261L;
                for (int i = 0; i < data.length(); i++)
                    hash = (hash ^ data.charAt(i)) * p;
                hash += hash << 13;
                hash ^= hash >> 7;
                hash += hash << 3;
                hash ^= hash >> 17;
                hash += hash << 5;
                return hash;
            }
        };

        ConsistentHash consistentHash = new ConsistentHash(hashFunc,replicates, nodes);

    }

    static class Node{

        public Node(String name) {
            this.name = name;
        }

        private String name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

       @Override
       public String toString() {
           return "Node{" +
                   "name='" + name + '\'' +
                   '}';
       }
   }

    private interface HashFunc {
        public Integer hash(Object key);
    }
}


