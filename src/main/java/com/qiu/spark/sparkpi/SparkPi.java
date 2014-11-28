package com.qiu.spark.sparkpi;

import com.qiu.spark.wordcount.SparkTest;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by bob on 14.11.28.
 */
public class SparkPi {
    static  final String SPARK_MASTER_ADDRESS="spark://10.1.251.122:7077";
    static final String SPARK_HOME="/opt/modules/hadoop/spark110220";
    static  final  String APP_LIB_PATH="lib";
    public static void main(String[] args) throws IOException {

        System.setProperty("user.name", "hadoop");
        System.setProperty("HADOOP_USER_NAME","hadoop");
        Map<String,String> envs=new HashMap<String, String>();
        envs.put("HADOOP_USER_NAME", "hadoop");
        System.setProperty("spark.executor.memory", "100m");
        System.setProperty("spark.cores.max", "1");
        //System.setProperty("SPARK_YARN_APP_JAR", "/home/hadoop/hadoop.jar");
        String[] jars=getApplicationLibrary();
        JavaSparkContext context = new JavaSparkContext(SPARK_MASTER_ADDRESS, "Spark PI app", SPARK_HOME, jars, envs);
        context.addJar("file:/D:/gitworkspace/hadoop/out/artifacts/hadooppi_jar/hadooppi_jar.jar");
        JavaSparkContext.jarOfClass(SparkTest.class);

        int slices = (args.length == 1) ? Integer.parseInt(args[0]) : 2;
        int n = 10000 * slices;
        List<Integer> l = new ArrayList<Integer>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }
        JavaRDD<Integer> dataSet = context.parallelize(l, slices);

        int count=dataSet.map(new Function<Integer,Integer>(){

            @Override
            public Integer call(Integer integer) throws Exception {
                double x=Math.random()*2-1;
                double y=Math.random()*2-1;
                return (x * x + y * y < 1) ? 1 : 0;
            }



        }).reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });
        System.out.println("============start output result================");
        System.out.println("pi is ::" + 4.0 * count / n);

    }
    private static String[] getApplicationLibrary() throws IOException {
        List<String> list = new LinkedList<String>();
        File lib = new File(APP_LIB_PATH);
        if (lib.exists()) {
            if (lib.isFile() && lib.getName().endsWith(".jar")) {
                list.add(lib.getCanonicalPath());
            }
        }
        String[] ret = new String[list.size()];
        int i=0;
        for (String s : list) {
            ret[i++]=s;
        }
        return ret;
    }
}
