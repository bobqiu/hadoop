package com.qiu.spark.wordcount;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Created by bob on 14.11.26.
 */
public class SparkTest {
    static  final String SPARK_MASTER_ADDRESS="spark://10.1.251.122:7077";
    static final String SPARK_HOME="/opt/modules/hadoop/spark110220";
    static  final  String APP_LIB_PATH="lib";
    public static  void main(String[] args) throws IOException {
        System.setProperty("user.name", "hadoop");
        System.setProperty("HADOOP_USER_NAME","hadoop");
        Map<String,String> envs=new HashMap<String, String>();
        envs.put("HADOOP_USER_NAME", "hadoop");
        System.setProperty("spark.executor.memory", "100m");
        System.setProperty("spark.cores.max", "1");
        //System.setProperty("SPARK_YARN_APP_JAR", "/home/hadoop/hadoop.jar");
        String[] jars=getApplicationLibrary();
        JavaSparkContext context = new JavaSparkContext(SPARK_MASTER_ADDRESS, "Spark App 0", SPARK_HOME, jars, envs);
       context.addJar("file:/D:/gitworkspace/hadoop/out/artifacts/hadoop_jar/hadoop.jar");
        JavaSparkContext.jarOfClass(SparkTest.class);
        countWords(context);
    }

    private static void countWords(JavaSparkContext context) {
        String input = "hdfs://DamHadoop1:9000/user/hadoop/in/test.txt";
        JavaRDD<String> data=context.textFile(input);
        JavaRDD<String> pairs;
        pairs = data.flatMap(new FlatMapFunction<String, String>() {

            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(Pattern.compile(" ").split(s));
            }
        });
        JavaPairRDD<String,Integer> ons=pairs.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });
        JavaPairRDD<String,Integer> counts=ons.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
       /* String output =  "hdfs://DamHadoop1:9000/user/hadoop/output";
        pairs.saveAsTextFile(output);*/
        System.out.println("====================qqq==test==================================");
        List<Tuple2<String,Integer>> out=counts.collect();
        for (Tuple2<String, Integer> tp : out) {
            System.out.println( tp._1()+":"+tp._2());
        }
        context.stop();
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
