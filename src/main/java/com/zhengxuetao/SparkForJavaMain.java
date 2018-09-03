package com.zhengxuetao;

import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkForJavaMain {

    public static void main(String[] args) throws InterruptedException {
        String checkpointPath = "hdfs://namenode:8020/data/checkpointPath";
        String nodeList = "dn1:9092,dn3:9092,nn:9092";
        String group = "java8";
        String topic = "flumetest";
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(
                checkpointPath,
                (Function0<JavaStreamingContext>) () -> new SparkForKafka().createStreamingContextForKafkaSaveOffset(nodeList,group,topic));
        jssc.start();
        jssc.awaitTermination();
    }
}
