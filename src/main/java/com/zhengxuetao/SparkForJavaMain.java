package com.zhengxuetao;

import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.api.java.*;

public class SparkForJavaMain {

    public static void main(String[] args) throws InterruptedException {
        String checkpointPath = "hdfs://namenode:8082/data/checkpointPath";
        String nodeList = "datanode1:9092,datanode2:9092";
        String group ="java";
        String topic = "flumetest";
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(
                "/data",
                (Function0<JavaStreamingContext>) () -> new SparkForKafka().createStreamingContextForKafka(nodeList,group,topic));
        jssc.start();
        jssc.awaitTermination();
    }
}
