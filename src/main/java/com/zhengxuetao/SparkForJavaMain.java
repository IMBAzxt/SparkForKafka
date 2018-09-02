package com.zhengxuetao;

import org.apache.spark.streaming.api.java.*;

public class SparkForJavaMain {

    public static void main(String[] args) {
        String checkpointPath = "/data";
        String nodeList = "datanode1:9092,datanode2:9092";
        String group ="java";
        String topic = "flumetest";
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(
                "/data",
                (JavaStreamingContextFactory) () -> new SparkForKafka().createStreamingContextForKafka(nodeList,group,topic));
        jssc.start();
        jssc.awaitTermination();
    }
}
