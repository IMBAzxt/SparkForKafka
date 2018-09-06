package com.zhengxuetao;

import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class SparkForJavaMain {

    public static void main(String[] args) throws InterruptedException {
        if (args == null || args.length == 0) {
            System.out.println("need [group] [topic]");
            System.exit(0);
        }
        String hdfs = "hdfs://namenode:8020";
        String checkpointPath = hdfs + "/data/checkpointPath";
        String nodeList = "dn1:9092,dn3:9092,nn:9092";
        String group = args[0];
        String topic = args[1];
        JavaStreamingContext jssc = JavaStreamingContext.getOrCreate(
                checkpointPath,
                (Function0<JavaStreamingContext>) () -> new SparkStreamForKafka()
                        .createStreamingContextForHDFS());
//                        .createStreamingContextForKafkaSaveOffsetByZkClient(nodeList, group, topic));
        jssc.start();
        jssc.awaitTermination();
    }
}
