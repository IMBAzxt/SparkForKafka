package com.zhengxuetao;

import com.cloudera.org.codehaus.jackson.map.ObjectMapper;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author zhengxt
 */
public class SparkForKafka {
    /**
     * 从kafka读取数据，从最早开始读取，不保存offset。
     * @param nodeList
     * @param group
     * @param topic
     * @return
     */
    public JavaStreamingContext createStreamingContextForKafka(String nodeList, String group, String topic) {
        SparkConf sc = new SparkConf().setAppName("SparkForJava").setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(5));
        jssc.sparkContext().setLogLevel("ERROR");
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topic));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, nodeList);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest");

        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );

        JavaDStream<String> lines = messages.map(rdd->rdd._2);
        lines.foreachRDD(rdd -> {
            rdd.foreach(line -> System.out.println(line));
        });
        return jssc;
    }

    /**
     * 从kafka读取数据，保存offset，作业重启之后从上一次offset位置读取。
     * 参考：https://blog.csdn.net/sun_qiangwei/article/details/52089795
     * 参考：https://blog.csdn.net/zilongreco/article/details/77919027
     * @param nodeList
     * @param group
     * @param topic
     * @return
     */
    public JavaStreamingContext createStreamingContextForKafkaSaveOffset(String nodeList, String group, String topic) {
        SparkConf sc = new SparkConf().setAppName("SparkForJava").setMaster("local[*]");
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(5));
        jssc.sparkContext().setLogLevel("ERROR");
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topic));
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, nodeList);
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "FALSE");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        CuratorFramework curatorFramework = CuratorFrameworkFactory
                .builder()
                .connectString("nn:2181")
                .connectionTimeoutMs(1000)
                .sessionTimeoutMs(10000)
                .retryPolicy(new RetryUntilElapsed(1000, 1000))
                .build();
        curatorFramework.start();
        String offsetPath = "/consumers/" + group + "/offsets/" + topic;
        List<String> list = null;
        try {
            list = curatorFramework.getChildren().forPath(offsetPath);
        } catch (Exception e) {
            list = new ArrayList<>();
        }

        JavaPairDStream<String,String> messages = null;
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
        if(list.isEmpty()){
            messages  = KafkaUtils.createDirectStream(
                    jssc,
                    String.class,
                    String.class,
                    StringDecoder.class,
                    StringDecoder.class,
                    kafkaParams,
                    topicsSet
            ).transformToPair((Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>) rdd -> {
                OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                offsetRanges.set(offsets);
                return rdd;
            });
        } else {
            Map<TopicAndPartition,Long> fromOffsets = new HashMap<>();
            for(int i=0; i < list.size(); i++){
                TopicAndPartition topicAndPartition = new TopicAndPartition(topic, i);
                byte[] value = new byte[0];
                try {
                    value = curatorFramework.getData().forPath(offsetPath + "/" + i);
                    fromOffsets.put(topicAndPartition, Long.parseLong(new String(value)));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            Class<MessageAndMetadata<String,String>> streamClass =
                    (Class<MessageAndMetadata<String,String>>) (Class<?>) MessageAndMetadata.class;
            KafkaUtils.createDirectStream(
                    jssc,
                    String.class,
                    String.class,
                    StringDecoder.class,
                    StringDecoder.class,
                    streamClass,
                    kafkaParams,
                    fromOffsets,
                    (Function<MessageAndMetadata<String, String>, MessageAndMetadata<String, String>>) v1 -> v1
            ).transform(new Function<JavaRDD<MessageAndMetadata<String, String>>, JavaRDD<MessageAndMetadata<String, String>>>() {
                @Override
                public JavaRDD<MessageAndMetadata<String, String>> call(JavaRDD<MessageAndMetadata<String, String>> rdd) throws Exception {
                    OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                    offsetRanges.set(offsets);
                    return rdd;
                }
            }).foreachRDD(rdd -> {
                CuratorFramework cf = CuratorFrameworkFactory
                        .builder()
                        .connectString("nn:2181")
                        .connectionTimeoutMs(1000)
                        .sessionTimeoutMs(10000)
                        .retryPolicy(new RetryUntilElapsed(1000, 1000))
                        .build();

                cf.start();
                ObjectMapper objectMapper = new ObjectMapper();
                for (OffsetRange o : offsetRanges.get()) {
                    System.out.println(
                            o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset()
                    );
                    final byte[] offsetBytes = objectMapper.writeValueAsBytes(o.untilOffset());
                    String nodePath = "/consumers/" + group + "/offsets/" + o.topic() + "/" + o.partition();
                    if (cf.checkExists().forPath(nodePath) != null) {
                        cf.setData().forPath(nodePath, offsetBytes);
                    } else {
                        cf.create().creatingParentsIfNeeded().forPath(nodePath, offsetBytes);
                    }
                }
                rdd.foreach(line -> System.out.println(line));
                cf.close();
            });
        }

        curatorFramework.close();
        return jssc;
    }
}
