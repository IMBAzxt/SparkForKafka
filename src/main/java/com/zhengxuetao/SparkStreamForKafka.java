package com.zhengxuetao;

import com.cloudera.org.codehaus.jackson.map.ObjectMapper;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author zhengxt
 */
public class SparkStreamForKafka implements Serializable {
    /**
     * 从kafka读取数据，从最早开始读取，不保存offset。
     * @param nodeList
     * @param group
     * @param topic
     * @return
     */
    public JavaStreamingContext createStreamingContextForKafka(String nodeList, String group, String topic) {
//        SparkConf sc = new SparkConf().setAppName("SparkForJava").setMaster("local[*]");
        SparkConf sc = new SparkConf();
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
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
        JavaDStream<String> lines = messages
                .transformToPair(new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
                    @Override
                    public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
                        OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                        offsetRanges.set(offsets);
                        return rdd;
                    }
                })
                .map(rdd -> rdd._2);
        lines.foreachRDD(rdd -> {
            for (OffsetRange o : offsetRanges.get()) {
                System.out.println(
                        o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset()
                );
            }
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
//        SparkConf sc = new SparkConf().setAppName("SparkForJava").setMaster("local[*]");
        SparkConf sc = new SparkConf();
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(5));
        jssc.sparkContext().setLogLevel("ERROR");
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

        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
        Map<TopicAndPartition, Long> fromOffsets = new HashMap<>();

        //从SimpleConsumer中获取topic的partition信息
        TreeMap<Integer, PartitionVO> map = MyKafkaConsumer.findLeader("dn1", 9092, topic, group);
        for (Map.Entry<Integer, PartitionVO> entry : map.entrySet()) {
            TopicAndPartition topicAndPartition = new TopicAndPartition(topic, entry.getKey());
            long fromOffset = entry.getValue().getFromOffset();
            if (list.isEmpty()) {
                //zookeeper中没有topic的信息，则从smallest开始
                fromOffsets.put(topicAndPartition, fromOffset);
            } else {
                try {
                    //从zookeeper中获取offset信息，并从保存的offset处开始读取
                    byte[] value = curatorFramework.getData().forPath(offsetPath + "/" + entry.getKey());
                    long offs = Long.parseLong(new String(value));
                    if (offs < fromOffset) {
                        offs = fromOffset;
                    }
                    fromOffsets.put(topicAndPartition, offs);
                } catch (Exception e) {
                    //如果zookeeper中没有partition的offset，也从0开始。
                    fromOffsets.put(topicAndPartition, fromOffset);
                }
            }
        }


        Class<MessageAndMetadata<String, String>> streamClass =
                (Class<MessageAndMetadata<String, String>>) (Class<?>) MessageAndMetadata.class;
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


        curatorFramework.close();
        return jssc;
    }

    public JavaStreamingContext createStreamingContextForKafkaSaveOffsetByZkClient(String nodeList, String group, String topic) {
//        SparkConf sc = new SparkConf().setAppName("SparkForJava").setMaster("local[*]");
        SparkConf sc = new SparkConf();
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(20));
        jssc.sparkContext().setLogLevel("DEBUG");
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, nodeList);
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "largest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "FALSE");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        ZKGroupTopicDirs zkGroupTopicDirs = new ZKGroupTopicDirs(group, topic);
        ZkClient zkClient = new ZkClient("dn1:2181");
        int count = zkClient.countChildren(zkGroupTopicDirs.consumerOffsetDir());

        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
        Map<TopicAndPartition, Long> fromOffsets = new HashMap<>();

        //从SimpleConsumer中获取topic的partition信息
        TreeMap<Integer, PartitionVO> map = MyKafkaConsumer.findLeader("dn1", 9092, topic, group);
        for (Map.Entry<Integer, PartitionVO> entry : map.entrySet()) {
            TopicAndPartition topicAndPartition = new TopicAndPartition(topic, entry.getKey());
            long fromOffset = entry.getValue().getFromOffset();
            if (count == 0) {
                //zookeeper中没有topic的信息，则从fromOffset开始
                fromOffsets.put(topicAndPartition, fromOffset);
            } else {
                try {
                    //从zookeeper中获取offset信息，并从保存的offset处开始读取
                    String value = zkClient.readData(zkGroupTopicDirs.consumerOffsetDir() + "/" + entry.getKey());
                    Long offs = Long.parseLong(value);
                    if (offs < fromOffset) {
                        offs = fromOffset;
                    }
                    fromOffsets.put(topicAndPartition, offs);
                } catch (Exception e) {
                    e.printStackTrace();
                    //如果zookeeper中没有partition的offset，也从fromOffset开始。
                    fromOffsets.put(topicAndPartition, fromOffset);
                }
            }
        }

        Class<MessageAndMetadata<String, String>> streamClass =
                (Class<MessageAndMetadata<String, String>>) (Class<?>) MessageAndMetadata.class;
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
            for (OffsetRange o : offsetRanges.get()) {
                System.out.println(
                        o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset()
                );
                String offsetPath = "/consumers/" + group + "/offsets/" + o.topic() + "/" + o.partition();
                ZkUtils zkUtils = ZkUtils.apply(zkClient, false);
                zkUtils.updatePersistentPath(offsetPath, String.valueOf(o.fromOffset()), zkUtils.DefaultAcls());
            }
//往kafka写数据
            //            rdd.foreach(line -> {
//                String writeTopic = "result";
//                ProducerRecord<String, String> msg = new ProducerRecord<>(writeTopic, line.message());
//                KafkaWriter kafkaWriter = KafkaWriter.instance();
//                kafkaWriter.send(msg);
//                System.out.println(line);
//            });
            //往hdfs写数据
//            rdd.saveAsTextFile("hdfs://namenode:8020/data/output/" + System.currentTimeMillis());
            rdd
                    .mapToPair(new PairFunction<MessageAndMetadata<String, String>, String, String>() {
                        @Override
                        public Tuple2<String, String> call(MessageAndMetadata<String, String> s) throws Exception {
                            return new Tuple2<>(s.key(), s.message());
                        }
                    })
                    .saveAsHadoopFile("hdfs://namenode:8020/data/output/" + System.currentTimeMillis(), Text.class, Text.class, TextOutputFormat.class);
        });

        return jssc;
    }


    public JavaStreamingContext createStreamingContextForHDFS() {
//        SparkConf sc = new SparkConf().setAppName("SparkForHDFS").setMaster("local[*]");
        SparkConf sc = new SparkConf();
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(20));
        jssc.sparkContext().setLogLevel("ERROR");
        JavaDStream<String> message = jssc.textFileStream("hdfs://namenode:8020/data/input");
        JavaDStream<String> rdd = message.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(","));
            }
        });
        rdd.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).saveAsHadoopFiles("hdfs://namenode:8020/data/output/", "log", Text.class, IntWritable.class, TextOutputFormat.class);
        return jssc;
    }

}
