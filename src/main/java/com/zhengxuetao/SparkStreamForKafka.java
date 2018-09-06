package com.zhengxuetao;

import com.cloudera.org.codehaus.jackson.map.ObjectMapper;
import kafka.common.TopicAndPartition;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
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
        TreeMap<Integer, PartitionMetadata> map = findLeader("dn1", 9092, topic);
        for (Map.Entry<Integer, PartitionMetadata> entry : map.entrySet()) {
            TopicAndPartition topicAndPartition = new TopicAndPartition(topic, entry.getKey());
            if (list.isEmpty()) {
                //zookeeper中没有topic的信息，则从0开始
                fromOffsets.put(topicAndPartition, 0L);
            } else {
                try {
                    //从zookeeper中获取offset信息，并从保存的offset处开始读取
                    byte[] value = curatorFramework.getData().forPath(offsetPath + "/" + entry.getKey());
                    fromOffsets.put(topicAndPartition, Long.parseLong(new String(value)));
                } catch (Exception e) {
                    //如果zookeeper中没有partition的offset，也从0开始。
                    fromOffsets.put(topicAndPartition, 0L);
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
        SparkConf sc = new SparkConf().setAppName("SparkForJava").setMaster("local[*]");
//        SparkConf sc = new SparkConf();
        JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(5));
        jssc.sparkContext().setLogLevel("DEBUG");
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, nodeList);
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "smallest");
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "FALSE");
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        ZKGroupTopicDirs zkGroupTopicDirs = new ZKGroupTopicDirs(group, topic);
        ZkClient zkClient = new ZkClient("dn1:2181");
        int count = zkClient.countChildren(zkGroupTopicDirs.consumerOffsetDir());

        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();
        Map<TopicAndPartition, Long> fromOffsets = new HashMap<>();

        //从SimpleConsumer中获取topic的partition信息
        TreeMap<Integer, PartitionMetadata> map = findLeader("dn1", 9092, topic);
        for (Map.Entry<Integer, PartitionMetadata> entry : map.entrySet()) {
            TopicAndPartition topicAndPartition = new TopicAndPartition(topic, entry.getKey());
            if (count == 0) {
                //zookeeper中没有topic的信息，则从0开始
                fromOffsets.put(topicAndPartition, 0L);
            } else {
                try {
                    //从zookeeper中获取offset信息，并从保存的offset处开始读取
                    String value = zkClient.readData(zkGroupTopicDirs.consumerOffsetDir() + "/" + entry.getKey());
                    //"OffsetRange(topic: 'flumetest', partition: 0, range: [38 -> 38])"，获取offset值，range表示from->end
                    Long offs = Long.parseLong(value);
                    fromOffsets.put(topicAndPartition, offs);
                } catch (Exception e) {
                    e.printStackTrace();
                    //如果zookeeper中没有partition的offset，也从0开始。
                    fromOffsets.put(topicAndPartition, 0L);
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
            rdd.foreach(line -> {
                String writeTopic = "result";
                ProducerRecord<String, String> msg = new ProducerRecord<>(writeTopic, line.message());
                KafkaWriter kafkaWriter = KafkaWriter.instance();
                kafkaWriter.send(msg);
                System.out.println(line);
            });
        });

        return jssc;
    }

    /**
     * 根据topic获取对应的partition信息，
     * 参考：http://blackproof.iteye.com/blog/2217388
     *
     * @param host
     * @param port
     * @param topic
     * @return
     */
    private TreeMap<Integer, PartitionMetadata> findLeader(String host, int port, String topic) {
        TreeMap<Integer, PartitionMetadata> map = new TreeMap<>();
        SimpleConsumer consumer = null;
        try {
            consumer = new SimpleConsumer(host, port, 100000, 64 * 1024,
                    "leaderLookup" + System.currentTimeMillis());
            List<String> topics = Collections.singletonList(topic);
            TopicMetadataRequest req = new TopicMetadataRequest(topics);
            kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

            List<TopicMetadata> metaData = resp.topicsMetadata();
            for (TopicMetadata item : metaData) {
                for (PartitionMetadata part : item.partitionsMetadata()) {
                    map.put(part.partitionId(), part);
                }
            }
        } catch (Exception e) {
            System.out.println("Error communicating with Broker [" + host
                    + "] to find Leader for [" + topic + ", ] Reason: " + e);
        } finally {
            if (consumer != null)
                consumer.close();
        }
        return map;
    }


    public JavaStreamingContext createStreamingContextForHDFS() {
        SparkConf sc = new SparkConf().setAppName("SparkForHDFS").setMaster("local[*]");
//        SparkConf sc = new SparkConf();
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
