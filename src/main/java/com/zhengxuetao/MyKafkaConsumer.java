package com.zhengxuetao;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

import java.util.*;

public class MyKafkaConsumer {
    private List<String> m_replicaBrokers = new ArrayList<String>();


    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String group) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), group);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    /**
     * 根据topic获取对应的partition信息，
     * 参考：http://blackproof.iteye.com/blog/2217388
     * 参考：https://blog.csdn.net/suifeng3051/article/details/38657465
     *
     * @param host
     * @param port
     * @param topic
     * @return
     */
    public static TreeMap<Integer, PartitionVO> findLeader(String host, int port, String topic, String group) {
        TreeMap<Integer, PartitionVO> map = new TreeMap<>();
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

                    SimpleConsumer c = new SimpleConsumer(part.leader().host(), port, 100000, 64 * 1024, group);
                    long fromOffset = MyKafkaConsumer.getLastOffset(c, topic, part.partitionId(), kafka.api.OffsetRequest.EarliestTime(), group);
                    map.put(part.partitionId(), new PartitionVO(part, fromOffset));
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
}
