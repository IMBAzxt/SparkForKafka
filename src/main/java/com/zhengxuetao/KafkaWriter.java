package com.zhengxuetao;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.io.Serializable;
import java.util.List;
import java.util.Properties;

public class KafkaWriter implements Serializable {

    private static KafkaWriter kafkaWriter = null;
    private Producer<String, String> producer;

    private KafkaWriter() {
        //参数详解：https://my.oschina.net/u/218540/blog/1794669
        Properties props = new Properties();
        props.put("bootstrap.servers", "dn1:9092");
        props.put("acks", "all");
        props.put("retries", 1);
        props.put("batch.size", 163840); //每批发送的大小
        props.put("linger.ms", 5000);    //每批发送的间隔时间，当batch.size或者linger.ms达到设定值，将往kafka写数据。
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //生产者发送消息
        this.producer = new KafkaProducer<String, String>(props);
    }

    public static synchronized KafkaWriter instance() {
        if (kafkaWriter == null) {
            kafkaWriter = new KafkaWriter();
        }
        return kafkaWriter;
    }

    public void send(ProducerRecord<String, String> msg) {
        this.producer.send(msg);
    }

    public List<PartitionInfo> partitionsFor(String topic) {
        return producer.partitionsFor(topic);
    }

//    public void send(List<ProducerRecord<String, String>> msg) {
//        this.producer.send(msg);
//    }
}
