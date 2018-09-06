package com.zhengxuetao;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkRDDTest {


    /**
     * 参考：http://www.voidcn.com/article/p-mqhbtyjs-bcq.html
     *
     * @return
     */
    public JavaStreamingContext createStreamingContextForHive() {
        SparkConf sc = new SparkConf()
                .setAppName("SparkForHDFS")
                .setMaster("local[*]")
                .set("spark.streaming.stopGracefullyOnShutdown", "true");
        JavaSparkContext sparkContext = new JavaSparkContext(sc);
        JavaStreamingContext jssc = new JavaStreamingContext(sparkContext, Durations.seconds(20));
        sparkContext.setLogLevel("ERROR");
        HiveContext hiveContext = new HiveContext(sparkContext);
        //默认设置以支持动态分区
        hiveContext.setConf("hive.exec.dynamic.partition", "true");
        hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict");
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
