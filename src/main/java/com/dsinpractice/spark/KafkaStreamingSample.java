package com.dsinpractice.spark;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;

public class KafkaStreamingSample {

    public static void main(String[] args) {

        JavaStreamingContext context = new JavaStreamingContext("local[3]", "Kafka Streaming Sample",
                new Duration(10000), "/Users/yhemanth/software/spark-0.8.1-incubating-bin-hadoop2",
                new String[]{"/Users/yhemanth/projects/personal/spark/spark-samples/target/spark-samples-1.0-SNAPSHOT-jar-with-dependencies.jar"});

        HashMap<String, Integer> topics = new HashMap<String, Integer>();
        topics.put("word_count", 1);
        JavaDStream<String> messages = null;
        messages = context.kafkaStream("localhost:2181", "2", topics);

        JavaDStream<String> words = messages.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });

        JavaPairDStream<String, Integer> partialWordCounts =
                words.map(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word, 1);
            }
        });

        JavaPairDStream wordCounts = partialWordCounts.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });


        wordCounts.print();

//        JobConf conf = new JobConf();
//        conf.setOutputFormat(TextOutputFormat.class);
//        wordCounts.saveAsHadoopFiles("hdfs://localhost:9100/user/yhemanth/spark-streaming-kafka/wc-out",
//                "txt", Text.class, IntWritable.class, TextOutputFormat.class);

        context.start();

    }

}
