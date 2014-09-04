package com.yhemanth.spark;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class SparkStreamingSample
{
    public static void main( String[] args )
    {
        JavaStreamingContext context = new JavaStreamingContext("yarn-cluster", "Spark Streaming Sample",
                new Duration(10000));

        JavaReceiverInputDStream<String> lines = context.socketTextStream("localhost", 9999);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });

        JavaPairDStream<String, Integer> partialWordCounts = words.mapToPair(new PairFunction<String, String, Integer>() {
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
        JobConf conf = new JobConf();
        conf.setOutputFormat(TextOutputFormat.class);
        wordCounts.saveAsHadoopFiles("hdfs://localhost:9100/user/yhemanth/spark-streaming/wc-out",
                "txt", Text.class, IntWritable.class, TextOutputFormat.class);

        context.start();
        context.awaitTermination();
    }
}
