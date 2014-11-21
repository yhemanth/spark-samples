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

public class SparkStreamingSample
{
    public static void main( String[] args )
    {
        JavaStreamingContext context = new JavaStreamingContext("local[3]", "Spark Streaming Sample",
                new Duration(10000), "/Users/yhemanth/software/spark-0.8.1-incubating-bin-hadoop2",
                new String[]{"target/spark-samples-1.0-SNAPSHOT-jar-with-dependencies.jar"});

        JavaDStream<String> lines = context.socketTextStream("localhost", 9999);

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });

        JavaPairDStream<String, Integer> partialWordCounts = words.map(new PairFunction<String, String, Integer>() {
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
//        wordCounts.saveAsHadoopFiles("hdfs://localhost:9100/user/yhemanth/spark-streaming/wc-out",
//                "txt", Text.class, IntWritable.class, TextOutputFormat.class);

        context.start();
    }
}
