package com.dsinpractice.spark.samples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.util.Arrays;

public class BroadcastVars implements Serializable {

    public static final String[] STOP_WORDS = new String[]{"the", "to", "a", "on", "it", "of", "for", "in", "by", "is"};

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: " + BroadcastVars.class.getName() + " <input path> <output path>");
            System.exit(-1);
        }
        BroadcastVars broadcastVars = new BroadcastVars();
        broadcastVars.run(args);
    }

    private void run(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("broadcast vars");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        final Broadcast<String[]> stopWords = context.broadcast(STOP_WORDS);
        JavaRDD<String> lines = context.textFile(args[0]);
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" "));
            }
        });
        JavaRDD<String> validWords = words.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String word) throws Exception {
                return (!Arrays.asList(stopWords.value()).contains(word));
            }
        });
        validWords.saveAsTextFile(args[1]);
    }
}
