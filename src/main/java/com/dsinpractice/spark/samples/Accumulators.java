package com.dsinpractice.spark.samples;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;

import java.io.Serializable;
import java.util.Arrays;

public class Accumulators implements Serializable {

    public static final String[] STOP_WORDS = new String[]{"the", "to", "a", "on", "it", "of", "for", "in", "by", "is"};

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: " + Accumulators.class.getName() + " <input path> <output path>");
            System.exit(-1);
        }
        Accumulators accumulators = new Accumulators();
        accumulators.run(args);
    }

    private void run(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("accumulators");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        final Broadcast<String[]> stopWords = context.broadcast(STOP_WORDS);
        final Accumulator<Integer> stopWordsCount = context.accumulator(0);
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
                boolean isStopWord = Arrays.asList(stopWords.value()).contains(word);
                if (isStopWord) stopWordsCount.add(1);
                return (!isStopWord);
            }
        });
        validWords.saveAsTextFile(args[1]);
        // The accumulator value is valid only after the RDD is materialized.
        System.out.println("Number of words ignored:" + stopWordsCount.value());
    }
}
