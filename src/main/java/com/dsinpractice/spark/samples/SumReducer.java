package com.dsinpractice.spark.samples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SumReducer implements Serializable {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java " + SumReducer.class.getName() + " <input> <output>");
            System.exit(-1);
        }

        SumReducer sumReducer = new SumReducer();
        sumReducer.run(args);
    }

    private void run(String[] args) {
        SparkConf aggregatorConf = new SparkConf().setAppName("Sum Aggregator");
        JavaSparkContext javaSparkContext = new JavaSparkContext(aggregatorConf);
        JavaRDD<String> lines = javaSparkContext.textFile(args[0]);
        JavaPairRDD<String, Integer> finalCounts = countWords(lines);
        finalCounts.saveAsTextFile(args[1]);
    }

    public JavaPairRDD<String, Integer> countWords(JavaRDD<String> lines) {
        JavaPairRDD<String, Integer> wordCounts =
                lines.flatMapToPair(new LineToWordCountPair());
        return wordCounts.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });
    }

    // Implement this separately to allow testing via JUnit.
    static class LineToWordCountPair implements PairFlatMapFunction<String, String, Integer> {

        @Override
        public Iterable<Tuple2<String, Integer>> call(String line) throws Exception {
            List<Tuple2<String, Integer>> wordCounts = new ArrayList<Tuple2<String, Integer>>();
            String[] words = line.split(" ");
            for (String word : words) {
                wordCounts.add(new Tuple2<String, Integer>(word, 1));
            }
            return wordCounts;
        }
    }
}
