package com.dsinpractice.spark.samples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

public class Joiner implements Serializable {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: " + Joiner.class.getName() + " <input-set1> <input-set2> <output>");
            System.exit(-1);
        }
        Joiner joiner = new Joiner();
        joiner.run(args);
    }

    private void run(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Joiner");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> set1 = javaSparkContext.textFile(args[0]);
        JavaPairRDD<String, String> words1 = selectWords(set1);
        JavaRDD<String> set2 = javaSparkContext.textFile(args[1]);
        JavaPairRDD<String, String> words2 = selectWords(set2);
        JavaPairRDD<String, Tuple2<String, String>> wordPairs = words1.join(words2);
        wordPairs.saveAsTextFile(args[2]);
    }

    private JavaPairRDD<String, String> selectWords(JavaRDD<String> set) {
        JavaPairRDD<String, String> words = set.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String line) throws Exception {
                String[] words = line.split(" ");
                return new Tuple2<String, String>(words[0], words[2]);
            }
        });
        return words;
    }
}
