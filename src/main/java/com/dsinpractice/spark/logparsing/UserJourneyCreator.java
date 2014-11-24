package com.dsinpractice.spark.logparsing;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

public class UserJourneyCreator implements Serializable {

    private String inputPath;
    private String outputPath;

    public UserJourneyCreator(String inputPath, String outputPath) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
    }

    public static void main(String[] args) {
        UserJourneyCreator userJourneyCreator = new UserJourneyCreator(args[0], args[1]);
        userJourneyCreator.create();
    }

    private void create() {
        JavaSparkContext javaSparkContext = new JavaSparkContext("yarn-client", "User journey creator",
                "/Users/yhemanth/software/spark-0.8.1-incubating-bin-hadoop2",
                new String[]{"target/spark-samples-1.0-SNAPSHOT-jar-with-dependencies.jar"});

        JavaRDD<String> userVisits = javaSparkContext.textFile(inputPath);

        JavaPairRDD<String, String> userVisitUrls = userVisits.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String userVisit) throws Exception {
                String[] tokens = userVisit.split(" ");
                return new Tuple2(tokens[0], tokens[6]); // user address, url
            }
        });

        JavaPairRDD<String, String> userJourneys = userVisitUrls.reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String url1, String url2) throws Exception {
                System.out.println("url1: " + url1 + ", url2: " + url2);
                return url1 + "," + url2;
            }
        });

        userJourneys.saveAsTextFile(outputPath);
    }
}
