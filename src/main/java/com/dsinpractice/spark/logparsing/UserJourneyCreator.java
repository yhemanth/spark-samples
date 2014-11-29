package com.dsinpractice.spark.logparsing;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

public class UserJourneyCreator implements Serializable {

    private String appName;
    private String masterUrl;
    private String inputPath;
    private String outputPath;

    public UserJourneyCreator(String[] args) {
        this.masterUrl = args[0];
        this.appName = "User journey creator";
        this.inputPath = args[1];
        this.outputPath = args[2];
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: java " +
                    UserJourneyCreator.class.getName() + " <master> <input path> <output path>");
            System.exit(-1);
        }
        UserJourneyCreator userJourneyCreator = new UserJourneyCreator(args);
        userJourneyCreator.create();
    }

    private void create() {
        JavaSparkContext javaSparkContext = new JavaSparkContext(masterUrl, appName,
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
                return url1 + "," + url2;
            }
        });

        userJourneys.saveAsTextFile(outputPath);
    }
}
