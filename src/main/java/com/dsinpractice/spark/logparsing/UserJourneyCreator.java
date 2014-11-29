package com.dsinpractice.spark.logparsing;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

public class UserJourneyCreator implements Serializable {

    private String appName;
    private String inputPath;
    private String outputPath;

    public UserJourneyCreator(String[] args) {
        this.appName = "User journey creator";
        this.inputPath = args[0];
        this.outputPath = args[1];
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java " +
                    UserJourneyCreator.class.getName() + " <input path> <output path>");
            System.exit(-1);
        }
        UserJourneyCreator userJourneyCreator = new UserJourneyCreator(args);
        userJourneyCreator.create();
    }

    private void create() {
        SparkConf sparkConf = new SparkConf().setAppName(this.appName);
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

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
