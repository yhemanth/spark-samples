package com.yhemanth.spark;

import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.Function;

public class SimpleSparkApplication {

    public static void main(String[] args) {

        String logFile = "/Users/yhemanth/temp/textfile.txt";

        JavaSparkContext sc = new JavaSparkContext("local", "Simple App",
                "/Users/yhemanth/software/spark-0.8.1-incubating-bin-hadoop2",
                new String[]{"target/spark-samples-1.0-SNAPSHOT-jar-with-dependencies.jar"});

        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("a"); }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) { return s.contains("b"); }
        }).count();

        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
    }
}