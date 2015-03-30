package com.dsinpractice.spark.samples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

public class WholeTextFiles implements Serializable {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: " + WholeTextFiles.class.getName() + " <path with small files>");
            System.out.println("Input files to use at resources/whole-text-files");
            System.exit(-1);
        }
        WholeTextFiles wholeTextFiles = new WholeTextFiles();
        wholeTextFiles.run(args);
    }

    private void run(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("whole text files");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaPairRDD<String, String> fileNameContentsRDD = javaSparkContext.wholeTextFiles(args[0]);
        JavaRDD<String> lineCounts = fileNameContentsRDD.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> fileNameContent) throws Exception {
                String content = fileNameContent._2();
                int numLines = content.split("[\r\n]+").length;
                return fileNameContent._1() + ":  " + numLines;
            }
        });
        List<String> output = lineCounts.collect();
        for (String line : output) {
            System.out.println(line);
        }
    }
}
