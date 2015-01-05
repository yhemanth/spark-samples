package com.dsinpractice.spark.samples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

public class PipedCommand implements Serializable {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: " + PipedCommand.class.getName() + " <input> <output>");
            System.exit(-1);
        }
        PipedCommand pipedCommand = new PipedCommand();
        pipedCommand.run(args);
    }

    private void run(String[] args) {
        SparkConf pipedConf = new SparkConf().setAppName("Piped command");
        JavaSparkContext javaSparkContext = new JavaSparkContext(pipedConf);
        JavaRDD<String> input = javaSparkContext.textFile(args[0]);
        // Pass file path through --files argument
        JavaRDD<String> words = input.pipe("python split_lines.py");
        words.saveAsTextFile(args[1]);
    }
}
