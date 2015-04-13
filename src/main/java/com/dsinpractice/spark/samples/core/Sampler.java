package com.dsinpractice.spark.samples.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

public class Sampler implements Serializable {

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("Usage: " + Sampler.class.getName() + " <inpath> <outpath> <samplesize>");
            System.out.println("Input files to use at resources/common-text-data.");
            System.exit(-1);
        }
        Sampler sampler = new Sampler();
        sampler.run(args);
    }

    private void run(String[] args) {
        SparkConf samplerConf = new SparkConf().setAppName("Sampler");
        JavaSparkContext javaSparkContext = new JavaSparkContext(samplerConf);
        JavaRDD<String> lines = javaSparkContext.textFile(args[0]);
        JavaRDD<String> sample = lines.sample(false, Double.parseDouble(args[2]));
        sample.saveAsTextFile(args[1]);
    }
}
