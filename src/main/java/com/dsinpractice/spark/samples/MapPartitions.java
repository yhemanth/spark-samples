package com.dsinpractice.spark.samples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class MapPartitions implements Serializable {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: " + MapPartitions.class.getName() + " <inpath> <outpath>");
            System.out.println("Input files to use at resources/common-text-data");
            System.exit(-1);
        }
        MapPartitions mapPartitions = new MapPartitions();
        mapPartitions.run(args);
    }

    private void run(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("map partitions demo");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = javaSparkContext.textFile(args[0], 2);
        JavaRDD<String> lowerCaseLines = lines.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            @Override
            public Iterable<String> call(Iterator<String> linesPerPartition) throws Exception {
                List<String> lowerCaseLines = new ArrayList<String>();
                while (linesPerPartition.hasNext()) {
                    String line = linesPerPartition.next();
                    lowerCaseLines.add(line.toLowerCase());
                }
                return lowerCaseLines;
            }
        });
        lowerCaseLines.saveAsTextFile(args[1]);
    }
}
