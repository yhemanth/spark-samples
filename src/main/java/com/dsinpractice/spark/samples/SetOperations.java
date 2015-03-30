package com.dsinpractice.spark.samples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

public class SetOperations implements Serializable {

    public static void main(String[] args) {
        if (args.length<4) {
            System.out.println("Usage: " + SetOperations.class.getName() +
                    " <inpath1> <inpath2> <outpath> <operation = union | intersection>");
            System.out.println("Input files to use at resources/set-operations");
            System.exit(-1);
        }
        SetOperations setOperations = new SetOperations();
        setOperations.run(args);
    }

    private void run(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("set operations - " + args[3]);
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> set1 = javaSparkContext.textFile(args[0]);
        JavaRDD<String> set2 = javaSparkContext.textFile(args[1]);
        if (args[3].equals("union")) {
            JavaRDD<String> set3 = set1.union(set2);
            set3.saveAsTextFile(args[2]);
        } else if (args[3].equals("intersection")) {
            JavaRDD<String> set3 = set1.intersection(set2);
            set3.saveAsTextFile(args[2]);
        }
    }
}
