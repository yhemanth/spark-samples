package com.dsinpractice.spark.samples;

import com.google.common.collect.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;

public class ParallelizeCollections implements Serializable {

    public static void main(String[] args) {
        ParallelizeCollections parallelizeCollections = new ParallelizeCollections();
        parallelizeCollections.run();
    }

    private void run() {
        SparkConf sparkConf = new SparkConf().setAppName("Parallelize Collections Demo");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        ContiguousSet<Integer> numberSet = ContiguousSet.create(Range.closed(1, 1000000), DiscreteDomain.integers());
        ImmutableList<Integer> numbers = numberSet.asList();
        JavaRDD<Integer> numbersRDD = javaSparkContext.parallelize(numbers, 10);
        Integer sum = numbersRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        });
        System.out.println("Sum is:  " + sum);
    }
}
