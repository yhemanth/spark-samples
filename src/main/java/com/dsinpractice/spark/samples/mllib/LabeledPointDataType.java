package com.dsinpractice.spark.samples.mllib;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

public class LabeledPointDataType implements Serializable {

    private String[] args;

    public LabeledPointDataType(String[] args) {
        this.args = args;
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: " + LabeledPointDataType.class.getName() + " <input-data>");
            System.out.printf("Input files to use are at resources/libsvm-sample");
            System.exit(-1);
        }
        LabeledPointDataType labeledPointDataType = new LabeledPointDataType(args);
        labeledPointDataType.run();
    }

    private void run() {
        JavaSparkContext javaSparkContext = new JavaSparkContext();
        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(javaSparkContext.sc(), args[0]).toJavaRDD();
        System.out.println("Size of LIBSVM data: " + data.count());
        List<Tuple2<Double, Integer>> countsByKey = data.mapToPair(new PairFunction<LabeledPoint, Double, Integer>() {
            public Tuple2<Double, Integer> call(LabeledPoint labeledPoint) throws Exception {
                return new Tuple2<Double, Integer>(labeledPoint.label(), 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        }).collect();
        for (Tuple2<Double, Integer> keyCount : countsByKey) {
            System.out.println(keyCount._1() + ": " + keyCount._2());
        }

    }
}
