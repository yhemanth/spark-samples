package com.dsinpractice.spark.samples.mllib;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.Statistics;

import java.io.Serializable;
import java.util.ArrayList;

public class Correlator implements Serializable {

    private static final int MAX_TEMP_INDEX = 1;
    public static final int MIN_TEMP_INDEX = 2;
    public static final int MAX_WIND_GUSH_INDEX = 7;
    private String[] args;

    public Correlator(String[] args) {
        this.args = args;
    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: " + Correlator.class.getName() + " <input data> <pearson | spearman>");
            System.out.println("Input files to use at resources/temparature-data-set");
            System.exit(-1);
        }
        Correlator correlator = new Correlator(args);
        correlator.run();
    }

    private void run() {
        SparkConf conf = new SparkConf().setAppName("Summarizer");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        JavaRDD<String> temparatureReadings = javaSparkContext.textFile(args[0]);
        JavaRDD<Vector> numericTemparatureReadings = temparatureReadings.map(new Function<String, Vector>() {
            @Override
            public Vector call(String temparatureReading) throws Exception {
                String[] temparatureData = temparatureReading.split(",");
                ArrayList<Integer> indices = new ArrayList<Integer>();
                ArrayList<Double> values = new ArrayList<Double>();
                int currentIndex = 0;
                doubleValue(currentIndex++, temparatureData[MAX_TEMP_INDEX], indices, values);
                doubleValue(currentIndex++, temparatureData[MIN_TEMP_INDEX], indices, values);
                doubleValue(currentIndex++, temparatureData[MAX_WIND_GUSH_INDEX], indices, values);
                return Vectors.sparse(3, Ints.toArray(indices), Doubles.toArray(values));
            }
        });

        Matrix corrMatrix = Statistics.corr(numericTemparatureReadings.rdd(), args[1]);
        System.out.println("Correlation Matrix: " + corrMatrix);
    }

    private void doubleValue(int currentIndex, String stringValue, ArrayList<Integer> indices, ArrayList<Double> values) {
        if (!stringValue.isEmpty()) {
            indices.add(currentIndex);
            values.add(Double.parseDouble(stringValue));
        }
    }

}
