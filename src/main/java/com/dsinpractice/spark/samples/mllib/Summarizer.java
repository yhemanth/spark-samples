package com.dsinpractice.spark.samples.mllib;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Ints;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

import java.io.Serializable;
import java.util.ArrayList;

public class Summarizer implements Serializable {

    public static final int MAX_TEMPARATURE_INDEX = 1;
    public static final int MIN_TEMPARATURE_INDEX = 2;
    public static final int RAINFALL_INDEX = 3;
    public static final int MAX_WIND_GUSH_INDEX = 7;
    private String[] args;

    public Summarizer(String[] args) {
        this.args = args;
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: " + Summarizer.class.getName() + " <input_file>");
            System.out.println("Input files to use at resources/temparature-data-set");
            System.exit(-1);
        }
        Summarizer summarizer = new Summarizer(args);
        summarizer.run();
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
                doubleValue(currentIndex++, temparatureData[MAX_TEMPARATURE_INDEX], indices, values);
                doubleValue(currentIndex++, temparatureData[MIN_TEMPARATURE_INDEX], indices, values);
                doubleValue(currentIndex++, temparatureData[RAINFALL_INDEX], indices, values);
                doubleValue(currentIndex++, temparatureData[MAX_WIND_GUSH_INDEX], indices, values);

                return Vectors.sparse(4, Ints.toArray(indices), Doubles.toArray(values));
            }
        });

        MultivariateStatisticalSummary colSummary = Statistics.colStats(numericTemparatureReadings.rdd());
        long count = colSummary.count();
        System.out.println("Number of columns: " + count);

        // Mean is computed by assigning 0 to missing values
        double[] meanValues = colSummary.mean().toArray();
        System.out.println("Mean Minimum Temparature: "  +  meanValues[0]);
        System.out.println("Mean Maximum Temparature: "  +  meanValues[1]);
        System.out.println("Mean Rainfall: "  +  meanValues[2]);
        System.out.println("Mean Max Wind Gush: "  +  meanValues[3]);

        double[] nonZeroValues = colSummary.numNonzeros().toArray();
        System.out.println("# of zero counts Minimum temparature: " + (count - nonZeroValues[0]));
        System.out.println("# of zero counts Maximum temparature: " + (count - nonZeroValues[1]));
        System.out.println("# of zero counts Rainfall: " + (count - nonZeroValues[2]));
        System.out.println("# of zero counts Max Wind Gush: " + (count - nonZeroValues[3]));
    }

    private void doubleValue(int currentIndex, String stringValue, ArrayList<Integer> indices, ArrayList<Double> values) {
        if (!stringValue.isEmpty()) {
            indices.add(currentIndex);
            values.add(Double.parseDouble(stringValue));
        }
    }
}
