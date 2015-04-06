package com.dsinpractice.spark.samples;

import com.google.common.collect.Ordering;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Joiner implements Serializable {

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: " + Joiner.class.getName() +
                    " <max-temps-file> <min-temps-file> <output> <operation = join | cogroup>");
            System.out.println("Input files to use at resources/join-cogroup.");
            System.exit(-1);
        }
        Joiner joiner = new Joiner();
        joiner.run(args);
    }

    private void run(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Joiner");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaPairRDD<String, Double> maxTemps = selectCities(javaSparkContext.textFile(args[0]));
        JavaPairRDD<String, Double> minTemps = selectCities(javaSparkContext.textFile(args[1]));
        if (args[3].equals("join")) {
            handleJoin(args[2], maxTemps, minTemps);
        } else if (args[3].equals("cogroup")) {
            handleCoGroup(args[2], maxTemps.cogroup(minTemps));
        }
    }

    private void handleCoGroup(String outputFile,
                               JavaPairRDD<String, Tuple2<Iterable<Double>, Iterable<Double>>> cityTempLists) {
        JavaPairRDD<String, Tuple2<Double, Double>> cityTemps =
                cityTempLists.mapToPair(new PairFunction<Tuple2<String, Tuple2<Iterable<Double>, Iterable<Double>>>,
                        String, Tuple2<Double, Double>>() {
                    @Override
                    public Tuple2<String, Tuple2<Double, Double>> call(Tuple2<String,
                            Tuple2<Iterable<Double>, Iterable<Double>>> cityTemps) throws Exception {
                        Iterable<Double> maxTempsForCity = cityTemps._2()._1();
                        Iterable<Double> minTempsForCity = cityTemps._2()._2();
                        Double maxTemp = Double.MIN_VALUE;
                        for (Double temp : maxTempsForCity) {
                            if (temp > maxTemp) {
                                maxTemp = temp;
                            }
                        }
                        Double minTemp = Double.MAX_VALUE;
                        for (Double temp : minTempsForCity) {
                            if (temp < minTemp) {
                                minTemp = temp;
                            }
                        }
                        return new Tuple2<String, Tuple2<Double, Double>>(cityTemps._1(),
                                new Tuple2<Double, Double>(maxTemp, minTemp));
                    }
                });
        cityTemps.saveAsTextFile(outputFile);
    }

    private void handleJoin(String outputFile,
                            JavaPairRDD<String, Double> maxTemps, JavaPairRDD<String, Double> minTemps) {
        JavaPairRDD<String, Double> maxTempsByCity = maxTemps.reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double d1, Double d2) throws Exception {
                return Math.max(d1, d2);
            }
        });

        JavaPairRDD<String, Double> minTempsByCity = minTemps.reduceByKey(new Function2<Double, Double, Double>() {
            @Override
            public Double call(Double d1, Double d2) throws Exception {
                return Math.min(d1, d2);
            }
        });
        JavaPairRDD<String, Tuple2<Double, Double>> cityTemps = maxTempsByCity.join(minTempsByCity);
        cityTemps.saveAsTextFile(outputFile);
    }

    private JavaPairRDD<String, Double> selectCities(JavaRDD<String> cityTempsFile) {
        return cityTempsFile.mapToPair(new PairFunction<String, String, Double>() {
            @Override
            public Tuple2<String, Double> call(String line) throws Exception {
                String[] tokens = line.split(",");
                return new Tuple2<String, Double>(tokens[0], Double.parseDouble(tokens[1]));
            }
        });
    }
}
