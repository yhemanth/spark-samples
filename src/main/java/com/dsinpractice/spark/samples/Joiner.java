package com.dsinpractice.spark.samples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

public class Joiner implements Serializable {

    public static void main(String[] args) {
        if (args.length < 4) {
            System.out.println("Usage: " + Joiner.class.getName() +
                    " <city-file> <language-file> <output> <operation = join | cogroup");
            System.exit(-1);
        }
        Joiner joiner = new Joiner();
        joiner.run(args);
    }

    private void run(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Joiner");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        JavaRDD<String> cityFile = javaSparkContext.textFile(args[0]);
        JavaPairRDD<String, String> cities = selectCities(cityFile);
        JavaRDD<String> languageFile = javaSparkContext.textFile(args[1]);
        JavaPairRDD<String, String> languages = selectLanguages(languageFile);
        if (args[3].equals("join")) {
            JavaPairRDD<String, Tuple2<String, String>> countryLanguages = cities.join(languages);
            countryLanguages.saveAsTextFile(args[2]);
        } else if (args[3].equals("cogroup")) {
            JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> countryLanguages
                    = cities.cogroup(languages);
            countryLanguages.saveAsTextFile(args[2]);
        }
    }

    private JavaPairRDD<String, String> selectLanguages(JavaRDD<String> languageFile) {
        JavaPairRDD<String, String> cityLanguagePairs = languageFile.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String line) throws Exception {
                String[] tokens = line.split(",");
                return new Tuple2<String, String>(tokens[0], tokens[1]);
            }
        });
        return cityLanguagePairs;
    }

    private JavaPairRDD<String, String> selectCities(JavaRDD<String> cityFile) {
        JavaPairRDD<String, String> cityCountryPairs = cityFile.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String line) throws Exception {
                String[] tokens = line.split(",");
                return new Tuple2<String, String>(tokens[1], tokens[0]);
            }
        });
        return cityCountryPairs;
    }
}
