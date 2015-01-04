package com.dsinpractice.spark.samples;

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
import java.util.List;

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
            JavaPairRDD<String, Tuple2<String, String>> cityDetails = cities.join(languages);
            JavaPairRDD<String, String> countryLanguages = languagesByCountry(cityDetails);
            countryLanguages.saveAsTextFile(args[2]);
        } else if (args[3].equals("cogroup")) {
            JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> cityDetails
                    = cities.cogroup(languages);
            JavaPairRDD<String, String> countryLanguages = cogroupedLanguagesByCountry(cityDetails);
            countryLanguages.saveAsTextFile(args[2]);
        }
    }

    private JavaPairRDD<String, String> cogroupedLanguagesByCountry(
            JavaPairRDD<String, Tuple2<Iterable<String>, Iterable<String>>> cityDetails) {
        return cityDetails.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>>, String, String>() {
            @Override
            public Iterable<Tuple2<String, String>> call(
                    Tuple2<String, Tuple2<Iterable<String>, Iterable<String>>> cityDetail) throws Exception {
                Tuple2<Iterable<String>, Iterable<String>> countryLanguagesList = cityDetail._2();
                String country = countryLanguagesList._1().iterator().next();
                Iterable<String> languages = countryLanguagesList._2();
                List<Tuple2<String, String>> countryLanguageTuples = new ArrayList<Tuple2<String, String>>();
                for (String language : languages) {
                    countryLanguageTuples.add(new Tuple2<String, String>(country, language));
                }
                return countryLanguageTuples;
            }
        }).reduceByKey(concat());
    }

    private JavaPairRDD<String, String> languagesByCountry(JavaPairRDD<String, Tuple2<String, String>> cityDetails) {
        return cityDetails.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, String>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, String>> countryLanguage) throws Exception {
                return countryLanguage._2();
            }
        }).reduceByKey(concat());
    }

    private Function2<String, String, String> concat() {
        return new Function2<String, String, String>() {
            @Override
            public String call(String s1, String s2) throws Exception {
                return s1 + "," + s2;
            }
        };
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
