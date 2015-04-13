package com.dsinpractice.spark.samples.core;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SumReducerTest implements Serializable {

    @Test
    public void shouldBreakLineIntoWordCountPairs() throws Exception {
        SumReducer.LineToWordCountPair lineToWordCountPair = new SumReducer.LineToWordCountPair();
        Iterable<Tuple2<String, Integer>> wordCounts = lineToWordCountPair.call("This is a test line. This line has two test sentences.");
        List<Tuple2<String, Integer>> expectedWordCounts = Arrays.asList(
                new Tuple2<String, Integer>("This", 1),
                new Tuple2<String, Integer>("is", 1),
                new Tuple2<String, Integer>("a", 1),
                new Tuple2<String, Integer>("test", 1),
                new Tuple2<String, Integer>("line.", 1),
                new Tuple2<String, Integer>("This", 1),
                new Tuple2<String, Integer>("line", 1),
                new Tuple2<String, Integer>("has", 1),
                new Tuple2<String, Integer>("two", 1),
                new Tuple2<String, Integer>("test", 1),
                new Tuple2<String, Integer>("sentences.", 1));
        assertEquals(expectedWordCounts, wordCounts);
    }

    @Test
    public void testWordCountEndToEnd() {
        JavaSparkContext localSparkContext = new JavaSparkContext("local", "test word count");
        try {
            List<String> lines = Arrays.asList("This is a test line.", "This line has two test sentences.");
            JavaRDD<String> linesRDD = localSparkContext.parallelize(lines, 1);
            SumReducer sumReducer = new SumReducer();
            JavaPairRDD<String, Integer> wordCounts = sumReducer.countWords(linesRDD);
            final Map<String, Integer> expectedWordCounts = new HashMap<String, Integer>();
            expectedWordCounts.put("This", 2);
            expectedWordCounts.put("is", 1);
            expectedWordCounts.put("a", 1);
            expectedWordCounts.put("test", 2);
            expectedWordCounts.put("line.", 1);
            expectedWordCounts.put("line", 1);
            expectedWordCounts.put("has", 1);
            expectedWordCounts.put("two", 1);
            expectedWordCounts.put("sentences.", 1);
            wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
                @Override
                public void call(Tuple2<String, Integer> wordCount) throws Exception {
                    String word = wordCount._1();
                    assertEquals("For " + word, expectedWordCounts.get(word), wordCount._2());
                }
            });
        } finally {
            localSparkContext.stop();
        }
    }
}
