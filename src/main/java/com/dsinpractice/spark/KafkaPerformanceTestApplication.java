package com.dsinpractice.spark;

import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.HashMap;

public class KafkaPerformanceTestApplication {

    public static void main(String[] args) {

        System.setProperty("spark.executor.memory", "2g");
        String numWorkers = args[2];
        int duration = Integer.parseInt(args[3]);

        String master = String.format("local[%s]", numWorkers);

        JavaStreamingContext context = new JavaStreamingContext(master, "Kafka Performance Test App",
                new Duration(duration),
                System.getenv("SPARK_HOME"),
                new String[]{System.getenv("SPARK_APP_JARS")});

        HashMap<String, Integer> topics = new HashMap<String, Integer>();
        topics.put(args[1], 1);
        JavaDStream<String> messages = context.kafkaStream(args[0], "1", topics, StorageLevel.MEMORY_AND_DISK());

        JavaPairDStream<Long, Long> latencies = messages.map(new PairFunction<String, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(String message) throws Exception {
                String[] timeAndMessage = message.split(",");
                if (timeAndMessage.length >= 2) {
                    long latency = System.currentTimeMillis() - Long.parseLong(timeAndMessage[0]);
                    return new Tuple2<Long, Long>(latency, 1L);
                }
                return new Tuple2<Long, Long>(0L, 0L);
            }
        });

        JavaDStream<Tuple2<Long, Long>> avgLatencyAndCounts = latencies.reduce(
                new Function2<Tuple2<Long, Long>, Tuple2<Long, Long>, Tuple2<Long, Long>>() {
            @Override
            public Tuple2<Long, Long> call(Tuple2<Long, Long> sum, Tuple2<Long, Long> value) throws Exception {
                return new Tuple2<Long, Long>(sum._1() + value._1(), sum._2() + value._2());
            }
        });

        avgLatencyAndCounts.print();

        context.start();
    }
}
