package com.dsinpractice.spark.samples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

import java.io.Serializable;

public class Persistence implements Serializable {

    public static void main(String[] args) throws InterruptedException {
        if (args.length < 2) {
            System.out.println("Usage: " + Persistence.class.getName() + " <input path> " +
                    "<persistence level=MEMORY|DISK|BOTH>");
            System.exit(-1);
        }
        Persistence persistence = new Persistence();
        persistence.run(args);
        System.out.println("Waiting...");
        Thread.sleep(Long.MAX_VALUE);
    }

    // NOTE: Presently, the disk option is only working with small files and in local mode.
    // The files are saved to directories like
    // /var/folders/mt/51srrjc15wl3n829qkgnh2dm0000gp/T/spark-local-20150118201458-6147/15
    private void run(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("Persistence");
        JavaSparkContext context = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = context.textFile(args[0]);
        if (args[1].equals("MEMORY")) {
            lines.persist(StorageLevel.MEMORY_ONLY());
        } else if (args[1].equals("DISK")) {
            lines.persist(StorageLevel.DISK_ONLY());
        } else if (args[1].equals("BOTH")) {
            lines.persist(StorageLevel.MEMORY_AND_DISK());
        } else {
            System.out.println("Not a valid persistence option");
        }
        lines.collect();
    }
}
