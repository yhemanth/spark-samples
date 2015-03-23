package com.dsinpractice.spark;

import com.dsinpractice.spark.samples.WholeTextFiles;
import org.apache.hadoop.util.ProgramDriver;

public class SparkSampleDriver {

    public static void main(String[] args) throws Throwable {
        ProgramDriver programDriver = new ProgramDriver();
        programDriver.addClass("whole-text-files", WholeTextFiles.class,
                "Demonstrates Spark's ability to process the entire contents of a text file in a single map invocation");
        programDriver.driver(args);
    }
}
