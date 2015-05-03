package com.dsinpractice.spark;

import com.dsinpractice.spark.samples.core.*;
import com.dsinpractice.spark.samples.mllib.LabeledPointDataType;
import com.dsinpractice.spark.samples.mllib.MatrixDataType;
import com.dsinpractice.spark.samples.mllib.Summarizer;
import com.dsinpractice.spark.samples.mllib.VectorDataType;
import org.apache.hadoop.util.ProgramDriver;
import org.apache.spark.mllib.linalg.Matrix;

public class SparkSampleDriver {

    public static void main(String[] args) throws Throwable {
        ProgramDriver programDriver = new ProgramDriver();
        programDriver.addClass("whole-text-files", WholeTextFiles.class,
                "Demonstrates Spark's ability to process the entire contents of a text file in a single map invocation");
        programDriver.addClass("parallelize-collections", ParallelizeCollections.class,
                "Demonstrates how we can parallelize a collection into an RDD with 'n' partitions.");
        programDriver.addClass("map-partitions", MapPartitions.class,
                "Demonstrates mapPartitions API where map gets entire partition data at once.");
        programDriver.addClass("set-operations", SetOperations.class,
                "Demonstrates APIs related to set opertions like union, etc.");
        programDriver.addClass("sampler", Sampler.class,
                "Demonstrates API related to sampling data.");
        programDriver.addClass("persistence", Persistence.class,
                "Demonstrates API related to persisting RDDs in memory / disk etc.");
        programDriver.addClass("joiner", Joiner.class,
                "Demonstrates APIs related to joining / co-grouping RDDs.");
        programDriver.addClass("piped-command", PipedCommand.class,
                "Demonstrates API that passes each record in input to an external script via the pipe API.");
        programDriver.addClass("broadcast-vars", BroadcastVars.class,
                "Demonstrates API that can be used to broadcast a variable to Spark tasks.");
        programDriver.addClass("accumulators", Accumulators.class,
                "Demonstrates API that can be used to count some metrics across tasks using " +
                        "special variables called accumulators.");
        programDriver.addClass("sum-reducer", SumReducer.class,
                "Demonstrates how unit testable Spark code can be written.");
        programDriver.addClass("vector-data-types", VectorDataType.class,
                "Demonstrates the local vector data type and some APIs on it.");
        programDriver.addClass("labeled-point-data-types", LabeledPointDataType.class,
                "Demonstrates loading LabeledPoint data with a LibSVM file, and some APIs on loaded data types.");
        programDriver.addClass("matrix-data-types", MatrixDataType.class,
                "Demonstrates defining and using matrix data type.");
        programDriver.addClass("summarizer", Summarizer.class,
                "Demonstrates summarizing numeric columns.");
        programDriver.driver(args);
    }
}
