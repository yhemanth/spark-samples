package com.dsinpractice.spark;

import com.dsinpractice.spark.samples.core.*;
import com.dsinpractice.spark.samples.mllib.*;
import org.apache.hadoop.util.ProgramDriver;
import org.apache.spark.mllib.linalg.Matrix;

public class SparkSampleDriver {

    public static void main(String[] args) throws Throwable {
        ProgramDriver programDriver = new ProgramDriver();
        programDriver.addClass("01. whole-text-files", WholeTextFiles.class,
                "Demonstrates Spark's ability to process the entire contents of a text file in a single map invocation");
        programDriver.addClass("02. parallelize-collections", ParallelizeCollections.class,
                "Demonstrates how we can parallelize a collection into an RDD with 'n' partitions.");
        programDriver.addClass("03. map-partitions", MapPartitions.class,
                "Demonstrates mapPartitions API where map gets entire partition data at once.");
        programDriver.addClass("04. set-operations", SetOperations.class,
                "Demonstrates APIs related to set opertions like union, etc.");
        programDriver.addClass("05. sampler", Sampler.class,
                "Demonstrates API related to sampling data.");
        programDriver.addClass("06. persistence", Persistence.class,
                "Demonstrates API related to persisting RDDs in memory / disk etc.");
        programDriver.addClass("07. joiner", Joiner.class,
                "Demonstrates APIs related to joining / co-grouping RDDs.");
        programDriver.addClass("08. piped-command", PipedCommand.class,
                "Demonstrates API that passes each record in input to an external script via the pipe API.");
        programDriver.addClass("09. broadcast-vars", BroadcastVars.class,
                "Demonstrates API that can be used to broadcast a variable to Spark tasks.");
        programDriver.addClass("10. accumulators", Accumulators.class,
                "Demonstrates API that can be used to count some metrics across tasks using " +
                        "special variables called accumulators.");
        programDriver.addClass("11. sum-reducer", SumReducer.class,
                "Demonstrates how unit testable Spark code can be written.");
        programDriver.addClass("12. vector-data-types", VectorDataType.class,
                "Demonstrates the local vector data type and some APIs on it.");
        programDriver.addClass("13. labeled-point-data-types", LabeledPointDataType.class,
                "Demonstrates loading LabeledPoint data with a LibSVM file, and some APIs on loaded data types.");
        programDriver.addClass("14. matrix-data-types", MatrixDataType.class,
                "Demonstrates defining and using matrix data type.");
        programDriver.addClass("15. summarizer", Summarizer.class,
                "Demonstrates summarizing numeric columns.");
        programDriver.addClass("16. correlator", Correlator.class,
                "Demonstrates correlating columns.");
        programDriver.addClass("17. stratified-sampler", StratifiedSampler.class,
                "Demonstrates sampling among strata or classes.");
        programDriver.driver(args);
    }
}
