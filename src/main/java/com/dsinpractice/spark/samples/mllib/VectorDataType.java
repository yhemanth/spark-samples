package com.dsinpractice.spark.samples.mllib;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.google.common.primitives.Doubles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

public class VectorDataType implements Serializable {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: " + VectorDataType.class.getName() + " <dense | sparse>");
            System.exit(-1);
        }
        VectorDataType vectorDataType = new VectorDataType();
        vectorDataType.run(args);
    }

    private void run(String[] args) {
        if (args[0].equalsIgnoreCase("dense")) {
            makeDenseVector();
        } else if (args[0].equalsIgnoreCase("sparse")) {
            makeSparseVector();
        }
    }

    private void makeSparseVector() {
        double[] samples = getSamples();
        int stepSize = 100;
        int[] sparseIndices = new int[samples.length/stepSize];
        double[] sparseSamples = new double[samples.length/stepSize];
        for (int i = 0, j = 0; i < samples.length && j < sparseSamples.length; i+=stepSize, j++) {
            sparseIndices[j] = i;
            sparseSamples[j] = samples[i];
        }
        SparseVector sparseVector = (SparseVector)Vectors.sparse(samples.length, sparseIndices, sparseSamples);
        System.out.println("Size of spare vector: " + sparseVector.size());
        System.out.println("Size of indices: " + sparseVector.indices().length);
    }

    private void makeDenseVector() {
        double[] samples = getSamples();

        Vector v = Vectors.dense(samples);
        System.out.println("Size of the dense vector: " + v.size());
    }

    private double[] getSamples() {
        ContiguousSet<Integer> numberSet = ContiguousSet.create(Range.closed(1, 1000000), DiscreteDomain.integers());
        return Doubles.toArray(numberSet);
    }
}
