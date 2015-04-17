package com.dsinpractice.spark.samples.mllib;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.google.common.primitives.Doubles;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

public class MLDataTypes implements Serializable {

    private boolean isDense;


    public static void main(String[] args) {
        if (args.length < 1) {
            printUsage();
        }
        if (args[0].equals("libsvm") & args.length != 2 ) {
            printUsage();
        }
        MLDataTypes mlDataTypes = new MLDataTypes();
        mlDataTypes.run(args);
    }

    private static void printUsage() {
        System.out.println("Usage: " + MLDataTypes.class.getName() + " <dense | sparse | libsvm | matrix> [input-data]");
        System.out.printf("Input files to use are at resources/libsvm-sample");
        System.exit(-1);
    }

    private void run(String[] args) {
        if (args[0].equalsIgnoreCase("dense")) {
            makeDenseVector();
        } else if (args[0].equalsIgnoreCase("sparse")) {
            makeSparseVector();
        } else if (args[0].equalsIgnoreCase("libsvm")) {
            loadSVMData(args);
        } else if (args[0].equalsIgnoreCase("matrix")) {
            makeLocalMatrix();
        }
    }

    private void makeLocalMatrix() {
        Matrix matrix = Matrices.dense(3, 2, new double[]{1.0, 2.0, 3.0, 2.0, 4.0, 6.0});
        System.out.println(matrix);
        System.out.println("Row count: " + matrix.numRows() + ", Column count: " + matrix.numCols());
        System.out.println();
        DenseMatrix transpose = (DenseMatrix) matrix.transpose();
        DenseMatrix result = matrix.multiply(transpose);
        System.out.println(result);
    }

    private void loadSVMData(String[] args) {
        JavaSparkContext javaSparkContext = new JavaSparkContext();
        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(javaSparkContext.sc(), args[1]).toJavaRDD();
        System.out.println("Size of LIBSVM data: " + data.count());
        List<Tuple2<Double, Integer>> countsByKey = data.mapToPair(new PairFunction<LabeledPoint, Double, Integer>() {
            public Tuple2<Double, Integer> call(LabeledPoint labeledPoint) throws Exception {
                return new Tuple2<Double, Integer>(labeledPoint.label(), 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1 + i2;
            }
        }).collect();
        for (Tuple2<Double, Integer> keyCount : countsByKey) {
            System.out.println(keyCount._1() + ": " + keyCount._2());
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
