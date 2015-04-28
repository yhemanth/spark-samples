package com.dsinpractice.spark.samples.mllib;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.DenseMatrix;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix;
import org.apache.spark.mllib.linalg.distributed.IndexedRow;
import org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix;
import org.apache.spark.mllib.linalg.distributed.MatrixEntry;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MatrixDataType implements Serializable {

    private String[] args;

    public MatrixDataType(String[] args) {
        this.args = args;
    }

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: " + MatrixDataType.class.getName() + " <local | coordinate | indexed-row>");
            System.exit(-1);
        }
        MatrixDataType matrixDataType = new MatrixDataType(args);
        matrixDataType.run();
    }

    private void run() {
        if (args[0].equals("local")) {
            handleLocalMatrix();
        } else if (args[0].equals("coordinate")) {
            handleCoordinateMatrix();
        } else if (args[0].equals("indexed-row")) {
            handleIndexedRowMatrix();
        }
    }

    private void handleIndexedRowMatrix() {
        IndexedRow iRow1 = new IndexedRow(0, Vectors.sparse(2, new int[]{0}, new double[]{1.0}));
        IndexedRow iRow2 = new IndexedRow(1, Vectors.sparse(2, new int[]{0, 1}, new double[]{3.0, 2.0}));
        IndexedRow iRow3 = new IndexedRow(2, Vectors.sparse(2, new int[]{1}, new double[]{6.0}));

        List<IndexedRow> inputList = new ArrayList<IndexedRow>();
        inputList.add(iRow1);
        inputList.add(iRow2);
        inputList.add(iRow3);

        JavaSparkContext context = new JavaSparkContext();
        JavaRDD<IndexedRow> rows = context.parallelize(inputList);

        IndexedRowMatrix matrix = new IndexedRowMatrix(rows.rdd());
        System.out.println("Row count: " + matrix.numRows() + ", Column count: " + matrix.numCols());
    }

    private void handleCoordinateMatrix() {
        MatrixEntry m1 = new MatrixEntry(0, 0, 1.0);
        MatrixEntry m3 = new MatrixEntry(1, 0, 3.0);
        MatrixEntry m4 = new MatrixEntry(1, 1, 2.0);
        MatrixEntry m6 = new MatrixEntry(2, 1, 6.0);

        List<MatrixEntry> matrixEntries = Arrays.asList(new MatrixEntry[]{m1, m3, m4, m6});
        JavaSparkContext javaSparkContext = new JavaSparkContext();
        JavaRDD<MatrixEntry> matrixEntryRDD = javaSparkContext.parallelize(matrixEntries);

        CoordinateMatrix coordinateMatrix = new CoordinateMatrix(matrixEntryRDD.rdd());
        System.out.println("Row count: " + coordinateMatrix.numRows());
        System.out.println("Column count: " + coordinateMatrix.numCols());
    }

    private void handleLocalMatrix() {
        Matrix matrix = Matrices.dense(3, 2, new double[]{1.0, 2.0, 3.0, 2.0, 4.0, 6.0});
        System.out.println(matrix);
        System.out.println("Row count: " + matrix.numRows() + ", Column count: " + matrix.numCols());
        System.out.println();
        DenseMatrix transpose = (DenseMatrix) matrix.transpose();
        DenseMatrix result = matrix.multiply(transpose);
        System.out.println(result);
    }
}
