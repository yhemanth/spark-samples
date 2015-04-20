package com.dsinpractice.spark.samples.mllib;

import org.apache.spark.mllib.linalg.DenseMatrix;
import org.apache.spark.mllib.linalg.Matrices;
import org.apache.spark.mllib.linalg.Matrix;

import java.io.Serializable;

public class MatrixDataType implements Serializable {

    public static void main(String[] args) {
        MatrixDataType matrixDataType = new MatrixDataType();
        matrixDataType.run();
    }

    private void run() {
        Matrix matrix = Matrices.dense(3, 2, new double[]{1.0, 2.0, 3.0, 2.0, 4.0, 6.0});
        System.out.println(matrix);
        System.out.println("Row count: " + matrix.numRows() + ", Column count: " + matrix.numCols());
        System.out.println();
        DenseMatrix transpose = (DenseMatrix) matrix.transpose();
        DenseMatrix result = matrix.multiply(transpose);
        System.out.println(result);
    }
}
