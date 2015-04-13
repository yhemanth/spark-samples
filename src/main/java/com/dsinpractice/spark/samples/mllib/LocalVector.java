package com.dsinpractice.spark.samples.mllib;

import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.Range;
import com.google.common.primitives.Doubles;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

public class LocalVector {

    private boolean isDense;


    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: " + LocalVector.class.getName() + " <dense | sparse>");
            System.exit(-1);
        }
        LocalVector localVector = new LocalVector();
        localVector.run(args);
    }

    private void run(String[] args) {
        if (args[0].equalsIgnoreCase("dense")) {
            makeDenseVector();
        }
    }

    private void makeDenseVector() {
        ContiguousSet<Integer> numberSet = ContiguousSet.create(Range.closed(1, 1000000), DiscreteDomain.integers());
        double[] samples = Doubles.toArray(numberSet);

        Vector v = Vectors.dense(samples);
        System.out.println("Size of the vector: " + v.size());
    }
}
