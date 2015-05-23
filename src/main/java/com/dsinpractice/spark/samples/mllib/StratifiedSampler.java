package com.dsinpractice.spark.samples.mllib;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StratifiedSampler implements Serializable {

    private String[] args;

    public StratifiedSampler(String[] args) {
        this.args = args;
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: " + StratifiedSampler.class.getName() +
                    " <input file> <sample,[sample,sample,sample]> <output file> exact|inexact");
            System.out.println("Input files to use are at resources/stratified-sample.");
            System.exit(-1);
        }

        StratifiedSampler stratifiedSampler = new StratifiedSampler(args);
        stratifiedSampler.run();
    }

    private void run() {
        JavaSparkContext javaSparkContext = new JavaSparkContext();
        JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(javaSparkContext.sc(), args[0]).toJavaRDD();
        JavaPairRDD<Double, Vector> labelledData = data.mapToPair(new PairFunction<LabeledPoint, Double, Vector>() {
            @Override
            public Tuple2<Double, Vector> call(LabeledPoint labeledPoint) throws Exception {
                return new Tuple2<Double, Vector>(labeledPoint.label(), labeledPoint.features());
            }
        });
        Map<Double, Object> samplingFractions = getSamplingFractions(labelledData);
        if (samplingFractions == null) return;
        JavaPairRDD<Double, Vector> samplesByClass;
        if (args[3].equalsIgnoreCase("exact")) {
            samplesByClass = labelledData.sampleByKeyExact(false, samplingFractions);
        } else {
            samplesByClass = labelledData.sampleByKey(false, samplingFractions);
        }
        samplesByClass.saveAsTextFile(args[2]);
    }

    private Map<Double, Object> getSamplingFractions(JavaPairRDD<Double, Vector> labelledData) {
        List<Double> labels = labelledData.keys().distinct().collect();
        Double[] sortedLabels = new Double[labels.size()];
        Arrays.sort(labels.toArray(sortedLabels));
        String[] samplingFractionStrings = args[1].split(",");
        if ((samplingFractionStrings.length != 1) && (samplingFractionStrings.length != 4)) {
            System.out.println("Error: Expect 1 or 4 sampling fractions.");
            return null;
        }
        Map<Double, Object> samplingFractions = new HashMap<Double, Object>();
        if (samplingFractionStrings.length == 1) {
            createProportionateSamplingFractions(Arrays.asList(sortedLabels), samplingFractionStrings[0],
                    samplingFractions);
        } else {
            createDisproportionateSamplingFractions(Arrays.asList(sortedLabels), samplingFractionStrings,
                    samplingFractions);
        }
        return samplingFractions;
    }

    private void createDisproportionateSamplingFractions(List<Double> labels, String[] samplingFractionStrings,
                                                         Map<Double, Object> samplingFractions) {
        Double[] disproportionateSamplingFractions = new Double[4];
        for (int i = 0; i < 4; i++) {
            disproportionateSamplingFractions[i] = Double.parseDouble(samplingFractionStrings[i]);
        }
        for (int i = 0; i < 4; i++) {
            samplingFractions.put(labels.get(i), disproportionateSamplingFractions[i]);
        }
    }

    private void createProportionateSamplingFractions(List<Double> labels, String samplingFractionString,
                                                      Map<Double, Object> samplingFractions) {
        double proportionateSamplingFraction = Double.parseDouble(samplingFractionString);
        for (Double label : labels) {
            samplingFractions.put(label, proportionateSamplingFraction);
        }
    }
}
