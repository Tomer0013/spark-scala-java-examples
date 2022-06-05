package spark.java;

import org.apache.log4j.*;
import org.apache.spark.sql.*;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.types.*;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class LinearRegressionDataFrameDataset {

    public static class Regressionschema implements Serializable {
        private double label;
        private double features_raw;

        public double getLabel() {
            return label;
        }

        public void setLabel(double label) {
            this.label = label;
        }

        public double getFeatures_raw() {
            return features_raw;
        }

        public void setFeatures_raw(double features_raw) {
            this.features_raw = features_raw;
        }
    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkSession spark = SparkSession.builder().appName("LinearRegressionExample").master("local[*]").getOrCreate();
        StructType regSchema = DataTypes.createStructType(
                new ArrayList<>(Arrays.asList(
                        DataTypes.createStructField("label", DataTypes.DoubleType, true),
                        DataTypes.createStructField("features_raw", DataTypes.DoubleType, true)
        )));
        Encoder<Regressionschema> regressionschemaEncoder = Encoders.bean(Regressionschema.class);
        Dataset<Regressionschema> dsRaw = spark.read().option("sep", ",").schema(regSchema)
                .csv("./data/regression.txt").as(regressionschemaEncoder);

        VectorAssembler assembler = new VectorAssembler().setInputCols(new String[]{"features_raw"})
                        .setOutputCol("features");
        Dataset<Row> df = assembler.transform(dsRaw).select("label", "features");
        Dataset<Row>[] trainTest = df.randomSplit(new double[]{0.5, 0.5});
        Dataset<Row> trainDf = trainTest[0];
        Dataset<Row> testDf = trainTest[1];

        LinearRegression lir = new LinearRegression().setMaxIter(100).setRegParam(0.3).setElasticNetParam(0.8).setTol(1E-6);
        LinearRegressionModel model = lir.fit(trainDf);

        Dataset<Row> fullPreds = model.transform(testDf).cache();
        List<Row> predAndLabel = fullPreds.select("prediction", "label").collectAsList();
        predAndLabel.forEach(x -> System.out.println(x.get(0) + ", " + x.get(1)));

        spark.stop();
    }

}
